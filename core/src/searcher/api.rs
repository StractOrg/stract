// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use itertools::{intersperse, Itertools};
use optics::Optic;
use url::Url;

use crate::bangs::{Bang, BangHit};
use crate::config::{ApiConfig, ApiThresholds, CollectorConfig};
use crate::image_store::Image;
use crate::inverted_index::RetrievedWebpage;
#[cfg(not(feature = "libtorch"))]
use crate::ranking::models::cross_encoder::DummyCrossEncoder;
use crate::ranking::pipeline::{AsRankingWebsite, RankingWebsite};
use crate::ranking::ALL_SIGNALS;
use crate::search_prettifier::{
    create_stackoverflow_sidebar, DisplayedAnswer, DisplayedEntity, DisplayedSidebar,
    DisplayedWebpage, HighlightedSpellCorrection,
};
use crate::widgets::{Widget, Widgets};
use crate::{
    bangs::Bangs,
    collector::BucketCollector,
    distributed::cluster::Cluster,
    ranking::{models::lambdamart::LambdaMART, pipeline::RankingPipeline},
};
use crate::{ceil_char_boundary, floor_char_boundary, query, Result};
#[cfg(feature = "libtorch")]
use crate::{qa_model::QaModel, ranking::models::cross_encoder::CrossEncoderModel};

use super::live::LiveSearcher;
use super::{distributed, live, DistributedSearcher, SearchQuery, SearchResult, WebsitesResult};

#[derive(Clone)]
enum ScoredWebsitePointer {
    Normal(distributed::ScoredWebsitePointer),
    Live(live::ScoredWebsitePointer),
}

impl AsRankingWebsite for ScoredWebsitePointer {
    fn as_ranking(&self) -> &RankingWebsite {
        match self {
            ScoredWebsitePointer::Normal(p) => &p.website,
            ScoredWebsitePointer::Live(p) => &p.website,
        }
    }

    fn as_mut_ranking(&mut self) -> &mut RankingWebsite {
        match self {
            ScoredWebsitePointer::Normal(p) => &mut p.website,
            ScoredWebsitePointer::Live(p) => &mut p.website,
        }
    }
}

#[cfg(feature = "libtorch")]
pub struct ApiSearcher {
    distributed_searcher: DistributedSearcher,
    live_searcher: LiveSearcher,
    cross_encoder: Option<Arc<CrossEncoderModel>>,
    lambda_model: Option<Arc<LambdaMART>>,
    qa_model: Option<Arc<QaModel>>,
    bangs: Bangs,
    collector_config: CollectorConfig,
    thresholds: ApiThresholds,
    widgets: Widgets,
}

#[cfg(not(feature = "libtorch"))]
pub struct ApiSearcher {
    distributed_searcher: DistributedSearcher,
    lambda_model: Option<Arc<LambdaMART>>,
    bangs: Bangs,
    collector_config: CollectorConfig,
    thresholds: ApiThresholds,
    widgets: Widgets,
}

impl ApiSearcher {
    #[cfg(feature = "libtorch")]
    pub fn new(
        cluster: Arc<Cluster>,
        cross_encoder: Option<CrossEncoderModel>,
        lambda_model: Option<LambdaMART>,
        qa_model: Option<QaModel>,
        bangs: Bangs,
        config: ApiConfig,
    ) -> Self {
        Self {
            distributed_searcher: DistributedSearcher::new(Arc::clone(&cluster)),
            live_searcher: LiveSearcher::new(Arc::clone(&cluster)),
            cross_encoder: cross_encoder.map(Arc::new),
            lambda_model: lambda_model.map(Arc::new),
            qa_model: qa_model.map(Arc::new),
            bangs,
            collector_config: config.collector,
            thresholds: config.thresholds,
            widgets: Widgets::new(config.widgets).unwrap(),
        }
    }

    #[cfg(not(feature = "libtorch"))]
    pub fn new(
        cluster: Arc<Cluster>,
        lambda_model: Option<LambdaMART>,
        bangs: Bangs,
        config: ApiConfig,
    ) -> Self {
        Self {
            distributed_searcher: DistributedSearcher::new(cluster),
            lambda_model: lambda_model.map(Arc::new),
            bangs,
            collector_config: config.collector,
            thresholds: config.thresholds,
            widgets: Widgets::new(config.widgets).unwrap(),
        }
    }

    fn combine_results(
        &self,
        initial_results: Vec<distributed::InitialSearchResultShard>,
        live_results: Vec<live::InitialSearchResultSplit>,
        pipeline: RankingPipeline<ScoredWebsitePointer>,
    ) -> (Vec<ScoredWebsitePointer>, bool) {
        let mut collector =
            BucketCollector::new(pipeline.collector_top_n(), self.collector_config.clone());

        let mut num_sites: usize = 0;
        for result in initial_results {
            for website in result.local_result.websites {
                num_sites += 1;
                let pointer = distributed::ScoredWebsitePointer {
                    website,
                    shard: result.shard,
                };

                let pointer = ScoredWebsitePointer::Normal(pointer);

                collector.insert(pointer);
            }
        }

        for result in live_results {
            for website in result.local_result.websites {
                num_sites += 1;
                let pointer = live::ScoredWebsitePointer {
                    website,
                    split_id: result.split_id.clone(),
                };

                let pointer = ScoredWebsitePointer::Live(pointer);

                collector.insert(pointer);
            }
        }

        let top_websites = collector
            .into_sorted_vec(true)
            .into_iter()
            .take(pipeline.collector_top_n())
            .collect::<Vec<_>>();

        let offset = pipeline.offset();

        let res = pipeline.apply(top_websites);
        let has_more = num_sites.saturating_sub(offset) > res.len();

        (res, has_more)
    }

    async fn stackoverflow_sidebar(&self, query: &SearchQuery) -> Result<Option<DisplayedSidebar>> {
        let query = SearchQuery {
            query: query.query.clone(),
            num_results: 1,
            optic: Some(Optic::parse(include_str!("stackoverflow.optic")).unwrap()),
            ..Default::default()
        };

        let mut results: Vec<_> = self
            .distributed_searcher
            .search_initial(&query)
            .await
            .into_iter()
            .filter_map(|result| {
                result
                    .local_result
                    .websites
                    .first()
                    .cloned()
                    .map(|website| (result.shard, website))
            })
            .collect();

        results.sort_by(|(_, a), (_, b)| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal));

        if let Some((shard, website)) = results.pop() {
            if website.score > self.thresholds.stackoverflow {
                let scored_websites =
                    vec![(0, distributed::ScoredWebsitePointer { website, shard })];
                let mut retrieved = self
                    .distributed_searcher
                    .retrieve_webpages(&scored_websites, &query.query)
                    .await;

                if let Some((_, res)) = retrieved.pop() {
                    return Ok(Some(create_stackoverflow_sidebar(
                        res.schema_org,
                        Url::parse(&res.url).unwrap(),
                    )?));
                }
            }
        }

        Ok(None)
    }

    fn sidebar(
        &self,
        initial_results: &[distributed::InitialSearchResultShard],
        stackoverflow: Option<DisplayedSidebar>,
    ) -> Option<DisplayedSidebar> {
        let entity = initial_results
            .iter()
            .filter_map(|res| res.local_result.entity_sidebar.clone())
            .map(DisplayedEntity::from)
            .filter(|entity| entity.match_score as f64 > self.thresholds.entity_sidebar)
            .max_by(|a, b| {
                a.match_score
                    .partial_cmp(&b.match_score)
                    .unwrap_or(Ordering::Equal)
            });

        match entity {
            Some(entity) => Some(DisplayedSidebar::Entity(entity)),
            None => stackoverflow,
        }
    }

    async fn check_bangs(&self, query: &SearchQuery) -> Result<Option<BangHit>> {
        let parsed_terms = query::parser::parse(&query.query);

        if parsed_terms.iter().any(|term| match term.as_ref() {
            query::parser::Term::PossibleBang(t) => t.is_empty(),
            _ => false,
        }) {
            let q: String = intersperse(
                parsed_terms
                    .iter()
                    .filter(|term| !matches!(term.as_ref(), query::parser::Term::PossibleBang(_)))
                    .map(|term| term.to_string()),
                " ".to_string(),
            )
            .collect();

            let mut query = query.clone();
            query.query = q;

            let res = self.search_websites(&query).await?;

            return Ok(res.webpages.first().map(|webpage| BangHit {
                bang: Bang {
                    category: None,
                    sub_category: None,
                    domain: None,
                    ranking: None,
                    site: None,
                    tag: String::new(),
                    url: webpage.url.clone(),
                },
                redirect_to: Url::parse(&webpage.url).unwrap().into(),
            }));
        }

        Ok(self.bangs.get(&parsed_terms))
    }

    async fn discussions_widget(
        &self,
        query: &SearchQuery,
    ) -> Result<Option<Vec<DisplayedWebpage>>> {
        if !query.fetch_discussions || query.optic.is_some() || query.page > 0 {
            return Ok(None);
        }

        const NUM_RESULTS: usize = 10;

        let mut query = SearchQuery {
            query: query.query.clone(),
            num_results: NUM_RESULTS,
            optic: Some(Optic::parse(include_str!("discussions.optic")).unwrap()),
            site_rankings: query.site_rankings.clone(),
            return_ranking_signals: query.return_ranking_signals,
            ..Default::default()
        };

        #[cfg(feature = "libtorch")]
        let pipeline: RankingPipeline<ScoredWebsitePointer> = RankingPipeline::reranking_for_query(
            &mut query,
            self.cross_encoder.as_ref().map(Arc::clone),
            self.lambda_model.clone(),
            self.collector_config.clone(),
        )?;

        #[cfg(not(feature = "libtorch"))]
        let pipeline: RankingPipeline<ScoredWebsitePointer> =
            RankingPipeline::reranking_for_query::<DummyCrossEncoder>(
                &mut query,
                None,
                self.lambda_model.clone(),
                self.collector_config.clone(),
            )?;

        let initial_results = self.distributed_searcher.search_initial(&query).await;

        if initial_results.is_empty() {
            return Ok(None);
        }

        let num_results: usize = initial_results
            .iter()
            .map(|res| res.local_result.websites.len())
            .sum();

        if num_results < NUM_RESULTS / 2 {
            return Ok(None);
        }

        let (top_websites, _) = self.combine_results(initial_results, vec![], pipeline);

        let scores: Vec<_> = top_websites
            .iter()
            .map(|pointer| pointer.as_ranking().score)
            .collect();

        let median = if scores.len() % 2 == 0 {
            (scores[(scores.len() / 2) - 1] + scores[scores.len() / 2]) / 2.0
        } else {
            scores[scores.len() / 2]
        };

        if median < self.thresholds.discussions_widget {
            return Ok(None);
        }

        let mut result: Vec<DisplayedWebpage> = self
            .distributed_searcher
            .retrieve_webpages(
                &top_websites
                    .clone()
                    .into_iter()
                    .filter_map(|pointer| {
                        if let ScoredWebsitePointer::Normal(p) = pointer {
                            Some(p)
                        } else {
                            None
                        }
                    })
                    .enumerate()
                    .collect_vec(),
                &query.query,
            )
            .await
            .into_iter()
            .map(|(_, webpage)| webpage)
            .map(DisplayedWebpage::from)
            .collect();

        if query.return_ranking_signals {
            self.add_ranking_signals(&mut result, &top_websites);
        }

        Ok(Some(result))
    }

    fn add_ranking_signals(
        &self,
        websites: &mut [DisplayedWebpage],
        pointers: &[ScoredWebsitePointer],
    ) {
        for (website, pointer) in websites.iter_mut().zip(pointers.iter()) {
            let mut signals = HashMap::with_capacity(ALL_SIGNALS.len());

            for signal in ALL_SIGNALS {
                if let Some(signal_value) = pointer.as_ranking().signals.get(signal) {
                    signals.insert(signal, *signal_value);
                }
            }

            website.ranking_signals = Some(signals);
        }
    }

    async fn search_websites(&self, query: &SearchQuery) -> Result<WebsitesResult> {
        let start = Instant::now();

        if query.is_empty() {
            return Err(distributed::Error::EmptyQuery.into());
        }

        let mut search_query = query.clone();
        #[cfg(feature = "libtorch")]
        let pipeline: RankingPipeline<ScoredWebsitePointer> = RankingPipeline::reranking_for_query(
            &mut search_query,
            self.cross_encoder.as_ref().map(Arc::clone),
            self.lambda_model.clone(),
            self.collector_config.clone(),
        )?;

        #[cfg(not(feature = "libtorch"))]
        let pipeline: RankingPipeline<ScoredWebsitePointer> =
            RankingPipeline::reranking_for_query::<DummyCrossEncoder>(
                &mut search_query,
                None,
                self.lambda_model.clone(),
                self.collector_config.clone(),
            )?;

        let (initial_results, live_results, widget, discussions, stackoverflow) = tokio::join!(
            self.distributed_searcher.search_initial(&search_query),
            self.live_searcher.search_initial(&search_query),
            self.widget(query),
            self.discussions_widget(query),
            self.stackoverflow_sidebar(query),
        );

        let discussions = discussions?;
        let stackoverflow = stackoverflow?;
        let sidebar = self.sidebar(&initial_results, stackoverflow);

        let spell_corrected_query = if widget.is_none() {
            initial_results
                .first()
                .and_then(|result| result.local_result.spell_corrected_query.clone())
                .map(HighlightedSpellCorrection::from)
        } else {
            None
        };

        let num_docs = initial_results
            .iter()
            .map(|result| result.local_result.num_websites)
            .sum();

        let (top_websites, has_more_results) =
            self.combine_results(initial_results, live_results, pipeline);

        // retrieve webpages
        let normal: Vec<_> = top_websites
            .iter()
            .enumerate()
            .filter_map(|(i, pointer)| {
                if let ScoredWebsitePointer::Normal(p) = pointer {
                    Some((i, p.clone()))
                } else {
                    None
                }
            })
            .collect();

        let live: Vec<_> = top_websites
            .iter()
            .enumerate()
            .filter_map(|(i, pointer)| {
                if let ScoredWebsitePointer::Live(p) = pointer {
                    Some((i, p.clone()))
                } else {
                    None
                }
            })
            .collect();

        let (retrieved_normal, retrieved_live) = tokio::join!(
            self.distributed_searcher
                .retrieve_webpages(&normal, &query.query),
            self.live_searcher.retrieve_webpages(&live, &query.query),
        );

        let mut retrieved_webpages: Vec<_> =
            retrieved_normal.into_iter().chain(retrieved_live).collect();
        retrieved_webpages.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut retrieved_webpages: Vec<_> = retrieved_webpages
            .into_iter()
            .map(|(_, webpage)| webpage)
            .map(DisplayedWebpage::from)
            .collect();

        if retrieved_webpages.len() != top_websites.len() {
            return Err(distributed::Error::SearchFailed.into());
        }

        if query.return_ranking_signals {
            self.add_ranking_signals(&mut retrieved_webpages, &top_websites);
        }

        let direct_answer = self.answer(&query.query, &mut retrieved_webpages);

        let search_duration_ms = start.elapsed().as_millis();

        Ok(WebsitesResult {
            spell_corrected_query,
            num_hits: num_docs,
            webpages: retrieved_webpages,
            direct_answer,
            sidebar,
            widget,
            discussions,
            search_duration_ms,
            has_more_results,
        })
    }

    pub async fn search(&self, query: &SearchQuery) -> Result<SearchResult> {
        if let Some(bang) = self.check_bangs(query).await? {
            return Ok(SearchResult::Bang(Box::new(bang)));
        }

        Ok(SearchResult::Websites(self.search_websites(query).await?))
    }

    async fn widget(&self, query: &SearchQuery) -> Option<Widget> {
        if query.page > 0 {
            return None;
        }

        let parsed_terms = query::parser::parse(&query.query);

        self.widgets.widget(
            parsed_terms
                .into_iter()
                .filter_map(|term| {
                    if let query::parser::Term::Simple(simple) = *term {
                        Some(String::from(simple))
                    } else {
                        None
                    }
                })
                .join(" ")
                .as_str(),
        )
    }

    #[cfg(feature = "libtorch")]
    fn answer(&self, query: &str, webpages: &mut Vec<DisplayedWebpage>) -> Option<DisplayedAnswer> {
        self.qa_model.as_ref().and_then(|qa_model| {
            let contexts: Vec<_> = webpages
                .iter()
                .take(1)
                .filter_map(|webpage| webpage.snippet.text())
                .map(|t| t.fragments.iter().map(|f| f.text()).join(""))
                .collect();

            match qa_model.run(query, &contexts) {
                Some(answer) => {
                    let answer_webpage = webpages.remove(answer.context_idx);
                    let snip = answer_webpage
                        .snippet
                        .text()
                        .unwrap()
                        .fragments
                        .iter()
                        .map(|f| f.text())
                        .join("");
                    Some(DisplayedAnswer {
                        title: answer_webpage.title,
                        url: answer_webpage.url,
                        pretty_url: answer_webpage.pretty_url,
                        snippet: generate_answer_snippet(&snip, answer.offset.clone()),
                        answer: snip[answer.offset].to_string(),
                    })
                }
                None => None,
            }
        })
    }

    #[cfg(not(feature = "libtorch"))]
    fn answer(&self, query: &str, webpages: &mut Vec<DisplayedWebpage>) -> Option<DisplayedAnswer> {
        None
    }

    pub async fn get_webpage(&self, url: &str) -> Result<Option<RetrievedWebpage>> {
        self.distributed_searcher.get_webpage(url).await
    }

    pub async fn get_entity_image(&self, image_id: &str) -> Result<Option<Image>> {
        self.distributed_searcher.get_entity_image(image_id).await
    }
}

fn generate_answer_snippet(body: &str, answer_offset: Range<usize>) -> String {
    let mut best_start = 0;
    let mut best_end = 0;
    const SNIPPET_LENGTH: usize = 200;

    if body.is_empty() || answer_offset.start > body.len() - 1 {
        return body.to_string();
    }

    for (idx, _) in body.char_indices().filter(|(_, c)| *c == '.') {
        if idx > answer_offset.end + SNIPPET_LENGTH {
            break;
        }

        if idx < answer_offset.start {
            best_start = idx;
        }

        if idx > answer_offset.end {
            best_end = idx;
        }
    }

    if (answer_offset.end - best_start > SNIPPET_LENGTH) || (best_start >= best_end) {
        if answer_offset.end - answer_offset.start >= SNIPPET_LENGTH {
            let end = floor_char_boundary(body, answer_offset.start + SNIPPET_LENGTH);

            return "<b>".to_string() + &body[answer_offset.start..end] + "</b>";
        }

        let chars_either_side = (SNIPPET_LENGTH - (answer_offset.end - answer_offset.start)) / 2;

        let start = ceil_char_boundary(
            body,
            answer_offset
                .start
                .checked_sub(chars_either_side)
                .unwrap_or_default(),
        );
        let mut end = ceil_char_boundary(body, answer_offset.end + chars_either_side);

        if end >= body.len() {
            end = floor_char_boundary(body, body.len());
        }

        body[start..answer_offset.start].to_string()
            + "<b>"
            + &body[answer_offset.clone()]
            + "</b>"
            + &body[answer_offset.end..end]
    } else {
        let mut res = body[best_start..answer_offset.start].to_string()
            + "<b>"
            + &body[answer_offset.clone()]
            + "</b>";

        let remaining_chars = SNIPPET_LENGTH - (res.len() - 7);
        let end = ceil_char_boundary(body, (remaining_chars + answer_offset.end).min(best_end));

        res += &body[answer_offset.end..end];

        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_generate_answer_snippet() {
        assert_eq!(
            generate_answer_snippet("this is a test", 0..4),
            "<b>this</b> is a test".to_string()
        );

        assert_eq!(
            generate_answer_snippet("this is a test", 0..1000),
            "<b>this is a test</b>".to_string()
        );
        assert_eq!(
            generate_answer_snippet("this is a test", 1000..2000),
            "this is a test".to_string()
        );
        let input = r#"
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
            "#;

        let res = generate_answer_snippet(input, 0..500);
        assert!(!res.is_empty());
        assert!(res.len() > 100);
        assert!(res.len() < input.len());
        assert!(res.starts_with("<b>"));
        assert!(res.ends_with("</b>"));

        assert_eq!(generate_answer_snippet("", 0..2000), "".to_string());
    }
}
