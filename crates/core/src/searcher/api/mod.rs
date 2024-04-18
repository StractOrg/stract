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

mod sidebar;
mod widget;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use itertools::{intersperse, Itertools};
use url::Url;

use crate::bangs::{Bang, BangHit};
use crate::collector::{self, Doc};
use crate::config::{ApiConfig, CollectorConfig};
use crate::image_store::Image;
use crate::inverted_index::RetrievedWebpage;
use crate::models::dual_encoder::DualEncoder;
use crate::ranking::models::cross_encoder::CrossEncoderModel;
use crate::ranking::pipeline::{PrecisionRankingWebpage, RankableWebpage, RecallRankingWebpage};
use crate::ranking::SignalEnum;
use crate::search_prettifier::{DisplayedSidebar, DisplayedWebpage, HighlightedSpellCorrection};
use crate::web_spell::SpellChecker;
use crate::widgets::{Widget, Widgets};
use crate::{
    bangs::Bangs,
    collector::BucketCollector,
    ranking::{models::lambdamart::LambdaMART, pipeline::RankingPipeline},
};
use crate::{query, Result};

use self::sidebar::SidebarManager;
use self::widget::WidgetManager;

use super::{distributed, live, SearchQuery, SearchResult, WebsitesResult};

#[derive(Clone)]
pub enum ScoredWebpagePointer {
    Normal(distributed::ScoredWebpagePointer),
    Live(live::ScoredWebpagePointer),
}

impl ScoredWebpagePointer {
    pub fn as_ranking(&self) -> &RecallRankingWebpage {
        match self {
            ScoredWebpagePointer::Normal(p) => &p.website,
            ScoredWebpagePointer::Live(p) => &p.website,
        }
    }

    pub fn as_ranking_mut(&mut self) -> &mut RecallRankingWebpage {
        match self {
            ScoredWebpagePointer::Normal(p) => &mut p.website,
            ScoredWebpagePointer::Live(p) => &mut p.website,
        }
    }
}

impl RankableWebpage for ScoredWebpagePointer {
    fn set_score(&mut self, score: f64) {
        self.as_ranking_mut().set_score(score);
    }

    fn boost(&self) -> Option<f64> {
        self.as_ranking().boost()
    }
}

impl collector::Doc for ScoredWebpagePointer {
    fn score(&self) -> f64 {
        self.as_ranking().score()
    }

    fn hashes(&self) -> collector::Hashes {
        self.as_ranking().hashes()
    }
}

pub fn combine_results(
    collector_config: CollectorConfig,
    initial_results: Vec<distributed::InitialSearchResultShard>,
    live_results: Vec<live::InitialSearchResultSplit>,
    pipeline: RankingPipeline<ScoredWebpagePointer>,
) -> (Vec<ScoredWebpagePointer>, bool) {
    let mut collector = BucketCollector::new(pipeline.collector_top_n(), collector_config);

    let mut has_more = false;
    for result in initial_results {
        if result.local_result.has_more {
            has_more = true;
        }

        for website in result.local_result.websites {
            let pointer = distributed::ScoredWebpagePointer {
                website,
                shard: result.shard,
            };

            let pointer = ScoredWebpagePointer::Normal(pointer);

            collector.insert(pointer);
        }
    }

    for result in live_results {
        if result.local_result.has_more {
            has_more = true;
        }

        for website in result.local_result.websites {
            let pointer = live::ScoredWebpagePointer {
                website,
                split_id: result.split_id.clone(),
            };

            let pointer = ScoredWebpagePointer::Live(pointer);

            collector.insert(pointer);
        }
    }

    let top_websites = collector
        .into_sorted_vec(true)
        .into_iter()
        .take(pipeline.collector_top_n())
        .collect::<Vec<_>>();

    let res = pipeline.apply(top_websites);

    (res, has_more)
}
pub fn add_ranking_signals(websites: &mut [DisplayedWebpage], pointers: &[ScoredWebpagePointer]) {
    for (website, pointer) in websites.iter_mut().zip(pointers.iter()) {
        let mut signals = HashMap::new();

        for signal in SignalEnum::all() {
            if let Some(signal_value) = pointer.as_ranking().signals.get(signal) {
                signals.insert(signal.into(), *signal_value);
            }
        }

        website.ranking_signals = Some(signals);
    }
}

pub struct ApiSearcher<S, L> {
    distributed_searcher: Arc<S>,
    sidebar_manager: SidebarManager<S>,
    live_searcher: Option<L>,
    cross_encoder: Option<Arc<CrossEncoderModel>>,
    lambda_model: Option<Arc<LambdaMART>>,
    dual_encoder: Option<Arc<DualEncoder>>,
    bangs: Bangs,
    collector_config: CollectorConfig,
    widget_manager: WidgetManager,
    spell_checker: Option<SpellChecker>,
}

impl<S, L> ApiSearcher<S, L>
where
    S: distributed::SearchClient,
    L: live::SearchClient,
{
    pub fn new(
        dist_searcher: S,
        live_searcher: Option<L>,
        cross_encoder: Option<CrossEncoderModel>,
        lambda_model: Option<LambdaMART>,
        dual_encoder: Option<DualEncoder>,
        bangs: Bangs,
        config: ApiConfig,
    ) -> Self {
        let dist_searcher = Arc::new(dist_searcher);
        let sidebar_manager =
            SidebarManager::new(Arc::clone(&dist_searcher), config.thresholds.clone());

        let lambda_model = lambda_model.map(Arc::new);
        let dual_encoder = dual_encoder.map(Arc::new);

        let widget_manager = WidgetManager::new(Widgets::new(config.widgets).unwrap());

        Self {
            distributed_searcher: dist_searcher,
            sidebar_manager,
            live_searcher,
            cross_encoder: cross_encoder.map(Arc::new),
            lambda_model,
            dual_encoder,
            bangs,
            collector_config: config.collector,
            widget_manager,
            spell_checker: config
                .spell_checker_path
                .map(|c| SpellChecker::open(c, config.correction_config).unwrap()),
        }
    }

    async fn check_bangs(&self, query: &SearchQuery) -> Result<Option<BangHit>> {
        let parsed_terms = query::parser::parse(&query.query)?;

        if parsed_terms.iter().any(|term| match term {
            query::parser::Term::PossibleBang(t) => t.is_empty(),
            _ => false,
        }) {
            let q: String = intersperse(
                parsed_terms
                    .iter()
                    .filter(|term| !matches!(term, query::parser::Term::PossibleBang(_)))
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

    pub async fn widget(&self, query: &str) -> Option<Widget> {
        self.widget_manager.widget(query).await
    }

    pub async fn sidebar(&self, query: &str) -> Option<DisplayedSidebar> {
        self.sidebar_manager.sidebar(query).await
    }

    pub fn spell_check(&self, query: &str) -> Option<HighlightedSpellCorrection> {
        let query = query.to_lowercase();

        let terms = query::parser::parse(&query).ok()?;

        let simple_query = terms
            .clone()
            .into_iter()
            .filter_map(|term| match term {
                query::parser::Term::SimpleOrPhrase(query::parser::SimpleOrPhrase::Simple(t)) => {
                    Some(String::from(t))
                }
                _ => None,
            })
            .join(" ");

        let corrections = self
            .spell_checker
            .as_ref()
            .and_then(|s| s.correct(&simple_query, &whatlang::Lang::Eng))?;

        let correction_map: HashMap<String, String> = corrections
            .terms
            .into_iter()
            .filter_map(|t| match t {
                crate::web_spell::CorrectionTerm::Corrected { orig, correction } => {
                    Some((orig, correction))
                }
                crate::web_spell::CorrectionTerm::NotCorrected(_) => None,
            })
            .collect();

        let mut correction = crate::web_spell::Correction::empty(query);

        for term in terms {
            match term {
                query::parser::Term::SimpleOrPhrase(query::parser::SimpleOrPhrase::Simple(t)) => {
                    if let Some(term_correction) = correction_map.get(t.as_str()) {
                        correction.push(crate::web_spell::CorrectionTerm::Corrected {
                            orig: String::from(t),
                            correction: term_correction.to_string(),
                        });
                    } else {
                        correction.push(crate::web_spell::CorrectionTerm::NotCorrected(
                            String::from(t),
                        ));
                    }
                }
                _ => {
                    correction.push(crate::web_spell::CorrectionTerm::NotCorrected(
                        term.to_string(),
                    ));
                }
            }
        }

        Some(HighlightedSpellCorrection::from(correction))
    }

    async fn retrieve_webpages(
        &self,
        query: &str,
        top_websites: &[ScoredWebpagePointer],
    ) -> Vec<PrecisionRankingWebpage> {
        let normal: Vec<_> = top_websites
            .iter()
            .enumerate()
            .filter_map(|(i, pointer)| {
                if let ScoredWebpagePointer::Normal(p) = pointer {
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
                if let ScoredWebpagePointer::Live(p) = pointer {
                    Some((i, p.clone()))
                } else {
                    None
                }
            })
            .collect();

        let (retrieved_normal, retrieved_live) = tokio::join!(
            self.distributed_searcher.retrieve_webpages(&normal, query),
            self.retrieve_webpages_from_live(&live, query),
        );

        let mut retrieved_webpages: Vec<_> =
            retrieved_normal.into_iter().chain(retrieved_live).collect();
        retrieved_webpages.sort_by(|(a, _), (b, _)| a.cmp(b));

        retrieved_webpages
            .into_iter()
            .map(|(_, webpage)| webpage)
            .collect::<Vec<_>>()
    }

    async fn search_initial_from_live(
        &self,
        query: &SearchQuery,
    ) -> Option<Vec<live::InitialSearchResultSplit>> {
        match &self.live_searcher {
            Some(searcher) => Some(searcher.search_initial(query).await),
            None => None,
        }
    }

    async fn retrieve_webpages_from_live(
        &self,
        pointers: &[(usize, live::ScoredWebpagePointer)],
        query: &str,
    ) -> Vec<(usize, PrecisionRankingWebpage)> {
        match &self.live_searcher {
            Some(searcher) => searcher.retrieve_webpages(pointers, query).await,
            None => vec![],
        }
    }

    async fn search_websites(&self, query: &SearchQuery) -> Result<WebsitesResult> {
        let start = Instant::now();

        if query.is_empty() {
            return Err(distributed::Error::EmptyQuery.into());
        }

        let mut search_query = query.clone();
        let top_n = search_query.num_results;

        // This pipeline should be created before the first search is performed
        // so the query knows how many results to fetch from the indices
        let recall_pipeline: RankingPipeline<ScoredWebpagePointer> =
            RankingPipeline::<ScoredWebpagePointer>::recall_stage(
                &mut search_query,
                self.lambda_model.clone(),
                self.dual_encoder.clone(),
                self.collector_config.clone(),
                top_n,
            );

        let (initial_results, live_results) = tokio::join!(
            self.distributed_searcher.search_initial(&search_query),
            self.search_initial_from_live(&search_query),
        );

        let num_docs = initial_results
            .iter()
            .map(|result| result.local_result.num_websites)
            .sum();

        let (top_websites, has_more_results) = combine_results(
            self.collector_config.clone(),
            initial_results,
            live_results.unwrap_or_default(),
            recall_pipeline,
        );

        let retrieved_webpages = self
            .retrieve_webpages(&search_query.query, &top_websites)
            .await;

        let mut search_query = SearchQuery {
            page: 0,
            ..query.clone()
        };

        let reranking_pipeline: RankingPipeline<PrecisionRankingWebpage> =
            RankingPipeline::<PrecisionRankingWebpage>::reranker(
                &mut search_query,
                self.cross_encoder.clone(),
                self.lambda_model.clone(),
                self.collector_config.clone(),
                query.num_results,
            )?;

        let retrieved_webpages = reranking_pipeline.apply(retrieved_webpages);

        let mut retrieved_webpages: Vec<_> = retrieved_webpages
            .into_iter()
            .map(|webpage| webpage.into_retrieved_webpage())
            .map(|webpage| DisplayedWebpage::new(webpage, query))
            .collect();

        if retrieved_webpages.len() != top_websites.len() {
            return Err(distributed::Error::SearchFailed.into());
        }

        if query.return_ranking_signals {
            add_ranking_signals(&mut retrieved_webpages, &top_websites);
        }

        for (website, pointer) in retrieved_webpages.iter_mut().zip(top_websites.iter()) {
            website.score = Some(pointer.score());
        }

        let search_duration_ms = start.elapsed().as_millis();

        Ok(WebsitesResult {
            num_hits: num_docs,
            webpages: retrieved_webpages,
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

    pub async fn get_webpage(&self, url: &str) -> Result<Option<RetrievedWebpage>> {
        self.distributed_searcher.get_webpage(url).await
    }

    pub async fn get_entity_image(
        &self,
        image_id: &str,
        max_height: Option<u64>,
        max_width: Option<u64>,
    ) -> Result<Option<Image>> {
        self.distributed_searcher
            .get_entity_image(image_id, max_height, max_width)
            .await
    }
}
