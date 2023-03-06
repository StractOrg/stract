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

use crate::{
    bangs::{Bang, BangHit, Bangs},
    ceil_char_boundary,
    cluster::{member::Service, Cluster},
    collector::{self, BucketCollector},
    exponential_backoff::ExponentialBackoff,
    floor_char_boundary,
    inverted_index::{self, RetrievedWebpage},
    qa_model::QaModel,
    query,
    ranking::{
        models::cross_encoder::CrossEncoderModel,
        pipeline::{AsRankingWebsite, RankingPipeline, RankingWebsite},
    },
    search_prettifier::{
        create_stackoverflow_sidebar, DisplayedAnswer, DisplayedWebpage,
        HighlightedSpellCorrection, Sidebar,
    },
    searcher::WebsitesResult,
    widgets::Widget,
    Result,
};

use std::{
    cmp::Ordering,
    collections::HashMap,
    net::SocketAddr,
    ops::Range,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use itertools::intersperse;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::sonic;

use super::{InitialWebsiteResult, SearchQuery, SearchResult};

const STACKOVERFLOW_SIDEBAR_THRESHOLD: f64 = 100.0;
const DISCUSSIONS_WIDGET_THRESHOLD: f64 = 100.0;

struct RemoteSearcher {
    addr: SocketAddr,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to get search result")]
    SearchFailed,

    #[error("Query cannot be empty")]
    EmptyQuery,

    #[error("Webpage not found")]
    WebpageNotFound,
}

impl RemoteSearcher {
    async fn search(&self, query: &SearchQuery) -> Result<InitialWebsiteResult> {
        for timeout in ExponentialBackoff::from_millis(30)
            .with_limit(Duration::from_millis(200))
            .take(5)
        {
            if let Ok(connection) = sonic::Connection::create_with_timeout(self.addr, timeout).await
            {
                if let Ok(sonic::Response::Content(body)) =
                    connection.send(Request::Search(query.clone())).await
                {
                    return Ok(body);
                }
            }
        }

        Err(Error::SearchFailed.into())
    }

    async fn retrieve_websites(
        &self,
        pointers: &[inverted_index::WebsitePointer],
        original_query: &str,
    ) -> Result<Vec<RetrievedWebpage>> {
        for timeout in ExponentialBackoff::from_millis(30)
            .with_limit(Duration::from_millis(200))
            .take(5)
        {
            if let Ok(connection) = sonic::Connection::create_with_timeout(self.addr, timeout).await
            {
                if let Ok(sonic::Response::Content(body)) = connection
                    .send(Request::RetrieveWebsites {
                        websites: pointers.to_vec(),
                        query: original_query.to_string(),
                    })
                    .await
                {
                    return Ok(body);
                }
            }
        }

        Err(Error::SearchFailed.into())
    }

    async fn get_webpage(&self, url: &str) -> Result<Option<RetrievedWebpage>> {
        for timeout in ExponentialBackoff::from_millis(30)
            .with_limit(Duration::from_millis(200))
            .take(5)
        {
            if let Ok(connection) = sonic::Connection::create_with_timeout(self.addr, timeout).await
            {
                if let Ok(sonic::Response::Content(body)) = connection
                    .send(Request::GetWebpage {
                        url: url.to_string(),
                    })
                    .await
                {
                    return Ok(body);
                }
            }
        }

        Err(Error::WebpageNotFound.into())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug)]
pub struct ShardId(u64);

pub struct Shard {
    id: ShardId,
    replicas: Vec<RemoteSearcher>,
}

impl Shard {
    pub fn new(id: u64, replicas: Vec<String>) -> Self {
        let mut parsed_replicas = Vec::new();

        for replica in replicas {
            parsed_replicas.push(RemoteSearcher {
                addr: replica.parse().unwrap(),
            });
        }

        Self {
            id: ShardId(id),
            replicas: parsed_replicas,
        }
    }

    async fn search(&self, query: &SearchQuery) -> Result<InitialSearchResultShard> {
        match self
            .replicas
            .iter()
            .map(|remote| remote.search(query))
            .collect::<FuturesUnordered<_>>()
            .next()
            .await
        {
            Some(result) => Ok(InitialSearchResultShard {
                local_result: result?,
                shard: self.id.clone(),
            }),
            None => Err(Error::SearchFailed.into()),
        }
    }

    async fn retrieve_websites(
        &self,
        pointers: &[inverted_index::WebsitePointer],
        original_query: &str,
    ) -> Result<Vec<RetrievedWebpage>> {
        match self
            .replicas
            .iter()
            .map(|remote| remote.retrieve_websites(pointers, original_query))
            .collect::<FuturesUnordered<_>>()
            .next()
            .await
        {
            Some(Ok(websites)) => Ok(websites),
            _ => Err(Error::SearchFailed.into()),
        }
    }

    async fn get_webpage(&self, url: &str) -> Result<Option<RetrievedWebpage>> {
        match self
            .replicas
            .iter()
            .map(|remote| remote.get_webpage(url))
            .collect::<FuturesUnordered<_>>()
            .next()
            .await
        {
            Some(t) => t,
            _ => Err(Error::WebpageNotFound.into()),
        }
    }
}

#[derive(Debug)]
struct InitialSearchResultShard {
    local_result: InitialWebsiteResult,
    shard: ShardId,
}

#[derive(Serialize, Deserialize)]
pub enum Request {
    Search(SearchQuery),
    RetrieveWebsites {
        websites: Vec<inverted_index::WebsitePointer>,
        query: String,
    },
    GetWebpage {
        url: String,
    },
}

pub struct DistributedSearcher {
    cluster: Cluster,
    cross_encoder: Arc<CrossEncoderModel>,
    qa_model: Option<Arc<QaModel>>,
    bangs: Bangs,
}

#[derive(Clone)]
struct ScoredWebsitePointer {
    website: RankingWebsite,
    shard: ShardId,
}

impl AsRankingWebsite for ScoredWebsitePointer {
    fn as_ranking(&self) -> &RankingWebsite {
        &self.website
    }

    fn as_mut_ranking(&mut self) -> &mut RankingWebsite {
        &mut self.website
    }
}

impl collector::Doc for ScoredWebsitePointer {
    fn score(&self) -> &f64 {
        &self.website.pointer.score.total
    }

    fn id(&self) -> &tantivy::DocId {
        &self.website.pointer.address.doc_id
    }

    fn hashes(&self) -> collector::Hashes {
        self.website.pointer.hashes
    }
}

impl DistributedSearcher {
    pub fn new(
        cluster: Cluster,
        model: CrossEncoderModel,
        qa_model: Option<QaModel>,
        bangs: Bangs,
    ) -> Self {
        Self {
            cluster,
            cross_encoder: Arc::new(model),
            qa_model: qa_model.map(Arc::new),
            bangs,
        }
    }

    async fn shards(&self) -> Vec<Shard> {
        let mut shards = HashMap::new();
        for member in self.cluster.members().await {
            if let Service::Searcher { host, shard } = member.service {
                shards.entry(shard).or_insert_with(Vec::new).push(host);
            }
        }

        shards
            .into_iter()
            .map(|(shard, replicas)| Shard {
                id: shard,
                replicas: replicas
                    .into_iter()
                    .map(|addr| RemoteSearcher { addr })
                    .collect(),
            })
            .collect()
    }

    fn combine_results(
        &self,
        initial_results: Vec<InitialSearchResultShard>,
        pipeline: RankingPipeline<ScoredWebsitePointer>,
    ) -> Vec<ScoredWebsitePointer> {
        let mut collector = BucketCollector::new(pipeline.collector_top_n());

        for result in initial_results {
            for website in result.local_result.websites {
                let pointer = ScoredWebsitePointer {
                    website,
                    shard: result.shard.clone(),
                };

                collector.insert(pointer);
            }
        }

        let top_websites = collector
            .into_sorted_vec(true)
            .into_iter()
            .take(pipeline.collector_top_n())
            .collect::<Vec<_>>();

        pipeline.apply(top_websites)
    }

    async fn retrieve_webpages(
        &self,
        top_websites: &[ScoredWebsitePointer],
        query: &str,
    ) -> Vec<RetrievedWebpage> {
        let mut retrieved_webpages = Vec::new();

        for _ in 0..top_websites.len() {
            retrieved_webpages.push(None);
        }

        for shard in self.shards().await.iter() {
            let (indexes, pointers): (Vec<_>, Vec<_>) = top_websites
                .iter()
                .enumerate()
                .filter(|(_, pointer)| pointer.shard == shard.id)
                .map(|(idx, pointer)| (idx, pointer.website.pointer.clone()))
                .unzip();

            if let Ok(websites) = shard.retrieve_websites(&pointers, query).await {
                for (index, website) in indexes.into_iter().zip(websites.into_iter()) {
                    retrieved_webpages[index] = Some(website);
                }
            }
        }

        let retrieved_webpages: Vec<_> = retrieved_webpages.into_iter().flatten().collect();

        debug_assert_eq!(retrieved_webpages.len(), top_websites.len());

        retrieved_webpages
    }

    async fn search_initial(&self, query: &SearchQuery) -> Vec<InitialSearchResultShard> {
        self.shards()
            .await
            .iter()
            .map(|shard| shard.search(query))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|result| result.ok())
            .collect::<Vec<_>>()
    }

    async fn stackoverflow_sidebar(&self, query: &SearchQuery) -> Result<Option<Sidebar>> {
        let query = SearchQuery {
            query: query.query.clone(),
            num_results: 1,
            optic_program: Some(include_str!("stackoverflow.optic").to_string()),
            ..Default::default()
        };

        let mut results: Vec<_> = self
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
            if website.score > STACKOVERFLOW_SIDEBAR_THRESHOLD {
                let scored_websites = vec![ScoredWebsitePointer { website, shard }];
                let mut retrieved = self.retrieve_webpages(&scored_websites, &query.query).await;

                if let Some(res) = retrieved.pop() {
                    return Ok(Some(create_stackoverflow_sidebar(res.schema_org, res.url)?));
                }
            }
        }

        Ok(None)
    }

    async fn sidebar(
        &self,
        initial_results: &[InitialSearchResultShard],
        query: &SearchQuery,
    ) -> Result<Option<Sidebar>> {
        let entity = initial_results
            .first()
            .and_then(|result| result.local_result.entity_sidebar.clone());

        match entity {
            Some(entity) => Ok(Some(Sidebar::Entity(entity))),
            None => Ok(self.stackoverflow_sidebar(query).await?),
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
                redirect_to: webpage.url.clone().into(),
            }));
        }

        Ok(self.bangs.get(&parsed_terms))
    }

    async fn discussions_widget(
        &self,
        query: &SearchQuery,
    ) -> Result<Option<Vec<DisplayedWebpage>>> {
        if query.optic_program.is_some() || query.offset > 0 {
            return Ok(None);
        }

        const NUM_RESULTS: usize = 10;

        let mut query = SearchQuery {
            query: query.query.clone(),
            num_results: NUM_RESULTS,
            optic_program: Some(include_str!("discussions.optic").to_string()),
            site_rankings: query.site_rankings.clone(),
            ..Default::default()
        };

        let pipeline: RankingPipeline<ScoredWebsitePointer> =
            RankingPipeline::for_query(&mut query, self.cross_encoder.clone())?;

        let initial_results = self.search_initial(&query).await;

        if initial_results.is_empty() {
            return Ok(None);
        }

        let num_results: usize = initial_results
            .iter()
            .map(|res| res.local_result.num_websites)
            .sum();

        if num_results < NUM_RESULTS / 2 {
            return Ok(None);
        }

        let results = self.combine_results(initial_results, pipeline);

        let scores: Vec<_> = results
            .iter()
            .map(|pointer| pointer.website.score)
            .collect();

        let median = if scores.len() % 2 == 0 {
            (scores[(scores.len() / 2) - 1] + scores[(scores.len() / 2)]) / 2.0
        } else {
            scores[scores.len() / 2]
        };

        if median < DISCUSSIONS_WIDGET_THRESHOLD {
            return Ok(None);
        }

        let result = self
            .retrieve_webpages(&results, &query.query)
            .await
            .into_iter()
            .map(DisplayedWebpage::from)
            .collect();

        Ok(Some(result))
    }

    async fn search_websites(&self, query: &SearchQuery) -> Result<WebsitesResult> {
        let start = Instant::now();

        if query.is_empty() {
            return Err(Error::EmptyQuery.into());
        }

        let mut search_query = query.clone();
        let pipeline: RankingPipeline<ScoredWebsitePointer> =
            RankingPipeline::for_query(&mut search_query, self.cross_encoder.clone())?;

        let initial_results = self.search_initial(&search_query).await;

        let sidebar = self.sidebar(&initial_results, query).await?;
        let discussions = self.discussions_widget(query).await?;

        let widget = self.widget(query);

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

        let num_shard_websites: usize = initial_results
            .iter()
            .map(|res| res.local_result.websites.len())
            .sum();

        let top_websites = self.combine_results(initial_results, pipeline);

        let has_more_results = num_shard_websites != top_websites.len();

        // retrieve webpages
        let mut retrieved_webpages: Vec<_> = self
            .retrieve_webpages(&top_websites, &query.query)
            .await
            .into_iter()
            .map(DisplayedWebpage::from)
            .collect();

        if retrieved_webpages.is_empty() && !top_websites.is_empty() {
            return Err(Error::SearchFailed.into());
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
            return Ok(SearchResult::Bang(bang));
        }

        Ok(SearchResult::Websites(self.search_websites(query).await?))
    }

    fn widget(&self, query: &SearchQuery) -> Option<Widget> {
        if query.offset > 0 {
            return None;
        }

        Widget::try_new(&query.query)
    }

    fn answer(&self, query: &str, webpages: &mut Vec<DisplayedWebpage>) -> Option<DisplayedAnswer> {
        self.qa_model.as_ref().and_then(|qa_model| {
            let contexts: Vec<_> = webpages
                .iter()
                .take(1)
                .map(|webpage| webpage.body.as_str())
                .collect();

            match qa_model.run(query, &contexts) {
                Some(answer) => {
                    let answer_webpage = webpages.remove(answer.context_idx);
                    Some(DisplayedAnswer {
                        title: answer_webpage.title,
                        url: answer_webpage.url,
                        pretty_url: answer_webpage.pretty_url,
                        snippet: generate_answer_snippet(
                            &answer_webpage.body,
                            answer.offset.clone(),
                        ),
                        answer: answer_webpage.body[answer.offset].to_string(),
                        body: answer_webpage.body,
                    })
                }
                None => None,
            }
        })
    }

    pub(crate) async fn get_webpage(&self, url: &str) -> Result<RetrievedWebpage> {
        self.shards()
            .await
            .iter()
            .map(|shard| shard.get_webpage(url))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|result| result.ok())
            .flatten()
            .collect::<Vec<_>>()
            .pop()
            .ok_or(Error::WebpageNotFound.into())
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
