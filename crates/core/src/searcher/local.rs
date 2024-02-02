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

use std::collections::HashMap;
use std::sync::{Arc, RwLockReadGuard};

use url::Url;

use crate::config::{CollectorConfig, SnippetConfig};
use crate::index::Index;
use crate::inverted_index::{InvertedIndex, RetrievedWebpage};
use crate::query::Query;
use crate::ranking::inbound_similarity::InboundSimilarity;
use crate::ranking::models::lambdamart::LambdaMART;
use crate::ranking::models::linear::LinearRegression;
use crate::ranking::pipeline::{RankingPipeline, RankingWebsite};
use crate::ranking::{query_centrality, Ranker, Signal, SignalAggregator, ALL_SIGNALS};
use crate::search_ctx::Ctx;
use crate::search_prettifier::DisplayedWebpage;
use crate::webgraph::Node;
use crate::{inverted_index, live_index, Error, Result};

use super::WebsitesResult;
use super::{InitialWebsiteResult, SearchQuery};

pub trait SearchableIndex {
    type SearchGuard<'a>: SearchGuard<'a>
    where
        Self: 'a;

    fn guard(&self) -> Self::SearchGuard<'_>;
    fn optimize_for_search(&mut self);
    fn set_snippet_config(&mut self, config: SnippetConfig);
}

pub trait SearchGuard<'a> {
    fn search_index(&self) -> &'_ Index;
    fn inverted_index(&self) -> &'_ InvertedIndex {
        &self.search_index().inverted_index
    }
}

impl SearchableIndex for Index {
    type SearchGuard<'a> = NormalIndexSearchGuard<'a>;

    fn guard(&self) -> Self::SearchGuard<'_> {
        NormalIndexSearchGuard { search_index: self }
    }

    fn optimize_for_search(&mut self) {
        self.optimize_for_search().unwrap();
    }

    fn set_snippet_config(&mut self, config: SnippetConfig) {
        self.inverted_index.set_snippet_config(config);
    }
}

pub struct NormalIndexSearchGuard<'a> {
    search_index: &'a Index,
}

impl<'a> SearchGuard<'a> for NormalIndexSearchGuard<'a> {
    fn search_index(&self) -> &'_ Index {
        self.search_index
    }
}

impl SearchableIndex for Arc<live_index::Index> {
    type SearchGuard<'a> = LiveIndexSearchGuard<'a>;

    fn guard(&self) -> Self::SearchGuard<'_> {
        LiveIndexSearchGuard {
            lock_guard: self.read(),
        }
    }

    fn optimize_for_search(&mut self) {
        self.write().optimize_for_search().unwrap();
    }

    fn set_snippet_config(&mut self, config: SnippetConfig) {
        self.write().inverted_index.set_snippet_config(config);
    }
}

pub struct LiveIndexSearchGuard<'a> {
    lock_guard: RwLockReadGuard<'a, crate::index::Index>,
}

impl<'a> SearchGuard<'a> for LiveIndexSearchGuard<'a> {
    fn search_index(&self) -> &'_ Index {
        &self.lock_guard
    }
}

pub struct LocalSearcher<I: SearchableIndex> {
    index: I,
    inbound_similarity: Option<InboundSimilarity>,
    linear_regression: Option<Arc<LinearRegression>>,
    lambda_model: Option<Arc<LambdaMART>>,
    collector_config: CollectorConfig,
}

impl<I> From<I> for LocalSearcher<I>
where
    I: SearchableIndex,
{
    fn from(index: I) -> Self {
        Self::new(index)
    }
}

struct InvertedIndexResult {
    webpages: Vec<RankingWebsite>,
    num_hits: Option<usize>,
    has_more: bool,
}

impl<I> LocalSearcher<I>
where
    I: SearchableIndex,
{
    pub fn new(index: I) -> Self {
        let mut index = index;
        index.optimize_for_search();

        LocalSearcher {
            index,
            inbound_similarity: None,
            linear_regression: None,
            lambda_model: None,
            collector_config: CollectorConfig::default(),
        }
    }

    pub fn set_inbound_similarity(&mut self, inbound: InboundSimilarity) {
        self.inbound_similarity = Some(inbound);
    }

    pub fn set_linear_model(&mut self, model: LinearRegression) {
        self.linear_regression = Some(Arc::new(model));
    }

    pub fn set_lambda_model(&mut self, model: LambdaMART) {
        self.lambda_model = Some(Arc::new(model));
    }

    pub fn set_collector_config(&mut self, config: CollectorConfig) {
        self.collector_config = config;
    }

    pub fn set_snippet_config(&mut self, config: SnippetConfig) {
        self.index.set_snippet_config(config);
    }

    fn parse_query<'a, G: SearchGuard<'a>>(
        &'a self,
        ctx: &Ctx,
        guard: &G,
        query: &SearchQuery,
    ) -> Result<Query> {
        let parsed_query = Query::parse(ctx, query, guard.inverted_index())?;

        if parsed_query.is_empty() {
            Err(Error::EmptyQuery.into())
        } else {
            Ok(parsed_query)
        }
    }

    fn ranker<'a, G: SearchGuard<'a>>(
        &'a self,
        query: &Query,
        ctx: &Ctx,
        guard: &G,
        de_rank_similar: bool,
        aggregator: SignalAggregator,
    ) -> Result<Ranker> {
        let query_centrality_coeff = aggregator.coefficient(&Signal::QueryCentrality);

        let mut ranker = Ranker::new(
            aggregator,
            guard.inverted_index().fastfield_reader(&ctx.tv_searcher),
            self.collector_config.clone(),
        );

        ranker.de_rank_similar(de_rank_similar);

        if query_centrality_coeff > 0.0 {
            if let Some(inbound_sim) = self.inbound_similarity.as_ref() {
                ranker = ranker
                    .with_max_docs(1_000, guard.inverted_index().num_segments())
                    .with_num_results(100);

                let top_host_nodes =
                    guard
                        .search_index()
                        .top_nodes(query, ctx, ranker.collector(ctx.clone()))?;

                if !top_host_nodes.is_empty() {
                    let inbound = inbound_sim.scorer(&top_host_nodes, &[], false);

                    let query_centrality = query_centrality::Scorer::new(inbound);

                    ranker.set_query_centrality(query_centrality);
                }
            }
        }

        Ok(ranker
            .with_max_docs(
                self.collector_config.max_docs_considered,
                guard.inverted_index().num_segments(),
            )
            .with_num_results(query.num_results())
            .with_offset(query.offset()))
    }

    fn search_inverted_index<'a, G: SearchGuard<'a>>(
        &'a self,
        ctx: &Ctx,
        guard: &G,
        query: &SearchQuery,
        de_rank_similar: bool,
    ) -> Result<InvertedIndexResult> {
        let mut query = query.clone();
        let pipeline: RankingPipeline<RankingWebsite> = RankingPipeline::recall_stage(
            &mut query,
            self.lambda_model.clone(),
            self.collector_config.clone(),
            100,
        );
        let parsed_query = self.parse_query(ctx, guard, &query)?;

        let mut aggregator = SignalAggregator::new(Some(&parsed_query));

        if let Some(inbound_sim) = &self.inbound_similarity {
            let liked_hosts: Vec<_> = parsed_query
                .host_rankings()
                .liked
                .iter()
                .map(|site| Node::from(site.clone()).into_host())
                .map(|node| node.id())
                .collect();

            let disliked_hosts: Vec<_> = parsed_query
                .host_rankings()
                .disliked
                .iter()
                .map(|site| Node::from(site.clone()).into_host())
                .map(|node| node.id())
                .collect();

            let scorer = inbound_sim.scorer(&liked_hosts, &disliked_hosts, false);
            aggregator.set_inbound_similarity(scorer);
        }

        aggregator.set_region_count(
            guard
                .search_index()
                .region_count
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        );

        if let Some(model) = self.linear_regression.as_ref() {
            aggregator.set_linear_model(model.clone());
        }

        let ranker = self.ranker(&parsed_query, ctx, guard, de_rank_similar, aggregator)?;

        let res = guard.inverted_index().search_initial(
            &parsed_query,
            ctx,
            ranker.collector(ctx.clone()),
        )?;

        let fastfield_reader = guard.inverted_index().fastfield_reader(&ctx.tv_searcher);

        let ranking_websites = guard.inverted_index().retrieve_ranking_websites(
            ctx,
            res.top_websites,
            ranker.aggregator(),
            &fastfield_reader,
        )?;

        let pipe_top_n = pipeline.top_n;
        let has_more = ranking_websites.len() > pipe_top_n;

        let ranking_websites = pipeline.apply(ranking_websites);

        Ok(InvertedIndexResult {
            webpages: ranking_websites,
            num_hits: res.num_websites,
            has_more,
        })
    }

    pub fn search_initial(
        &self,
        query: &SearchQuery,
        de_rank_similar: bool,
    ) -> Result<InitialWebsiteResult> {
        let guard = self.index.guard();
        let ctx = guard.inverted_index().local_search_ctx();
        let inverted_index_result =
            self.search_inverted_index(&ctx, &guard, query, de_rank_similar)?;

        Ok(InitialWebsiteResult {
            websites: inverted_index_result.webpages,
            num_websites: inverted_index_result.num_hits,
            has_more: inverted_index_result.has_more,
        })
    }

    pub fn retrieve_websites(
        &self,
        websites: &[inverted_index::WebsitePointer],
        query: &str,
    ) -> Result<Vec<inverted_index::RetrievedWebpage>> {
        let guard = self.index.guard();
        let ctx = guard.inverted_index().local_search_ctx();
        let query = SearchQuery {
            query: query.to_string(),
            ..Default::default()
        };
        let query = Query::parse(&ctx, &query, guard.inverted_index())?;

        if query.is_empty() {
            return Err(Error::EmptyQuery.into());
        }

        guard.inverted_index().retrieve_websites(websites, &query)
    }

    /// This function is mainly used for tests and benchmarks
    pub fn search(&self, query: &SearchQuery) -> Result<WebsitesResult> {
        use std::time::Instant;

        let start = Instant::now();
        let mut search_query = query.clone();

        let pipeline = {
            use crate::ranking::models::cross_encoder::CrossEncoderModel;
            match CrossEncoderModel::open("data/cross_encoder") {
                Ok(model) => RankingPipeline::reranker::<CrossEncoderModel>(
                    &mut search_query,
                    Some(Arc::new(model)),
                    None,
                    self.collector_config.clone(),
                    query.num_results,
                )?,
                Err(_) => RankingPipeline::reranker::<CrossEncoderModel>(
                    &mut search_query,
                    None,
                    None,
                    self.collector_config.clone(),
                    query.num_results,
                )?,
            }
        };

        let search_result = self.search_initial(&search_query, true)?;

        let search_len = search_result.websites.len();

        let top_websites = pipeline.apply(search_result.websites);

        let has_more_results = search_len != top_websites.len();

        let pointers: Vec<_> = top_websites
            .iter()
            .map(|website| website.pointer.clone())
            .collect();

        let retrieved_sites = self.retrieve_websites(&pointers, &search_query.query)?;

        let mut webpages: Vec<_> = retrieved_sites
            .into_iter()
            .map(DisplayedWebpage::from)
            .collect();

        for (webpage, ranking) in webpages.iter_mut().zip(top_websites) {
            let mut ranking_signals = HashMap::new();

            for signal in ALL_SIGNALS {
                if let Some(score) = ranking.signals.get(signal) {
                    ranking_signals.insert(signal, *score);
                }
            }

            webpage.ranking_signals = Some(ranking_signals);
        }

        Ok(WebsitesResult {
            num_hits: search_result.num_websites,
            webpages,
            search_duration_ms: start.elapsed().as_millis(),
            has_more_results,
        })
    }

    pub fn get_webpage(&self, url: &str) -> Option<RetrievedWebpage> {
        self.index.guard().inverted_index().get_webpage(url)
    }

    pub fn get_homepage(&self, url: &Url) -> Option<RetrievedWebpage> {
        self.index.guard().inverted_index().get_homepage(url)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        searcher::NUM_RESULTS_PER_PAGE,
        webpage::{Html, Webpage},
    };

    use super::*;

    #[test]
    fn offset_page() {
        const NUM_PAGES: usize = 50;
        const NUM_WEBSITES: usize = NUM_PAGES * NUM_RESULTS_PER_PAGE;

        let mut index = Index::temporary().expect("Unable to open index");

        for i in 0..NUM_WEBSITES {
            index
                .insert(Webpage {
                    html: Html::parse(
                        r#"
            <html>
                <head>
                    <title>Example website</title>
                </head>
                <body>
                    test
                </body>
            </html>
            "#,
                        &format!("https://www.{i}.com"),
                    )
                    .unwrap(),
                    host_centrality: (NUM_WEBSITES - i) as f64,
                    fetch_time_ms: 500,
                    ..Default::default()
                })
                .expect("failed to insert webpage");
        }

        index.commit().unwrap();

        let searcher = LocalSearcher::new(index);

        for p in 0..NUM_PAGES {
            let urls: Vec<_> = searcher
                .search(&SearchQuery {
                    query: "test".to_string(),
                    page: p,
                    ..Default::default()
                })
                .unwrap()
                .webpages
                .into_iter()
                .map(|page| page.url)
                .collect();

            assert!(!urls.is_empty());

            for (i, url) in urls.into_iter().enumerate() {
                assert_eq!(
                    url,
                    format!("https://www.{}.com/", i + (p * NUM_RESULTS_PER_PAGE))
                )
            }
        }
    }
}
