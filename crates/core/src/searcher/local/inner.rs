// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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
    generic_query::{self, GenericQuery},
    inverted_index,
    ranking::{LocalRanker, SignalComputer},
    searcher::InitialWebsiteResult,
    Result,
};
use std::sync::Arc;

use crate::{
    config::CollectorConfig, models::dual_encoder::DualEncoder, query::Query,
    ranking::models::linear::LinearRegression, search_ctx::Ctx, searcher::SearchQuery,
};

use super::{InvertedIndexResult, SearchGuard, SearchableIndex};

pub struct InnerLocalSearcher<I: SearchableIndex> {
    index: I,
    linear_regression: Option<Arc<LinearRegression>>,
    dual_encoder: Option<Arc<DualEncoder>>,
    collector_config: CollectorConfig,
}

impl<I> InnerLocalSearcher<I>
where
    I: SearchableIndex,
{
    pub fn new(index: I) -> Self {
        Self {
            index,
            linear_regression: None,
            dual_encoder: None,
            collector_config: CollectorConfig::default(),
        }
    }

    pub async fn guard(&self) -> I::SearchGuard {
        self.index.guard().await
    }

    pub fn set_linear_model(&mut self, model: LinearRegression) {
        self.linear_regression = Some(Arc::new(model));
    }

    pub fn set_dual_encoder(&mut self, dual_encoder: DualEncoder) {
        self.dual_encoder = Some(Arc::new(dual_encoder));
    }

    pub fn set_collector_config(&mut self, config: CollectorConfig) {
        self.collector_config = config;
    }

    fn parse_query<G: SearchGuard>(
        &self,
        ctx: &Ctx,
        guard: &G,
        query: &SearchQuery,
    ) -> Result<Query> {
        Query::parse(ctx, query, guard.inverted_index())
    }

    fn ranker<G: SearchGuard>(
        &self,
        query: &Query,
        guard: &G,
        de_rank_similar: bool,
        computer: SignalComputer,
    ) -> Result<LocalRanker> {
        let mut ranker = LocalRanker::new(
            computer,
            guard.inverted_index().columnfield_reader(),
            self.collector_config.clone(),
        );

        ranker.de_rank_similar(de_rank_similar);

        Ok(ranker
            .with_max_docs(
                self.collector_config.max_docs_considered,
                guard.inverted_index().num_segments(),
            )
            .with_num_results(query.num_results())
            .with_offset(query.offset()))
    }

    fn search_inverted_index<G: SearchGuard>(
        &self,
        ctx: &Ctx,
        guard: &G,
        query: &SearchQuery,
        de_rank_similar: bool,
    ) -> Result<InvertedIndexResult> {
        let parsed_query = self.parse_query(ctx, guard, query)?;

        let mut computer = SignalComputer::new(Some(&parsed_query));

        computer.set_region_count(
            guard
                .search_index()
                .region_count
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        );

        if let Some(model) = self.linear_regression.as_ref() {
            computer.set_linear_model(model.clone());
        }

        let ranker = self.ranker(&parsed_query, guard, de_rank_similar, computer)?;

        let res = guard.inverted_index().search_initial(
            &parsed_query,
            ctx,
            ranker.collector(ctx.clone()),
        )?;

        let columnfield_reader = guard.inverted_index().columnfield_reader();

        let ranking_websites = guard.inverted_index().retrieve_ranking_websites(
            ctx,
            res.top_websites,
            ranker.computer(),
            &columnfield_reader,
        )?;

        Ok(InvertedIndexResult {
            webpages: ranking_websites,
            num_hits: res.num_websites,
        })
    }

    pub fn search_initial(
        &self,
        query: &SearchQuery,
        guard: &I::SearchGuard,
        de_rank_similar: bool,
    ) -> Result<InitialWebsiteResult> {
        let query = query.clone();

        let ctx = guard.inverted_index().local_search_ctx();
        let inverted_index_result =
            self.search_inverted_index(&ctx, guard, &query, de_rank_similar)?;

        Ok(InitialWebsiteResult {
            websites: inverted_index_result.webpages,
            num_websites: inverted_index_result.num_hits,
        })
    }

    pub fn retrieve_websites(
        &self,
        websites: &[inverted_index::WebpagePointer],
        query: &str,
        guard: &I::SearchGuard,
    ) -> Result<Vec<inverted_index::RetrievedWebpage>> {
        let ctx = guard.inverted_index().local_search_ctx();
        let query = SearchQuery {
            query: query.to_string(),
            ..Default::default()
        };
        let query = Query::parse(&ctx, &query, guard.inverted_index())?;

        guard.inverted_index().retrieve_websites(websites, &query)
    }

    pub fn search_initial_generic<Q: GenericQuery>(
        &self,
        query: &Q,
        guard: &I::SearchGuard,
    ) -> Result<<Q::Collector as generic_query::Collector>::Fruit> {
        guard.inverted_index().search_initial_generic(query)
    }

    pub fn retrieve_generic<Q: GenericQuery>(
        &self,
        query: &Q,
        fruit: <Q::Collector as generic_query::Collector>::Fruit,
        guard: &I::SearchGuard,
    ) -> Result<Q::IntermediateOutput> {
        guard.inverted_index().retrieve_generic(query, fruit)
    }

    pub fn search_generic<Q: GenericQuery>(
        &self,
        query: Q,
        guard: &I::SearchGuard,
    ) -> Result<Q::Output> {
        let fruit = self.search_initial_generic(&query, guard)?;
        Ok(Q::merge_results(vec![
            self.retrieve_generic(&query, fruit, guard)?
        ]))
    }
}
