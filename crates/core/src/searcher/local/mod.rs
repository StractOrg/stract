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

mod guard;
use guard::ReadGuard;

mod inner;
use inner::InnerLocalSearcher;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use itertools::Itertools;
use url::Url;

use crate::collector::approx_count;
use crate::config::CollectorConfig;
use crate::generic_query::{self, GenericQuery};
use crate::index::Index;
use crate::models::dual_encoder::DualEncoder;
use crate::ranking::models::linear::LinearRegression;
use crate::ranking::pipeline::{
    LocalRecallRankingWebpage, PrecisionRankingWebpage, RankableWebpage, RecallRankingWebpage,
};
use crate::ranking::{SignalEnum, SignalScore};
use crate::search_prettifier::DisplayedWebpage;
use crate::{inverted_index, Result};

use super::WebsitesResult;
use super::{InitialWebsiteResult, SearchQuery};

pub trait SearchableIndex: Send + Sync + 'static {
    type ReadGuard: ReadGuard;

    fn read_guard(&self) -> impl Future<Output = Self::ReadGuard>;
}

impl SearchableIndex for Arc<RwLock<Index>> {
    type ReadGuard = OwnedRwLockReadGuard<Index>;

    async fn read_guard(&self) -> Self::ReadGuard {
        self.clone().read_owned().await
    }
}

pub struct LocalSearcherBuilder<I: SearchableIndex> {
    inner: InnerLocalSearcher<I>,
}

impl<I> LocalSearcherBuilder<I>
where
    I: SearchableIndex,
{
    pub fn new(index: I) -> Self {
        Self {
            inner: InnerLocalSearcher::new(index),
        }
    }

    pub fn set_linear_model(mut self, model: LinearRegression) -> Self {
        self.inner.set_linear_model(model);
        self
    }

    pub fn set_dual_encoder(mut self, dual_encoder: DualEncoder) -> Self {
        self.inner.set_dual_encoder(dual_encoder);
        self
    }

    pub fn set_collector_config(mut self, config: CollectorConfig) -> Self {
        self.inner.set_collector_config(config);
        self
    }

    pub fn build(self) -> LocalSearcher<I> {
        LocalSearcher {
            inner: Arc::new(self.inner),
        }
    }
}

pub struct LocalSearcher<I: SearchableIndex> {
    inner: Arc<InnerLocalSearcher<I>>,
}

impl<I> LocalSearcher<I>
where
    I: SearchableIndex,
{
    pub fn builder(index: I) -> LocalSearcherBuilder<I> {
        LocalSearcherBuilder::new(index)
    }

    pub async fn search_initial(
        &self,
        query: &SearchQuery,
        de_rank_similar: bool,
    ) -> Result<InitialWebsiteResult> {
        let inner = self.inner.clone();
        let query = query.clone();
        let guard = inner.guard().await;

        tokio::task::spawn_blocking(move || inner.search_initial(&query, &guard, de_rank_similar))
            .await
            .unwrap()
    }

    pub async fn retrieve_websites(
        &self,
        websites: &[inverted_index::WebpagePointer],
        query: &str,
    ) -> Result<Vec<inverted_index::RetrievedWebpage>> {
        let inner = self.inner.clone();
        let guard = inner.guard().await;
        let query = query.to_string();
        let websites = websites.to_vec();

        tokio::task::spawn_blocking(move || inner.retrieve_websites(&websites, &query, &guard))
            .await
            .unwrap()
    }

    pub async fn search(&self, query: &SearchQuery) -> Result<WebsitesResult> {
        use std::time::Instant;

        let start = Instant::now();
        let search_query = query.clone();

        let search_result = self.search_initial(&search_query, true).await?;

        let pointers: Vec<_> = search_result
            .websites
            .iter()
            .map(|website| website.pointer().clone())
            .collect();

        let websites: Vec<_> = self
            .retrieve_websites(&pointers, &query.query)
            .await?
            .into_iter()
            .zip_eq(search_result.websites)
            .map(|(webpage, ranking)| {
                let ranking = RecallRankingWebpage::new(ranking, Default::default());
                PrecisionRankingWebpage::new(webpage, ranking)
            })
            .collect();

        let pointers: Vec<_> = websites
            .iter()
            .map(|website| website.ranking().pointer().clone())
            .collect();

        let retrieved_sites = self
            .retrieve_websites(&pointers, &search_query.query)
            .await?;

        let coefficients = query.signal_coefficients();

        let mut webpages: Vec<_> = retrieved_sites
            .into_iter()
            .map(|webpage| DisplayedWebpage::new(webpage, query))
            .collect();

        for (webpage, ranking) in webpages.iter_mut().zip(websites) {
            let mut ranking_signals = HashMap::new();

            for signal in SignalEnum::all() {
                if let Some(calc) = ranking.ranking().signals().get(signal) {
                    ranking_signals.insert(
                        signal.into(),
                        SignalScore {
                            value: calc.score,
                            coefficient: coefficients.get(&signal),
                        },
                    );
                }
            }

            webpage.ranking_signals = Some(ranking_signals);
        }

        Ok(WebsitesResult {
            num_hits: search_result.num_websites,
            webpages,
            search_duration_ms: start.elapsed().as_millis(),
            has_more_results: (search_result.num_websites.as_u64() as usize)
                > query.offset() + query.num_results(),
        })
    }

    /// This function is mainly used for tests and benchmarks
    pub fn search_sync(&self, query: &SearchQuery) -> Result<WebsitesResult> {
        crate::block_on(self.search(query))
    }

    pub async fn get_site_urls(&self, site: &str, offset: usize, limit: usize) -> Vec<Url> {
        self.inner
            .guard()
            .await
            .inverted_index()
            .get_site_urls(site, offset, limit)
    }

    pub async fn num_documents(&self) -> u64 {
        self.inner.guard().await.inverted_index().num_documents()
    }

    pub async fn search_initial_generic<Q: GenericQuery + 'static>(
        &self,
        query: Q,
    ) -> Result<<Q::Collector as generic_query::Collector>::Fruit> {
        let inner = self.inner.clone();
        let guard = inner.guard().await;
        tokio::task::spawn_blocking(move || inner.search_initial_generic(&query, &guard))
            .await
            .unwrap()
    }

    pub async fn retrieve_generic<Q: GenericQuery + 'static>(
        &self,
        query: Q,
        fruit: <Q::Collector as generic_query::Collector>::Fruit,
    ) -> Result<Q::IntermediateOutput> {
        let inner = self.inner.clone();
        let guard = inner.guard().await;
        tokio::task::spawn_blocking(move || inner.retrieve_generic(&query, fruit, &guard))
            .await
            .unwrap()
    }

    pub async fn search_generic<Q: GenericQuery + 'static>(&self, query: Q) -> Result<Q::Output> {
        let inner = self.inner.clone();
        let guard = inner.guard().await;
        tokio::task::spawn_blocking(move || inner.search_generic(query, &guard))
            .await
            .unwrap()
    }
}

struct InvertedIndexResult {
    webpages: Vec<LocalRecallRankingWebpage>,
    num_hits: approx_count::Count,
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

        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        for i in 0..NUM_WEBSITES {
            index
                .insert(&Webpage {
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

        let searcher = LocalSearcher::builder(Arc::new(RwLock::new(index))).build();

        for p in 0..NUM_PAGES {
            let urls: Vec<_> = searcher
                .search_sync(&SearchQuery {
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
