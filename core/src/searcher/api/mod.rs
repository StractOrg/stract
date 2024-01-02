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

use itertools::intersperse;
use url::Url;

use crate::bangs::{Bang, BangHit};
use crate::config::{ApiConfig, CollectorConfig};
use crate::image_store::Image;
use crate::inverted_index::RetrievedWebpage;
#[cfg(not(feature = "libtorch"))]
use crate::ranking::models::cross_encoder::DummyCrossEncoder;
use crate::ranking::pipeline::{AsRankingWebsite, RankingWebsite, RetrievedWebpageRanking};
use crate::ranking::ALL_SIGNALS;
use crate::search_prettifier::{DisplayedWebpage, HighlightedSpellCorrection};
use crate::web_spell::SpellChecker;
use crate::widgets::Widgets;
use crate::{
    bangs::Bangs,
    collector::BucketCollector,
    distributed::cluster::Cluster,
    ranking::{models::lambdamart::LambdaMART, pipeline::RankingPipeline},
};
#[cfg(feature = "libtorch")]
use crate::{qa_model::QaModel, ranking::models::cross_encoder::CrossEncoderModel};
use crate::{query, Result};

use self::sidebar::SidebarManager;
use self::widget::WidgetManager;

use super::live::LiveSearcher;
use super::{distributed, live, DistributedSearcher, SearchQuery, SearchResult, WebsitesResult};

#[derive(Clone)]
pub enum ScoredWebsitePointer {
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

pub fn combine_results(
    collector_config: CollectorConfig,
    initial_results: Vec<distributed::InitialSearchResultShard>,
    live_results: Vec<live::InitialSearchResultSplit>,
    pipeline: RankingPipeline<ScoredWebsitePointer>,
) -> (Vec<ScoredWebsitePointer>, bool) {
    let mut collector = BucketCollector::new(pipeline.collector_top_n(), collector_config);

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
pub fn add_ranking_signals(websites: &mut [DisplayedWebpage], pointers: &[ScoredWebsitePointer]) {
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

#[cfg(feature = "libtorch")]
pub struct ApiSearcher {
    distributed_searcher: Arc<DistributedSearcher>,
    sidebar_manager: SidebarManager,
    live_searcher: LiveSearcher,
    cross_encoder: Option<Arc<CrossEncoderModel>>,
    lambda_model: Option<Arc<LambdaMART>>,
    bangs: Bangs,
    collector_config: CollectorConfig,
    widget_manager: WidgetManager,
    spell_checker: Option<SpellChecker>,
}

#[cfg(not(feature = "libtorch"))]
pub struct ApiSearcher {
    distributed_searcher: Arc<DistributedSearcher>,
    live_searcher: LiveSearcher,
    sidebar_manager: SidebarManager,
    lambda_model: Option<Arc<LambdaMART>>,
    bangs: Bangs,
    collector_config: CollectorConfig,
    widget_manager: WidgetManager,
    spell_checker: Option<SpellChecker>,
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
        let dist_searcher = Arc::new(DistributedSearcher::new(Arc::clone(&cluster)));
        let sidebar_manager =
            SidebarManager::new(Arc::clone(&dist_searcher), config.thresholds.clone());

        let lambda_model = lambda_model.map(Arc::new);
        let qa_model = qa_model.map(Arc::new);

        let widget_manager = WidgetManager::new(
            Widgets::new(config.widgets).unwrap(),
            Arc::clone(&dist_searcher),
            config.thresholds.clone(),
            config.collector.clone(),
            lambda_model.clone(),
            qa_model,
        );

        Self {
            distributed_searcher: dist_searcher,
            sidebar_manager,
            live_searcher: LiveSearcher::new(Arc::clone(&cluster)),
            cross_encoder: cross_encoder.map(Arc::new),
            lambda_model,
            bangs,
            collector_config: config.collector,
            widget_manager,
            spell_checker: config
                .spell_checker_path
                .map(|c| SpellChecker::open(c, config.correction_config).unwrap()),
        }
    }

    #[cfg(not(feature = "libtorch"))]
    pub fn new(
        cluster: Arc<Cluster>,
        lambda_model: Option<LambdaMART>,
        bangs: Bangs,
        config: ApiConfig,
    ) -> Self {
        let dist_searcher = Arc::new(DistributedSearcher::new(Arc::clone(&cluster)));
        let sidebar_manager =
            SidebarManager::new(Arc::clone(&dist_searcher), config.thresholds.clone());
        let lambda_model = lambda_model.map(Arc::new);

        let widget_manager = WidgetManager::new(
            Widgets::new(config.widgets).unwrap(),
            Arc::clone(&dist_searcher),
            config.thresholds.clone(),
            config.collector.clone(),
            lambda_model.clone(),
        );

        Self {
            distributed_searcher: dist_searcher,
            sidebar_manager,
            live_searcher: LiveSearcher::new(Arc::clone(&cluster)),
            lambda_model,
            bangs,
            collector_config: config.collector,
            widget_manager,
            spell_checker: config
                .spell_checker_path
                .map(|c| SpellChecker::open(c, config.correction_config).unwrap()),
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

    async fn retrieve_webpages(
        &self,
        query: &str,
        top_websites: &[ScoredWebsitePointer],
    ) -> Vec<RetrievedWebpageRanking> {
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
            self.distributed_searcher.retrieve_webpages(&normal, query),
            self.live_searcher.retrieve_webpages(&live, query),
        );

        let mut retrieved_webpages: Vec<_> =
            retrieved_normal.into_iter().chain(retrieved_live).collect();
        retrieved_webpages.sort_by(|(a, _), (b, _)| a.cmp(b));

        retrieved_webpages
            .into_iter()
            .map(|(_, webpage)| webpage)
            .collect::<Vec<_>>()
    }

    async fn search_websites(&self, query: &SearchQuery) -> Result<WebsitesResult> {
        let start = Instant::now();

        if query.is_empty() {
            return Err(distributed::Error::EmptyQuery.into());
        }

        let mut search_query = query.clone();

        // This pipeline should be created before the first search is performed
        // so the query knows how many results to fetch from the indices
        let recall_pipeline: RankingPipeline<ScoredWebsitePointer> = RankingPipeline::recall_stage(
            &mut search_query,
            self.lambda_model.clone(),
            self.collector_config.clone(),
            20,
        );

        let (initial_results, live_results, widget, discussions, stackoverflow) = tokio::join!(
            self.distributed_searcher.search_initial(&search_query),
            self.live_searcher.search_initial(&search_query),
            self.widget_manager.widget(query),
            self.widget_manager.discussions(query),
            self.sidebar_manager.stackoverflow(query),
        );

        let discussions = discussions?;
        let stackoverflow = stackoverflow?;
        let sidebar = self
            .sidebar_manager
            .sidebar(&initial_results, stackoverflow);

        let spell_corrected_query = if widget.is_none() {
            self.spell_checker
                .as_ref()
                .and_then(|s| s.correct(&query.query, &whatlang::Lang::Eng))
                .map(HighlightedSpellCorrection::from)
        } else {
            None
        };

        let num_docs = initial_results
            .iter()
            .map(|result| result.local_result.num_websites)
            .sum();

        let (top_websites, has_more_results) = combine_results(
            self.collector_config.clone(),
            initial_results,
            live_results,
            recall_pipeline,
        );

        let retrieved_webpages = self
            .retrieve_webpages(&search_query.query, &top_websites)
            .await;

        let mut search_query = SearchQuery {
            page: 0,
            ..query.clone()
        };

        #[cfg(feature = "libtorch")]
        let reranking_pipeline: RankingPipeline<RetrievedWebpageRanking> =
            RankingPipeline::reranker(
                &mut search_query,
                self.cross_encoder.clone(),
                self.lambda_model.clone(),
                self.collector_config.clone(),
                query.num_results,
            )?;

        #[cfg(not(feature = "libtorch"))]
        let reranking_pipeline: RankingPipeline<RetrievedWebpageRanking> =
            RankingPipeline::reranker::<DummyCrossEncoder>(
                &mut search_query,
                None,
                self.lambda_model.clone(),
                self.collector_config.clone(),
                query.num_results,
            )?;

        let retrieved_webpages = reranking_pipeline.apply(retrieved_webpages);

        let mut retrieved_webpages: Vec<_> = retrieved_webpages
            .into_iter()
            .map(|webpage| webpage.into_retrieved_webpage())
            .map(DisplayedWebpage::from)
            .collect();

        if retrieved_webpages.len() != top_websites.len() {
            return Err(distributed::Error::SearchFailed.into());
        }

        if query.return_ranking_signals {
            add_ranking_signals(&mut retrieved_webpages, &top_websites);
        }

        let direct_answer = self
            .widget_manager
            .answer(&query.query, &mut retrieved_webpages);

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
