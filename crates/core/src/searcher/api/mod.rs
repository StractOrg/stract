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

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use itertools::{intersperse, Itertools};
use url::Url;

use ahash::AHashMap as HashMap;

use crate::bangs::{Bang, BangHit};
use crate::collector::{self, approx_count};
use crate::config::{ApiConfig, ApiSpellCheck, ApiThresholds, CollectorConfig, WidgetsConfig};
use crate::enum_map::EnumMap;
use crate::image_store::Image;
use crate::models::dual_encoder::DualEncoder;
use crate::ranking::models::cross_encoder::CrossEncoderModel;
use crate::ranking::pipeline::{PrecisionRankingWebpage, RankableWebpage, RecallRankingWebpage};
use crate::ranking::{
    bitvec_similarity, inbound_similarity, SignalCalculation, SignalCoefficients, SignalEnum,
    SignalScore,
};
use crate::search_prettifier::{DisplayedSidebar, DisplayedWebpage, HighlightedSpellCorrection};
use crate::web_spell::SpellChecker;
use crate::webgraph::remote::RemoteWebgraph;
use crate::webgraph::EdgeLimit;
use crate::webpage::html::links::RelFlags;
use crate::widgets::{Widget, Widgets};
use crate::{
    bangs::Bangs,
    collector::BucketCollector,
    ranking::{models::lambdamart::LambdaMART, pipeline::RankingPipeline},
};
use crate::{query, webgraph, Result};

use self::sidebar::SidebarManager;
use self::widget::WidgetManager;

use super::{distributed, live, SearchQuery, SearchResult, WebsitesResult};

const NUM_PIPELINE_RANKING_RESULTS: usize = 300;

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
    fn set_raw_score(&mut self, score: f64) {
        self.as_ranking_mut().set_raw_score(score);
    }

    fn unboosted_score(&self) -> f64 {
        self.as_ranking().unboosted_score()
    }

    fn boost(&self) -> f64 {
        self.as_ranking().boost()
    }

    fn set_boost(&mut self, boost: f64) {
        self.as_ranking_mut().set_boost(boost)
    }

    fn as_local_recall(&self) -> &crate::ranking::pipeline::LocalRecallRankingWebpage {
        self.as_ranking().as_local_recall()
    }

    fn signals(&self) -> &EnumMap<SignalEnum, SignalCalculation> {
        match self {
            ScoredWebpagePointer::Normal(p) => p.website.signals(),
            ScoredWebpagePointer::Live(p) => p.website.signals(),
        }
    }

    fn signals_mut(&mut self) -> &mut EnumMap<SignalEnum, SignalCalculation> {
        match self {
            ScoredWebpagePointer::Normal(p) => p.website.signals_mut(),
            ScoredWebpagePointer::Live(p) => p.website.signals_mut(),
        }
    }
}

impl collector::Doc for ScoredWebpagePointer {
    fn score(&self) -> f64 {
        RankableWebpage::score(self)
    }

    fn hashes(&self) -> collector::Hashes {
        self.as_ranking().hashes()
    }
}

pub fn add_ranking_signals(
    websites: &mut [DisplayedWebpage],
    pointers: &[ScoredWebpagePointer],
    coeffs: &SignalCoefficients,
) {
    for (website, pointer) in websites.iter_mut().zip(pointers.iter()) {
        let mut signals = std::collections::HashMap::new();

        for signal in SignalEnum::all() {
            if let Some(signal_value) = pointer.as_ranking().signals().get(signal) {
                signals.insert(
                    signal.into(),
                    SignalScore {
                        value: signal_value.score,
                        coefficient: coeffs.get(&signal),
                    },
                );
            }
        }

        website.ranking_signals = Some(signals);
    }
}

#[derive(Default)]
pub struct Config {
    pub thresholds: ApiThresholds,
    pub widgets: WidgetsConfig,
    pub collector: CollectorConfig,
    pub spell_check: Option<ApiSpellCheck>,
}

impl From<ApiConfig> for Config {
    fn from(conf: ApiConfig) -> Self {
        Self {
            thresholds: conf.thresholds,
            widgets: conf.widgets,
            collector: conf.collector,
            spell_check: conf.spell_check,
        }
    }
}

pub trait Graph {
    fn batch_raw_ingoing_hosts(
        &self,
        nodes: &[webgraph::NodeID],
        limit: EdgeLimit,
    ) -> impl Future<Output = Vec<Vec<webgraph::SmallEdge>>>;
}

impl Graph for RemoteWebgraph {
    async fn batch_raw_ingoing_hosts(
        &self,
        nodes: &[webgraph::NodeID],
        limit: EdgeLimit,
    ) -> Vec<Vec<webgraph::SmallEdge>> {
        self.batch_search(
            nodes
                .iter()
                .map(|n| webgraph::query::HostBacklinksQuery::new(*n).with_limit(limit))
                .collect(),
        )
        .await
        .unwrap_or_default()
    }
}

impl Graph for webgraph::Webgraph {
    async fn batch_raw_ingoing_hosts(
        &self,
        nodes: &[webgraph::NodeID],
        limit: EdgeLimit,
    ) -> Vec<Vec<webgraph::SmallEdge>> {
        nodes
            .iter()
            .map(|n| {
                self.search(&webgraph::query::HostBacklinksQuery::new(*n).with_limit(limit))
                    .unwrap_or_default()
            })
            .collect()
    }
}

impl<T> Graph for Arc<T>
where
    T: Graph,
{
    fn batch_raw_ingoing_hosts(
        &self,
        nodes: &[webgraph::NodeID],
        limit: EdgeLimit,
    ) -> impl Future<Output = Vec<Vec<webgraph::SmallEdge>>> {
        self.as_ref().batch_raw_ingoing_hosts(nodes, limit)
    }
}

impl<T> bitvec_similarity::Graph for T
where
    T: Graph,
{
    async fn batch_ingoing(&self, nodes: &[webgraph::NodeID]) -> Vec<Vec<webgraph::NodeID>> {
        self.batch_raw_ingoing_hosts(nodes, EdgeLimit::Limit(512))
            .await
            .into_iter()
            .map(|edges| {
                edges
                    .into_iter()
                    .filter(|edge| !edge.rel_flags.contains(RelFlags::NOFOLLOW))
                    .map(|edge| edge.from)
                    .collect()
            })
            .collect()
    }
}

pub struct ApiSearcher<S, L, G> {
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
    webgraph: Option<G>,
}

impl<S, L, G> ApiSearcher<S, L, G>
where
    S: distributed::SearchClient,
    L: live::SearchClient,
    G: Graph,
{
    pub fn new<C>(dist_searcher: S, bangs: Bangs, config: C) -> Self
    where
        C: Into<Config>,
    {
        let config: Config = config.into();
        let dist_searcher = Arc::new(dist_searcher);
        let sidebar_manager =
            SidebarManager::new(Arc::clone(&dist_searcher), config.thresholds.clone());

        let widget_manager = WidgetManager::new(Widgets::new(config.widgets).unwrap());

        Self {
            distributed_searcher: dist_searcher,
            sidebar_manager,
            live_searcher: None,
            cross_encoder: None,
            lambda_model: None,
            dual_encoder: None,
            bangs,
            collector_config: config.collector,
            widget_manager,
            spell_checker: config
                .spell_check
                .map(|c| SpellChecker::open(c.path, c.correction_config).unwrap()),
            webgraph: None,
        }
    }

    pub fn with_live(mut self, live_searcher: L) -> Self {
        self.live_searcher = Some(live_searcher);
        self
    }

    pub fn with_cross_encoder(mut self, cross_encoder: CrossEncoderModel) -> Self {
        self.cross_encoder = Some(Arc::new(cross_encoder));
        self
    }

    pub fn with_dual_encoder(mut self, dual_encoder: DualEncoder) -> Self {
        self.dual_encoder = Some(Arc::new(dual_encoder));
        self
    }

    pub fn with_lambda_model(mut self, lambda_model: LambdaMART) -> Self {
        self.lambda_model = Some(Arc::new(lambda_model));
        self
    }

    pub fn with_webgraph(mut self, webgraph: G) -> Self {
        self.webgraph = Some(webgraph);
        self
    }

    async fn check_bangs(&self, query: &SearchQuery) -> Result<Option<BangHit>> {
        let parsed_terms = query::parser::parse(&query.query)?;

        if parsed_terms.iter().any(|term| match term {
            query::parser::Term::PossibleBang { prefix: _, bang } => bang.is_empty(),
            _ => false,
        }) {
            let q: String = intersperse(
                parsed_terms
                    .iter()
                    .filter(|term| !matches!(term, query::parser::Term::PossibleBang { .. }))
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
    ) -> Option<Vec<live::InitialSearchResultShard>> {
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

    async fn inbound_vecs(&self, ids: &[webgraph::NodeID]) -> Vec<bitvec_similarity::BitVec> {
        match self.webgraph.as_ref() {
            Some(webgraph) => bitvec_similarity::BitVec::batch_new_for(ids, webgraph).await,
            None => vec![bitvec_similarity::BitVec::default(); ids.len()],
        }
    }

    async fn combine_results(
        &self,
        query: &SearchQuery,
        initial_results: Vec<distributed::InitialSearchResultShard>,
        live_results: Vec<live::InitialSearchResultShard>,
    ) -> (Vec<ScoredWebpagePointer>, bool) {
        let mut collector =
            BucketCollector::new(NUM_PIPELINE_RANKING_RESULTS, self.collector_config.clone());

        let initial_host_nodes = initial_results
            .iter()
            .flat_map(|r| r.local_result.websites.iter())
            .map(|r| *r.host_id())
            .collect::<Vec<_>>();

        let live_host_nodes = live_results
            .iter()
            .flat_map(|r| r.local_result.websites.iter())
            .map(|r| *r.host_id())
            .collect::<Vec<_>>();

        let host_nodes = initial_host_nodes
            .into_iter()
            .chain(live_host_nodes)
            .unique()
            .collect::<Vec<_>>();

        let inbound_vecs = if !query.fetch_backlinks() {
            HashMap::default()
        } else {
            self.inbound_vecs(&host_nodes)
                .await
                .into_iter()
                .zip_eq(host_nodes)
                .map(|(v, n)| (n, v))
                .collect::<HashMap<_, _>>()
        };

        let mut num_results = 0;

        for result in initial_results {
            num_results += result.local_result.websites.len();
            for website in result.local_result.websites {
                let inbound = inbound_vecs
                    .get(website.host_id())
                    .cloned()
                    .unwrap_or_default();
                let pointer = distributed::ScoredWebpagePointer {
                    website: RecallRankingWebpage::new(website, inbound),
                    shard: result.shard,
                };

                let pointer = ScoredWebpagePointer::Normal(pointer);

                collector.insert(pointer);
            }
        }

        for result in live_results {
            num_results += result.local_result.websites.len();
            for website in result.local_result.websites {
                let inbound = inbound_vecs
                    .get(website.host_id())
                    .cloned()
                    .unwrap_or_default();
                let pointer = live::ScoredWebpagePointer {
                    website: RecallRankingWebpage::new(website, inbound),
                    shard_id: result.shard_id,
                };

                let pointer = ScoredWebpagePointer::Live(pointer);

                collector.insert(pointer);
            }
        }

        let has_more = query.offset() + query.num_results() < num_results;

        let res = collector
            .into_sorted_vec(true)
            .into_iter()
            .take(NUM_PIPELINE_RANKING_RESULTS)
            .collect::<Vec<_>>();

        (res, has_more)
    }

    async fn inbound_scorer(&self, query: &SearchQuery) -> inbound_similarity::Scorer {
        if !query.fetch_backlinks() {
            return inbound_similarity::Scorer::empty();
        }

        match self.webgraph.as_ref() {
            Some(webgraph) => {
                let host_rankings = query.host_rankings();

                let liked: Vec<_> = host_rankings
                    .liked
                    .iter()
                    .filter_map(|n| {
                        Url::parse(n)
                            .or_else(|_| Url::parse(&format!("http://{}", n)))
                            .ok()
                    })
                    .map(|n| webgraph::Node::from(n).into_host().id())
                    .collect();

                let disliked: Vec<_> = host_rankings
                    .disliked
                    .iter()
                    .filter_map(|n| {
                        Url::parse(n)
                            .or_else(|_| Url::parse(&format!("http://{}", n)))
                            .ok()
                    })
                    .map(|n| webgraph::Node::from(n).into_host().id())
                    .collect();
                inbound_similarity::Scorer::new(webgraph, &liked, &disliked, false).await
            }
            None => inbound_similarity::Scorer::empty(),
        }
    }

    async fn search_websites_approx_offsets(&self, query: &SearchQuery) -> Result<WebsitesResult> {
        let start = Instant::now();

        let search_query = SearchQuery {
            num_results: query.num_results + 1,
            ..query.clone()
        };

        let results = self
            .distributed_searcher
            .search_initial(&search_query)
            .await;

        let has_more_results = results
            .iter()
            .any(|res| res.local_result.websites.len() > query.num_results());

        let num_docs = results
            .iter()
            .map(|result| result.local_result.num_websites)
            .fold(approx_count::Count::Exact(0), |acc, count| acc + count);

        let (combined, _) = self.combine_results(query, results, vec![]).await;
        let combined: Vec<_> = combined.into_iter().take(query.num_results).collect();

        let mut retrieved_webpages: Vec<_> = self
            .retrieve_webpages(&query.query, &combined)
            .await
            .into_iter()
            .map(|webpage| webpage.into_retrieved_webpage())
            .map(|webpage| DisplayedWebpage::new(webpage, query))
            .collect();

        if query.return_ranking_signals {
            add_ranking_signals(
                &mut retrieved_webpages,
                &combined,
                &query.signal_coefficients(),
            );
        }

        let search_duration_ms = start.elapsed().as_millis();

        Ok(WebsitesResult {
            num_hits: num_docs,
            webpages: retrieved_webpages,
            search_duration_ms,
            has_more_results,
        })
    }

    async fn search_websites(&self, query: &SearchQuery) -> Result<WebsitesResult> {
        let start = Instant::now();

        if query.is_empty() {
            return Err(distributed::Error::EmptyQuery.into());
        }

        if query.offset() + query.num_results() > NUM_PIPELINE_RANKING_RESULTS {
            // this is most likely a bot
            // let's not spend too much time correctly offsetting+ranking results
            return self.search_websites_approx_offsets(query).await;
        }

        let search_query = SearchQuery {
            num_results: NUM_PIPELINE_RANKING_RESULTS,
            page: 0,
            ..query.clone()
        };

        let (initial_results, live_results) = tokio::join!(
            self.distributed_searcher.search_initial(&search_query),
            self.search_initial_from_live(&search_query),
        );

        let num_docs = initial_results
            .iter()
            .map(|result| result.local_result.num_websites)
            .chain(live_results.iter().flat_map(|results| {
                results
                    .iter()
                    .map(|result| result.local_result.num_websites)
            }))
            .fold(approx_count::Count::Exact(0), |acc, count| acc + count);

        let (top_websites, has_more_results) = self
            .combine_results(query, initial_results, live_results.unwrap_or_default())
            .await;

        let inbound_scorer = self.inbound_scorer(query).await;

        let pipeline: RankingPipeline<ScoredWebpagePointer> =
            RankingPipeline::<ScoredWebpagePointer>::recall_stage(
                query,
                inbound_scorer,
                self.lambda_model.clone(),
                self.dual_encoder.clone(),
            );

        let top_websites = pipeline.apply(top_websites, query);

        let mut retrieved_webpages = self.retrieve_webpages(&query.query, &top_websites).await;

        if let Some(cross_encoder) = self.cross_encoder.clone() {
            if query.page < 2 {
                let query = SearchQuery {
                    page: 1,
                    ..query.clone()
                };

                let reranking_pipeline: RankingPipeline<PrecisionRankingWebpage> =
                    RankingPipeline::<PrecisionRankingWebpage>::reranker(
                        &query,
                        cross_encoder,
                        self.lambda_model.clone(),
                    );

                retrieved_webpages = reranking_pipeline.apply(retrieved_webpages, &query);
            }
        }

        let mut retrieved_webpages: Vec<_> = retrieved_webpages
            .into_iter()
            .map(|webpage| webpage.into_retrieved_webpage())
            .map(|webpage| DisplayedWebpage::new(webpage, query))
            .collect();

        if retrieved_webpages.len() != top_websites.len() {
            return Err(distributed::Error::SearchFailed.into());
        }

        if query.return_ranking_signals {
            add_ranking_signals(
                &mut retrieved_webpages,
                &top_websites,
                &query.signal_coefficients(),
            );
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

    pub async fn warmup(&self, queries: impl Iterator<Item = String>) {
        for query in queries {
            self.search_websites(&SearchQuery {
                query,
                ..Default::default()
            })
            .await
            .ok();
        }
    }
}
