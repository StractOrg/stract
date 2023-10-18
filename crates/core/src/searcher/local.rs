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
use std::sync::Arc;

use tantivy::schema::Value;
use tantivy::TantivyDocument;
use url::Url;

use crate::config::{CollectorConfig, SnippetConfig};
use crate::entity_index::{EntityIndex, EntityMatch};
use crate::image_store::Image;
use crate::index::Index;
use crate::inverted_index::RetrievedWebpage;
use crate::query::Query;
use crate::ranking::inbound_similarity::InboundSimilarity;
#[cfg(not(feature = "libtorch"))]
use crate::ranking::models::cross_encoder::DummyCrossEncoder;
use crate::ranking::models::lambdamart::LambdaMART;
use crate::ranking::models::linear::LinearRegression;
use crate::ranking::pipeline::{RankingPipeline, RankingWebsite};
use crate::ranking::{query_centrality, Ranker, Signal, SignalAggregator, ALL_SIGNALS};
use crate::schema::TextField;
use crate::search_ctx::Ctx;
use crate::search_prettifier::{DisplayedEntity, DisplayedWebpage, HighlightedSpellCorrection};
use crate::spell::Spell;
use crate::webgraph::Node;
use crate::webpage::region::Region;
use crate::{inverted_index, Error, Result};

use super::WebsitesResult;
use super::{InitialWebsiteResult, SearchQuery};

pub struct LocalSearcher {
    index: Index,
    spell: Option<Spell>,
    entity_index: Option<EntityIndex>,
    inbound_similarity: Option<InboundSimilarity>,
    linear_regression: Option<Arc<LinearRegression>>,
    lambda_model: Option<Arc<LambdaMART>>,
    collector_config: CollectorConfig,
}

impl From<Index> for LocalSearcher {
    fn from(index: Index) -> Self {
        Self::new(index)
    }
}

struct InvertedIndexResult {
    webpages: Vec<RankingWebsite>,
    num_hits: Option<usize>,
    has_more: bool,
}

impl LocalSearcher {
    pub fn new(index: Index) -> Self {
        let mut index = index;
        index.optimize_for_search().unwrap();

        LocalSearcher {
            index,
            spell: None,
            entity_index: None,
            inbound_similarity: None,
            linear_regression: None,
            lambda_model: None,
            collector_config: CollectorConfig::default(),
        }
    }

    pub fn build_spell_dict(&mut self) {
        self.spell = Some(Spell::for_index(&self.index));
    }

    pub fn set_entity_index(&mut self, entity_index: EntityIndex) {
        self.entity_index = Some(entity_index);
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
        self.index.inverted_index.set_snippet_config(config);
    }

    fn parse_query(&self, ctx: &Ctx, query: &SearchQuery) -> Result<Query> {
        let parsed_query = Query::parse(ctx, query, &self.index.inverted_index)?;

        if parsed_query.is_empty() {
            Err(Error::EmptyQuery.into())
        } else {
            Ok(parsed_query)
        }
    }

    fn ranker(
        &self,
        query: &Query,
        ctx: &Ctx,
        de_rank_similar: bool,
        aggregator: SignalAggregator,
    ) -> Result<Ranker> {
        let query_centrality_coeff = aggregator.coefficient(&Signal::QueryCentrality);

        let mut ranker = Ranker::new(
            aggregator,
            self.index.inverted_index.fastfield_reader(&ctx.tv_searcher),
            self.collector_config.clone(),
        );

        if let Some(region) = query.region() {
            if *region != Region::All {
                ranker = ranker.with_region(*region);
            }
        }

        ranker.de_rank_similar(de_rank_similar);

        if query_centrality_coeff > 0.0 {
            if let Some(inbound_sim) = self.inbound_similarity.as_ref() {
                ranker = ranker
                    .with_max_docs(1_000, self.index.num_segments())
                    .with_num_results(100);

                let top_host_nodes =
                    self.index
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
                self.index.num_segments(),
            )
            .with_num_results(query.num_results())
            .with_offset(query.offset()))
    }

    fn search_inverted_index(
        &self,
        ctx: &Ctx,
        query: &SearchQuery,
        de_rank_similar: bool,
    ) -> Result<InvertedIndexResult> {
        let mut query = query.clone();
        let pipeline: RankingPipeline<RankingWebsite> = RankingPipeline::ltr_for_query(
            &mut query,
            self.lambda_model.clone(),
            self.collector_config.clone(),
        );
        let parsed_query = self.parse_query(ctx, &query)?;

        let mut aggregator = SignalAggregator::new(Some(&parsed_query));

        if let Some(inbound_sim) = &self.inbound_similarity {
            let liked_sites: Vec<_> = parsed_query
                .site_rankings()
                .liked
                .iter()
                .map(|site| Node::from(site.clone()).into_host())
                .map(|node| node.id())
                .collect();

            let disliked_sites: Vec<_> = parsed_query
                .site_rankings()
                .disliked
                .iter()
                .map(|site| Node::from(site.clone()).into_host())
                .map(|node| node.id())
                .collect();

            let scorer = inbound_sim.scorer(&liked_sites, &disliked_sites, false);

            aggregator.set_inbound_similarity(scorer);
        }

        aggregator.set_region_count(self.index.region_count.clone());

        if let Some(model) = self.linear_regression.as_ref() {
            aggregator.set_linear_model(model.clone());
        }

        let ranker = self.ranker(&parsed_query, ctx, de_rank_similar, aggregator)?;

        let res = self.index.inverted_index.search_initial(
            &parsed_query,
            ctx,
            ranker.collector(ctx.clone()),
        )?;

        let fastfield_reader = self.index.inverted_index.fastfield_reader(&ctx.tv_searcher);

        let ranking_websites = self.index.inverted_index.retrieve_ranking_websites(
            ctx,
            res.top_websites,
            ranker.aggregator(),
            &fastfield_reader,
        )?;

        let pipe_top_n = pipeline.top_n;
        let mut ranking_websites = pipeline.apply(ranking_websites);

        let schema = self.index.schema();
        for website in &mut ranking_websites {
            let doc: TantivyDocument = ctx.tv_searcher.doc(website.pointer.address.into())?;
            website.title = Some(
                doc.get_first(schema.get_field(TextField::Title.name()).unwrap())
                    .map(|text| text.as_value().as_str().unwrap().to_string())
                    .unwrap_or_default(),
            );
            website.clean_body = Some(
                doc.get_first(
                    schema
                        .get_field(TextField::StemmedCleanBody.name())
                        .unwrap(),
                )
                .map(|text| text.as_value().as_str().unwrap().to_string())
                .unwrap_or_default(),
            );
        }

        let has_more = pipe_top_n == ranking_websites.len();

        Ok(InvertedIndexResult {
            webpages: ranking_websites,
            num_hits: res.num_websites,
            has_more,
        })
    }

    fn entity_sidebar(&self, query: &SearchQuery) -> Option<EntityMatch> {
        self.entity_index
            .as_ref()
            .and_then(|index| index.search(&query.query))
    }

    pub fn search_initial(
        &self,
        query: &SearchQuery,
        de_rank_similar: bool,
    ) -> Result<InitialWebsiteResult> {
        let ctx = self.index.inverted_index.local_search_ctx();
        let inverted_index_result = self.search_inverted_index(&ctx, query, de_rank_similar)?;
        let correction = self.spell.as_ref().and_then(|s| s.correction(query));
        let sidebar = self.entity_sidebar(query);

        Ok(InitialWebsiteResult {
            spell_corrected_query: correction,
            websites: inverted_index_result.webpages,
            num_websites: inverted_index_result.num_hits,
            has_more: inverted_index_result.has_more,
            entity_sidebar: sidebar,
        })
    }

    pub fn retrieve_websites(
        &self,
        websites: &[inverted_index::WebsitePointer],
        query: &str,
    ) -> Result<Vec<inverted_index::RetrievedWebpage>> {
        let ctx = self.index.inverted_index.local_search_ctx();
        let query = SearchQuery {
            query: query.to_string(),
            ..Default::default()
        };
        let query = Query::parse(&ctx, &query, &self.index.inverted_index)?;

        if query.is_empty() {
            return Err(Error::EmptyQuery.into());
        }

        self.index.retrieve_websites(websites, &query)
    }

    /// This function is mainly used for tests and benchmarks
    pub fn search(&self, query: &SearchQuery) -> Result<WebsitesResult> {
        use std::time::Instant;

        use crate::search_prettifier::DisplayedSidebar;

        let start = Instant::now();
        let mut search_query = query.clone();

        #[cfg(feature = "libtorch")]
        let pipeline = {
            use crate::ranking::models::cross_encoder::CrossEncoderModel;
            match CrossEncoderModel::open("data/cross_encoder") {
                Ok(model) => RankingPipeline::reranking_for_query::<CrossEncoderModel>(
                    &mut search_query,
                    Some(Arc::new(model)),
                    None,
                    self.collector_config.clone(),
                )?,
                Err(_) => RankingPipeline::reranking_for_query::<CrossEncoderModel>(
                    &mut search_query,
                    None,
                    None,
                    self.collector_config.clone(),
                )?,
            }
        };

        #[cfg(not(feature = "libtorch"))]
        let pipeline = {
            use crate::ranking::models::cross_encoder::DummyCrossEncoder;
            RankingPipeline::reranking_for_query::<DummyCrossEncoder>(
                &mut search_query,
                None,
                None,
                self.collector_config.clone(),
            )?
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
            spell_corrected_query: search_result
                .spell_corrected_query
                .map(HighlightedSpellCorrection::from),
            num_hits: search_result.num_websites,
            webpages,
            discussions: None,
            widget: None,
            direct_answer: None,
            sidebar: search_result
                .entity_sidebar
                .map(DisplayedEntity::from)
                .map(DisplayedSidebar::Entity),
            search_duration_ms: start.elapsed().as_millis(),
            has_more_results,
        })
    }

    pub fn get_webpage(&self, url: &str) -> Option<RetrievedWebpage> {
        self.index.get_webpage(url)
    }

    pub fn get_homepage(&self, url: &Url) -> Option<RetrievedWebpage> {
        self.index.get_homepage(url)
    }

    pub fn get_entity_image(&self, image_id: &str) -> Option<Image> {
        self.entity_index
            .as_ref()
            .and_then(|index| index.retrieve_image(image_id))
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
                    backlink_labels: vec![],
                    host_centrality: (NUM_WEBSITES - i) as f64,
                    fetch_time_ms: 500,
                    page_centrality: 0.0,
                    pre_computed_score: 0.0,
                    node_id: None,
                    dmoz_description: None,
                    safety_classification: None,
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

    #[test]
    fn sentence_spell_correction() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
                    r#"
            <html>
                <head>
                    <title>Test website</title>
                </head>
                <body>
    this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever
                </body>
            </html>
            "#
                ,
                "https://www.example.com",
            ).unwrap())
            .expect("failed to insert webpage");

        index.commit().unwrap();

        let mut searcher = LocalSearcher::new(index);
        searcher.build_spell_dict();

        assert_eq!(
            String::from(
                searcher
                    .spell
                    .as_ref()
                    .unwrap()
                    .correction(&SearchQuery {
                        query: "th best".to_string(),
                        ..Default::default()
                    })
                    .unwrap()
            ),
            "the best".to_string()
        );
        assert_eq!(
            searcher.spell.as_ref().unwrap().correction(&SearchQuery {
                query: "the best".to_string(),
                ..Default::default()
            }),
            None
        );
    }
}