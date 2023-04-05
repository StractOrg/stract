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
use std::str::FromStr;
use std::sync::Arc;

use uuid::Uuid;

use crate::entity_index::{EntityIndex, StoredEntity};
use crate::image_store::Image;
use crate::index::Index;
use crate::inverted_index::RetrievedWebpage;
use crate::query::Query;
use crate::ranking::centrality_store::SearchCentralityStore;
use crate::ranking::models::lambdamart::LambdaMART;
use crate::ranking::models::linear::LinearRegression;
use crate::ranking::pipeline::{RankingPipeline, RankingWebsite};
use crate::ranking::{online_centrality_scorer, Ranker, SignalAggregator, ALL_SIGNALS};
use crate::schema::TextField;
use crate::search_ctx::Ctx;
use crate::search_prettifier::{DisplayedEntity, DisplayedWebpage, HighlightedSpellCorrection};
use crate::spell::Spell;
use crate::webgraph::centrality::topic::TopicCentrality;
use crate::webgraph::Node;
use crate::webpage::region::Region;
use crate::webpage::Url;
use crate::{inverted_index, Error, Result};

use super::WebsitesResult;
use super::{InitialWebsiteResult, SearchQuery};

pub struct LocalSearcher {
    index: Index,
    spell: Spell,
    entity_index: Option<EntityIndex>,
    centrality_store: Option<SearchCentralityStore>,
    topic_centrality: Option<TopicCentrality>,
    linear_regression: Option<Arc<LinearRegression>>,
    lambda_model: Option<Arc<LambdaMART>>,
}

impl From<Index> for LocalSearcher {
    fn from(index: Index) -> Self {
        Self::new(index)
    }
}

struct InvertedIndexResult {
    webpages: Vec<RankingWebsite>,
    num_hits: usize,
    has_more: bool,
}

impl LocalSearcher {
    pub fn new(index: Index) -> Self {
        let spell = Spell::for_index(&index);
        LocalSearcher {
            index,
            spell,
            entity_index: None,
            centrality_store: None,
            topic_centrality: None,
            linear_regression: None,
            lambda_model: None,
        }
    }

    pub fn set_entity_index(&mut self, entity_index: EntityIndex) {
        self.entity_index = Some(entity_index);
    }

    pub fn set_centrality_store(&mut self, centrality_store: SearchCentralityStore) {
        self.centrality_store = Some(centrality_store);
    }

    pub fn set_topic_centrality(&mut self, topic_centrality: TopicCentrality) {
        self.topic_centrality = Some(topic_centrality);
    }

    pub fn set_linear_model(&mut self, model: LinearRegression) {
        self.linear_regression = Some(Arc::new(model));
    }

    pub fn set_lambda_model(&mut self, model: LambdaMART) {
        self.lambda_model = Some(Arc::new(model));
    }

    fn parse_query(&self, ctx: &Ctx, query: &SearchQuery) -> Result<Query> {
        let parsed_query = Query::parse(ctx, query, &self.index.inverted_index)?;

        if parsed_query.is_empty() {
            Err(Error::EmptyQuery)
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
        let mut ranker = Ranker::new(
            aggregator,
            self.index.inverted_index.fastfield_reader(&ctx.tv_searcher),
        );

        if let Some(region) = query.region() {
            if *region != Region::All {
                ranker = ranker.with_region(*region);
            }
        }

        if let Some(topic_centrality) = self.topic_centrality.as_ref() {
            let topic_scorer = topic_centrality.scorer(query);
            ranker.set_topic_scorer(topic_scorer);
        }

        ranker.de_rank_similar(de_rank_similar);

        if let Some(centrality_store) = self.centrality_store.as_ref() {
            ranker = ranker
                .with_max_docs(10_000, self.index.num_segments())
                .with_num_results(100);

            let top_host_nodes = self
                .index
                .top_nodes(query, ctx, ranker.collector(ctx.clone()))?;
            if !top_host_nodes.is_empty() {
                let harmonic = centrality_store
                    .online_harmonic
                    .scorer(&top_host_nodes, &[]);
                ranker.set_query_centrality(harmonic);
            }
        }

        Ok(ranker
            .with_max_docs(10_000_000, self.index.num_segments())
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
        let pipeline: RankingPipeline<RankingWebsite> =
            RankingPipeline::ltr_for_query(&mut query, self.lambda_model.clone());
        let parsed_query = self.parse_query(ctx, &query)?;

        let mut aggregator = SignalAggregator::new(Some(parsed_query.clone()));

        if let Some(store) = &self.centrality_store {
            aggregator.set_personal_harmonic(online_centrality_scorer(
                parsed_query.site_rankings(),
                &store.online_harmonic,
                &store.node2id,
            ));

            let liked_sites: Vec<_> = parsed_query
                .site_rankings()
                .liked
                .iter()
                .map(|site| Node::from(site.clone()).into_host())
                .filter_map(|node| store.node2id.get(&node).copied())
                .collect();

            let disliked_sites: Vec<_> = parsed_query
                .site_rankings()
                .disliked
                .iter()
                .map(|site| Node::from(site.clone()).into_host())
                .filter_map(|node| store.node2id.get(&node).copied())
                .collect();

            aggregator.set_inbound_similarity(
                store
                    .inbound_similarity
                    .scorer(&liked_sites, &disliked_sites),
            )
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
            let doc = ctx.tv_searcher.doc(website.pointer.address.into())?;
            website.title = Some(
                doc.get_first(schema.get_field(TextField::Title.name()).unwrap())
                    .map(|text| text.as_text().unwrap().to_string())
                    .unwrap_or_default(),
            );
            website.clean_body = Some(
                doc.get_first(
                    schema
                        .get_field(TextField::StemmedCleanBody.name())
                        .unwrap(),
                )
                .map(|text| text.as_text().unwrap().to_string())
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

    fn entity_sidebar(&self, query: &SearchQuery) -> Option<StoredEntity> {
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
        let correction = self.spell.correction(query);
        let sidebar = self
            .entity_sidebar(query)
            .map(|entity| DisplayedEntity::from(entity, self));

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
            return Err(Error::EmptyQuery);
        }

        self.index.retrieve_websites(websites, &query)
    }

    /// This function is mainly used for tests and benchmarks
    pub fn search(&self, query: &SearchQuery) -> Result<WebsitesResult> {
        use std::time::Instant;

        use crate::{
            ranking::models::cross_encoder::{CrossEncoderModel, DummyCrossEncoder},
            search_prettifier::Sidebar,
        };

        let start = Instant::now();
        let mut search_query = query.clone();

        let pipeline = match CrossEncoderModel::open("data/cross_encoder") {
            Ok(model) => {
                RankingPipeline::reranking_for_query(&mut search_query, Arc::new(model), None)?
            }
            Err(_) => RankingPipeline::reranking_for_query(
                &mut search_query,
                Arc::new(DummyCrossEncoder {}),
                None,
            )?,
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

        for (webpage, ranking) in webpages.iter_mut().zip(top_websites.into_iter()) {
            let mut ranking_signals = HashMap::new();

            for signal in ALL_SIGNALS {
                ranking_signals.insert(signal, *ranking.signals.get(signal).unwrap_or(&0.0));
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
            sidebar: search_result.entity_sidebar.map(Sidebar::Entity),
            search_duration_ms: start.elapsed().as_millis(),
            has_more_results,
        })
    }

    pub fn favicon(&self, site: &Url) -> Option<Image> {
        self.index.retrieve_favicon(site)
    }

    pub fn primary_image(&self, uuid: String) -> Option<Image> {
        if let Ok(uuid) = Uuid::from_str(uuid.as_str()) {
            return self.index.retrieve_primary_image(&uuid);
        }
        None
    }

    pub fn entity_image(&self, entity: String) -> Option<Image> {
        self.entity_index
            .as_ref()
            .and_then(|index| index.retrieve_image(&entity))
    }

    pub fn attribute_occurrence(&self, attribute: &String) -> Option<u32> {
        self.entity_index
            .as_ref()
            .and_then(|index| index.get_attribute_occurrence(attribute))
    }

    pub fn get_webpage(&self, url: &str) -> Option<RetrievedWebpage> {
        self.index.get_webpage(url)
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
                    ),
                    backlinks: vec![],
                    host_centrality: (NUM_WEBSITES - i) as f64,
                    fetch_time_ms: 500,
                    page_centrality: 0.0,
                    pre_computed_score: 0.0,
                    primary_image: None,
                    node_id: None,
                    host_topic: None,
                    crawl_stability: 0.0,
                    dmoz_description: None,
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
                    format!("https://www.{}.com", i + (p * NUM_RESULTS_PER_PAGE))
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
            ))
            .expect("failed to insert webpage");

        index.commit().unwrap();

        let searcher = LocalSearcher::new(index);

        assert_eq!(
            String::from(
                searcher
                    .spell
                    .correction(&SearchQuery {
                        query: "th best".to_string(),
                        ..Default::default()
                    })
                    .unwrap()
            ),
            "the best".to_string()
        );
        assert_eq!(
            searcher.spell.correction(&SearchQuery {
                query: "the best".to_string(),
                ..Default::default()
            }),
            None
        );
    }
}
