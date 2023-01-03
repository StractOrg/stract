// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use std::str::FromStr;

use optics::Optic;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::bangs::{BangHit, Bangs};
use crate::entity_index::EntityIndex;
use crate::image_store::Image;
use crate::index::Index;
use crate::query::Query;
use crate::ranking::centrality_store::CentralityStore;
use crate::ranking::optics::CreateAggregator;
use crate::ranking::pipeline::RankingWebsite;
use crate::ranking::{online_centrality_scorer, Ranker, SignalAggregator};
use crate::spell::Correction;
use crate::webgraph::centrality::topic::TopicCentrality;
use crate::webpage::region::Region;
use crate::webpage::Url;
use crate::{inverted_index, Error, Result};

use super::{InitialSearchResult, SearchQuery, SearchResult, Sidebar};

#[cfg(test)]
use super::WebsitesResult;

const STACKOVERFLOW_SIDEBAR_SCORE_THRESHOLD: f64 = 250.0;

pub struct LocalSearcher {
    index: Index,
    entity_index: Option<EntityIndex>,
    bangs: Option<Bangs>,
    centrality_store: Option<CentralityStore>,
    topic_centrality: Option<TopicCentrality>,
    stackoverflow_sidebar_threshold: f64,
}

impl From<Index> for LocalSearcher {
    fn from(index: Index) -> Self {
        Self::new(index)
    }
}

impl LocalSearcher {
    pub fn new(index: Index) -> Self {
        LocalSearcher {
            index,
            entity_index: None,
            bangs: None,
            centrality_store: None,
            topic_centrality: None,
            stackoverflow_sidebar_threshold: STACKOVERFLOW_SIDEBAR_SCORE_THRESHOLD,
        }
    }

    pub fn set_entity_index(&mut self, entity_index: EntityIndex) {
        self.entity_index = Some(entity_index);
    }

    pub fn set_bangs(&mut self, bangs: Bangs) {
        self.bangs = Some(bangs);
    }

    pub fn set_centrality_store(&mut self, centrality_store: CentralityStore) {
        self.centrality_store = Some(centrality_store);
    }

    pub fn set_topic_centrality(&mut self, topic_centrality: TopicCentrality) {
        self.topic_centrality = Some(topic_centrality);
    }

    fn parse_query(&self, query: &SearchQuery, optic: Option<&Optic>) -> Result<Query> {
        let mut query_aggregator = optic
            .and_then(|optic| {
                optic
                    .pipeline
                    .as_ref()
                    .and_then(|pipeline| pipeline.stages.first().map(|stage| stage.aggregator()))
            })
            .unwrap_or_default();

        if let (Some(optic), Some(harmonic)) = (
            optic,
            self.centrality_store
                .as_ref()
                .map(|store| &store.online_harmonic),
        ) {
            query_aggregator
                .add_personal_harmonic(online_centrality_scorer(&optic.site_rankings, harmonic));
        }

        let mut parsed_query = Query::parse(
            &query.query,
            self.index.schema(),
            self.index.tokenizers(),
            &query_aggregator,
        )?;

        parsed_query.set_num_results(query.num_results);

        if parsed_query.is_empty() {
            Err(Error::EmptyQuery)
        } else {
            Ok(parsed_query)
        }
    }

    fn search_inverted_index(
        &self,
        query: &SearchQuery,
        de_rank_similar: bool,
    ) -> Result<(Vec<RankingWebsite>, usize)> {
        let optic = match query.optic_program.as_ref() {
            Some(program) => Some(optics::parse(program)?),
            None => None,
        };

        let mut parsed_query = self.parse_query(query, optic.as_ref())?;

        let mut optics = Vec::new();

        if let Some(optic) = &optic {
            optics.push(optic.clone());
        }

        if let Some(site_rankings) = &query.site_rankings {
            optics.push(site_rankings.clone().into_optic())
        }

        let mut optic = Optic::default();

        for o in optics {
            optic = optic.try_merge(o)?;
        }

        parsed_query.set_optic(&optic, &self.index);

        let mut aggregator = optic
            .pipeline
            .and_then(|pipeline| pipeline.stages.first().map(|stage| stage.aggregator()))
            .unwrap_or_default();

        if let Some(harmonic) = self
            .centrality_store
            .as_ref()
            .map(|store| &store.online_harmonic)
        {
            aggregator
                .add_personal_harmonic(online_centrality_scorer(&optic.site_rankings, harmonic));
        }

        let mut ranker = Ranker::new(
            self.index.region_count.clone(),
            aggregator,
            self.index.inverted_index.fastfield_cache(),
        );

        if let Some(region) = query.selected_region {
            if region != Region::All {
                ranker = ranker.with_region(region);
            }
        }

        if let Some(topic_centrality) = self.topic_centrality.as_ref() {
            let topic_scorer = topic_centrality.scorer(&parsed_query);
            ranker.set_topic_scorer(topic_scorer);
        }

        ranker.de_rank_similar(de_rank_similar);

        if let Some(centrality_store) = self.centrality_store.as_ref() {
            ranker = ranker
                .with_max_docs(10_000, self.index.num_segments())
                .with_num_results(100);

            let top_host_nodes = self.index.top_nodes(&parsed_query, ranker.collector())?;
            if !top_host_nodes.is_empty() {
                let harmonic = centrality_store
                    .online_harmonic
                    .scorer_from_ids(&top_host_nodes, &[]);
                ranker.set_query_centrality(harmonic);
            }
        }

        ranker = ranker
            .with_max_docs(10_000_000, self.index.num_segments())
            .with_num_results(parsed_query.num_results())
            .with_offset(query.offset);

        let res = self
            .index
            .search_initial(&parsed_query, ranker.collector())?;

        let ranking_websites = self
            .index
            .inverted_index
            .retrieve_ranking_websites(res.top_websites, &ranker.aggregator())?;

        Ok((ranking_websites, res.num_websites))
    }

    fn spell_correction(&self, query: &SearchQuery) -> Result<Option<Correction>> {
        let parsed_query = self.parse_query(query, None)?;
        Ok(self.index.spell_correction(&parsed_query.simple_terms()))
    }

    fn check_bangs(&self, query: &SearchQuery) -> Result<Option<BangHit>> {
        let parsed_query = self.parse_query(query, None)?;

        if let Some(bangs) = self.bangs.as_ref() {
            return Ok(bangs.get(&parsed_query));
        }

        Ok(None)
    }

    fn entity_sidebar(&self, query: &SearchQuery) -> Option<Sidebar> {
        self.entity_index
            .as_ref()
            .and_then(|index| index.search(&query.query))
            .map(Sidebar::Entity)
    }

    fn stackoverflow_sidebar(&self, query: &SearchQuery) -> Result<Option<Sidebar>> {
        let mut query = query.clone();
        query.optic_program = Some(include_str!("stackoverflow.optic").to_string());
        query.num_results = 1;

        let (mut top_websites, num_websites) = self.search_inverted_index(&query, false)?;

        if num_websites > 0 && !top_websites.is_empty() {
            let top = top_websites.remove(0);
            if top.score > self.stackoverflow_sidebar_threshold {
                let mut retrieved = self.retrieve_websites(&[top.pointer], &query.query)?;
                let retrieved = retrieved.remove(0);
                return Ok(Some(Sidebar::StackOverflow {
                    schema_org: retrieved.schema_org,
                    url: retrieved.url,
                }));
            }
        }

        Ok(None)
    }

    fn sidebar(&self, query: &SearchQuery) -> Result<Option<Sidebar>> {
        if let Some(entity) = self.entity_sidebar(query) {
            return Ok(Some(entity));
        }

        if let Some(stackoverflow) = self.stackoverflow_sidebar(query)? {
            return Ok(Some(stackoverflow));
        }

        Ok(None)
    }

    pub fn search_initial(
        &self,
        query: &SearchQuery,
        de_rank_similar: bool,
    ) -> Result<InitialSearchResult> {
        if let Some(bang) = self.check_bangs(query)? {
            return Ok(InitialSearchResult::Bang(bang));
        }

        let (websites, num_websites) = self.search_inverted_index(query, de_rank_similar)?;
        let correction = self.spell_correction(query)?;
        let sidebar = self.sidebar(query)?;

        Ok(InitialSearchResult::Websites(InitialWebsiteResult {
            spell_corrected_query: correction,
            websites,
            num_websites,
            sidebar,
        }))
    }

    pub fn retrieve_websites(
        &self,
        websites: &[inverted_index::WebsitePointer],
        query: &str,
    ) -> Result<Vec<inverted_index::RetrievedWebpage>> {
        let query = Query::parse(
            query,
            self.index.schema(),
            self.index.tokenizers(),
            &SignalAggregator::default(),
        )?;

        if query.is_empty() {
            return Err(Error::EmptyQuery);
        }

        self.index.retrieve_websites(websites, &query)
    }

    #[cfg(test)]
    pub fn search(&self, query: &SearchQuery) -> Result<SearchResult> {
        use std::{sync::Arc, time::Instant};

        use crate::ranking::{
            models::cross_encoder::{CrossEncoderModel, DummyCrossEncoder},
            pipeline::RankingPipeline,
        };

        let start = Instant::now();
        let mut search_query = query.clone();

        let pipeline = match CrossEncoderModel::open("data/cross_encoder") {
            Ok(model) => RankingPipeline::for_query(&mut search_query, Arc::new(model))?,
            Err(_) => {
                RankingPipeline::for_query(&mut search_query, Arc::new(DummyCrossEncoder {}))?
            }
        };

        let initial_result = self.search_initial(&search_query, true)?;

        match initial_result {
            InitialSearchResult::Websites(search_result) => {
                let top_websites = pipeline.apply(search_result.websites);
                let pointers: Vec<_> = top_websites
                    .into_iter()
                    .map(|website| website.pointer)
                    .collect();

                let retrieved_sites = self.retrieve_websites(&pointers, &search_query.query)?;
                Ok(SearchResult::Websites(WebsitesResult {
                    spell_corrected_query: search_result.spell_corrected_query,
                    webpages: inverted_index::SearchResult {
                        num_docs: search_result.num_websites,
                        documents: retrieved_sites,
                    },
                    sidebar: search_result.sidebar,
                    search_duration_ms: start.elapsed().as_millis(),
                }))
            }
            InitialSearchResult::Bang(bang) => Ok(SearchResult::Bang(bang)),
        }
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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InitialWebsiteResult {
    pub spell_corrected_query: Option<Correction>,
    pub num_websites: usize,
    pub websites: Vec<RankingWebsite>,
    pub sidebar: Option<Sidebar>,
}

impl SearchResult {
    #[cfg(test)]
    pub fn into_websites(self) -> Option<WebsitesResult> {
        if let SearchResult::Websites(res) = self {
            Some(res)
        } else {
            None
        }
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
                    offset: p * NUM_RESULTS_PER_PAGE,
                    ..Default::default()
                })
                .unwrap()
                .into_websites()
                .unwrap()
                .webpages
                .documents
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
    fn stackoverflow_sidebar() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
                include_str!("../../testcases/schema_org/stackoverflow.html"),
                "https://www.stackoverflow.com",
            ))
            .expect("failed to insert webpage");

        index.commit().unwrap();

        let mut searcher = LocalSearcher::new(index);
        searcher.stackoverflow_sidebar_threshold = 0.0;

        let res = searcher
            .search(&SearchQuery {
                query: "regex parse html".to_string(),
                ..Default::default()
            })
            .unwrap();

        assert!(matches!(
            res.into_websites().unwrap().sidebar.unwrap(),
            Sidebar::StackOverflow {
                schema_org: _,
                url: _
            }
        ));
    }
}
