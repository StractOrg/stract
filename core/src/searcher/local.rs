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
use uuid::Uuid;

use crate::entity_index::{EntityIndex, StoredEntity};
use crate::image_store::Image;
use crate::index::Index;
use crate::query::parser::Term;
use crate::query::{self, Query};
use crate::ranking::centrality_store::SearchCentralityStore;
use crate::ranking::optics::CreateAggregator;
use crate::ranking::pipeline::RankingWebsite;
use crate::ranking::{online_centrality_scorer, Ranker, SignalAggregator};
use crate::search_prettifier::{DisplayedEntity, DisplayedWebpage, HighlightedSpellCorrection};
use crate::spell::Correction;
use crate::webgraph::centrality::topic::TopicCentrality;
use crate::webgraph::Node;
use crate::webpage::region::Region;
use crate::webpage::Url;
use crate::{inverted_index, Error, Result};

use super::WebsitesResult;
use super::{InitialWebsiteResult, SearchQuery};

pub struct LocalSearcher {
    index: Index,
    entity_index: Option<EntityIndex>,
    centrality_store: Option<SearchCentralityStore>,
    topic_centrality: Option<TopicCentrality>,
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
            centrality_store: None,
            topic_centrality: None,
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

    fn parse_query(&self, query: &SearchQuery) -> Result<(Query, Optic)> {
        let optic = match query.optic_program.as_ref() {
            Some(program) => Some(optics::parse(program)?),
            None => None,
        };

        let query_aggregator = optic
            .as_ref()
            .and_then(|optic| {
                optic
                    .pipeline
                    .as_ref()
                    .and_then(|pipeline| pipeline.stages.first().map(|stage| stage.aggregator()))
            })
            .unwrap_or_default();

        let mut parsed_query = Query::parse(
            &query.query,
            self.index.schema(),
            self.index.tokenizers(),
            &query_aggregator,
        )?;

        parsed_query.set_num_results(query.num_results);
        parsed_query.set_offset(query.offset);

        if let Some(region) = query.selected_region {
            parsed_query.set_region(region);
        }

        let mut optics = Vec::new();

        if let Some(optic) = optic {
            optics.push(optic);
        }

        if let Some(site_rankings) = &query.site_rankings {
            optics.push(site_rankings.clone().into_optic())
        }

        let mut optic = Optic::default();

        for o in optics {
            optic = optic.try_merge(o)?;
        }

        parsed_query.set_optic(&optic, &self.index);

        if parsed_query.is_empty() {
            Err(Error::EmptyQuery)
        } else {
            Ok((parsed_query, optic))
        }
    }

    fn ranker(
        &self,
        query: &Query,
        de_rank_similar: bool,
        aggregator: SignalAggregator,
    ) -> Result<Ranker> {
        let mut ranker = Ranker::new(
            self.index.region_count.clone(),
            aggregator,
            self.index.inverted_index.fastfield_cache(),
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

            let top_host_nodes = self.index.top_nodes(query, ranker.collector())?;
            if !top_host_nodes.is_empty() {
                let harmonic = centrality_store
                    .online_harmonic
                    .scorer_from_ids(&top_host_nodes, &[]);
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
        query: &SearchQuery,
        de_rank_similar: bool,
    ) -> Result<(Vec<RankingWebsite>, usize)> {
        let (parsed_query, optic) = self.parse_query(query)?;

        let mut aggregator = optic
            .pipeline
            .and_then(|pipeline| pipeline.stages.first().map(|stage| stage.aggregator()))
            .unwrap_or_default();

        if let Some(store) = &self.centrality_store {
            aggregator.set_personal_harmonic(online_centrality_scorer(
                &optic.site_rankings,
                &store.online_harmonic,
            ));

            let liked_sites: Vec<_> = optic
                .site_rankings
                .liked
                .iter()
                .map(|site| Node::from(site.clone()).into_host())
                .filter_map(|node| store.node2id.get(&node).copied())
                .collect();

            let disliked_sites: Vec<_> = optic
                .site_rankings
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

        let ranker = self.ranker(&parsed_query, de_rank_similar, aggregator)?;

        let res = self
            .index
            .search_initial(&parsed_query, ranker.collector())?;

        let ranking_websites = self
            .index
            .inverted_index
            .retrieve_ranking_websites(res.top_websites, &ranker.aggregator())?;

        Ok((ranking_websites, res.num_websites))
    }

    fn spell_correction(&self, query: &SearchQuery) -> Option<Correction> {
        let terms: Vec<_> = query::parser::parse(&query.query)
            .into_iter()
            .filter_map(|term| match *term {
                Term::Simple(s) => Some(s),
                _ => None,
            })
            .collect();

        self.index.spell_correction(&terms)
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
        let (websites, num_websites) = self.search_inverted_index(query, de_rank_similar)?;
        let correction = self.spell_correction(query);
        let sidebar = self
            .entity_sidebar(query)
            .map(|entity| DisplayedEntity::from(entity, self));

        Ok(InitialWebsiteResult {
            spell_corrected_query: correction,
            websites,
            num_websites,
            entity_sidebar: sidebar,
        })
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

    /// This function is mainly used for tests and benchmarks
    pub fn search(&self, query: &SearchQuery) -> Result<WebsitesResult> {
        use std::{sync::Arc, time::Instant};

        use crate::{
            ranking::{
                models::cross_encoder::{CrossEncoderModel, DummyCrossEncoder},
                pipeline::RankingPipeline,
            },
            search_prettifier::Sidebar,
        };

        let start = Instant::now();
        let mut search_query = query.clone();

        let pipeline = match CrossEncoderModel::open("data/cross_encoder") {
            Ok(model) => RankingPipeline::for_query(&mut search_query, Arc::new(model))?,
            Err(_) => {
                RankingPipeline::for_query(&mut search_query, Arc::new(DummyCrossEncoder {}))?
            }
        };

        let search_result = self.search_initial(&search_query, true)?;

        let top_websites = pipeline.apply(search_result.websites);
        let pointers: Vec<_> = top_websites
            .into_iter()
            .map(|website| website.pointer)
            .collect();

        let retrieved_sites = self.retrieve_websites(&pointers, &search_query.query)?;

        Ok(WebsitesResult {
            spell_corrected_query: search_result
                .spell_corrected_query
                .map(HighlightedSpellCorrection::from),
            num_hits: search_result.num_websites,
            webpages: retrieved_sites
                .into_iter()
                .map(DisplayedWebpage::from)
                .collect(),
            discussions: None,
            sidebar: search_result.entity_sidebar.map(Sidebar::Entity),
            search_duration_ms: start.elapsed().as_millis(),
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
}
