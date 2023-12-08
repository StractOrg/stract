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

use crate::{config::ApiThresholds, Result};
use std::{cmp::Ordering, sync::Arc};

use optics::Optic;
use url::Url;

use crate::{
    search_prettifier::{create_stackoverflow_sidebar, DisplayedEntity, DisplayedSidebar},
    searcher::{distributed, DistributedSearcher, SearchQuery},
};

pub struct SidebarManager {
    distributed_searcher: Arc<DistributedSearcher>,
    thresholds: ApiThresholds,
}

impl SidebarManager {
    pub fn new(
        distributed_searcher: Arc<DistributedSearcher>,
        thresholds: ApiThresholds,
    ) -> SidebarManager {
        Self {
            distributed_searcher,
            thresholds,
        }
    }

    pub async fn stackoverflow(&self, query: &SearchQuery) -> Result<Option<DisplayedSidebar>> {
        let query = SearchQuery {
            query: query.query.clone(),
            num_results: 1,
            optic: Some(Optic::parse(include_str!("stackoverflow.optic")).unwrap()),
            ..Default::default()
        };

        let mut results: Vec<_> = self
            .distributed_searcher
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
            tracing::debug!(?website.score, ?self.thresholds.stackoverflow, "stackoverflow score");
            if website.score > self.thresholds.stackoverflow {
                let scored_websites =
                    vec![(0, distributed::ScoredWebsitePointer { website, shard })];
                let mut retrieved = self
                    .distributed_searcher
                    .retrieve_webpages(&scored_websites, &query.query)
                    .await;

                if let Some((_, res)) = retrieved.pop() {
                    let res = res.into_retrieved_webpage();
                    return Ok(Some(create_stackoverflow_sidebar(
                        res.schema_org,
                        Url::parse(&res.url).unwrap(),
                    )?));
                }
            }
        }

        Ok(None)
    }

    pub fn sidebar(
        &self,
        initial_results: &[distributed::InitialSearchResultShard],
        stackoverflow: Option<DisplayedSidebar>,
    ) -> Option<DisplayedSidebar> {
        let entity = initial_results
            .iter()
            .filter_map(|res| res.local_result.entity_sidebar.clone())
            .map(DisplayedEntity::from)
            .filter(|entity| entity.match_score as f64 > self.thresholds.entity_sidebar)
            .max_by(|a, b| {
                a.match_score
                    .partial_cmp(&b.match_score)
                    .unwrap_or(Ordering::Equal)
            });

        match entity {
            Some(entity) => Some(DisplayedSidebar::Entity(entity)),
            None => stackoverflow,
        }
    }
}
