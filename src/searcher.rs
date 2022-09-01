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
use std::time::Instant;

use uuid::Uuid;

use crate::bangs::{BangHit, Bangs};
use crate::entity_index::{EntityIndex, StoredEntity};
use crate::image_store::Image;
use crate::index::Index;
use crate::inverted_index::InvertedIndexSearchResult;
use crate::query::Query;
use crate::ranking::signal_aggregator;
use crate::ranking::Ranker;
use crate::webpage::region::Region;
use crate::webpage::Url;
use crate::{Error, Result};

pub const NUM_RESULTS_PER_PAGE: usize = 20;

#[derive(Debug)]
pub struct WebsitesResult {
    pub spell_corrected_query: Option<String>,
    pub webpages: InvertedIndexSearchResult,
    pub entity: Option<StoredEntity>,
    pub search_duration_ms: u128,
}

#[derive(Debug)]
pub enum SearchResult {
    Websites(WebsitesResult),
    Bang(BangHit),
}

pub struct Searcher {
    index: Index,
    entity_index: Option<EntityIndex>,
    bangs: Option<Bangs>,
}

impl From<Index> for Searcher {
    fn from(index: Index) -> Self {
        Self::new(index, None, None)
    }
}

impl Searcher {
    pub fn new(index: Index, entity_index: Option<EntityIndex>, bangs: Option<Bangs>) -> Self {
        Searcher {
            index,
            entity_index,
            bangs,
        }
    }

    pub fn search(
        &self,
        query: &str,
        selected_region: Option<Region>,
        goggle_program: Option<&str>,
        skip_pages: Option<usize>,
    ) -> Result<SearchResult> {
        let start = Instant::now();

        let raw_query = query.to_string();
        let aggregator = goggle_program
            .and_then(|program| signal_aggregator::parse(program).ok())
            .unwrap_or_default();

        let query = Query::parse(
            query,
            self.index.schema(),
            self.index.tokenizers(),
            &aggregator,
        )?;

        if query.is_empty() {
            return Err(Error::EmptyQuery);
        }

        if let Some(bangs) = self.bangs.as_ref() {
            if let Some(bang) = bangs.get(&query) {
                return Ok(SearchResult::Bang(bang));
            }
        }

        let mut ranker = Ranker::new(self.index.region_count.clone(), aggregator);

        if let Some(skip_pages) = skip_pages {
            ranker = ranker.with_offset(NUM_RESULTS_PER_PAGE * skip_pages);
        }

        if let Some(region) = selected_region {
            if region != Region::All {
                ranker = ranker.with_region(region);
            }
        }

        let webpages = self.index.search(&query, ranker.collector())?;
        let correction = self.index.spell_correction(&query.simple_terms());

        let entity = self
            .entity_index
            .as_ref()
            .and_then(|index| index.search(&raw_query));

        let search_duration_ms = start.elapsed().as_millis();

        Ok(SearchResult::Websites(WebsitesResult {
            webpages,
            entity,
            spell_corrected_query: correction,
            search_duration_ms,
        }))
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
    use crate::webpage::Webpage;

    use super::*;

    #[test]
    fn custom_signal_aggregation() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
                r#"
            <html>
                <head>
                    <title>Test website</title>
                </head>
                <body>
                    example
                </body>
            </html>
            "#,
                "https://www.body.com",
                vec![],
                1.0,
                20,
            ))
            .expect("failed to parse webpage");

        index
            .insert(Webpage::new(
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
                "https://www.title.com",
                vec![],
                1.0,
                20,
            ))
            .expect("failed to parse webpage");

        index
            .insert(Webpage::new(
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
                "https://www.centrality.com",
                vec![],
                1.0002,
                500,
            ))
            .expect("failed to parse webpage");

        index.commit().unwrap();

        let searcher = Searcher::new(index, None, None);

        let res = searcher
            .search(
                "example",
                None,
                Some(
                    r#"
                        @field_title = 20000000
                        @host_centrality = 0
                    "#,
                ),
                None,
            )
            .unwrap()
            .into_websites()
            .unwrap();

        assert_eq!(res.webpages.num_docs, 3);
        assert_eq!(&res.webpages.documents[0].url, "https://www.title.com");

        let res = searcher
            .search(
                "example",
                None,
                Some(
                    r#"
                        @field_all_body= 20000000
                        @host_centrality = 0
                    "#,
                ),
                None,
            )
            .unwrap()
            .into_websites()
            .unwrap();

        assert_eq!(res.webpages.num_docs, 3);
        assert_eq!(&res.webpages.documents[0].url, "https://www.body.com");

        let res = searcher
            .search("example", None, Some("@host_centrality= 2000000"), None)
            .unwrap()
            .into_websites()
            .unwrap();

        assert_eq!(res.webpages.num_docs, 3);
        assert_eq!(&res.webpages.documents[0].url, "https://www.centrality.com");
    }

    #[test]
    fn offset_page() {
        const NUM_PAGES: usize = 10;
        const NUM_WEBSITES: usize = NUM_PAGES * NUM_RESULTS_PER_PAGE;

        let mut index = Index::temporary().expect("Unable to open index");

        for i in 0..NUM_WEBSITES {
            index
                .insert(Webpage::new(
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
                    vec![],
                    (NUM_WEBSITES - i) as f64,
                    500,
                ))
                .expect("failed to parse webpage");
        }

        index.commit().unwrap();

        let searcher = Searcher::new(index, None, None);

        for p in 0..NUM_PAGES {
            let urls = searcher
                .search("test", None, None, Some(p))
                .unwrap()
                .into_websites()
                .unwrap()
                .webpages
                .documents
                .into_iter()
                .map(|page| page.url);

            for (i, url) in urls.enumerate() {
                assert_eq!(
                    url,
                    format!("https://www.{}.com", i + (p * NUM_RESULTS_PER_PAGE))
                )
            }
        }
    }
}
