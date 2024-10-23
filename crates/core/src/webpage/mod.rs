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
    backlink_grouper::GroupedBacklinks,
    inverted_index::InvertedIndex,
    schema::{numerical_field::NumericalField, text_field::TextField, Field},
    webgraph::{NodeID, SmallEdgeWithLabel},
    Result,
};
use candle_core::Tensor;
use chrono::{DateTime, Utc};

use std::collections::HashMap;
use tantivy::TantivyDocument;
use url::Url;

mod adservers;
pub mod html;
mod just_text;
pub mod region;
pub mod safety_classifier;
pub mod schema_org;
pub mod url_ext;
pub use self::html::links::RelFlags;
pub use self::html::Html;

pub use region::Region;

#[derive(Debug)]
pub struct Webpage {
    pub html: Html,

    #[cfg(test)]
    pub backlinks: Vec<SmallEdgeWithLabel>,

    #[cfg(not(test))]
    backlinks: Vec<SmallEdgeWithLabel>,

    #[cfg(test)]
    pub grouped_backlinks: GroupedBacklinks,

    #[cfg(not(test))]
    grouped_backlinks: GroupedBacklinks,

    pub host_centrality: f64,
    pub host_centrality_rank: u64,
    pub page_centrality: f64,
    pub page_centrality_rank: u64,
    pub fetch_time_ms: u64,
    pub pre_computed_score: f64,
    pub node_id: Option<NodeID>,
    pub dmoz_description: Option<String>,
    pub safety_classification: Option<safety_classifier::Label>,
    pub inserted_at: DateTime<Utc>,
    pub keywords: Vec<String>,
    pub title_embedding: Option<Tensor>,
    pub keyword_embedding: Option<Tensor>,
}

#[cfg(test)]
impl Default for Webpage {
    fn default() -> Self {
        Self {
            html: Html::parse_without_text("<html></html>", "https://example.com/").unwrap(),
            backlinks: Default::default(),
            grouped_backlinks: GroupedBacklinks::empty(),
            host_centrality: Default::default(),
            host_centrality_rank: u64::MAX,
            page_centrality: Default::default(),
            page_centrality_rank: u64::MAX,
            fetch_time_ms: Default::default(),
            pre_computed_score: Default::default(),
            node_id: Default::default(),
            dmoz_description: Default::default(),
            safety_classification: Default::default(),
            inserted_at: Utc::now(),
            keywords: Default::default(),
            title_embedding: Default::default(),
            keyword_embedding: Default::default(),
        }
    }
}

impl From<Html> for Webpage {
    fn from(html: Html) -> Self {
        Self {
            html,
            grouped_backlinks: GroupedBacklinks::empty(),
            backlinks: Default::default(),
            host_centrality: Default::default(),
            host_centrality_rank: u64::MAX,
            page_centrality: Default::default(),
            page_centrality_rank: u64::MAX,
            fetch_time_ms: Default::default(),
            pre_computed_score: Default::default(),
            node_id: Default::default(),
            dmoz_description: Default::default(),
            safety_classification: Default::default(),
            inserted_at: Utc::now(),
            keywords: Default::default(),
            title_embedding: Default::default(),
            keyword_embedding: Default::default(),
        }
    }
}

impl Webpage {
    #[cfg(test)]
    pub fn test_parse(html: &str, url: &str) -> Result<Self> {
        let html = Html::parse(html, url)?;

        Ok(Self {
            html,
            ..Default::default()
        })
    }

    pub fn dmoz_description(&self) -> Option<String> {
        self.dmoz_description.as_ref().and_then(|desc| {
            if !self.html.metadata().iter().any(|metadata| {
                if let Some(content) = metadata.get(&"content".to_string()) {
                    content.contains("noodp")
                } else {
                    false
                }
            }) {
                Some(desc.clone())
            } else {
                None
            }
        })
    }

    pub fn backlinks(&self) -> &[SmallEdgeWithLabel] {
        &self.backlinks
    }

    pub fn set_backlinks(&mut self, backlinks: Vec<SmallEdgeWithLabel>) {
        let backlinks = backlinks
            .into_iter()
            .filter(|e| !e.rel_flags.contains(RelFlags::NOFOLLOW))
            .collect();

        self.backlinks = backlinks;
    }

    pub fn set_grouped_backlinks(&mut self, grouped_backlinks: GroupedBacklinks) {
        self.grouped_backlinks = grouped_backlinks;
    }

    pub fn grouped_backlinks(&self) -> &GroupedBacklinks {
        &self.grouped_backlinks
    }

    pub fn as_tantivy(&self, index: &InvertedIndex) -> Result<TantivyDocument> {
        let mut doc = self.html.as_tantivy(index)?;

        for field in Field::all() {
            match field {
                Field::Numerical(f) => f.add_webpage_tantivy(self, &mut doc, index)?,
                Field::Text(f) => f.add_webpage_tantivy(self, &mut doc, index)?,
            }
        }

        Ok(doc)
    }
}

struct Script {
    attributes: HashMap<String, String>,
    content: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Link {
    pub source: Url,
    pub destination: Url,
    pub rel: RelFlags,
    pub text: String,
}

pub type Meta = HashMap<String, String>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dmoz_description() {
        let html = Html::parse(
            r#"
                    <html>
                        <head>
                            <title>Test site</title>
                        </head>
                        <body>
                            test
                        </body>
                    </html>
                "#,
            "https://example.com",
        )
        .unwrap();

        let webpage = Webpage {
            html,
            fetch_time_ms: 500,
            dmoz_description: Some("dmoz description".to_string()),
            ..Default::default()
        };

        assert_eq!(
            webpage.dmoz_description(),
            Some("dmoz description".to_string())
        )
    }

    #[test]
    fn noodp_ignores_dmoz() {
        let html = Html::parse(
            r#"
                    <html>
                        <head>
                            <meta name="robots" content="noodp" />
                            <title>Test site</title>
                        </head>
                        <body>
                            test
                        </body>
                    </html>
                "#,
            "http://example.com",
        )
        .unwrap();
        let webpage = Webpage {
            html,
            fetch_time_ms: 500,
            dmoz_description: Some("dmoz description".to_string()),
            ..Default::default()
        };

        assert_eq!(webpage.dmoz_description(), None)
    }
}
