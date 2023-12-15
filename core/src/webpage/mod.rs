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

use crate::{
    schema::{FastField, TextField},
    split_u128,
    webgraph::NodeID,
    Result,
};
use chrono::{DateTime, Utc};

#[cfg(test)]
use kuchiki::traits::TendrilSink;
use std::collections::HashMap;
use tantivy::{time::OffsetDateTime, TantivyDocument};
use url::Url;

use crate::schema::{Field, FLOAT_SCALING};

use self::region::Region;

mod adservers;
mod html;
mod just_text;
pub mod region;
pub mod safety_classifier;
pub mod schema_org;
pub mod url_ext;
pub use self::html::Html;

#[derive(Debug)]
pub struct Webpage {
    pub html: Html,
    pub backlink_labels: Vec<String>,
    pub host_centrality: f64,
    pub page_centrality: f64,
    pub fetch_time_ms: u64,
    pub pre_computed_score: f64,
    pub node_id: Option<NodeID>,
    pub dmoz_description: Option<String>,
    pub safety_classification: Option<safety_classifier::Label>,
    pub inserted_at: DateTime<Utc>,
}

#[cfg(test)]
impl Default for Webpage {
    fn default() -> Self {
        Self {
            html: Html {
                url: Url::parse("https://example.com").expect("Failed to parse url"),
                root: kuchiki::parse_html().one("<html></html>"),
                all_text: None,
                clean_text: None,
                lang: None,
                robots: None,
            },
            backlink_labels: Default::default(),
            host_centrality: Default::default(),
            page_centrality: Default::default(),
            fetch_time_ms: Default::default(),
            pre_computed_score: Default::default(),
            node_id: Default::default(),
            dmoz_description: Default::default(),
            safety_classification: Default::default(),
            inserted_at: Utc::now(),
        }
    }
}

impl Webpage {
    #[cfg(test)]
    pub fn new(html: &str, url: &str) -> Result<Self> {
        let html = Html::parse(html, url)?;

        Ok(Self {
            html,
            backlink_labels: Vec::new(),
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 0,
            pre_computed_score: 0.0,
            node_id: None,
            dmoz_description: None,
            safety_classification: None,
            inserted_at: Utc::now(),
        })
    }

    fn dmoz_description(&self) -> Option<String> {
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

    pub fn into_tantivy(self, schema: &tantivy::schema::Schema) -> Result<TantivyDocument> {
        let region = Region::guess_from(&self);

        let dmoz_description = self.dmoz_description();

        let mut doc = self.html.into_tantivy(schema)?;

        if let Ok(region) = region {
            doc.add_u64(
                schema
                    .get_field(Field::Fast(FastField::Region).name())
                    .expect("Failed to get region field"),
                region.id(),
            );
        } else {
            doc.add_u64(
                schema
                    .get_field(Field::Fast(FastField::Region).name())
                    .expect("Failed to get region field"),
                Region::All.id(),
            );
        }

        let backlink_text: String =
            itertools::intersperse(self.backlink_labels, "\n".to_string()).collect();

        doc.add_text(
            schema
                .get_field(Field::Text(TextField::BacklinkText).name())
                .expect("Failed to get backlink-text field"),
            backlink_text,
        );

        doc.add_date(
            schema
                .get_field(Field::Text(TextField::InsertionTimestamp).name())
                .expect("Failed to get insertion-timestamp field"),
            tantivy::DateTime::from_utc(OffsetDateTime::from_unix_timestamp(
                self.inserted_at.timestamp(),
            )?),
        );

        let safety = self
            .safety_classification
            .map(|label| label.to_string())
            .unwrap_or_default();

        doc.add_text(
            schema
                .get_field(Field::Text(TextField::SafetyClassification).name())
                .expect("Failed to get safety_classification field"),
            safety,
        );

        doc.add_u64(
            schema
                .get_field(Field::Fast(FastField::HostCentrality).name())
                .expect("Failed to get host_centrality field"),
            (self.host_centrality * FLOAT_SCALING as f64) as u64,
        );

        doc.add_u64(
            schema
                .get_field(Field::Fast(FastField::PageCentrality).name())
                .expect("Failed to get page_centrality field"),
            (self.page_centrality * FLOAT_SCALING as f64) as u64,
        );

        doc.add_u64(
            schema
                .get_field(Field::Fast(FastField::FetchTimeMs).name())
                .expect("Failed to get fetch_time_ms field"),
            self.fetch_time_ms,
        );

        doc.add_u64(
            schema
                .get_field(Field::Fast(FastField::PreComputedScore).name())
                .expect("failed to get pre_computed_score field"),
            (self.pre_computed_score * FLOAT_SCALING as f64) as u64,
        );

        match &self.node_id {
            Some(node_id) => {
                let [node_id1, node_id2] = split_u128(node_id.bit_128());
                doc.add_u64(
                    schema
                        .get_field(Field::Fast(FastField::HostNodeID1).name())
                        .expect("Failed to get node_id field 1"),
                    node_id1,
                );
                doc.add_u64(
                    schema
                        .get_field(Field::Fast(FastField::HostNodeID2).name())
                        .expect("Failed to get node_id field 2"),
                    node_id2,
                );
            }
            None => {
                doc.add_u64(
                    schema
                        .get_field(Field::Fast(FastField::HostNodeID1).name())
                        .expect("Failed to get node_id field 1"),
                    u64::MAX,
                );
                doc.add_u64(
                    schema
                        .get_field(Field::Fast(FastField::HostNodeID2).name())
                        .expect("Failed to get node_id field 2"),
                    u64::MAX,
                );
            }
        }

        doc.add_text(
            schema
                .get_field(Field::Text(TextField::DmozDescription).name())
                .expect("failed to get dmoz_description field"),
            dmoz_description.unwrap_or_default(),
        );

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
