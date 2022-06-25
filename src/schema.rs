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

use tantivy::schema::{
    Cardinality, IndexRecordOption, NumericOptions, TextFieldIndexing, TextOptions,
};

pub const CENTRALITY_SCALING: u64 = 1_000_000_000;

#[derive(Clone)]
pub enum Field {
    Title,
    Body,
    Url,
    Host,
    BacklinkText,
    Centrality,
}
pub static ALL_FIELDS: [Field; 6] = [
    Field::Title,
    Field::Body,
    Field::Url,
    Field::Host,
    Field::BacklinkText,
    Field::Centrality,
];

impl Field {
    fn default_text_options(&self) -> tantivy::schema::TextOptions {
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("tokenizer")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
    }

    fn options(&self) -> IndexingOption {
        match self {
            Field::Title => IndexingOption::Text(self.default_text_options().set_stored()),
            Field::Body => IndexingOption::Text(self.default_text_options().set_stored()),
            Field::Url => IndexingOption::Text(self.default_text_options().set_stored()),
            Field::Host => IndexingOption::Text(self.default_text_options()),
            Field::BacklinkText => IndexingOption::Text(self.default_text_options()),
            Field::Centrality => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Field::Title => "title",
            Field::Body => "body",
            Field::Url => "url",
            Field::Host => "host",
            Field::BacklinkText => "backlink_text",
            Field::Centrality => "centrality",
        }
    }

    pub fn boost(&self) -> Option<f32> {
        match self {
            Field::Host => Some(30.0),
            Field::Title | Field::Body | Field::BacklinkText | Field::Centrality | Field::Url => {
                None
            }
        }
    }
}

pub fn create_schema() -> tantivy::schema::Schema {
    let mut builder = tantivy::schema::Schema::builder();

    for field in &ALL_FIELDS {
        match field.options() {
            IndexingOption::Text(options) => builder.add_text_field(field.as_str(), options),
            IndexingOption::Numeric(options) => builder.add_u64_field(field.as_str(), options),
        };
    }

    builder.build()
}

enum IndexingOption {
    Text(tantivy::schema::TextOptions),
    Numeric(tantivy::schema::NumericOptions),
}
