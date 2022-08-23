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

use crate::tokenizer::{NormalTokenizer, StemmedTokenizer};

pub const CENTRALITY_SCALING: u64 = 1_000_000_000;

#[derive(Clone)]
pub enum Field {
    Title,
    CleanBody,
    StemmedTitle,
    StemmedCleanBody,
    AllBody,
    StemmedAllBody,
    Url,
    Host,
    Domain,
    DomainIfHomepage, // this field is only set if the webpage is the homepage for the site. Allows us to boost
    IsHomepage,
    BacklinkText,
    Centrality,
    FetchTimeMs,
    PrimaryImageUuid,
    LastUpdated,
    Description,
    NumTrackers,
}
pub static ALL_FIELDS: [Field; 18] = [
    Field::Title,
    Field::CleanBody,
    Field::StemmedTitle,
    Field::StemmedCleanBody,
    Field::AllBody,
    Field::StemmedAllBody,
    Field::Url,
    Field::Host,
    Field::Domain,
    Field::DomainIfHomepage,
    Field::IsHomepage,
    Field::BacklinkText,
    Field::Centrality,
    Field::FetchTimeMs,
    Field::PrimaryImageUuid,
    Field::LastUpdated,
    Field::Description,
    Field::NumTrackers,
];

impl Field {
    fn default_text_options_with_tokenizer(
        &self,
        tokenizer_name: &str,
    ) -> tantivy::schema::TextOptions {
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(tokenizer_name)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
    }

    fn default_text_options(&self) -> tantivy::schema::TextOptions {
        self.default_text_options_with_tokenizer(NormalTokenizer::as_str())
    }

    pub fn options(&self) -> IndexingOption {
        match self {
            Field::Title => IndexingOption::Text(self.default_text_options().set_stored()),
            Field::CleanBody => IndexingOption::Text(self.default_text_options()),
            Field::Url => IndexingOption::Text(self.default_text_options().set_stored()),
            Field::Host => IndexingOption::Text(self.default_text_options()),
            Field::Domain => IndexingOption::Text(self.default_text_options()),
            Field::AllBody => IndexingOption::Text(self.default_text_options()),
            Field::StemmedAllBody => IndexingOption::Text(
                self.default_text_options_with_tokenizer(StemmedTokenizer::as_str())
                    .set_stored(),
            ),
            Field::DomainIfHomepage => IndexingOption::Text(self.default_text_options()),
            Field::IsHomepage => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::BacklinkText => IndexingOption::Text(self.default_text_options()),
            Field::Centrality => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::StemmedTitle => IndexingOption::Text(
                self.default_text_options_with_tokenizer(StemmedTokenizer::as_str()),
            ),
            Field::StemmedCleanBody => IndexingOption::Text(
                self.default_text_options_with_tokenizer(StemmedTokenizer::as_str())
                    .set_stored(),
            ),
            Field::FetchTimeMs => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::NumTrackers => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::PrimaryImageUuid => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::LastUpdated => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_stored()
                    .set_indexed(),
            ),
            Field::Description => IndexingOption::Text(self.default_text_options().set_stored()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Field::Title => "title",
            Field::CleanBody => "body",
            Field::Url => "url",
            Field::Host => "host",
            Field::BacklinkText => "backlink_text",
            Field::Centrality => "centrality",
            Field::StemmedTitle => "stemmed_title",
            Field::StemmedCleanBody => "stemmed_body",
            Field::Domain => "domain",
            Field::DomainIfHomepage => "domain_if_homepage",
            Field::IsHomepage => "is_homepage",
            Field::FetchTimeMs => "fetch_time_ms",
            Field::PrimaryImageUuid => "primary_image_uuid",
            Field::LastUpdated => "last_updated",
            Field::Description => "description",
            Field::AllBody => "all_body",
            Field::StemmedAllBody => "stemmed_all_body",
            Field::NumTrackers => "num_trackers",
        }
    }

    pub fn boost(&self) -> Option<f32> {
        match self {
            Field::Host => Some(6.0),
            Field::DomainIfHomepage => Some(50.0),
            Field::StemmedCleanBody | Field::StemmedTitle => Some(0.1),
            Field::CleanBody => Some(4.0),
            Field::Title => Some(10.0),
            Field::Url => Some(1.0),
            Field::Domain => Some(4.0),
            Field::AllBody => Some(0.01),
            Field::StemmedAllBody => Some(0.001),
            Field::BacklinkText
            | Field::Centrality
            | Field::IsHomepage
            | Field::PrimaryImageUuid
            | Field::FetchTimeMs
            | Field::Description
            | Field::NumTrackers
            | Field::LastUpdated => None,
        }
    }

    pub fn is_searchable(&self) -> bool {
        !matches!(self, Field::PrimaryImageUuid)
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

pub enum IndexingOption {
    Text(tantivy::schema::TextOptions),
    Numeric(tantivy::schema::NumericOptions),
}
