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
    BytesOptions, Cardinality, IndexRecordOption, NumericOptions, TextFieldIndexing, TextOptions,
};

use crate::tokenizer::{Normal, Stemmed};

pub const CENTRALITY_SCALING: u64 = 1_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Field {
    Title,
    CleanBody,
    StemmedTitle,
    StemmedCleanBody,
    AllBody,
    Url,
    Site,
    Domain,
    SiteNoTokenizer,
    DomainNoTokenizer,
    /// this field is only set if the webpage is the homepage for the site. Allows us to boost
    DomainIfHomepage,
    /// this field is only set if the webpage is the homepage for the site. Allows us to boost
    DomainNameIfHomepageNoTokenizer,
    IsHomepage,
    BacklinkText,
    HostCentrality,
    PageCentrality,
    FetchTimeMs,
    PrimaryImage,
    LastUpdated,
    Description,
    NumTrackers,
    Region,
    NumUrlTokens,
    NumTitleTokens,
    NumCleanBodyTokens,
    NumDescriptionTokens,
    SiteHash,
    UrlWithoutQueryHash,
}

pub static ALL_FIELDS: [Field; 28] = [
    Field::Title,
    Field::CleanBody,
    Field::StemmedTitle,
    Field::StemmedCleanBody,
    Field::AllBody,
    Field::Url,
    Field::Site,
    Field::Domain,
    Field::SiteNoTokenizer,
    Field::DomainNoTokenizer,
    Field::DomainIfHomepage,
    Field::DomainNameIfHomepageNoTokenizer,
    Field::IsHomepage,
    Field::BacklinkText,
    Field::HostCentrality,
    Field::PageCentrality,
    Field::FetchTimeMs,
    Field::PrimaryImage,
    Field::LastUpdated,
    Field::Description,
    Field::NumTrackers,
    Field::Region,
    Field::NumUrlTokens,
    Field::NumTitleTokens,
    Field::NumCleanBodyTokens,
    Field::NumDescriptionTokens,
    Field::SiteHash,
    Field::UrlWithoutQueryHash,
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
        self.default_text_options_with_tokenizer(Normal::as_str())
    }

    pub fn options(&self) -> IndexingOption {
        match self {
            Field::Title => IndexingOption::Text(self.default_text_options().set_stored()),
            Field::CleanBody => IndexingOption::Text(self.default_text_options()),
            Field::Url => IndexingOption::Text(self.default_text_options().set_stored()),
            Field::Site => IndexingOption::Text(self.default_text_options()),
            Field::Domain => IndexingOption::Text(self.default_text_options()),
            Field::SiteNoTokenizer => IndexingOption::Text(
                TextOptions::default().set_indexing_options(
                    TextFieldIndexing::default()
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                ),
            ),
            Field::DomainNoTokenizer => IndexingOption::Text(
                TextOptions::default().set_indexing_options(
                    TextFieldIndexing::default()
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                ),
            ),
            Field::AllBody => IndexingOption::Text(self.default_text_options().set_stored()),
            Field::DomainIfHomepage => IndexingOption::Text(self.default_text_options()),
            Field::DomainNameIfHomepageNoTokenizer => IndexingOption::Text(
                TextOptions::default().set_indexing_options(
                    TextFieldIndexing::default()
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                ),
            ),
            Field::IsHomepage => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::BacklinkText => IndexingOption::Text(self.default_text_options()),
            Field::HostCentrality => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::PageCentrality => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::StemmedTitle => {
                IndexingOption::Text(self.default_text_options_with_tokenizer(Stemmed::as_str()))
            }
            Field::StemmedCleanBody => IndexingOption::Text(
                self.default_text_options_with_tokenizer(Stemmed::as_str())
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
            Field::PrimaryImage => IndexingOption::Bytes(BytesOptions::default().set_stored()),
            Field::LastUpdated => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_stored()
                    .set_indexed(),
            ),
            Field::Description => IndexingOption::Text(self.default_text_options().set_stored()),
            Field::Region => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_stored()
                    .set_indexed(),
            ),
            Field::NumCleanBodyTokens => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::NumDescriptionTokens => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::NumTitleTokens => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::NumUrlTokens => IndexingOption::Numeric(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::SiteHash => IndexingOption::Numeric(
                NumericOptions::default().set_fast(Cardinality::MultiValues),
            ),
            Field::UrlWithoutQueryHash => IndexingOption::Numeric(
                NumericOptions::default().set_fast(Cardinality::MultiValues),
            ),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Field::Title => "title",
            Field::CleanBody => "body",
            Field::Url => "url",
            Field::Site => "site",
            Field::Domain => "domain",
            Field::SiteNoTokenizer => "site_no_tokenizer",
            Field::DomainNoTokenizer => "domain_no_tokenizer",
            Field::BacklinkText => "backlink_text",
            Field::HostCentrality => "host_centrality",
            Field::PageCentrality => "page_centrality",
            Field::StemmedTitle => "stemmed_title",
            Field::StemmedCleanBody => "stemmed_body",
            Field::DomainIfHomepage => "domain_if_homepage",
            Field::DomainNameIfHomepageNoTokenizer => "domain_name_if_homepage_no_tokenizer",
            Field::IsHomepage => "is_homepage",
            Field::FetchTimeMs => "fetch_time_ms",
            Field::PrimaryImage => "primary_image_uuid",
            Field::LastUpdated => "last_updated",
            Field::Description => "description",
            Field::AllBody => "all_body",
            Field::NumTrackers => "num_trackers",
            Field::Region => "region",
            Field::NumUrlTokens => "num_url_tokens",
            Field::NumTitleTokens => "num_title_tokens",
            Field::NumCleanBodyTokens => "num_clean_body_tokens",
            Field::NumDescriptionTokens => "num_description_tokens",
            Field::SiteHash => "site_hash",
            Field::UrlWithoutQueryHash => "url_without_query_hash",
        }
    }

    pub fn boost(&self) -> Option<f32> {
        match self {
            Field::Site => Some(3.0),
            Field::DomainIfHomepage => Some(10.0),
            Field::DomainNameIfHomepageNoTokenizer => Some(30.0),
            Field::StemmedCleanBody | Field::StemmedTitle => Some(0.1),
            Field::CleanBody => Some(4.0),
            Field::Title => Some(10.0),
            Field::Url => Some(1.0),
            Field::Domain => Some(1.0),
            Field::AllBody => Some(0.01),
            Field::BacklinkText => Some(4.0),
            Field::HostCentrality
            | Field::PageCentrality
            | Field::SiteHash
            | Field::UrlWithoutQueryHash
            | Field::SiteNoTokenizer
            | Field::DomainNoTokenizer
            | Field::IsHomepage
            | Field::PrimaryImage
            | Field::FetchTimeMs
            | Field::Description
            | Field::NumTrackers
            | Field::NumUrlTokens
            | Field::NumTitleTokens
            | Field::NumCleanBodyTokens
            | Field::NumDescriptionTokens
            | Field::Region
            | Field::LastUpdated => None,
        }
    }

    pub fn is_searchable(&self) -> bool {
        !matches!(self, Field::PrimaryImage | Field::BacklinkText) && !self.is_fast()
    }

    pub fn is_fast(&self) -> bool {
        matches!(
            self,
            Field::IsHomepage
                | Field::HostCentrality
                | Field::FetchTimeMs
                | Field::LastUpdated
                | Field::NumTrackers
                | Field::Region
                | Field::NumUrlTokens
                | Field::NumTitleTokens
                | Field::NumCleanBodyTokens
                | Field::NumDescriptionTokens
                | Field::SiteHash
                | Field::UrlWithoutQueryHash
        )
    }

    pub fn from_string(name: String) -> Option<Field> {
        match name.as_str() {
            "title" => Some(Field::Title),
            "body" => Some(Field::CleanBody),
            "url" => Some(Field::Url),
            "site" => Some(Field::Site),
            "backlink_text" => Some(Field::BacklinkText),
            "host_centrality" => Some(Field::HostCentrality),
            "page_centrality" => Some(Field::PageCentrality),
            "stemmed_title" => Some(Field::StemmedTitle),
            "stemmed_body" => Some(Field::StemmedCleanBody),
            "domain" => Some(Field::Domain),
            "domain_if_homepage" => Some(Field::DomainIfHomepage),
            "is_homepage" => Some(Field::IsHomepage),
            "fetch_time_ms" => Some(Field::FetchTimeMs),
            "primary_image_uuid" => Some(Field::PrimaryImage),
            "last_updated" => Some(Field::LastUpdated),
            "description" => Some(Field::Description),
            "all_body" => Some(Field::AllBody),
            "num_trackers" => Some(Field::NumTrackers),
            "region" => Some(Field::Region),
            "site_hash" => Some(Field::SiteHash),
            "url_without_query_hash" => Some(Field::UrlWithoutQueryHash),
            "domain_name_if_homepage_no_tokenizer" => Some(Field::DomainNameIfHomepageNoTokenizer),
            _ => None,
        }
    }
}

pub fn create_schema() -> tantivy::schema::Schema {
    let mut builder = tantivy::schema::Schema::builder();

    for field in &ALL_FIELDS {
        match field.options() {
            IndexingOption::Text(options) => builder.add_text_field(field.as_str(), options),
            IndexingOption::Numeric(options) => builder.add_u64_field(field.as_str(), options),
            IndexingOption::Bytes(options) => builder.add_bytes_field(field.as_str(), options),
        };
    }

    builder.build()
}

pub enum IndexingOption {
    Text(tantivy::schema::TextOptions),
    Numeric(tantivy::schema::NumericOptions),
    Bytes(tantivy::schema::BytesOptions),
}
