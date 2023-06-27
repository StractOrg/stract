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

use tantivy::schema::{
    Cardinality, IndexRecordOption, NumericOptions, TextFieldIndexing, TextOptions,
};

use crate::tokenizer::{BigramTokenizer, Identity, JsonField, Tokenizer, TrigramTokenizer};

pub const FLOAT_SCALING: u64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TextField {
    Title,
    CleanBody,
    StemmedTitle,
    StemmedCleanBody,
    AllBody,
    Url,
    UrlNoTokenizer,
    Site,
    Domain,
    SiteNoTokenizer,
    SiteIfHomepageNoTokenizer,
    DomainNoTokenizer,
    /// this field is only set if the webpage is the homepage for the site. Allows us to boost
    DomainIfHomepage,
    /// this field is only set if the webpage is the homepage for the site. Allows us to boost
    DomainNameIfHomepageNoTokenizer,
    /// this field is only set if the webpage is the homepage for the site. Allows us to boost
    TitleIfHomepage,
    BacklinkText,
    Description,
    HostTopic,
    DmozDescription,
    SchemaOrgJson,
    FlattenedSchemaOrgJson,
    CleanBodyBigrams,
    TitleBigrams,
    CleanBodyTrigrams,
    TitleTrigrams,
}

impl From<TextField> for usize {
    fn from(value: TextField) -> Self {
        value as usize
    }
}

impl TextField {
    pub fn tokenizer(&self) -> Tokenizer {
        match self {
            TextField::Title => Tokenizer::default(),
            TextField::CleanBody => Tokenizer::default(),
            TextField::StemmedTitle => Tokenizer::new_stemmed(),
            TextField::StemmedCleanBody => Tokenizer::new_stemmed(),
            TextField::AllBody => Tokenizer::default(),
            TextField::Url => Tokenizer::default(),
            TextField::UrlNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::Site => Tokenizer::default(),
            TextField::Domain => Tokenizer::default(),
            TextField::SiteNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::SiteIfHomepageNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::DomainNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::DomainIfHomepage => Tokenizer::default(),
            TextField::DomainNameIfHomepageNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::TitleIfHomepage => Tokenizer::default(),
            TextField::BacklinkText => Tokenizer::default(),
            TextField::Description => Tokenizer::default(),
            TextField::HostTopic => Tokenizer::default(),
            TextField::DmozDescription => Tokenizer::default(),
            TextField::SchemaOrgJson => Tokenizer::Identity(Identity {}),
            TextField::FlattenedSchemaOrgJson => Tokenizer::Json(JsonField),
            TextField::CleanBodyBigrams => Tokenizer::Bigram(BigramTokenizer::default()),
            TextField::TitleBigrams => Tokenizer::Bigram(BigramTokenizer::default()),
            TextField::CleanBodyTrigrams => Tokenizer::Trigram(TrigramTokenizer::default()),
            TextField::TitleTrigrams => Tokenizer::Trigram(TrigramTokenizer::default()),
        }
    }

    pub fn index_option(&self) -> IndexRecordOption {
        if self.has_pos() {
            IndexRecordOption::WithFreqsAndPositions
        } else {
            IndexRecordOption::WithFreqs
        }
    }

    pub fn has_pos(&self) -> bool {
        match self {
            TextField::Title => true,
            TextField::CleanBody => true,
            TextField::StemmedTitle => false,
            TextField::StemmedCleanBody => false,
            TextField::AllBody => false,
            TextField::Url => true,
            TextField::UrlNoTokenizer => false,
            TextField::Site => true,
            TextField::Domain => true,
            TextField::SiteNoTokenizer => false,
            TextField::SiteIfHomepageNoTokenizer => false,
            TextField::DomainNoTokenizer => false,
            TextField::DomainIfHomepage => false,
            TextField::DomainNameIfHomepageNoTokenizer => false,
            TextField::TitleIfHomepage => false,
            TextField::BacklinkText => false,
            TextField::Description => true,
            TextField::HostTopic => false,
            TextField::DmozDescription => true,
            TextField::SchemaOrgJson => false,
            TextField::FlattenedSchemaOrgJson => true,
            TextField::CleanBodyBigrams => false,
            TextField::TitleBigrams => false,
            TextField::CleanBodyTrigrams => false,
            TextField::TitleTrigrams => false,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            TextField::Title => "title",
            TextField::CleanBody => "body",
            TextField::Url => "url",
            TextField::UrlNoTokenizer => "url_no_tokenizer",
            TextField::Site => "site",
            TextField::Domain => "domain",
            TextField::SiteNoTokenizer => "site_no_tokenizer",
            TextField::SiteIfHomepageNoTokenizer => "site_if_homepage_no_tokenizer",
            TextField::DomainNoTokenizer => "domain_no_tokenizer",
            TextField::BacklinkText => "backlink_text",
            TextField::StemmedTitle => "stemmed_title",
            TextField::StemmedCleanBody => "stemmed_body",
            TextField::DomainIfHomepage => "domain_if_homepage",
            TextField::DomainNameIfHomepageNoTokenizer => "domain_name_if_homepage_no_tokenizer",
            TextField::Description => "description",
            TextField::TitleIfHomepage => "title_if_homepage",
            TextField::AllBody => "all_body",
            TextField::HostTopic => "host_topic",
            TextField::DmozDescription => "dmoz_description",
            TextField::SchemaOrgJson => "schema_org_json",
            TextField::FlattenedSchemaOrgJson => "flattened_schema_org_json",
            TextField::CleanBodyBigrams => "clean_body_bigrams",
            TextField::TitleBigrams => "title_bigrams",
            TextField::CleanBodyTrigrams => "clean_body_trigrams",
            TextField::TitleTrigrams => "title_trigrams",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FastField {
    IsHomepage,
    HostCentrality,
    PageCentrality,
    FetchTimeMs,
    LastUpdated,
    TrackerScore,
    Region,
    NumUrlTokens,
    NumTitleTokens,
    NumCleanBodyTokens,
    NumDescriptionTokens,
    NumSiteTokens,
    NumDomainTokens,
    SiteHash1,
    SiteHash2,
    UrlWithoutQueryHash1,
    UrlWithoutQueryHash2,
    TitleHash1,
    TitleHash2,
    UrlHash1,
    UrlHash2,
    DomainHash1,
    DomainHash2,
    PreComputedScore,
    HostNodeID,
    SimHash,
    NumFlattenedSchemaTokens,
}

impl FastField {
    pub fn name(&self) -> &str {
        match self {
            FastField::HostCentrality => "host_centrality",
            FastField::PageCentrality => "page_centrality",
            FastField::IsHomepage => "is_homepage",
            FastField::FetchTimeMs => "fetch_time_ms",
            FastField::LastUpdated => "last_updated",
            FastField::TrackerScore => "tracker_score",
            FastField::Region => "region",
            FastField::NumUrlTokens => "num_url_tokens",
            FastField::NumTitleTokens => "num_title_tokens",
            FastField::NumCleanBodyTokens => "num_clean_body_tokens",
            FastField::NumDescriptionTokens => "num_description_tokens",
            FastField::NumDomainTokens => "num_domain_tokens",
            FastField::NumSiteTokens => "num_site_tokens",
            FastField::NumFlattenedSchemaTokens => "num_flattened_schema_tokens",
            FastField::SiteHash1 => "site_hash1",
            FastField::SiteHash2 => "site_hash2",
            FastField::UrlWithoutQueryHash1 => "url_without_query_hash1",
            FastField::UrlWithoutQueryHash2 => "url_without_query_hash2",
            FastField::TitleHash1 => "title_hash1",
            FastField::TitleHash2 => "title_hash2",
            FastField::UrlHash1 => "url_hash1",
            FastField::UrlHash2 => "url_hash2",
            FastField::DomainHash1 => "domain_hash1",
            FastField::DomainHash2 => "domain_hash2",
            FastField::PreComputedScore => "pre_computed_score",
            FastField::HostNodeID => "host_node_id",
            FastField::SimHash => "sim_hash",
        }
    }
}

impl From<FastField> for usize {
    fn from(value: FastField) -> Self {
        value as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Field {
    Fast(FastField),
    Text(TextField),
}

pub static ALL_FIELDS: [Field; 52] = [
    Field::Text(TextField::Title),
    Field::Text(TextField::CleanBody),
    Field::Text(TextField::StemmedTitle),
    Field::Text(TextField::StemmedCleanBody),
    Field::Text(TextField::AllBody),
    Field::Text(TextField::Url),
    Field::Text(TextField::UrlNoTokenizer),
    Field::Text(TextField::Site),
    Field::Text(TextField::Domain),
    Field::Text(TextField::SiteNoTokenizer),
    Field::Text(TextField::SiteIfHomepageNoTokenizer),
    Field::Text(TextField::DomainNoTokenizer),
    Field::Text(TextField::DomainIfHomepage),
    Field::Text(TextField::DomainNameIfHomepageNoTokenizer),
    Field::Text(TextField::TitleIfHomepage),
    Field::Text(TextField::BacklinkText),
    Field::Text(TextField::Description),
    Field::Text(TextField::HostTopic),
    Field::Text(TextField::DmozDescription),
    Field::Text(TextField::SchemaOrgJson),
    Field::Text(TextField::FlattenedSchemaOrgJson),
    Field::Text(TextField::CleanBodyBigrams),
    Field::Text(TextField::TitleBigrams),
    Field::Text(TextField::CleanBodyTrigrams),
    Field::Text(TextField::TitleTrigrams),
    // FAST FIELDS
    Field::Fast(FastField::IsHomepage),
    Field::Fast(FastField::HostCentrality),
    Field::Fast(FastField::PageCentrality),
    Field::Fast(FastField::FetchTimeMs),
    Field::Fast(FastField::LastUpdated),
    Field::Fast(FastField::TrackerScore),
    Field::Fast(FastField::Region),
    Field::Fast(FastField::NumUrlTokens),
    Field::Fast(FastField::NumTitleTokens),
    Field::Fast(FastField::NumCleanBodyTokens),
    Field::Fast(FastField::NumDescriptionTokens),
    Field::Fast(FastField::NumDomainTokens),
    Field::Fast(FastField::NumSiteTokens),
    Field::Fast(FastField::NumFlattenedSchemaTokens),
    Field::Fast(FastField::SiteHash1),
    Field::Fast(FastField::SiteHash2),
    Field::Fast(FastField::UrlWithoutQueryHash1),
    Field::Fast(FastField::UrlWithoutQueryHash2),
    Field::Fast(FastField::TitleHash1),
    Field::Fast(FastField::TitleHash2),
    Field::Fast(FastField::UrlHash1),
    Field::Fast(FastField::UrlHash2),
    Field::Fast(FastField::DomainHash1),
    Field::Fast(FastField::DomainHash2),
    Field::Fast(FastField::PreComputedScore),
    Field::Fast(FastField::HostNodeID),
    Field::Fast(FastField::SimHash),
];

impl Field {
    fn default_text_options(&self) -> tantivy::schema::TextOptions {
        let tokenizer = self.as_text().unwrap().tokenizer();
        let option = self.as_text().unwrap().index_option();

        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(tokenizer.as_str())
                .set_index_option(option),
        )
    }

    pub fn has_pos(&self) -> bool {
        match self {
            Field::Fast(_) => false,
            Field::Text(text) => text.has_pos(),
        }
    }

    pub fn options(&self) -> IndexingOption {
        match self {
            Field::Text(TextField::Title) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextField::CleanBody) => IndexingOption::Text(self.default_text_options()),
            Field::Text(TextField::Url) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextField::UrlNoTokenizer) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::Site) => IndexingOption::Text(self.default_text_options()),
            Field::Text(TextField::SiteIfHomepageNoTokenizer) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::Domain) => IndexingOption::Text(self.default_text_options()),
            Field::Text(TextField::SiteNoTokenizer) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::DomainNoTokenizer) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::AllBody) => IndexingOption::Text(self.default_text_options()),
            Field::Text(TextField::DomainIfHomepage) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::TitleIfHomepage) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::DomainNameIfHomepageNoTokenizer) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::BacklinkText) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::StemmedTitle) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::StemmedCleanBody) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextField::Description) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextField::HostTopic) => {
                IndexingOption::Facet(tantivy::schema::FacetOptions::default())
            }
            Field::Text(TextField::DmozDescription) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextField::SchemaOrgJson) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextField::FlattenedSchemaOrgJson) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::CleanBodyBigrams) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::TitleBigrams) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::CleanBodyTrigrams) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::TitleTrigrams) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Fast(FastField::IsHomepage) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::Fast(FastField::HostCentrality) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::Fast(FastField::PageCentrality) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::Fast(FastField::FetchTimeMs) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::Fast(FastField::TrackerScore) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::Fast(FastField::LastUpdated) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_stored()
                    .set_indexed(),
            ),
            Field::Fast(FastField::Region) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_stored()
                    .set_indexed(),
            ),
            Field::Fast(FastField::NumCleanBodyTokens) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::Fast(FastField::NumDescriptionTokens) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::Fast(FastField::NumTitleTokens) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::Fast(FastField::NumUrlTokens) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::Fast(FastField::NumDomainTokens) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::Fast(FastField::NumSiteTokens) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::Fast(FastField::NumFlattenedSchemaTokens) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed(),
            ),
            Field::Fast(FastField::SiteHash1) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::SingleValue),
            ),
            Field::Fast(FastField::SiteHash2) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::SingleValue),
            ),
            Field::Fast(FastField::UrlWithoutQueryHash1) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::SingleValue),
            ),
            Field::Fast(FastField::UrlWithoutQueryHash2) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::SingleValue),
            ),
            Field::Fast(FastField::UrlHash1) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::SingleValue),
            ),
            Field::Fast(FastField::UrlHash2) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::SingleValue),
            ),
            Field::Fast(FastField::DomainHash1) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::SingleValue),
            ),
            Field::Fast(FastField::DomainHash2) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::SingleValue),
            ),
            Field::Fast(FastField::TitleHash1) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::SingleValue),
            ),
            Field::Fast(FastField::TitleHash2) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::SingleValue),
            ),
            Field::Fast(FastField::PreComputedScore) => IndexingOption::Float(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastField::HostNodeID) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastField::SimHash) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast(Cardinality::SingleValue)
                    .set_indexed()
                    .set_stored(),
            ),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Field::Text(t) => t.name(),
            Field::Fast(f) => f.name(),
        }
    }

    pub fn is_searchable(&self) -> bool {
        !matches!(
            self,
            Field::Text(TextField::BacklinkText)
                | Field::Text(TextField::HostTopic)
                | Field::Text(TextField::SchemaOrgJson)
                | Field::Text(TextField::CleanBodyBigrams)
                | Field::Text(TextField::TitleBigrams)
                | Field::Text(TextField::CleanBodyTrigrams)
                | Field::Text(TextField::TitleTrigrams)
        ) && !self.is_fast()
    }

    pub fn is_fast(&self) -> bool {
        matches!(self, Field::Fast(_))
    }

    pub fn as_text(&self) -> Option<TextField> {
        match self {
            Field::Fast(_) => None,
            Field::Text(field) => Some(*field),
        }
    }

    pub fn as_fast(&self) -> Option<FastField> {
        match self {
            Field::Fast(field) => Some(*field),
            Field::Text(_) => None,
        }
    }
}

pub fn create_schema() -> tantivy::schema::Schema {
    let mut builder = tantivy::schema::Schema::builder();

    for field in &ALL_FIELDS {
        match field.options() {
            IndexingOption::Text(options) => builder.add_text_field(field.name(), options),
            IndexingOption::Integer(options) => builder.add_u64_field(field.name(), options),
            IndexingOption::Float(options) => builder.add_f64_field(field.name(), options),
            IndexingOption::Facet(options) => builder.add_facet_field(field.name(), options),
        };
    }

    builder.build()
}

pub enum IndexingOption {
    Text(tantivy::schema::TextOptions),
    Integer(tantivy::schema::NumericOptions),
    Float(tantivy::schema::NumericOptions),
    Facet(tantivy::schema::FacetOptions),
}

pub enum DataType {
    U64,
    F64,
}

impl FastField {
    pub fn data_type(&self) -> DataType {
        match self {
            FastField::IsHomepage => DataType::U64,
            FastField::HostCentrality => DataType::U64,
            FastField::PageCentrality => DataType::U64,
            FastField::FetchTimeMs => DataType::U64,
            FastField::LastUpdated => DataType::U64,
            FastField::TrackerScore => DataType::U64,
            FastField::Region => DataType::U64,
            FastField::NumUrlTokens => DataType::U64,
            FastField::NumTitleTokens => DataType::U64,
            FastField::NumCleanBodyTokens => DataType::U64,
            FastField::NumDescriptionTokens => DataType::U64,
            FastField::NumDomainTokens => DataType::U64,
            FastField::NumSiteTokens => DataType::U64,
            FastField::NumFlattenedSchemaTokens => DataType::U64,
            FastField::SiteHash1 => DataType::U64,
            FastField::SiteHash2 => DataType::U64,
            FastField::UrlWithoutQueryHash1 => DataType::U64,
            FastField::UrlWithoutQueryHash2 => DataType::U64,
            FastField::TitleHash1 => DataType::U64,
            FastField::TitleHash2 => DataType::U64,
            FastField::UrlHash1 => DataType::U64,
            FastField::UrlHash2 => DataType::U64,
            FastField::DomainHash1 => DataType::U64,
            FastField::DomainHash2 => DataType::U64,
            FastField::PreComputedScore => DataType::F64,
            FastField::HostNodeID => DataType::U64,
            FastField::SimHash => DataType::U64,
        }
    }
}
