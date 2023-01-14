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

use crate::tokenizer::{Identity, JsonField, Tokenizer};

pub const FLOAT_SCALING: u64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TextField {
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
    /// this field is only set if the webpage is the homepage for the site. Allows us to boost
    TitleIfHomepage,
    BacklinkText,
    PrimaryImage,
    Description,
    HostTopic,
    DmozDescription,
    SchemaOrgJson,
    FlattenedSchemaOrgJson,
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
            TextField::Site => Tokenizer::default(),
            TextField::Domain => Tokenizer::default(),
            TextField::SiteNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::DomainNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::DomainIfHomepage => Tokenizer::default(),
            TextField::DomainNameIfHomepageNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::TitleIfHomepage => Tokenizer::default(),
            TextField::BacklinkText => Tokenizer::default(),
            TextField::PrimaryImage => Tokenizer::Identity(Identity {}),
            TextField::Description => Tokenizer::default(),
            TextField::HostTopic => Tokenizer::default(),
            TextField::DmozDescription => Tokenizer::default(),
            TextField::SchemaOrgJson => Tokenizer::Identity(Identity {}),
            TextField::FlattenedSchemaOrgJson => Tokenizer::Json(JsonField),
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
    SiteHash,
    UrlWithoutQueryHash,
    TitleHash,
    UrlHash,
    DomainHash,
    PreComputedScore,
    HostNodeID,
    SimHash,
    CrawlStability,
    NumFlattenedSchemaTokens,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Field {
    Fast(FastField),
    Text(TextField),
}

pub static ALL_FIELDS: [Field; 43] = [
    Field::Text(TextField::Title),
    Field::Text(TextField::CleanBody),
    Field::Text(TextField::StemmedTitle),
    Field::Text(TextField::StemmedCleanBody),
    Field::Text(TextField::AllBody),
    Field::Text(TextField::Url),
    Field::Text(TextField::Site),
    Field::Text(TextField::Domain),
    Field::Text(TextField::SiteNoTokenizer),
    Field::Text(TextField::DomainNoTokenizer),
    Field::Text(TextField::DomainIfHomepage),
    Field::Text(TextField::DomainNameIfHomepageNoTokenizer),
    Field::Text(TextField::TitleIfHomepage),
    Field::Text(TextField::BacklinkText),
    Field::Text(TextField::PrimaryImage),
    Field::Text(TextField::Description),
    Field::Text(TextField::HostTopic),
    Field::Text(TextField::DmozDescription),
    Field::Text(TextField::SchemaOrgJson),
    Field::Text(TextField::FlattenedSchemaOrgJson),
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
    Field::Fast(FastField::SiteHash),
    Field::Fast(FastField::UrlWithoutQueryHash),
    Field::Fast(FastField::TitleHash),
    Field::Fast(FastField::UrlHash),
    Field::Fast(FastField::DomainHash),
    Field::Fast(FastField::PreComputedScore),
    Field::Fast(FastField::HostNodeID),
    Field::Fast(FastField::SimHash),
    Field::Fast(FastField::CrawlStability),
];

impl Field {
    fn default_text_options(&self) -> tantivy::schema::TextOptions {
        let tokenizer = self.as_text().unwrap().tokenizer();
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(tokenizer.as_str())
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
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
            Field::Text(TextField::Site) => IndexingOption::Text(self.default_text_options()),
            Field::Text(TextField::Domain) => IndexingOption::Text(self.default_text_options()),
            Field::Text(TextField::SiteNoTokenizer) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::DomainNoTokenizer) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::AllBody) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
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
            Field::Text(TextField::PrimaryImage) => {
                IndexingOption::Bytes(BytesOptions::default().set_stored())
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
                IndexingOption::Text(self.default_text_options().set_stored())
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
            Field::Fast(FastField::SiteHash) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::MultiValues),
            ),
            Field::Fast(FastField::UrlWithoutQueryHash) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::MultiValues),
            ),
            Field::Fast(FastField::UrlHash) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::MultiValues),
            ),
            Field::Fast(FastField::DomainHash) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::MultiValues),
            ),
            Field::Fast(FastField::TitleHash) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::MultiValues),
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
            Field::Fast(FastField::CrawlStability) => IndexingOption::Integer(
                NumericOptions::default().set_fast(Cardinality::SingleValue),
            ),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Field::Text(TextField::Title) => "title",
            Field::Text(TextField::CleanBody) => "body",
            Field::Text(TextField::Url) => "url",
            Field::Text(TextField::Site) => "site",
            Field::Text(TextField::Domain) => "domain",
            Field::Text(TextField::SiteNoTokenizer) => "site_no_tokenizer",
            Field::Text(TextField::DomainNoTokenizer) => "domain_no_tokenizer",
            Field::Text(TextField::BacklinkText) => "backlink_text",
            Field::Text(TextField::StemmedTitle) => "stemmed_title",
            Field::Text(TextField::StemmedCleanBody) => "stemmed_body",
            Field::Text(TextField::DomainIfHomepage) => "domain_if_homepage",
            Field::Text(TextField::DomainNameIfHomepageNoTokenizer) => {
                "domain_name_if_homepage_no_tokenizer"
            }
            Field::Text(TextField::Description) => "description",
            Field::Text(TextField::PrimaryImage) => "primary_image_uuid",
            Field::Text(TextField::TitleIfHomepage) => "title_if_homepage",
            Field::Text(TextField::AllBody) => "all_body",
            Field::Text(TextField::HostTopic) => "host_topic",
            Field::Text(TextField::DmozDescription) => "dmoz_description",
            Field::Text(TextField::SchemaOrgJson) => "schema_org_json",
            Field::Text(TextField::FlattenedSchemaOrgJson) => "flattened_schema_org_json",
            // FAST FIELDS
            Field::Fast(FastField::HostCentrality) => "host_centrality",
            Field::Fast(FastField::PageCentrality) => "page_centrality",
            Field::Fast(FastField::IsHomepage) => "is_homepage",
            Field::Fast(FastField::FetchTimeMs) => "fetch_time_ms",
            Field::Fast(FastField::LastUpdated) => "last_updated",
            Field::Fast(FastField::TrackerScore) => "tracker_score",
            Field::Fast(FastField::Region) => "region",
            Field::Fast(FastField::NumUrlTokens) => "num_url_tokens",
            Field::Fast(FastField::NumTitleTokens) => "num_title_tokens",
            Field::Fast(FastField::NumCleanBodyTokens) => "num_clean_body_tokens",
            Field::Fast(FastField::NumDescriptionTokens) => "num_description_tokens",
            Field::Fast(FastField::NumDomainTokens) => "num_domain_tokens",
            Field::Fast(FastField::NumSiteTokens) => "num_site_tokens",
            Field::Fast(FastField::NumFlattenedSchemaTokens) => "num_flattened_schema_tokens",
            Field::Fast(FastField::SiteHash) => "site_hash",
            Field::Fast(FastField::UrlWithoutQueryHash) => "url_without_query_hash",
            Field::Fast(FastField::PreComputedScore) => "pre_computed_score",
            Field::Fast(FastField::TitleHash) => "title_hash",
            Field::Fast(FastField::UrlHash) => "url_hash",
            Field::Fast(FastField::DomainHash) => "domain_hash",
            Field::Fast(FastField::HostNodeID) => "host_node_id",
            Field::Fast(FastField::SimHash) => "sim_hash",
            Field::Fast(FastField::CrawlStability) => "crawl_stability",
        }
    }

    pub fn boost(&self) -> Option<f32> {
        match self {
            Field::Text(TextField::Site) => Some(2.0),
            Field::Text(TextField::TitleIfHomepage) => Some(3.0),
            Field::Text(TextField::DomainIfHomepage) => Some(6.0),
            Field::Text(TextField::DomainNameIfHomepageNoTokenizer) => Some(30.0),
            Field::Text(TextField::StemmedCleanBody) | Field::Text(TextField::StemmedTitle) => {
                Some(0.5)
            }
            Field::Text(TextField::CleanBody) => Some(4.0),
            Field::Text(TextField::DmozDescription) => Some(3.0),
            Field::Text(TextField::Title) => Some(10.0),
            Field::Text(TextField::Url) => Some(1.0),
            Field::Text(TextField::Domain) => Some(1.0),
            Field::Text(TextField::AllBody) => Some(0.01),
            Field::Text(TextField::HostTopic) => Some(1.0),
            Field::Text(TextField::BacklinkText) => Some(4.0),
            Field::Text(TextField::SiteNoTokenizer)
            | Field::Text(TextField::DomainNoTokenizer)
            | Field::Text(TextField::Description)
            | Field::Text(TextField::FlattenedSchemaOrgJson)
            | Field::Text(TextField::SchemaOrgJson)
            | Field::Text(TextField::PrimaryImage) => None,
            Field::Fast(_) => None,
        }
    }

    pub fn is_searchable(&self) -> bool {
        !matches!(
            self,
            Field::Text(TextField::PrimaryImage)
                | Field::Text(TextField::BacklinkText)
                | Field::Text(TextField::HostTopic)
                | Field::Text(TextField::SchemaOrgJson)
        ) && !self.is_fast()
    }

    pub fn is_fast(&self) -> bool {
        matches!(self, Field::Fast(_))
    }

    pub fn from_name(name: String) -> Option<Field> {
        match name.as_str() {
            "title" => Some(Field::Text(TextField::Title)),
            "body" => Some(Field::Text(TextField::CleanBody)),
            "url" => Some(Field::Text(TextField::Url)),
            "site" => Some(Field::Text(TextField::Site)),
            "backlink_text" => Some(Field::Text(TextField::BacklinkText)),
            "stemmed_title" => Some(Field::Text(TextField::StemmedTitle)),
            "stemmed_body" => Some(Field::Text(TextField::StemmedCleanBody)),
            "domain" => Some(Field::Text(TextField::Domain)),
            "domain_if_homepage" => Some(Field::Text(TextField::DomainIfHomepage)),
            "primary_image_uuid" => Some(Field::Text(TextField::PrimaryImage)),
            "domain_name_if_homepage_no_tokenizer" => {
                Some(Field::Text(TextField::DomainNameIfHomepageNoTokenizer))
            }
            "description" => Some(Field::Text(TextField::Description)),
            "all_body" => Some(Field::Text(TextField::AllBody)),
            "title_if_homepage" => Some(Field::Text(TextField::TitleIfHomepage)),
            "host_topic" => Some(Field::Text(TextField::HostTopic)),
            "dmoz_description" => Some(Field::Text(TextField::DmozDescription)),
            "schema_org_json" => Some(Field::Text(TextField::SchemaOrgJson)),
            "flattened_schema_org_json" => Some(Field::Text(TextField::FlattenedSchemaOrgJson)),
            "host_centrality" => Some(Field::Fast(FastField::HostCentrality)),
            "page_centrality" => Some(Field::Fast(FastField::PageCentrality)),
            "is_homepage" => Some(Field::Fast(FastField::IsHomepage)),
            "fetch_time_ms" => Some(Field::Fast(FastField::FetchTimeMs)),
            "last_updated" => Some(Field::Fast(FastField::LastUpdated)),
            "tracker_score" => Some(Field::Fast(FastField::TrackerScore)),
            "region" => Some(Field::Fast(FastField::Region)),
            "site_hash" => Some(Field::Fast(FastField::SiteHash)),
            "url_without_query_hash" => Some(Field::Fast(FastField::UrlWithoutQueryHash)),
            "pre_computed_score" => Some(Field::Fast(FastField::PreComputedScore)),
            "url_hash" => Some(Field::Fast(FastField::UrlHash)),
            "domain_hash" => Some(Field::Fast(FastField::DomainHash)),
            "title_hash" => Some(Field::Fast(FastField::TitleHash)),
            "host_node_id" => Some(Field::Fast(FastField::HostNodeID)),
            "sim_hash" => Some(Field::Fast(FastField::SimHash)),
            "crawl_stability" => Some(Field::Fast(FastField::CrawlStability)),
            _ => None,
        }
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
            IndexingOption::Bytes(options) => builder.add_bytes_field(field.name(), options),
            IndexingOption::Facet(options) => builder.add_facet_field(field.name(), options),
        };
    }

    builder.build()
}

pub enum IndexingOption {
    Text(tantivy::schema::TextOptions),
    Integer(tantivy::schema::NumericOptions),
    Float(tantivy::schema::NumericOptions),
    Bytes(tantivy::schema::BytesOptions),
    Facet(tantivy::schema::FacetOptions),
}

pub enum DataType {
    U64,
    U64s,
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
            FastField::SiteHash => DataType::U64s,
            FastField::UrlWithoutQueryHash => DataType::U64s,
            FastField::TitleHash => DataType::U64s,
            FastField::UrlHash => DataType::U64s,
            FastField::DomainHash => DataType::U64s,
            FastField::PreComputedScore => DataType::F64,
            FastField::HostNodeID => DataType::U64,
            FastField::SimHash => DataType::U64,
            FastField::CrawlStability => DataType::U64,
        }
    }
}
