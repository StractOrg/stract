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

use tantivy::schema::{IndexRecordOption, NumericOptions, TextFieldIndexing, TextOptions};

use crate::tokenizer::{
    BigramTokenizer, Identity, JsonField, SiteOperatorUrlTokenizer, Tokenizer, TrigramTokenizer,
};

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
    UrlForSiteOperator,
    SiteWithout,
    Domain,
    SiteNoTokenizer,
    DomainNoTokenizer,
    DomainNameNoTokenizer,
    /// this field is only set if the webpage is the homepage for the site. Allows us to boost
    SiteIfHomepageNoTokenizer,
    /// this field is only set if the webpage is the homepage for the site. Allows us to boost
    DomainIfHomepage,
    /// this field is only set if the webpage is the homepage for the site. Allows us to boost
    DomainNameIfHomepageNoTokenizer,
    /// this field is only set if the webpage is the homepage for the site. Allows us to boost
    DomainIfHomepageNoTokenizer,
    /// this field is only set if the webpage is the homepage for the site. Allows us to boost
    TitleIfHomepage,
    BacklinkText,
    Description,
    DmozDescription,
    SchemaOrgJson,
    FlattenedSchemaOrgJson,
    CleanBodyBigrams,
    TitleBigrams,
    CleanBodyTrigrams,
    TitleTrigrams,
    MicroformatTags,
    /// can either be NSFW or SFW (see safety classifier)
    SafetyClassification,
    InsertionTimestamp,
    RecipeFirstIngredientTagId,
}

impl From<TextField> for usize {
    fn from(value: TextField) -> Self {
        value as usize
    }
}

impl TextField {
    pub fn query_tokenizer(&self) -> Tokenizer {
        match self {
            TextField::TitleBigrams => Tokenizer::default(),
            TextField::CleanBodyBigrams => Tokenizer::default(),
            TextField::TitleTrigrams => Tokenizer::default(),
            TextField::CleanBodyTrigrams => Tokenizer::default(),
            _ => self.indexing_tokenizer(),
        }
    }

    pub fn indexing_tokenizer(&self) -> Tokenizer {
        match self {
            TextField::Title => Tokenizer::default(),
            TextField::CleanBody => Tokenizer::default(),
            TextField::StemmedTitle => Tokenizer::new_stemmed(),
            TextField::StemmedCleanBody => Tokenizer::new_stemmed(),
            TextField::AllBody => Tokenizer::default(),
            TextField::Url => Tokenizer::default(),
            TextField::UrlNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::UrlForSiteOperator => Tokenizer::SiteOperator(SiteOperatorUrlTokenizer),
            TextField::SiteWithout => Tokenizer::default(),
            TextField::Domain => Tokenizer::default(),
            TextField::SiteNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::SiteIfHomepageNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::DomainNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::DomainNameNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::DomainIfHomepage => Tokenizer::default(),
            TextField::DomainNameIfHomepageNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::DomainIfHomepageNoTokenizer => Tokenizer::Identity(Identity {}),
            TextField::TitleIfHomepage => Tokenizer::default(),
            TextField::BacklinkText => Tokenizer::default(),
            TextField::Description => Tokenizer::default(),
            TextField::DmozDescription => Tokenizer::default(),
            TextField::SchemaOrgJson => Tokenizer::Identity(Identity {}),
            TextField::FlattenedSchemaOrgJson => Tokenizer::Json(JsonField),
            TextField::CleanBodyBigrams => Tokenizer::Bigram(BigramTokenizer::default()),
            TextField::TitleBigrams => Tokenizer::Bigram(BigramTokenizer::default()),
            TextField::CleanBodyTrigrams => Tokenizer::Trigram(TrigramTokenizer::default()),
            TextField::TitleTrigrams => Tokenizer::Trigram(TrigramTokenizer::default()),
            TextField::MicroformatTags => Tokenizer::default(),
            TextField::SafetyClassification => Tokenizer::Identity(Identity {}),
            TextField::InsertionTimestamp => Tokenizer::Identity(Identity {}),
            TextField::RecipeFirstIngredientTagId => Tokenizer::Identity(Identity {}),
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
            TextField::UrlForSiteOperator => true,
            TextField::SiteWithout => true,
            TextField::Domain => true,
            TextField::SiteNoTokenizer => false,
            TextField::SiteIfHomepageNoTokenizer => false,
            TextField::DomainNoTokenizer => false,
            TextField::DomainNameNoTokenizer => false,
            TextField::DomainIfHomepage => false,
            TextField::DomainNameIfHomepageNoTokenizer => false,
            TextField::DomainIfHomepageNoTokenizer => false,
            TextField::TitleIfHomepage => false,
            TextField::BacklinkText => false,
            TextField::Description => true,
            TextField::DmozDescription => true,
            TextField::SchemaOrgJson => false,
            TextField::FlattenedSchemaOrgJson => true,
            TextField::CleanBodyBigrams => false,
            TextField::TitleBigrams => false,
            TextField::CleanBodyTrigrams => false,
            TextField::TitleTrigrams => false,
            TextField::MicroformatTags => true,
            TextField::SafetyClassification => false,
            TextField::InsertionTimestamp => false,
            TextField::RecipeFirstIngredientTagId => false,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            TextField::Title => "title",
            TextField::CleanBody => "body",
            TextField::Url => "url",
            TextField::UrlNoTokenizer => "url_no_tokenizer",
            TextField::UrlForSiteOperator => "url_for_site_operator",
            TextField::SiteWithout => "site",
            TextField::Domain => "domain",
            TextField::SiteNoTokenizer => "site_no_tokenizer",
            TextField::SiteIfHomepageNoTokenizer => "site_if_homepage_no_tokenizer",
            TextField::DomainNoTokenizer => "domain_no_tokenizer",
            TextField::DomainNameNoTokenizer => "domain_name_no_tokenizer",
            TextField::BacklinkText => "backlink_text",
            TextField::StemmedTitle => "stemmed_title",
            TextField::StemmedCleanBody => "stemmed_body",
            TextField::DomainIfHomepage => "domain_if_homepage",
            TextField::DomainNameIfHomepageNoTokenizer => "domain_name_if_homepage_no_tokenizer",
            TextField::DomainIfHomepageNoTokenizer => "domain_if_homepage_no_tokenizer",
            TextField::Description => "description",
            TextField::TitleIfHomepage => "title_if_homepage",
            TextField::AllBody => "all_body",
            TextField::DmozDescription => "dmoz_description",
            TextField::SchemaOrgJson => "schema_org_json",
            TextField::FlattenedSchemaOrgJson => "flattened_schema_org_json",
            TextField::CleanBodyBigrams => "clean_body_bigrams",
            TextField::TitleBigrams => "title_bigrams",
            TextField::CleanBodyTrigrams => "clean_body_trigrams",
            TextField::TitleTrigrams => "title_trigrams",
            TextField::MicroformatTags => "microformat_tags",
            TextField::SafetyClassification => "safety_classification",
            TextField::InsertionTimestamp => "insertion_timestamp",
            TextField::RecipeFirstIngredientTagId => "recipe_first_ingredient_tag_id",
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
    NumUrlForSiteOperatorTokens,
    NumDomainTokens,
    NumMicroformatTagsTokens,
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
    UrlWithoutTldHash1,
    UrlWithoutTldHash2,
    PreComputedScore,
    HostNodeID,
    SimHash,
    NumFlattenedSchemaTokens,
    NumPathAndQuerySlashes,
    NumPathAndQueryDigits,
    LikelyHasAds,
    LikelyHasPaywall,
    LinkDensity,
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
            FastField::NumUrlForSiteOperatorTokens => "num_url_for_site_operator_tokens",
            FastField::NumFlattenedSchemaTokens => "num_flattened_schema_tokens",
            FastField::NumMicroformatTagsTokens => "num_microformat_tags_tokens",
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
            FastField::UrlWithoutTldHash1 => "url_without_tld_hash1",
            FastField::UrlWithoutTldHash2 => "url_without_tld_hash2",
            FastField::PreComputedScore => "pre_computed_score",
            FastField::HostNodeID => "host_node_id",
            FastField::SimHash => "sim_hash",
            FastField::NumPathAndQuerySlashes => "num_path_and_query_slashes",
            FastField::NumPathAndQueryDigits => "num_path_and_query_digits",
            FastField::LikelyHasAds => "likely_has_ads",
            FastField::LikelyHasPaywall => "likely_has_paywall",
            FastField::LinkDensity => "link_density",
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

static ALL_FIELDS: [Field; 64] = [
    Field::Text(TextField::Title),
    Field::Text(TextField::CleanBody),
    Field::Text(TextField::StemmedTitle),
    Field::Text(TextField::StemmedCleanBody),
    Field::Text(TextField::AllBody),
    Field::Text(TextField::Url),
    Field::Text(TextField::UrlNoTokenizer),
    Field::Text(TextField::UrlForSiteOperator),
    Field::Text(TextField::SiteWithout),
    Field::Text(TextField::Domain),
    Field::Text(TextField::SiteNoTokenizer),
    Field::Text(TextField::SiteIfHomepageNoTokenizer),
    Field::Text(TextField::DomainNoTokenizer),
    Field::Text(TextField::DomainNameNoTokenizer),
    Field::Text(TextField::DomainIfHomepage),
    Field::Text(TextField::DomainNameIfHomepageNoTokenizer),
    Field::Text(TextField::DomainIfHomepageNoTokenizer),
    Field::Text(TextField::TitleIfHomepage),
    Field::Text(TextField::BacklinkText),
    Field::Text(TextField::Description),
    Field::Text(TextField::DmozDescription),
    Field::Text(TextField::SchemaOrgJson),
    Field::Text(TextField::FlattenedSchemaOrgJson),
    Field::Text(TextField::CleanBodyBigrams),
    Field::Text(TextField::TitleBigrams),
    Field::Text(TextField::CleanBodyTrigrams),
    Field::Text(TextField::TitleTrigrams),
    Field::Text(TextField::MicroformatTags),
    Field::Text(TextField::SafetyClassification),
    Field::Text(TextField::InsertionTimestamp),
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
    Field::Fast(FastField::NumUrlForSiteOperatorTokens),
    Field::Fast(FastField::NumFlattenedSchemaTokens),
    Field::Fast(FastField::NumMicroformatTagsTokens),
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
    Field::Fast(FastField::UrlWithoutTldHash1),
    Field::Fast(FastField::UrlWithoutTldHash2),
    Field::Fast(FastField::PreComputedScore),
    Field::Fast(FastField::HostNodeID),
    Field::Fast(FastField::SimHash),
    Field::Fast(FastField::NumPathAndQuerySlashes),
    Field::Fast(FastField::NumPathAndQueryDigits),
    Field::Fast(FastField::LikelyHasAds),
    Field::Fast(FastField::LikelyHasPaywall),
];

impl Field {
    #[inline]
    pub fn get(field_id: usize) -> Option<&'static Field> {
        ALL_FIELDS.get(field_id)
    }

    #[inline]
    pub fn all() -> impl Iterator<Item = &'static Field> {
        ALL_FIELDS.iter()
    }
    fn default_text_options(&self) -> tantivy::schema::TextOptions {
        let tokenizer = self.as_text().unwrap().indexing_tokenizer();
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
            Field::Text(TextField::UrlForSiteOperator) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::SiteWithout) => {
                IndexingOption::Text(self.default_text_options())
            }
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
            Field::Text(TextField::DomainNameNoTokenizer) => {
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
            Field::Text(TextField::DomainIfHomepageNoTokenizer) => {
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
            Field::Text(TextField::MicroformatTags) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::SafetyClassification) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextField::RecipeFirstIngredientTagId) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextField::InsertionTimestamp) => {
                IndexingOption::DateTime(tantivy::schema::DateOptions::default().set_indexed())
            }
            Field::Fast(FastField::IsHomepage) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::HostCentrality) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::PageCentrality) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::FetchTimeMs) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::TrackerScore) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::LastUpdated) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_stored()
                    .set_indexed(),
            ),
            Field::Fast(FastField::Region) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_stored()
                    .set_indexed(),
            ),
            Field::Fast(FastField::NumCleanBodyTokens) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::NumDescriptionTokens) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::NumTitleTokens) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::NumMicroformatTagsTokens) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::NumUrlTokens) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::NumDomainTokens) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::NumUrlForSiteOperatorTokens) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::NumFlattenedSchemaTokens) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::SiteHash1) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastField::SiteHash2) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastField::UrlWithoutQueryHash1) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastField::UrlWithoutQueryHash2) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastField::UrlHash1) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastField::UrlHash2) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastField::UrlWithoutTldHash1) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastField::UrlWithoutTldHash2) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastField::DomainHash1) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastField::DomainHash2) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastField::TitleHash1) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastField::TitleHash2) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastField::PreComputedScore) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastField::HostNodeID) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastField::SimHash) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastField::NumPathAndQuerySlashes) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastField::NumPathAndQueryDigits) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastField::LikelyHasAds) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastField::LikelyHasPaywall) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastField::LinkDensity) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_stored())
            }
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Field::Text(t) => t.name(),
            Field::Fast(f) => f.name(),
        }
    }

    /// Whether or not the field should be included
    /// in the fields that the `Query` searches.
    ///
    /// The fields can still be searched by manually
    /// constructing a tantivy query.
    pub fn is_searchable(&self) -> bool {
        !matches!(
            self,
            Field::Text(TextField::BacklinkText)
                | Field::Text(TextField::SchemaOrgJson)
                | Field::Text(TextField::MicroformatTags)
                | Field::Text(TextField::SafetyClassification)
                | Field::Text(TextField::FlattenedSchemaOrgJson)
                | Field::Text(TextField::UrlForSiteOperator)
                | Field::Text(TextField::Description)
                | Field::Text(TextField::DmozDescription)
                | Field::Text(TextField::SiteIfHomepageNoTokenizer)
                | Field::Text(TextField::DomainIfHomepage)
                | Field::Text(TextField::DomainNameIfHomepageNoTokenizer)
                | Field::Text(TextField::DomainIfHomepageNoTokenizer)
                | Field::Text(TextField::TitleIfHomepage)
                | Field::Text(TextField::SiteWithout) // will match url
                | Field::Text(TextField::Domain) // will match url
                | Field::Text(TextField::InsertionTimestamp)
                | Field::Text(TextField::RecipeFirstIngredientTagId)
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
            IndexingOption::DateTime(options) => builder.add_date_field(field.name(), options),
        };
    }

    builder.build()
}

pub enum IndexingOption {
    Text(tantivy::schema::TextOptions),
    Integer(tantivy::schema::NumericOptions),
    DateTime(tantivy::schema::DateOptions),
}

pub enum DataType {
    U64,
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
            FastField::NumMicroformatTagsTokens => DataType::U64,
            FastField::NumCleanBodyTokens => DataType::U64,
            FastField::NumDescriptionTokens => DataType::U64,
            FastField::NumDomainTokens => DataType::U64,
            FastField::NumUrlForSiteOperatorTokens => DataType::U64,
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
            FastField::UrlWithoutTldHash1 => DataType::U64,
            FastField::UrlWithoutTldHash2 => DataType::U64,
            FastField::PreComputedScore => DataType::U64,
            FastField::HostNodeID => DataType::U64,
            FastField::SimHash => DataType::U64,
            FastField::NumPathAndQuerySlashes => DataType::U64,
            FastField::NumPathAndQueryDigits => DataType::U64,
            FastField::LikelyHasAds => DataType::U64,
            FastField::LikelyHasPaywall => DataType::U64,
            FastField::LinkDensity => DataType::U64,
        }
    }
}
