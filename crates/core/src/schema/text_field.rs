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

use strum::VariantArray;
use tantivy::schema::IndexRecordOption;

use crate::{
    enum_map::InsertEnumMapKey,
    tokenizer::{
        BigramTokenizer, Identity, JsonField, SiteOperatorUrlTokenizer, Tokenizer, TrigramTokenizer,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, VariantArray)]
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
    Keywords,
}

impl From<TextField> for usize {
    fn from(value: TextField) -> Self {
        value as usize
    }
}

impl TextField {
    pub fn ngram_size(&self) -> usize {
        match self {
            TextField::Title => 1,
            TextField::CleanBody => 1,
            TextField::StemmedTitle => 1,
            TextField::StemmedCleanBody => 1,
            TextField::AllBody => 1,
            TextField::Url => 1,
            TextField::UrlNoTokenizer => 1,
            TextField::UrlForSiteOperator => 1,
            TextField::SiteWithout => 1,
            TextField::Domain => 1,
            TextField::SiteNoTokenizer => 1,
            TextField::DomainNoTokenizer => 1,
            TextField::DomainNameNoTokenizer => 1,
            TextField::SiteIfHomepageNoTokenizer => 1,
            TextField::DomainIfHomepage => 1,
            TextField::DomainNameIfHomepageNoTokenizer => 1,
            TextField::DomainIfHomepageNoTokenizer => 1,
            TextField::TitleIfHomepage => 1,
            TextField::BacklinkText => 1,
            TextField::Description => 1,
            TextField::DmozDescription => 1,
            TextField::SchemaOrgJson => 1,
            TextField::FlattenedSchemaOrgJson => 1,
            TextField::CleanBodyBigrams => 2,
            TextField::TitleBigrams => 2,
            TextField::CleanBodyTrigrams => 3,
            TextField::TitleTrigrams => 3,
            TextField::MicroformatTags => 1,
            TextField::SafetyClassification => 1,
            TextField::InsertionTimestamp => 1,
            TextField::RecipeFirstIngredientTagId => 1,
            TextField::Keywords => 1,
        }
    }

    pub fn monogram_field(&self) -> TextField {
        match self {
            TextField::Title => TextField::Title,
            TextField::CleanBody => TextField::CleanBody,
            TextField::StemmedTitle => TextField::StemmedTitle,
            TextField::StemmedCleanBody => TextField::StemmedCleanBody,
            TextField::AllBody => TextField::AllBody,
            TextField::Url => TextField::Url,
            TextField::UrlNoTokenizer => TextField::UrlNoTokenizer,
            TextField::UrlForSiteOperator => TextField::UrlForSiteOperator,
            TextField::SiteWithout => TextField::SiteWithout,
            TextField::Domain => TextField::Domain,
            TextField::SiteNoTokenizer => TextField::SiteNoTokenizer,
            TextField::DomainNoTokenizer => TextField::DomainNoTokenizer,
            TextField::DomainNameNoTokenizer => TextField::DomainNameNoTokenizer,
            TextField::SiteIfHomepageNoTokenizer => TextField::SiteIfHomepageNoTokenizer,
            TextField::DomainIfHomepage => TextField::DomainIfHomepage,
            TextField::DomainNameIfHomepageNoTokenizer => {
                TextField::DomainNameIfHomepageNoTokenizer
            }
            TextField::DomainIfHomepageNoTokenizer => TextField::DomainIfHomepageNoTokenizer,
            TextField::TitleIfHomepage => TextField::TitleIfHomepage,
            TextField::BacklinkText => TextField::BacklinkText,
            TextField::Description => TextField::Description,
            TextField::DmozDescription => TextField::DmozDescription,
            TextField::SchemaOrgJson => TextField::SchemaOrgJson,
            TextField::FlattenedSchemaOrgJson => TextField::FlattenedSchemaOrgJson,
            TextField::CleanBodyBigrams => TextField::CleanBody,
            TextField::TitleBigrams => TextField::Title,
            TextField::CleanBodyTrigrams => TextField::CleanBody,
            TextField::TitleTrigrams => TextField::Title,
            TextField::MicroformatTags => TextField::MicroformatTags,
            TextField::SafetyClassification => TextField::SafetyClassification,
            TextField::InsertionTimestamp => TextField::InsertionTimestamp,
            TextField::RecipeFirstIngredientTagId => TextField::RecipeFirstIngredientTagId,
            TextField::Keywords => TextField::Keywords,
        }
    }

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
            TextField::Keywords => Tokenizer::default(),
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
            TextField::Keywords => false,
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
            TextField::Keywords => "keywords",
        }
    }
}

impl InsertEnumMapKey for TextField {
    fn into_usize(self) -> usize {
        self as usize
    }
}
