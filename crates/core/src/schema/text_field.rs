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

use enum_dispatch::enum_dispatch;
use strum::{EnumDiscriminants, VariantArray};
use tantivy::schema::IndexRecordOption;

use crate::{
    enum_map::InsertEnumMapKey,
    from_discriminant,
    tokenizer::{
        BigramTokenizer, Identity, JsonField, SiteOperatorUrlTokenizer, Tokenizer, TrigramTokenizer,
    },
};

#[enum_dispatch]
pub trait TextField: Clone + Copy + std::fmt::Debug + PartialEq + Eq + std::hash::Hash {
    fn name(&self) -> &str;
}

#[enum_dispatch(TextField)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumDiscriminants)]
#[strum_discriminants(derive(VariantArray))]
pub enum TextFieldEnum {
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

from_discriminant!(TextFieldEnumDiscriminants => TextFieldEnum,
[
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
    SiteIfHomepageNoTokenizer,
    DomainIfHomepage,
    DomainNameIfHomepageNoTokenizer,
    DomainIfHomepageNoTokenizer,
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
    SafetyClassification,
    InsertionTimestamp,
    RecipeFirstIngredientTagId,
    Keywords,
]);

impl TextFieldEnum {
    pub fn num_variants() -> usize {
        TextFieldEnumDiscriminants::VARIANTS.len()
    }

    pub fn all() -> impl Iterator<Item = TextFieldEnum> {
        TextFieldEnumDiscriminants::VARIANTS
            .iter()
            .copied()
            .map(|v| v.into())
    }

    pub fn get(field_id: usize) -> Option<TextFieldEnum> {
        TextFieldEnumDiscriminants::VARIANTS
            .get(field_id)
            .copied()
            .map(TextFieldEnum::from)
    }

    pub fn ngram_size(&self) -> usize {
        match self {
            TextFieldEnum::Title(_) => 1,
            TextFieldEnum::CleanBody(_) => 1,
            TextFieldEnum::StemmedTitle(_) => 1,
            TextFieldEnum::StemmedCleanBody(_) => 1,
            TextFieldEnum::AllBody(_) => 1,
            TextFieldEnum::Url(_) => 1,
            TextFieldEnum::UrlNoTokenizer(_) => 1,
            TextFieldEnum::UrlForSiteOperator(_) => 1,
            TextFieldEnum::SiteWithout(_) => 1,
            TextFieldEnum::Domain(_) => 1,
            TextFieldEnum::SiteNoTokenizer(_) => 1,
            TextFieldEnum::DomainNoTokenizer(_) => 1,
            TextFieldEnum::DomainNameNoTokenizer(_) => 1,
            TextFieldEnum::SiteIfHomepageNoTokenizer(_) => 1,
            TextFieldEnum::DomainIfHomepage(_) => 1,
            TextFieldEnum::DomainNameIfHomepageNoTokenizer(_) => 1,
            TextFieldEnum::DomainIfHomepageNoTokenizer(_) => 1,
            TextFieldEnum::TitleIfHomepage(_) => 1,
            TextFieldEnum::BacklinkText(_) => 1,
            TextFieldEnum::Description(_) => 1,
            TextFieldEnum::DmozDescription(_) => 1,
            TextFieldEnum::SchemaOrgJson(_) => 1,
            TextFieldEnum::FlattenedSchemaOrgJson(_) => 1,
            TextFieldEnum::CleanBodyBigrams(_) => 2,
            TextFieldEnum::TitleBigrams(_) => 2,
            TextFieldEnum::CleanBodyTrigrams(_) => 3,
            TextFieldEnum::TitleTrigrams(_) => 3,
            TextFieldEnum::MicroformatTags(_) => 1,
            TextFieldEnum::SafetyClassification(_) => 1,
            TextFieldEnum::InsertionTimestamp(_) => 1,
            TextFieldEnum::RecipeFirstIngredientTagId(_) => 1,
            TextFieldEnum::Keywords(_) => 1,
        }
    }

    pub fn monogram_field(&self) -> TextFieldEnum {
        match self {
            TextFieldEnum::Title(_) => Title.into(),
            TextFieldEnum::CleanBody(_) => CleanBody.into(),
            TextFieldEnum::StemmedTitle(_) => StemmedTitle.into(),
            TextFieldEnum::StemmedCleanBody(_) => StemmedCleanBody.into(),
            TextFieldEnum::AllBody(_) => AllBody.into(),
            TextFieldEnum::Url(_) => Url.into(),
            TextFieldEnum::UrlNoTokenizer(_) => UrlNoTokenizer.into(),
            TextFieldEnum::UrlForSiteOperator(_) => UrlForSiteOperator.into(),
            TextFieldEnum::SiteWithout(_) => SiteWithout.into(),
            TextFieldEnum::Domain(_) => Domain.into(),
            TextFieldEnum::SiteNoTokenizer(_) => SiteNoTokenizer.into(),
            TextFieldEnum::DomainNoTokenizer(_) => DomainNoTokenizer.into(),
            TextFieldEnum::DomainNameNoTokenizer(_) => DomainNameNoTokenizer.into(),
            TextFieldEnum::SiteIfHomepageNoTokenizer(_) => SiteIfHomepageNoTokenizer.into(),
            TextFieldEnum::DomainIfHomepage(_) => DomainIfHomepage.into(),
            TextFieldEnum::DomainNameIfHomepageNoTokenizer(_) => {
                DomainNameIfHomepageNoTokenizer.into()
            }
            TextFieldEnum::DomainIfHomepageNoTokenizer(_) => DomainIfHomepageNoTokenizer.into(),
            TextFieldEnum::TitleIfHomepage(_) => TitleIfHomepage.into(),
            TextFieldEnum::BacklinkText(_) => BacklinkText.into(),
            TextFieldEnum::Description(_) => Description.into(),
            TextFieldEnum::DmozDescription(_) => DmozDescription.into(),
            TextFieldEnum::SchemaOrgJson(_) => SchemaOrgJson.into(),
            TextFieldEnum::FlattenedSchemaOrgJson(_) => FlattenedSchemaOrgJson.into(),
            TextFieldEnum::CleanBodyBigrams(_) => CleanBody.into(),
            TextFieldEnum::TitleBigrams(_) => Title.into(),
            TextFieldEnum::CleanBodyTrigrams(_) => CleanBody.into(),
            TextFieldEnum::TitleTrigrams(_) => Title.into(),
            TextFieldEnum::MicroformatTags(_) => MicroformatTags.into(),
            TextFieldEnum::SafetyClassification(_) => SafetyClassification.into(),
            TextFieldEnum::InsertionTimestamp(_) => InsertionTimestamp.into(),
            TextFieldEnum::RecipeFirstIngredientTagId(_) => RecipeFirstIngredientTagId.into(),
            TextFieldEnum::Keywords(_) => Keywords.into(),
        }
    }

    pub fn query_tokenizer(&self) -> Tokenizer {
        match self {
            TextFieldEnum::TitleBigrams(_) => Tokenizer::default(),
            TextFieldEnum::CleanBodyBigrams(_) => Tokenizer::default(),
            TextFieldEnum::TitleTrigrams(_) => Tokenizer::default(),
            TextFieldEnum::CleanBodyTrigrams(_) => Tokenizer::default(),
            _ => self.indexing_tokenizer(),
        }
    }

    pub fn indexing_tokenizer(&self) -> Tokenizer {
        match self {
            TextFieldEnum::Title(_) => Tokenizer::default(),
            TextFieldEnum::CleanBody(_) => Tokenizer::default(),
            TextFieldEnum::StemmedTitle(_) => Tokenizer::new_stemmed(),
            TextFieldEnum::StemmedCleanBody(_) => Tokenizer::new_stemmed(),
            TextFieldEnum::AllBody(_) => Tokenizer::default(),
            TextFieldEnum::Url(_) => Tokenizer::default(),
            TextFieldEnum::UrlNoTokenizer(_) => Tokenizer::Identity(Identity {}),
            TextFieldEnum::UrlForSiteOperator(_) => {
                Tokenizer::SiteOperator(SiteOperatorUrlTokenizer)
            }
            TextFieldEnum::SiteWithout(_) => Tokenizer::default(),
            TextFieldEnum::Domain(_) => Tokenizer::default(),
            TextFieldEnum::SiteNoTokenizer(_) => Tokenizer::Identity(Identity {}),
            TextFieldEnum::SiteIfHomepageNoTokenizer(_) => Tokenizer::Identity(Identity {}),
            TextFieldEnum::DomainNoTokenizer(_) => Tokenizer::Identity(Identity {}),
            TextFieldEnum::DomainNameNoTokenizer(_) => Tokenizer::Identity(Identity {}),
            TextFieldEnum::DomainIfHomepage(_) => Tokenizer::default(),
            TextFieldEnum::DomainNameIfHomepageNoTokenizer(_) => Tokenizer::Identity(Identity {}),
            TextFieldEnum::DomainIfHomepageNoTokenizer(_) => Tokenizer::Identity(Identity {}),
            TextFieldEnum::TitleIfHomepage(_) => Tokenizer::default(),
            TextFieldEnum::BacklinkText(_) => Tokenizer::default(),
            TextFieldEnum::Description(_) => Tokenizer::default(),
            TextFieldEnum::DmozDescription(_) => Tokenizer::default(),
            TextFieldEnum::SchemaOrgJson(_) => Tokenizer::Identity(Identity {}),
            TextFieldEnum::FlattenedSchemaOrgJson(_) => Tokenizer::Json(JsonField),
            TextFieldEnum::CleanBodyBigrams(_) => Tokenizer::Bigram(BigramTokenizer::default()),
            TextFieldEnum::TitleBigrams(_) => Tokenizer::Bigram(BigramTokenizer::default()),
            TextFieldEnum::CleanBodyTrigrams(_) => Tokenizer::Trigram(TrigramTokenizer::default()),
            TextFieldEnum::TitleTrigrams(_) => Tokenizer::Trigram(TrigramTokenizer::default()),
            TextFieldEnum::MicroformatTags(_) => Tokenizer::default(),
            TextFieldEnum::SafetyClassification(_) => Tokenizer::Identity(Identity {}),
            TextFieldEnum::InsertionTimestamp(_) => Tokenizer::Identity(Identity {}),
            TextFieldEnum::RecipeFirstIngredientTagId(_) => Tokenizer::Identity(Identity {}),
            TextFieldEnum::Keywords(_) => Tokenizer::default(),
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
            TextFieldEnum::Title(_) => true,
            TextFieldEnum::CleanBody(_) => true,
            TextFieldEnum::StemmedTitle(_) => false,
            TextFieldEnum::StemmedCleanBody(_) => false,
            TextFieldEnum::AllBody(_) => false,
            TextFieldEnum::Url(_) => true,
            TextFieldEnum::UrlNoTokenizer(_) => false,
            TextFieldEnum::UrlForSiteOperator(_) => true,
            TextFieldEnum::SiteWithout(_) => true,
            TextFieldEnum::Domain(_) => true,
            TextFieldEnum::SiteNoTokenizer(_) => false,
            TextFieldEnum::SiteIfHomepageNoTokenizer(_) => false,
            TextFieldEnum::DomainNoTokenizer(_) => false,
            TextFieldEnum::DomainNameNoTokenizer(_) => false,
            TextFieldEnum::DomainIfHomepage(_) => false,
            TextFieldEnum::DomainNameIfHomepageNoTokenizer(_) => false,
            TextFieldEnum::DomainIfHomepageNoTokenizer(_) => false,
            TextFieldEnum::TitleIfHomepage(_) => false,
            TextFieldEnum::BacklinkText(_) => false,
            TextFieldEnum::Description(_) => true,
            TextFieldEnum::DmozDescription(_) => true,
            TextFieldEnum::SchemaOrgJson(_) => false,
            TextFieldEnum::FlattenedSchemaOrgJson(_) => true,
            TextFieldEnum::CleanBodyBigrams(_) => false,
            TextFieldEnum::TitleBigrams(_) => false,
            TextFieldEnum::CleanBodyTrigrams(_) => false,
            TextFieldEnum::TitleTrigrams(_) => false,
            TextFieldEnum::MicroformatTags(_) => true,
            TextFieldEnum::SafetyClassification(_) => false,
            TextFieldEnum::InsertionTimestamp(_) => false,
            TextFieldEnum::RecipeFirstIngredientTagId(_) => false,
            TextFieldEnum::Keywords(_) => false,
        }
    }
}

impl InsertEnumMapKey for TextFieldEnum {
    fn into_usize(self) -> usize {
        TextFieldEnumDiscriminants::from(self) as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Title;
impl TextField for Title {
    fn name(&self) -> &str {
        "title"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CleanBody;
impl TextField for CleanBody {
    fn name(&self) -> &str {
        "body"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StemmedTitle;
impl TextField for StemmedTitle {
    fn name(&self) -> &str {
        "stemmed_title"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StemmedCleanBody;
impl TextField for StemmedCleanBody {
    fn name(&self) -> &str {
        "stemmed_body"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AllBody;
impl TextField for AllBody {
    fn name(&self) -> &str {
        "all_body"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Url;
impl TextField for Url {
    fn name(&self) -> &str {
        "url"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlNoTokenizer;
impl TextField for UrlNoTokenizer {
    fn name(&self) -> &str {
        "url_no_tokenizer"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlForSiteOperator;
impl TextField for UrlForSiteOperator {
    fn name(&self) -> &str {
        "url_for_site_operator"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteWithout;
impl TextField for SiteWithout {
    fn name(&self) -> &str {
        "site"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Domain;
impl TextField for Domain {
    fn name(&self) -> &str {
        "domain"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteNoTokenizer;
impl TextField for SiteNoTokenizer {
    fn name(&self) -> &str {
        "site_no_tokenizer"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainNoTokenizer;
impl TextField for DomainNoTokenizer {
    fn name(&self) -> &str {
        "domain_no_tokenizer"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainNameNoTokenizer;
impl TextField for DomainNameNoTokenizer {
    fn name(&self) -> &str {
        "domain_name_no_tokenizer"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteIfHomepageNoTokenizer;
impl TextField for SiteIfHomepageNoTokenizer {
    fn name(&self) -> &str {
        "site_if_homepage_no_tokenizer"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainIfHomepage;
impl TextField for DomainIfHomepage {
    fn name(&self) -> &str {
        "domain_if_homepage"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainNameIfHomepageNoTokenizer;
impl TextField for DomainNameIfHomepageNoTokenizer {
    fn name(&self) -> &str {
        "domain_name_if_homepage_no_tokenizer"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainIfHomepageNoTokenizer;
impl TextField for DomainIfHomepageNoTokenizer {
    fn name(&self) -> &str {
        "domain_if_homepage_no_tokenizer"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleIfHomepage;
impl TextField for TitleIfHomepage {
    fn name(&self) -> &str {
        "title_if_homepage"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BacklinkText;
impl TextField for BacklinkText {
    fn name(&self) -> &str {
        "backlink_text"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Description;
impl TextField for Description {
    fn name(&self) -> &str {
        "description"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DmozDescription;
impl TextField for DmozDescription {
    fn name(&self) -> &str {
        "dmoz_description"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaOrgJson;
impl TextField for SchemaOrgJson {
    fn name(&self) -> &str {
        "schema_org_json"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlattenedSchemaOrgJson;
impl TextField for FlattenedSchemaOrgJson {
    fn name(&self) -> &str {
        "flattened_schema_org_json"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CleanBodyBigrams;
impl TextField for CleanBodyBigrams {
    fn name(&self) -> &str {
        "clean_body_bigrams"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleBigrams;
impl TextField for TitleBigrams {
    fn name(&self) -> &str {
        "title_bigrams"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CleanBodyTrigrams;
impl TextField for CleanBodyTrigrams {
    fn name(&self) -> &str {
        "clean_body_trigrams"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleTrigrams;
impl TextField for TitleTrigrams {
    fn name(&self) -> &str {
        "title_trigrams"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MicroformatTags;
impl TextField for MicroformatTags {
    fn name(&self) -> &str {
        "microformat_tags"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SafetyClassification;
impl TextField for SafetyClassification {
    fn name(&self) -> &str {
        "safety_classification"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InsertionTimestamp;
impl TextField for InsertionTimestamp {
    fn name(&self) -> &str {
        "insertion_timestamp"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecipeFirstIngredientTagId;
impl TextField for RecipeFirstIngredientTagId {
    fn name(&self) -> &str {
        "recipe_first_ingredient_tag_id"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Keywords;
impl TextField for Keywords {
    fn name(&self) -> &str {
        "keywords"
    }
}
