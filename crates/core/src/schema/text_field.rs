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
use tantivy::schema::{IndexRecordOption, TextFieldIndexing, TextOptions};

use crate::{
    enum_map::InsertEnumMapKey,
    from_discriminant,
    tokenizer::{
        BigramTokenizer, Identity, JsonField, SiteOperatorUrlTokenizer, Tokenizer, TrigramTokenizer,
    },
};

use super::IndexingOption;

#[enum_dispatch]
pub trait TextField:
    Clone + Copy + std::fmt::Debug + PartialEq + Eq + std::hash::Hash + Into<TextFieldEnum>
{
    fn name(&self) -> &str;

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::default()
    }

    fn query_tokenizer(&self) -> Tokenizer {
        self.indexing_tokenizer()
    }

    fn ngram_size(&self) -> usize {
        1
    }

    fn monogram_field(&self) -> TextFieldEnum {
        debug_assert_eq!(self.ngram_size(), 1);
        (*self).into()
    }

    /// Whether or not the field should be included
    /// in the fields that the `Query` searches.
    ///
    /// The fields can still be searched by manually
    /// constructing a tantivy query.
    fn is_searchable(&self) -> bool {
        false
    }

    fn has_pos(&self) -> bool {
        false
    }

    fn is_stored(&self) -> bool {
        false
    }

    fn record_option(&self) -> IndexRecordOption {
        if self.has_pos() {
            IndexRecordOption::WithFreqsAndPositions
        } else {
            IndexRecordOption::WithFreqs
        }
    }

    fn indexing_option(&self) -> IndexingOption {
        let tokenizer = self.indexing_tokenizer();
        let option = self.record_option();

        let mut opt = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(tokenizer.as_str())
                .set_index_option(option),
        );

        if self.is_stored() {
            opt = opt.set_stored();
        }

        IndexingOption::Text(opt)
    }
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

    fn has_pos(&self) -> bool {
        true
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CleanBody;
impl TextField for CleanBody {
    fn name(&self) -> &str {
        "body"
    }

    fn has_pos(&self) -> bool {
        true
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StemmedTitle;
impl TextField for StemmedTitle {
    fn name(&self) -> &str {
        "stemmed_title"
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::new_stemmed()
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StemmedCleanBody;
impl TextField for StemmedCleanBody {
    fn name(&self) -> &str {
        "stemmed_body"
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::new_stemmed()
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AllBody;
impl TextField for AllBody {
    fn name(&self) -> &str {
        "all_body"
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Url;
impl TextField for Url {
    fn name(&self) -> &str {
        "url"
    }

    fn has_pos(&self) -> bool {
        true
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlNoTokenizer;
impl TextField for UrlNoTokenizer {
    fn name(&self) -> &str {
        "url_no_tokenizer"
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Identity(Identity {})
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlForSiteOperator;
impl TextField for UrlForSiteOperator {
    fn name(&self) -> &str {
        "url_for_site_operator"
    }

    fn has_pos(&self) -> bool {
        true
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::SiteOperator(SiteOperatorUrlTokenizer)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteWithout;
impl TextField for SiteWithout {
    fn name(&self) -> &str {
        "site"
    }

    fn has_pos(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Domain;
impl TextField for Domain {
    fn name(&self) -> &str {
        "domain"
    }

    fn has_pos(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteNoTokenizer;
impl TextField for SiteNoTokenizer {
    fn name(&self) -> &str {
        "site_no_tokenizer"
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Identity(Identity {})
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainNoTokenizer;
impl TextField for DomainNoTokenizer {
    fn name(&self) -> &str {
        "domain_no_tokenizer"
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Identity(Identity {})
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainNameNoTokenizer;
impl TextField for DomainNameNoTokenizer {
    fn name(&self) -> &str {
        "domain_name_no_tokenizer"
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Identity(Identity {})
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteIfHomepageNoTokenizer;
impl TextField for SiteIfHomepageNoTokenizer {
    fn name(&self) -> &str {
        "site_if_homepage_no_tokenizer"
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Identity(Identity {})
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

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Identity(Identity {})
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainIfHomepageNoTokenizer;
impl TextField for DomainIfHomepageNoTokenizer {
    fn name(&self) -> &str {
        "domain_if_homepage_no_tokenizer"
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Identity(Identity {})
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

    fn has_pos(&self) -> bool {
        true
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DmozDescription;
impl TextField for DmozDescription {
    fn name(&self) -> &str {
        "dmoz_description"
    }

    fn has_pos(&self) -> bool {
        true
    }

    fn is_stored(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaOrgJson;
impl TextField for SchemaOrgJson {
    fn name(&self) -> &str {
        "schema_org_json"
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Identity(Identity {})
    }

    fn is_stored(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlattenedSchemaOrgJson;
impl TextField for FlattenedSchemaOrgJson {
    fn name(&self) -> &str {
        "flattened_schema_org_json"
    }

    fn has_pos(&self) -> bool {
        true
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Json(JsonField)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CleanBodyBigrams;
impl TextField for CleanBodyBigrams {
    fn name(&self) -> &str {
        "clean_body_bigrams"
    }

    fn ngram_size(&self) -> usize {
        2
    }

    fn monogram_field(&self) -> TextFieldEnum {
        CleanBody.into()
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Bigram(BigramTokenizer::default())
    }

    fn query_tokenizer(&self) -> Tokenizer {
        Tokenizer::default()
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleBigrams;
impl TextField for TitleBigrams {
    fn name(&self) -> &str {
        "title_bigrams"
    }

    fn ngram_size(&self) -> usize {
        2
    }

    fn monogram_field(&self) -> TextFieldEnum {
        Title.into()
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Bigram(BigramTokenizer::default())
    }

    fn query_tokenizer(&self) -> Tokenizer {
        Tokenizer::default()
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CleanBodyTrigrams;
impl TextField for CleanBodyTrigrams {
    fn name(&self) -> &str {
        "clean_body_trigrams"
    }

    fn ngram_size(&self) -> usize {
        3
    }

    fn monogram_field(&self) -> TextFieldEnum {
        CleanBody.into()
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Trigram(TrigramTokenizer::default())
    }

    fn query_tokenizer(&self) -> Tokenizer {
        Tokenizer::default()
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleTrigrams;
impl TextField for TitleTrigrams {
    fn name(&self) -> &str {
        "title_trigrams"
    }

    fn ngram_size(&self) -> usize {
        3
    }

    fn monogram_field(&self) -> TextFieldEnum {
        Title.into()
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Trigram(TrigramTokenizer::default())
    }

    fn query_tokenizer(&self) -> Tokenizer {
        Tokenizer::default()
    }

    fn is_searchable(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MicroformatTags;
impl TextField for MicroformatTags {
    fn name(&self) -> &str {
        "microformat_tags"
    }

    fn has_pos(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SafetyClassification;
impl TextField for SafetyClassification {
    fn name(&self) -> &str {
        "safety_classification"
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Identity(Identity {})
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InsertionTimestamp;
impl TextField for InsertionTimestamp {
    fn name(&self) -> &str {
        "insertion_timestamp"
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Identity(Identity {})
    }

    fn indexing_option(&self) -> IndexingOption {
        IndexingOption::DateTime(tantivy::schema::DateOptions::default().set_indexed())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecipeFirstIngredientTagId;
impl TextField for RecipeFirstIngredientTagId {
    fn name(&self) -> &str {
        "recipe_first_ingredient_tag_id"
    }

    fn indexing_tokenizer(&self) -> Tokenizer {
        Tokenizer::Identity(Identity {})
    }

    fn is_stored(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Keywords;
impl TextField for Keywords {
    fn name(&self) -> &str {
        "keywords"
    }

    fn is_stored(&self) -> bool {
        true
    }
}
