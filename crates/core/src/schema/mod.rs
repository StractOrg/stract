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

mod fast_field;
mod text_field;

use strum::VariantArray;
use tantivy::{
    schema::{BytesOptions, NumericOptions, TextFieldIndexing, TextOptions},
    DateOptions,
};

pub use fast_field::FastField;
pub use text_field::TextField;

pub const FLOAT_SCALING: u64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Field {
    Fast(FastField),
    Text(TextField),
}

impl Field {
    #[inline]
    pub fn get(field_id: usize) -> Option<Field> {
        if field_id < TextField::VARIANTS.len() {
            Some(Field::Text(TextField::VARIANTS[field_id]))
        } else {
            let fast_id = field_id - TextField::VARIANTS.len();
            if fast_id < FastField::VARIANTS.len() {
                Some(Field::Fast(FastField::VARIANTS[fast_id]))
            } else {
                None
            }
        }
    }

    #[inline]
    pub fn all() -> impl Iterator<Item = Field> {
        TextField::VARIANTS
            .iter()
            .map(|&text| Field::Text(text))
            .chain(FastField::VARIANTS.iter().map(|&fast| Field::Fast(fast)))
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
            Field::Text(TextField::Keywords) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Fast(FastField::IsHomepage) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::HostCentrality) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::HostCentralityRank) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::PageCentrality) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastField::PageCentralityRank) => {
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
            Field::Fast(FastField::TitleEmbeddings) => {
                IndexingOption::Bytes(BytesOptions::default().set_fast())
            }
            Field::Fast(FastField::KeywordEmbeddings) => {
                IndexingOption::Bytes(BytesOptions::default().set_fast())
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

    for field in Field::all() {
        match field.options() {
            IndexingOption::Text(options) => builder.add_text_field(field.name(), options),
            IndexingOption::Integer(options) => builder.add_u64_field(field.name(), options),
            IndexingOption::DateTime(options) => builder.add_date_field(field.name(), options),
            IndexingOption::Bytes(options) => builder.add_bytes_field(field.name(), options),
        };
    }

    builder.build()
}

pub enum IndexingOption {
    Text(TextOptions),
    Integer(NumericOptions),
    DateTime(DateOptions),
    Bytes(BytesOptions),
}

pub enum DataType {
    U64,
    Bytes,
}

impl FastField {
    pub fn data_type(&self) -> DataType {
        match self {
            FastField::IsHomepage => DataType::U64,
            FastField::HostCentrality => DataType::U64,
            FastField::HostCentralityRank => DataType::U64,
            FastField::PageCentrality => DataType::U64,
            FastField::PageCentralityRank => DataType::U64,
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
            FastField::TitleEmbeddings => DataType::Bytes,
            FastField::KeywordEmbeddings => DataType::Bytes,
        }
    }
}
