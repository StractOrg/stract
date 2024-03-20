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

pub mod fast_field;
pub mod text_field;

use tantivy::{
    schema::{BytesOptions, NumericOptions, TextFieldIndexing, TextOptions},
    DateOptions,
};

pub use fast_field::{DataType, FastFieldEnum};
pub use text_field::TextFieldEnum;

pub const FLOAT_SCALING: u64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Field {
    Fast(FastFieldEnum),
    Text(TextFieldEnum),
}

impl Field {
    #[inline]
    pub fn get(field_id: usize) -> Option<Field> {
        if field_id < TextFieldEnum::num_variants() {
            return Some(Field::Text(TextFieldEnum::get(field_id).unwrap()));
        }
        let field_id = field_id - TextFieldEnum::num_variants();

        if field_id < FastFieldEnum::num_variants() {
            return Some(Field::Fast(FastFieldEnum::get(field_id).unwrap()));
        }
        let _field_id = field_id - FastFieldEnum::num_variants();

        return None;
    }

    #[inline]
    pub fn all() -> impl Iterator<Item = Field> {
        TextFieldEnum::all()
            .map(Field::Text)
            .chain(FastFieldEnum::all().map(Field::Fast))
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
            Field::Text(TextFieldEnum::Title(_)) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextFieldEnum::CleanBody(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::Url(_)) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextFieldEnum::UrlNoTokenizer(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::UrlForSiteOperator(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::SiteWithout(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::SiteIfHomepageNoTokenizer(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::Domain(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::SiteNoTokenizer(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::DomainNoTokenizer(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::DomainNameNoTokenizer(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::AllBody(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::DomainIfHomepage(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::TitleIfHomepage(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::DomainNameIfHomepageNoTokenizer(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::DomainIfHomepageNoTokenizer(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::BacklinkText(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::StemmedTitle(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::StemmedCleanBody(_)) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextFieldEnum::Description(_)) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextFieldEnum::DmozDescription(_)) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextFieldEnum::SchemaOrgJson(_)) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextFieldEnum::FlattenedSchemaOrgJson(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::CleanBodyBigrams(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::TitleBigrams(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::CleanBodyTrigrams(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::TitleTrigrams(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::MicroformatTags(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::SafetyClassification(_)) => {
                IndexingOption::Text(self.default_text_options())
            }
            Field::Text(TextFieldEnum::RecipeFirstIngredientTagId(_)) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Text(TextFieldEnum::InsertionTimestamp(_)) => {
                IndexingOption::DateTime(tantivy::schema::DateOptions::default().set_indexed())
            }
            Field::Text(TextFieldEnum::Keywords(_)) => {
                IndexingOption::Text(self.default_text_options().set_stored())
            }
            Field::Fast(FastFieldEnum::IsHomepage(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::HostCentrality(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::HostCentralityRank(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::PageCentrality(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::PageCentralityRank(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::FetchTimeMs(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::TrackerScore(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::LastUpdated(_)) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_stored()
                    .set_indexed(),
            ),
            Field::Fast(FastFieldEnum::Region(_)) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_stored()
                    .set_indexed(),
            ),
            Field::Fast(FastFieldEnum::NumCleanBodyTokens(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::NumDescriptionTokens(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::NumTitleTokens(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::NumMicroformatTagsTokens(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::NumUrlTokens(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::NumDomainTokens(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::NumUrlForSiteOperatorTokens(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::NumFlattenedSchemaTokens(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_indexed())
            }
            Field::Fast(FastFieldEnum::SiteHash1(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastFieldEnum::SiteHash2(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastFieldEnum::UrlWithoutQueryHash1(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastFieldEnum::UrlWithoutQueryHash2(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastFieldEnum::UrlHash1(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastFieldEnum::UrlHash2(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastFieldEnum::UrlWithoutTldHash1(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastFieldEnum::UrlWithoutTldHash2(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastFieldEnum::DomainHash1(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastFieldEnum::DomainHash2(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastFieldEnum::TitleHash1(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastFieldEnum::TitleHash2(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast())
            }
            Field::Fast(FastFieldEnum::PreComputedScore(_)) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastFieldEnum::HostNodeID(_)) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastFieldEnum::SimHash(_)) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastFieldEnum::NumPathAndQuerySlashes(_)) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastFieldEnum::NumPathAndQueryDigits(_)) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastFieldEnum::LikelyHasAds(_)) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastFieldEnum::LikelyHasPaywall(_)) => IndexingOption::Integer(
                NumericOptions::default()
                    .set_fast()
                    .set_indexed()
                    .set_stored(),
            ),
            Field::Fast(FastFieldEnum::LinkDensity(_)) => {
                IndexingOption::Integer(NumericOptions::default().set_fast().set_stored())
            }
            Field::Fast(FastFieldEnum::TitleEmbeddings(_)) => {
                IndexingOption::Bytes(BytesOptions::default().set_fast())
            }
            Field::Fast(FastFieldEnum::KeywordEmbeddings(_)) => {
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
            Field::Text(TextFieldEnum::BacklinkText(_))
                | Field::Text(TextFieldEnum::SchemaOrgJson(_))
                | Field::Text(TextFieldEnum::MicroformatTags(_))
                | Field::Text(TextFieldEnum::SafetyClassification(_))
                | Field::Text(TextFieldEnum::FlattenedSchemaOrgJson(_))
                | Field::Text(TextFieldEnum::UrlForSiteOperator(_))
                | Field::Text(TextFieldEnum::Description(_))
                | Field::Text(TextFieldEnum::DmozDescription(_))
                | Field::Text(TextFieldEnum::SiteIfHomepageNoTokenizer(_))
                | Field::Text(TextFieldEnum::DomainIfHomepage(_))
                | Field::Text(TextFieldEnum::DomainNameIfHomepageNoTokenizer(_))
                | Field::Text(TextFieldEnum::DomainIfHomepageNoTokenizer(_))
                | Field::Text(TextFieldEnum::TitleIfHomepage(_))
                | Field::Text(TextFieldEnum::SiteWithout(_)) // will match url
                | Field::Text(TextFieldEnum::Domain(_)) // will match url
                | Field::Text(TextFieldEnum::InsertionTimestamp(_))
                | Field::Text(TextFieldEnum::RecipeFirstIngredientTagId(_))
        ) && !self.is_fast()
    }

    pub fn is_fast(&self) -> bool {
        matches!(self, Field::Fast(_))
    }

    pub fn as_text(&self) -> Option<TextFieldEnum> {
        match self {
            Field::Fast(_) => None,
            Field::Text(field) => Some(*field),
        }
    }

    pub fn as_fast(&self) -> Option<FastFieldEnum> {
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

#[macro_export]
macro_rules! from_discriminant {
    ($discenum:ident => $enum:ident, [$($disc:ident),*$(,)?]) => {
        impl From<$discenum> for $enum {
            fn from(value: $discenum) -> Self {
                match value {
                    $(
                    $discenum::$disc => $disc.into(),
                    )*
                }
            }
        }
    };
}
