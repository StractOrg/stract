use crate::enum_dispatch_from_discriminant;
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
// along with this program.  If not, see <https://www.gnu.org/licenses/>
use crate::enum_map::{GetEnumMapKey, InsertEnumMapKey};

use crate::schema::Field;
use crate::{
    schema::{NumericalFieldEnum, TextFieldEnum},
    webpage::Webpage,
};
use enum_dispatch::enum_dispatch;

use strum::{EnumDiscriminants, VariantArray};

use super::non_text::*;
use super::text::*;
use super::SignalComputer;
use tantivy::DocId;

#[enum_dispatch]
pub trait Signal:
    Clone + Copy + std::fmt::Debug + PartialEq + Eq + std::hash::Hash + Into<SignalEnum>
{
    fn default_coefficient(&self) -> f64;
    fn as_field(&self) -> Option<Field>;
    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64>;

    fn precompute(self, _webpage: &Webpage, _signal_computer: &SignalComputer) -> Option<f64> {
        None
    }

    fn as_textfield(&self) -> Option<TextFieldEnum> {
        self.as_field().and_then(|field| field.as_text())
    }

    fn as_numericalfield(&self) -> Option<NumericalFieldEnum> {
        self.as_field().and_then(|field| field.as_numerical())
    }
}

#[enum_dispatch(Signal)]
#[derive(
    Debug,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    EnumDiscriminants,
)]
#[strum_discriminants(derive(
    VariantArray,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    PartialOrd,
    Ord,
    Hash,
    utoipa::ToSchema,
))]
#[strum_discriminants(serde(rename_all = "snake_case"))]
pub enum SignalEnum {
    Bm25F,
    Bm25Title,
    Bm25TitleBigrams,
    Bm25TitleTrigrams,
    Bm25CleanBody,
    Bm25CleanBodyBigrams,
    Bm25CleanBodyTrigrams,
    Bm25StemmedTitle,
    Bm25StemmedCleanBody,
    Bm25AllBody,
    Bm25Keywords,
    Bm25BacklinkText,
    IdfSumUrl,
    IdfSumSite,
    IdfSumDomain,
    IdfSumSiteNoTokenizer,
    IdfSumDomainNoTokenizer,
    IdfSumDomainNameNoTokenizer,
    IdfSumDomainIfHomepage,
    IdfSumDomainNameIfHomepageNoTokenizer,
    IdfSumDomainIfHomepageNoTokenizer,
    IdfSumTitleIfHomepage,
    CrossEncoderSnippet,
    CrossEncoderTitle,
    HostCentrality,
    HostCentralityRank,
    PageCentrality,
    PageCentralityRank,
    IsHomepage,
    FetchTimeMs,
    UpdateTimestamp,
    TrackerScore,
    Region,
    QueryCentrality,
    InboundSimilarity,
    LambdaMart,
    UrlDigits,
    UrlSlashes,
    LinkDensity,
    TitleEmbeddingSimilarity,
    KeywordEmbeddingSimilarity,
    HasAds,
}

enum_dispatch_from_discriminant!(SignalEnumDiscriminants => SignalEnum,
[
    Bm25F,
    Bm25Title,
    Bm25TitleBigrams,
    Bm25TitleTrigrams,
    Bm25CleanBody,
    Bm25CleanBodyBigrams,
    Bm25CleanBodyTrigrams,
    Bm25StemmedTitle,
    Bm25StemmedCleanBody,
    Bm25AllBody,
    Bm25Keywords,
    Bm25BacklinkText,
    IdfSumUrl,
    IdfSumSite,
    IdfSumDomain,
    IdfSumSiteNoTokenizer,
    IdfSumDomainNoTokenizer,
    IdfSumDomainNameNoTokenizer,
    IdfSumDomainIfHomepage,
    IdfSumDomainNameIfHomepageNoTokenizer,
    IdfSumDomainIfHomepageNoTokenizer,
    IdfSumTitleIfHomepage,
    CrossEncoderSnippet,
    CrossEncoderTitle,
    HostCentrality,
    HostCentralityRank,
    PageCentrality,
    PageCentralityRank,
    IsHomepage,
    FetchTimeMs,
    UpdateTimestamp,
    TrackerScore,
    Region,
    QueryCentrality,
    InboundSimilarity,
    LambdaMart,
    UrlDigits,
    UrlSlashes,
    LinkDensity,
    TitleEmbeddingSimilarity,
    KeywordEmbeddingSimilarity,
    HasAds,
]);

impl SignalEnum {
    pub fn num_variants() -> usize {
        SignalEnumDiscriminants::VARIANTS.len()
    }

    pub fn all() -> impl Iterator<Item = SignalEnum> {
        SignalEnumDiscriminants::VARIANTS
            .iter()
            .copied()
            .map(|v| v.into())
    }

    pub fn get(field_id: usize) -> Option<SignalEnum> {
        SignalEnumDiscriminants::VARIANTS
            .get(field_id)
            .copied()
            .map(SignalEnum::from)
    }
}

impl InsertEnumMapKey for SignalEnum {
    fn into_usize(self) -> usize {
        SignalEnumDiscriminants::from(self) as usize
    }
}

impl GetEnumMapKey for SignalEnum {
    fn from_usize(value: usize) -> Option<Self> {
        SignalEnumDiscriminants::VARIANTS
            .get(value)
            .copied()
            .map(SignalEnum::from)
    }
}
