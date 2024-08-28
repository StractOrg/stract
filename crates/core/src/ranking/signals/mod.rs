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

pub mod core;
pub mod non_core;

use std::str::FromStr;

use crate::enum_dispatch_from_discriminant;
use crate::enum_map::{EnumMap, GetEnumMapKey, InsertEnumMapKey};

use crate::schema::Field;
use crate::{
    schema::{NumericalFieldEnum, TextFieldEnum},
    webpage::Webpage,
};
use enum_dispatch::enum_dispatch;

use strum::{EnumDiscriminants, VariantArray};
use utoipa::ToSchema;

pub use self::core::{non_text::*, text::*};
pub use self::non_core::{non_text::*, text::*};

use crate::ranking::computer::SignalComputer;
use tantivy::DocId;

#[enum_dispatch]
pub trait CoreSignal: Clone + Copy + std::fmt::Debug + PartialEq + Eq + std::hash::Hash {
    fn default_coefficient(&self) -> f64;
    fn as_field(&self) -> Option<Field>;
    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation;

    fn precompute(
        self,
        _webpage: &Webpage,
        _signal_computer: &SignalComputer,
    ) -> Option<SignalCalculation> {
        None
    }

    fn as_textfield(&self) -> Option<TextFieldEnum> {
        self.as_field().and_then(|field| field.as_text())
    }

    fn has_sibling_ngrams(&self) -> bool {
        false
    }

    fn as_numericalfield(&self) -> Option<NumericalFieldEnum> {
        self.as_field().and_then(|field| field.as_numerical())
    }
}

#[enum_dispatch]
pub trait Signal: Clone + Copy + std::fmt::Debug + PartialEq + Eq + std::hash::Hash {
    fn default_coefficient(&self) -> f64;
}

impl<T> Signal for T
where
    T: CoreSignal,
{
    fn default_coefficient(&self) -> f64 {
        CoreSignal::default_coefficient(self)
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
    TitleCoverage,
    Bm25TitleBigrams,
    Bm25TitleTrigrams,
    Bm25CleanBody,
    CleanBodyCoverage,
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
    MinTitleSlop,
    MinCleanBodySlop,
}

#[enum_dispatch(CoreSignal)]
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
))]
#[strum_discriminants(serde(rename_all = "snake_case"))]
pub enum CoreSignalEnum {
    Bm25F,
    Bm25Title,
    TitleCoverage,
    Bm25TitleBigrams,
    Bm25TitleTrigrams,
    Bm25CleanBody,
    CleanBodyCoverage,
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
    HostCentrality,
    HostCentralityRank,
    PageCentrality,
    PageCentralityRank,
    IsHomepage,
    FetchTimeMs,
    UpdateTimestamp,
    TrackerScore,
    Region,
    UrlDigits,
    UrlSlashes,
    LinkDensity,
    HasAds,
}

// Note to future self: Tried to get the num definitions
// into the macro as well, but some of the procedural macros started failing (bincode and enum_dispatch).
// This is probably due to macro hygiene
macro_rules! register_signals {
    (core=[$($core_signal:ident),*$(,)?], rest=[$($signal:ident),*$(,)?]) => {
        enum_dispatch_from_discriminant!(SignalEnumDiscriminants => SignalEnum,
        [
            $($core_signal,)*
            $($signal,)*
        ]);


        impl From<CoreSignalEnum> for SignalEnum {
            fn from(core: CoreSignalEnum) -> Self {
                match core {
                    $(
                        CoreSignalEnum::$core_signal(signal) => SignalEnum::$core_signal(signal),
                    )*
                }
            }
        }

        enum_dispatch_from_discriminant!(CoreSignalEnumDiscriminants => CoreSignalEnum,
        [

            $($core_signal,)*
        ]);
    }
}

register_signals! {
    core=[
        Bm25F,
        Bm25Title,
        TitleCoverage,
        Bm25TitleBigrams,
        Bm25TitleTrigrams,
        Bm25CleanBody,
        CleanBodyCoverage,
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
        HostCentrality,
        HostCentralityRank,
        PageCentrality,
        PageCentralityRank,
        IsHomepage,
        FetchTimeMs,
        UpdateTimestamp,
        TrackerScore,
        Region,
        UrlDigits,
        UrlSlashes,
        LinkDensity,
        HasAds,
    ],
    rest=[
        QueryCentrality,
        InboundSimilarity,
        LambdaMart,
        MinTitleSlop,
        MinCleanBodySlop,
        CrossEncoderSnippet,
        CrossEncoderTitle,
        TitleEmbeddingSimilarity,
        KeywordEmbeddingSimilarity,
    ]
}

impl SignalEnum {
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
        Self::get(value)
    }
}

impl CoreSignalEnum {
    pub fn num_variants() -> usize {
        CoreSignalEnumDiscriminants::VARIANTS.len()
    }

    pub fn all() -> impl Iterator<Item = CoreSignalEnum> {
        CoreSignalEnumDiscriminants::VARIANTS
            .iter()
            .copied()
            .map(|v| v.into())
    }

    pub fn get(field_id: usize) -> Option<CoreSignalEnum> {
        CoreSignalEnumDiscriminants::VARIANTS
            .get(field_id)
            .copied()
            .map(CoreSignalEnum::from)
    }
}

impl InsertEnumMapKey for CoreSignalEnum {
    fn into_usize(self) -> usize {
        CoreSignalEnumDiscriminants::from(self) as usize
    }
}

impl GetEnumMapKey for CoreSignalEnum {
    fn from_usize(value: usize) -> Option<Self> {
        Self::get(value)
    }
}

impl FromStr for SignalEnumDiscriminants {
    type Err = anyhow::Error;

    fn from_str(name: &str) -> std::result::Result<Self, Self::Err> {
        let s = "\"".to_string() + name + "\"";
        let signal = serde_json::from_str(&s)?;
        Ok(signal)
    }
}

#[derive(Debug, Clone, Copy, bincode::Encode, bincode::Decode)]
pub struct SignalCalculation {
    pub value: f64,
    pub score: f64,
}

impl SignalCalculation {
    pub fn new_symmetrical(val: f64) -> Self {
        Self {
            value: val,
            score: val,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ComputedSignal {
    pub signal: SignalEnum,
    pub calc: SignalCalculation,
}

#[derive(
    Debug,
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct SignalScore {
    pub coefficient: f64,
    pub value: f64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone)]
pub struct SignalCoefficients {
    map: EnumMap<SignalEnum, f64>,
}

impl Default for SignalCoefficients {
    fn default() -> Self {
        Self::new(SignalEnum::all().map(|signal| (signal, signal.default_coefficient())))
    }
}

impl From<EnumMap<SignalEnum, f64>> for SignalCoefficients {
    fn from(map: EnumMap<SignalEnum, f64>) -> Self {
        Self { map }
    }
}

impl SignalCoefficients {
    pub fn get(&self, signal: &SignalEnum) -> f64 {
        self.map
            .get(*signal)
            .copied()
            .unwrap_or(signal.default_coefficient())
    }

    pub fn new(coefficients: impl Iterator<Item = (SignalEnum, f64)>) -> Self {
        let mut map = EnumMap::default();

        for (signal, coefficient) in coefficients {
            map.insert(signal, coefficient);
        }

        Self { map }
    }

    pub fn merge_add(&mut self, coeffs: SignalCoefficients) {
        for signal in SignalEnum::all() {
            if let Some(coeff) = coeffs.map.get(signal).copied() {
                match self.map.get_mut(signal) {
                    Some(existing_coeff) => *existing_coeff += coeff,
                    None => {
                        self.map.insert(signal, coeff);
                    }
                }
            }
        }
    }

    pub fn merge_overwrite(&mut self, coeffs: SignalCoefficients) {
        for signal in SignalEnum::all() {
            if let Some(coeff) = coeffs.map.get(signal).copied() {
                match self.map.get_mut(signal) {
                    Some(existing_coeff) => *existing_coeff = coeff,
                    None => {
                        self.map.insert(signal, coeff);
                    }
                }
            }
        }
    }
}
