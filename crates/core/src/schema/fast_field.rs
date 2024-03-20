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

use enum_dispatch::enum_dispatch;
use strum::{EnumDiscriminants, VariantArray};

use crate::{enum_map::InsertEnumMapKey, from_discriminant};

#[enum_dispatch]
pub trait FastField: Clone + Copy + std::fmt::Debug + PartialEq + Eq + std::hash::Hash {}

#[enum_dispatch(FastField)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumDiscriminants)]
#[strum_discriminants(derive(VariantArray))]
pub enum FastFieldEnum {
    IsHomepage,
    HostCentrality,
    HostCentralityRank,
    PageCentrality,
    PageCentralityRank,
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
    TitleEmbeddings,
    KeywordEmbeddings,
}

from_discriminant!(FastFieldEnumDiscriminants => FastFieldEnum,
[
    IsHomepage,
    HostCentrality,
    HostCentralityRank,
    PageCentrality,
    PageCentralityRank,
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
    TitleEmbeddings,
    KeywordEmbeddings,
]);

impl FastFieldEnum {
    pub fn all() -> impl Iterator<Item = FastFieldEnum> {
        FastFieldEnumDiscriminants::VARIANTS
            .iter()
            .copied()
            .map(|v| v.into())
    }

    pub fn get(field_id: usize) -> Option<FastFieldEnum> {
        FastFieldEnumDiscriminants::VARIANTS
            .get(field_id)
            .copied()
            .map(FastFieldEnum::from)
    }

    pub fn num_variants() -> usize {
        FastFieldEnumDiscriminants::VARIANTS.len()
    }

    pub fn name(&self) -> &str {
        match self {
            FastFieldEnum::HostCentrality(_) => "host_centrality",
            FastFieldEnum::HostCentralityRank(_) => "host_centrality_rank",
            FastFieldEnum::PageCentrality(_) => "page_centrality",
            FastFieldEnum::PageCentralityRank(_) => "page_centrality_rank",
            FastFieldEnum::IsHomepage(_) => "is_homepage",
            FastFieldEnum::FetchTimeMs(_) => "fetch_time_ms",
            FastFieldEnum::LastUpdated(_) => "last_updated",
            FastFieldEnum::TrackerScore(_) => "tracker_score",
            FastFieldEnum::Region(_) => "region",
            FastFieldEnum::NumUrlTokens(_) => "num_url_tokens",
            FastFieldEnum::NumTitleTokens(_) => "num_title_tokens",
            FastFieldEnum::NumCleanBodyTokens(_) => "num_clean_body_tokens",
            FastFieldEnum::NumDescriptionTokens(_) => "num_description_tokens",
            FastFieldEnum::NumDomainTokens(_) => "num_domain_tokens",
            FastFieldEnum::NumUrlForSiteOperatorTokens(_) => "num_url_for_site_operator_tokens",
            FastFieldEnum::NumFlattenedSchemaTokens(_) => "num_flattened_schema_tokens",
            FastFieldEnum::NumMicroformatTagsTokens(_) => "num_microformat_tags_tokens",
            FastFieldEnum::SiteHash1(_) => "site_hash1",
            FastFieldEnum::SiteHash2(_) => "site_hash2",
            FastFieldEnum::UrlWithoutQueryHash1(_) => "url_without_query_hash1",
            FastFieldEnum::UrlWithoutQueryHash2(_) => "url_without_query_hash2",
            FastFieldEnum::TitleHash1(_) => "title_hash1",
            FastFieldEnum::TitleHash2(_) => "title_hash2",
            FastFieldEnum::UrlHash1(_) => "url_hash1",
            FastFieldEnum::UrlHash2(_) => "url_hash2",
            FastFieldEnum::DomainHash1(_) => "domain_hash1",
            FastFieldEnum::DomainHash2(_) => "domain_hash2",
            FastFieldEnum::UrlWithoutTldHash1(_) => "url_without_tld_hash1",
            FastFieldEnum::UrlWithoutTldHash2(_) => "url_without_tld_hash2",
            FastFieldEnum::PreComputedScore(_) => "pre_computed_score",
            FastFieldEnum::HostNodeID(_) => "host_node_id",
            FastFieldEnum::SimHash(_) => "sim_hash",
            FastFieldEnum::NumPathAndQuerySlashes(_) => "num_path_and_query_slashes",
            FastFieldEnum::NumPathAndQueryDigits(_) => "num_path_and_query_digits",
            FastFieldEnum::LikelyHasAds(_) => "likely_has_ads",
            FastFieldEnum::LikelyHasPaywall(_) => "likely_has_paywall",
            FastFieldEnum::LinkDensity(_) => "link_density",
            FastFieldEnum::TitleEmbeddings(_) => "title_embeddings",
            FastFieldEnum::KeywordEmbeddings(_) => "keyword_embeddings",
        }
    }
}

pub enum DataType {
    U64,
    Bytes,
}

impl FastFieldEnum {
    pub fn data_type(&self) -> DataType {
        match self {
            FastFieldEnum::IsHomepage(_) => DataType::U64,
            FastFieldEnum::HostCentrality(_) => DataType::U64,
            FastFieldEnum::HostCentralityRank(_) => DataType::U64,
            FastFieldEnum::PageCentrality(_) => DataType::U64,
            FastFieldEnum::PageCentralityRank(_) => DataType::U64,
            FastFieldEnum::FetchTimeMs(_) => DataType::U64,
            FastFieldEnum::LastUpdated(_) => DataType::U64,
            FastFieldEnum::TrackerScore(_) => DataType::U64,
            FastFieldEnum::Region(_) => DataType::U64,
            FastFieldEnum::NumUrlTokens(_) => DataType::U64,
            FastFieldEnum::NumTitleTokens(_) => DataType::U64,
            FastFieldEnum::NumMicroformatTagsTokens(_) => DataType::U64,
            FastFieldEnum::NumCleanBodyTokens(_) => DataType::U64,
            FastFieldEnum::NumDescriptionTokens(_) => DataType::U64,
            FastFieldEnum::NumDomainTokens(_) => DataType::U64,
            FastFieldEnum::NumUrlForSiteOperatorTokens(_) => DataType::U64,
            FastFieldEnum::NumFlattenedSchemaTokens(_) => DataType::U64,
            FastFieldEnum::SiteHash1(_) => DataType::U64,
            FastFieldEnum::SiteHash2(_) => DataType::U64,
            FastFieldEnum::UrlWithoutQueryHash1(_) => DataType::U64,
            FastFieldEnum::UrlWithoutQueryHash2(_) => DataType::U64,
            FastFieldEnum::TitleHash1(_) => DataType::U64,
            FastFieldEnum::TitleHash2(_) => DataType::U64,
            FastFieldEnum::UrlHash1(_) => DataType::U64,
            FastFieldEnum::UrlHash2(_) => DataType::U64,
            FastFieldEnum::DomainHash1(_) => DataType::U64,
            FastFieldEnum::DomainHash2(_) => DataType::U64,
            FastFieldEnum::UrlWithoutTldHash1(_) => DataType::U64,
            FastFieldEnum::UrlWithoutTldHash2(_) => DataType::U64,
            FastFieldEnum::PreComputedScore(_) => DataType::U64,
            FastFieldEnum::HostNodeID(_) => DataType::U64,
            FastFieldEnum::SimHash(_) => DataType::U64,
            FastFieldEnum::NumPathAndQuerySlashes(_) => DataType::U64,
            FastFieldEnum::NumPathAndQueryDigits(_) => DataType::U64,
            FastFieldEnum::LikelyHasAds(_) => DataType::U64,
            FastFieldEnum::LikelyHasPaywall(_) => DataType::U64,
            FastFieldEnum::LinkDensity(_) => DataType::U64,
            FastFieldEnum::TitleEmbeddings(_) => DataType::Bytes,
            FastFieldEnum::KeywordEmbeddings(_) => DataType::Bytes,
        }
    }
}

impl InsertEnumMapKey for FastFieldEnum {
    fn into_usize(self) -> usize {
        FastFieldEnumDiscriminants::from(self) as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IsHomepage;
impl FastField for IsHomepage {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostCentrality;
impl FastField for HostCentrality {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostCentralityRank;
impl FastField for HostCentralityRank {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageCentrality;
impl FastField for PageCentrality {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageCentralityRank;
impl FastField for PageCentralityRank {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FetchTimeMs;
impl FastField for FetchTimeMs {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LastUpdated;
impl FastField for LastUpdated {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TrackerScore;
impl FastField for TrackerScore {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Region;
impl FastField for Region {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumUrlTokens;
impl FastField for NumUrlTokens {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumTitleTokens;
impl FastField for NumTitleTokens {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumCleanBodyTokens;
impl FastField for NumCleanBodyTokens {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumDescriptionTokens;
impl FastField for NumDescriptionTokens {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumUrlForSiteOperatorTokens;
impl FastField for NumUrlForSiteOperatorTokens {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumDomainTokens;
impl FastField for NumDomainTokens {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumMicroformatTagsTokens;
impl FastField for NumMicroformatTagsTokens {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteHash1;
impl FastField for SiteHash1 {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteHash2;
impl FastField for SiteHash2 {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutQueryHash1;
impl FastField for UrlWithoutQueryHash1 {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutQueryHash2;
impl FastField for UrlWithoutQueryHash2 {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleHash1;
impl FastField for TitleHash1 {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleHash2;
impl FastField for TitleHash2 {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlHash1;
impl FastField for UrlHash1 {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlHash2;
impl FastField for UrlHash2 {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainHash1;
impl FastField for DomainHash1 {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainHash2;
impl FastField for DomainHash2 {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutTldHash1;
impl FastField for UrlWithoutTldHash1 {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutTldHash2;
impl FastField for UrlWithoutTldHash2 {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PreComputedScore;
impl FastField for PreComputedScore {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostNodeID;
impl FastField for HostNodeID {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SimHash;
impl FastField for SimHash {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumFlattenedSchemaTokens;
impl FastField for NumFlattenedSchemaTokens {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumPathAndQuerySlashes;
impl FastField for NumPathAndQuerySlashes {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumPathAndQueryDigits;
impl FastField for NumPathAndQueryDigits {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LikelyHasAds;
impl FastField for LikelyHasAds {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LikelyHasPaywall;
impl FastField for LikelyHasPaywall {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LinkDensity;
impl FastField for LinkDensity {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleEmbeddings;
impl FastField for TitleEmbeddings {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeywordEmbeddings;
impl FastField for KeywordEmbeddings {}
