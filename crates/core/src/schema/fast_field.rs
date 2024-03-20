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
pub trait FastField: Clone + Copy + std::fmt::Debug + PartialEq + Eq + std::hash::Hash {
    fn name(&self) -> &str;

    fn data_type(&self) -> DataType {
        DataType::U64
    }
}

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
}

pub enum DataType {
    U64,
    Bytes,
}

impl InsertEnumMapKey for FastFieldEnum {
    fn into_usize(self) -> usize {
        FastFieldEnumDiscriminants::from(self) as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IsHomepage;
impl FastField for IsHomepage {
    fn name(&self) -> &str {
        "is_homepage"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostCentrality;
impl FastField for HostCentrality {
    fn name(&self) -> &str {
        "host_centrality"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostCentralityRank;
impl FastField for HostCentralityRank {
    fn name(&self) -> &str {
        "host_centrality_rank"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageCentrality;
impl FastField for PageCentrality {
    fn name(&self) -> &str {
        "page_centrality"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageCentralityRank;
impl FastField for PageCentralityRank {
    fn name(&self) -> &str {
        "page_centrality_rank"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FetchTimeMs;
impl FastField for FetchTimeMs {
    fn name(&self) -> &str {
        "fetch_time_ms"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LastUpdated;
impl FastField for LastUpdated {
    fn name(&self) -> &str {
        "last_updated"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TrackerScore;
impl FastField for TrackerScore {
    fn name(&self) -> &str {
        "tracker_score"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Region;
impl FastField for Region {
    fn name(&self) -> &str {
        "region"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumUrlTokens;
impl FastField for NumUrlTokens {
    fn name(&self) -> &str {
        "num_url_tokens"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumTitleTokens;
impl FastField for NumTitleTokens {
    fn name(&self) -> &str {
        "num_title_tokens"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumCleanBodyTokens;
impl FastField for NumCleanBodyTokens {
    fn name(&self) -> &str {
        "num_clean_body_tokens"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumDescriptionTokens;
impl FastField for NumDescriptionTokens {
    fn name(&self) -> &str {
        "num_description_tokens"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumUrlForSiteOperatorTokens;
impl FastField for NumUrlForSiteOperatorTokens {
    fn name(&self) -> &str {
        "num_url_for_site_operator_tokens"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumDomainTokens;
impl FastField for NumDomainTokens {
    fn name(&self) -> &str {
        "num_domain_tokens"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumMicroformatTagsTokens;
impl FastField for NumMicroformatTagsTokens {
    fn name(&self) -> &str {
        "num_microformat_tags_tokens"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteHash1;
impl FastField for SiteHash1 {
    fn name(&self) -> &str {
        "site_hash1"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteHash2;
impl FastField for SiteHash2 {
    fn name(&self) -> &str {
        "site_hash2"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutQueryHash1;
impl FastField for UrlWithoutQueryHash1 {
    fn name(&self) -> &str {
        "url_without_query_hash1"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutQueryHash2;
impl FastField for UrlWithoutQueryHash2 {
    fn name(&self) -> &str {
        "url_without_query_hash2"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleHash1;
impl FastField for TitleHash1 {
    fn name(&self) -> &str {
        "title_hash1"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleHash2;
impl FastField for TitleHash2 {
    fn name(&self) -> &str {
        "title_hash2"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlHash1;
impl FastField for UrlHash1 {
    fn name(&self) -> &str {
        "url_hash1"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlHash2;
impl FastField for UrlHash2 {
    fn name(&self) -> &str {
        "url_hash2"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainHash1;
impl FastField for DomainHash1 {
    fn name(&self) -> &str {
        "domain_hash1"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainHash2;
impl FastField for DomainHash2 {
    fn name(&self) -> &str {
        "domain_hash2"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutTldHash1;
impl FastField for UrlWithoutTldHash1 {
    fn name(&self) -> &str {
        "url_without_tld_hash1"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutTldHash2;
impl FastField for UrlWithoutTldHash2 {
    fn name(&self) -> &str {
        "url_without_tld_hash2"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PreComputedScore;
impl FastField for PreComputedScore {
    fn name(&self) -> &str {
        "pre_computed_score"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostNodeID;
impl FastField for HostNodeID {
    fn name(&self) -> &str {
        "host_node_id"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SimHash;
impl FastField for SimHash {
    fn name(&self) -> &str {
        "sim_hash"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumFlattenedSchemaTokens;
impl FastField for NumFlattenedSchemaTokens {
    fn name(&self) -> &str {
        "num_flattened_schema_tokens"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumPathAndQuerySlashes;
impl FastField for NumPathAndQuerySlashes {
    fn name(&self) -> &str {
        "num_path_and_query_slashes"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumPathAndQueryDigits;
impl FastField for NumPathAndQueryDigits {
    fn name(&self) -> &str {
        "num_path_and_query_digits"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LikelyHasAds;
impl FastField for LikelyHasAds {
    fn name(&self) -> &str {
        "likely_has_ads"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LikelyHasPaywall;
impl FastField for LikelyHasPaywall {
    fn name(&self) -> &str {
        "likely_has_paywall"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LinkDensity;
impl FastField for LinkDensity {
    fn name(&self) -> &str {
        "link_density"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleEmbeddings;
impl FastField for TitleEmbeddings {
    fn name(&self) -> &str {
        "title_embeddings"
    }

    fn data_type(&self) -> DataType {
        DataType::Bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeywordEmbeddings;
impl FastField for KeywordEmbeddings {
    fn name(&self) -> &str {
        "keyword_embeddings"
    }

    fn data_type(&self) -> DataType {
        DataType::Bytes
    }
}
