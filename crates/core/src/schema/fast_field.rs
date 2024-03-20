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

use strum::VariantArray;

use crate::enum_map::InsertEnumMapKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, VariantArray)]
pub enum FastField {
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

impl FastField {
    pub fn name(&self) -> &str {
        match self {
            FastField::HostCentrality => "host_centrality",
            FastField::HostCentralityRank => "host_centrality_rank",
            FastField::PageCentrality => "page_centrality",
            FastField::PageCentralityRank => "page_centrality_rank",
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
            FastField::TitleEmbeddings => "title_embeddings",
            FastField::KeywordEmbeddings => "keyword_embeddings",
        }
    }
}

impl InsertEnumMapKey for FastField {
    fn into_usize(self) -> usize {
        self as usize
    }
}
