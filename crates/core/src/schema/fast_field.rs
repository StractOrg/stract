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
use tantivy::{
    schema::{BytesOptions, NumericOptions},
    TantivyDocument,
};

use crate::{
    enum_dispatch_from_discriminant,
    enum_map::InsertEnumMapKey,
    simhash,
    webpage::{html::FnCache, Html, Webpage},
    Result,
};

use super::{IndexingOption, FLOAT_SCALING};

#[enum_dispatch]
pub trait FastField: Clone + Copy + std::fmt::Debug + PartialEq + Eq + std::hash::Hash {
    fn name(&self) -> &str;
    fn add_html_tantivy(
        &self,
        html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()>;

    fn add_webpage_tantivy(
        &self,
        _webpage: &crate::webpage::Webpage,
        _doc: &mut TantivyDocument,
        _schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        Ok(())
    }

    fn data_type(&self) -> DataType {
        DataType::U64
    }

    fn is_stored(&self) -> bool {
        false
    }

    fn is_indexed(&self) -> bool {
        true
    }

    fn indexing_option(&self) -> IndexingOption {
        debug_assert!(matches!(self.data_type(), DataType::U64));

        let mut opt = NumericOptions::default().set_fast();

        if self.is_stored() {
            opt = opt.set_stored();
        }

        if self.is_indexed() {
            opt = opt.set_indexed();
        }

        IndexingOption::Integer(opt)
    }

    fn tantivy_field(&self, schema: &tantivy::schema::Schema) -> tantivy::schema::Field {
        schema
            .get_field(self.name())
            .unwrap_or_else(|_| unreachable!("Unknown field: {}", self.name()))
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

enum_dispatch_from_discriminant!(FastFieldEnumDiscriminants => FastFieldEnum,
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

    fn add_html_tantivy(
        &self,
        html: &Html,
        _cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), (html.is_homepage()).into());

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostCentrality;
impl FastField for HostCentrality {
    fn name(&self) -> &str {
        "host_centrality"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            (webpage.host_centrality * FLOAT_SCALING as f64) as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostCentralityRank;
impl FastField for HostCentralityRank {
    fn name(&self) -> &str {
        "host_centrality_rank"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), webpage.host_centrality_rank);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageCentrality;
impl FastField for PageCentrality {
    fn name(&self) -> &str {
        "page_centrality"
    }
    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            (webpage.page_centrality * FLOAT_SCALING as f64) as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageCentralityRank;
impl FastField for PageCentralityRank {
    fn name(&self) -> &str {
        "page_centrality_rank"
    }
    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), webpage.page_centrality_rank);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FetchTimeMs;
impl FastField for FetchTimeMs {
    fn name(&self) -> &str {
        "fetch_time_ms"
    }
    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), webpage.fetch_time_ms);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LastUpdated;
impl FastField for LastUpdated {
    fn name(&self) -> &str {
        "last_updated"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn add_html_tantivy(
        &self,
        html: &Html,
        _cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            html.updated_time()
                .map_or(0, |time| time.timestamp().max(0) as u64),
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TrackerScore;
impl FastField for TrackerScore {
    fn name(&self) -> &str {
        "tracker_score"
    }

    fn add_html_tantivy(
        &self,
        html: &Html,
        _cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), html.trackers().len() as u64);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Region;
impl FastField for Region {
    fn name(&self) -> &str {
        "region"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        let region = crate::webpage::region::Region::guess_from(webpage);
        if let Ok(region) = region {
            doc.add_u64(self.tantivy_field(schema), region.id());
        } else {
            doc.add_u64(
                self.tantivy_field(schema),
                crate::webpage::region::Region::All.id(),
            );
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumUrlTokens;
impl FastField for NumUrlTokens {
    fn name(&self) -> &str {
        "num_url_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            cache.pretokenize_url().tokens.len() as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumTitleTokens;
impl FastField for NumTitleTokens {
    fn name(&self) -> &str {
        "num_title_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            cache
                .pretokenize_title()
                .as_ref()
                .map(|n| n.tokens.len() as u64)
                .map_err(|e| anyhow::anyhow!("{}", e))?,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumCleanBodyTokens;
impl FastField for NumCleanBodyTokens {
    fn name(&self) -> &str {
        "num_clean_body_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            cache.pretokenize_clean_text().tokens.len() as u64,
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumDescriptionTokens;
impl FastField for NumDescriptionTokens {
    fn name(&self) -> &str {
        "num_description_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            cache.pretokenize_description().tokens.len() as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumUrlForSiteOperatorTokens;
impl FastField for NumUrlForSiteOperatorTokens {
    fn name(&self) -> &str {
        "num_url_for_site_operator_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            cache.pretokenize_url_for_site_operator().tokens.len() as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumDomainTokens;
impl FastField for NumDomainTokens {
    fn name(&self) -> &str {
        "num_domain_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            cache.pretokenize_domain().tokens.len() as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumMicroformatTagsTokens;
impl FastField for NumMicroformatTagsTokens {
    fn name(&self) -> &str {
        "num_microformat_tags_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            cache.pretokenize_microformats().tokens.len() as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteHash1;
impl FastField for SiteHash1 {
    fn name(&self) -> &str {
        "site_hash1"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), cache.site_hash()[0]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteHash2;
impl FastField for SiteHash2 {
    fn name(&self) -> &str {
        "site_hash2"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), cache.site_hash()[1]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutQueryHash1;
impl FastField for UrlWithoutQueryHash1 {
    fn name(&self) -> &str {
        "url_without_query_hash1"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            cache.url_without_query_hash()[0],
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutQueryHash2;
impl FastField for UrlWithoutQueryHash2 {
    fn name(&self) -> &str {
        "url_without_query_hash2"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            cache.url_without_query_hash()[1],
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleHash1;
impl FastField for TitleHash1 {
    fn name(&self) -> &str {
        "title_hash1"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), cache.title_hash()[0]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleHash2;
impl FastField for TitleHash2 {
    fn name(&self) -> &str {
        "title_hash2"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), cache.title_hash()[1]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlHash1;
impl FastField for UrlHash1 {
    fn name(&self) -> &str {
        "url_hash1"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), cache.url_hash()[0]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlHash2;
impl FastField for UrlHash2 {
    fn name(&self) -> &str {
        "url_hash2"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), cache.url_hash()[1]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainHash1;
impl FastField for DomainHash1 {
    fn name(&self) -> &str {
        "domain_hash1"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), cache.domain_hash()[0]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainHash2;
impl FastField for DomainHash2 {
    fn name(&self) -> &str {
        "domain_hash2"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), cache.domain_hash()[1]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutTldHash1;
impl FastField for UrlWithoutTldHash1 {
    fn name(&self) -> &str {
        "url_without_tld_hash1"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), cache.url_without_tld_hash()[0]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutTldHash2;
impl FastField for UrlWithoutTldHash2 {
    fn name(&self) -> &str {
        "url_without_tld_hash2"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), cache.url_without_tld_hash()[1]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PreComputedScore;
impl FastField for PreComputedScore {
    fn name(&self) -> &str {
        "pre_computed_score"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            (webpage.pre_computed_score * FLOAT_SCALING as f64) as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostNodeID;
impl FastField for HostNodeID {
    fn name(&self) -> &str {
        "host_node_id"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        match &webpage.node_id {
            Some(node_id) => {
                doc.add_u64(self.tantivy_field(schema), node_id.as_u64());
            }
            None => {
                doc.add_u64(self.tantivy_field(schema), u64::MAX);
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SimHash;
impl FastField for SimHash {
    fn name(&self) -> &str {
        "sim_hash"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        let clean_text = cache.pretokenize_clean_text();

        let hash = if !clean_text.text.is_empty() {
            simhash::hash(&clean_text.text)
        } else {
            0
        };
        doc.add_u64(self.tantivy_field(schema), hash);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumFlattenedSchemaTokens;
impl FastField for NumFlattenedSchemaTokens {
    fn name(&self) -> &str {
        "num_flattened_schema_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            cache.pretokenized_schema_json().tokens.len() as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumPathAndQuerySlashes;
impl FastField for NumPathAndQuerySlashes {
    fn name(&self) -> &str {
        "num_path_and_query_slashes"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn add_html_tantivy(
        &self,
        html: &Html,
        _cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        let num_slashes = html
            .url()
            .path_segments()
            .map(|segments| segments.count())
            .unwrap_or(0);

        doc.add_u64(self.tantivy_field(schema), num_slashes as u64);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumPathAndQueryDigits;
impl FastField for NumPathAndQueryDigits {
    fn name(&self) -> &str {
        "num_path_and_query_digits"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn add_html_tantivy(
        &self,
        html: &Html,
        _cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        let num_digits = html
            .url()
            .path()
            .chars()
            .filter(|c| c.is_ascii_digit())
            .count()
            + html
                .url()
                .query()
                .unwrap_or_default()
                .chars()
                .filter(|c| c.is_ascii_digit())
                .count();

        doc.add_u64(self.tantivy_field(schema), num_digits as u64);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LikelyHasAds;
impl FastField for LikelyHasAds {
    fn name(&self) -> &str {
        "likely_has_ads"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn add_html_tantivy(
        &self,
        html: &Html,
        _cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), html.likely_has_ads() as u64);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LikelyHasPaywall;
impl FastField for LikelyHasPaywall {
    fn name(&self) -> &str {
        "likely_has_paywall"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn add_html_tantivy(
        &self,
        html: &Html,
        _cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(schema), html.likely_has_paywall() as u64);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LinkDensity;
impl FastField for LinkDensity {
    fn name(&self) -> &str {
        "link_density"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn add_html_tantivy(
        &self,
        html: &Html,
        _cache: &mut FnCache,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(schema),
            (html.link_density() * FLOAT_SCALING as f64) as u64,
        );

        Ok(())
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

    fn indexing_option(&self) -> IndexingOption {
        IndexingOption::Bytes(BytesOptions::default().set_fast().set_stored())
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        if let Some(emb) = &webpage.title_embedding {
            let mut serialized = Vec::new();
            emb.write_bytes(&mut serialized)?;

            doc.add_bytes(self.tantivy_field(schema), &serialized);
        } else {
            doc.add_bytes(self.tantivy_field(schema), &[]);
        }

        Ok(())
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

    fn indexing_option(&self) -> IndexingOption {
        IndexingOption::Bytes(BytesOptions::default().set_fast().set_stored())
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        schema: &tantivy::schema::Schema,
    ) -> Result<()> {
        if let Some(emb) = &webpage.keyword_embedding {
            let mut serialized = Vec::new();
            emb.write_bytes(&mut serialized)?;

            doc.add_bytes(self.tantivy_field(schema), &serialized);
        } else {
            doc.add_bytes(self.tantivy_field(schema), &[]);
        }

        Ok(())
    }
}
