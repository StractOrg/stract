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

use bitflags::bitflags;
use enum_dispatch::enum_dispatch;
use rustc_hash::FxHashMap;
use strum::{EnumDiscriminants, VariantArray};
use tantivy::{
    schema::{BytesOptions, NumericOptions},
    TantivyDocument,
};

use crate::{
    enum_dispatch_from_discriminant,
    enum_map::InsertEnumMapKey,
    simhash,
    webpage::{html::FnCache, url_ext::UrlExt, Html, Webpage},
    Result,
};

use super::IndexingOption;

#[derive(Debug, Clone, Copy, bincode::Encode, bincode::Decode, PartialEq, Eq, Hash)]
pub struct Orientation(u8);

bitflags! {
    impl Orientation: u8 {
        const ROW = 1 << 0;
        const COLUMNAR = 1 << 1;
    }
}

#[enum_dispatch]
pub trait NumericalField:
    Clone + Copy + std::fmt::Debug + PartialEq + Eq + std::hash::Hash
{
    fn name(&self) -> &str;
    fn add_html_tantivy(
        &self,
        html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()>;

    fn add_webpage_tantivy(
        &self,
        _webpage: &crate::webpage::Webpage,
        _doc: &mut TantivyDocument,
        _index: &crate::inverted_index::InvertedIndex,
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
        let orientation = self.orientation();
        match self.data_type() {
            DataType::U64 | DataType::Bool | DataType::F64 => {
                let mut opt = NumericOptions::default();

                if orientation.contains(Orientation::COLUMNAR) {
                    opt = opt.set_columnar();
                }

                if orientation.contains(Orientation::ROW) {
                    opt = opt.set_row_order();
                }

                if self.is_stored() {
                    opt = opt.set_stored();
                }

                if self.is_indexed() {
                    opt = opt.set_indexed();
                }

                IndexingOption::Integer(opt)
            }
            DataType::Bytes => {
                let mut opt = BytesOptions::default().set_indexed();

                if orientation.contains(Orientation::COLUMNAR) {
                    opt = opt.set_columnar();
                }

                if self.is_stored() {
                    opt = opt.set_stored();
                }

                IndexingOption::Bytes(opt)
            }
        }
    }

    fn tantivy_field(&self, schema: &tantivy::schema::Schema) -> tantivy::schema::Field {
        schema
            .get_field(self.name())
            .unwrap_or_else(|_| unreachable!("Unknown field: {}", self.name()))
    }

    fn orientation(&self) -> Orientation {
        Orientation::COLUMNAR
    }
}

#[enum_dispatch(NumericalField)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumDiscriminants)]
#[strum_discriminants(derive(VariantArray))]
pub enum NumericalFieldEnum {
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
    SuffixId,
}

enum_dispatch_from_discriminant!(NumericalFieldEnumDiscriminants => NumericalFieldEnum,
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
    SuffixId,
]);

impl NumericalFieldEnum {
    pub fn all() -> impl Iterator<Item = NumericalFieldEnum> {
        NumericalFieldEnumDiscriminants::VARIANTS
            .iter()
            .copied()
            .map(|v| v.into())
    }

    pub fn get(field_id: usize) -> Option<NumericalFieldEnum> {
        NumericalFieldEnumDiscriminants::VARIANTS
            .get(field_id)
            .copied()
            .map(NumericalFieldEnum::from)
    }

    pub fn num_variants() -> usize {
        NumericalFieldEnumDiscriminants::VARIANTS.len()
    }
}

pub enum DataType {
    U64,
    F64,
    Bool,
    Bytes,
}

impl InsertEnumMapKey for NumericalFieldEnum {
    fn into_usize(self) -> usize {
        NumericalFieldEnumDiscriminants::from(self) as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IsHomepage;
impl NumericalField for IsHomepage {
    fn name(&self) -> &str {
        "is_homepage"
    }

    fn data_type(&self) -> DataType {
        DataType::Bool
    }

    fn add_html_tantivy(
        &self,
        html: &Html,
        _cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_bool(self.tantivy_field(index.schema_ref()), html.is_homepage());

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostCentrality;
impl NumericalField for HostCentrality {
    fn name(&self) -> &str {
        "host_centrality"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        Ok(())
    }

    fn data_type(&self) -> DataType {
        DataType::F64
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_f64(
            self.tantivy_field(index.schema_ref()),
            webpage.host_centrality,
        );

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostCentralityRank;
impl NumericalField for HostCentralityRank {
    fn name(&self) -> &str {
        "host_centrality_rank"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            webpage.host_centrality_rank,
        );

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageCentrality;
impl NumericalField for PageCentrality {
    fn name(&self) -> &str {
        "page_centrality"
    }
    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        Ok(())
    }

    fn data_type(&self) -> DataType {
        DataType::F64
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_f64(
            self.tantivy_field(index.schema_ref()),
            webpage.page_centrality,
        );

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageCentralityRank;
impl NumericalField for PageCentralityRank {
    fn name(&self) -> &str {
        "page_centrality_rank"
    }
    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            webpage.page_centrality_rank,
        );

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FetchTimeMs;
impl NumericalField for FetchTimeMs {
    fn name(&self) -> &str {
        "fetch_time_ms"
    }
    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            webpage.fetch_time_ms,
        );

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LastUpdated;
impl NumericalField for LastUpdated {
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
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            html.updated_time()
                .map_or(0, |time| time.timestamp().max(0) as u64),
        );

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TrackerScore;
impl NumericalField for TrackerScore {
    fn name(&self) -> &str {
        "tracker_score"
    }

    fn add_html_tantivy(
        &self,
        html: &Html,
        _cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            html.trackers().len() as u64,
        );

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Region;
impl NumericalField for Region {
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
        _index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        let region = crate::webpage::region::Region::guess_from(webpage);
        if let Ok(region) = region {
            doc.add_u64(self.tantivy_field(index.schema_ref()), region.id());
        } else {
            doc.add_u64(
                self.tantivy_field(index.schema_ref()),
                crate::webpage::region::Region::All.id(),
            );
        }

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumUrlTokens;
impl NumericalField for NumUrlTokens {
    fn name(&self) -> &str {
        "num_url_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.pretokenize_url().tokens.len() as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumTitleTokens;
impl NumericalField for NumTitleTokens {
    fn name(&self) -> &str {
        "num_title_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
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
impl NumericalField for NumCleanBodyTokens {
    fn name(&self) -> &str {
        "num_clean_body_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.pretokenize_clean_text().tokens.len() as u64,
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumDescriptionTokens;
impl NumericalField for NumDescriptionTokens {
    fn name(&self) -> &str {
        "num_description_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.pretokenize_description().tokens.len() as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumUrlForSiteOperatorTokens;
impl NumericalField for NumUrlForSiteOperatorTokens {
    fn name(&self) -> &str {
        "num_url_for_site_operator_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.pretokenize_url_for_site_operator().tokens.len() as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumDomainTokens;
impl NumericalField for NumDomainTokens {
    fn name(&self) -> &str {
        "num_domain_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.pretokenize_domain().tokens.len() as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumMicroformatTagsTokens;
impl NumericalField for NumMicroformatTagsTokens {
    fn name(&self) -> &str {
        "num_microformat_tags_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.pretokenize_microformats().tokens.len() as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteHash1;
impl NumericalField for SiteHash1 {
    fn name(&self) -> &str {
        "site_hash1"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(index.schema_ref()), cache.site_hash()[0]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SiteHash2;
impl NumericalField for SiteHash2 {
    fn name(&self) -> &str {
        "site_hash2"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(index.schema_ref()), cache.site_hash()[1]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutQueryHash1;
impl NumericalField for UrlWithoutQueryHash1 {
    fn name(&self) -> &str {
        "url_without_query_hash1"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.url_without_query_hash()[0],
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutQueryHash2;
impl NumericalField for UrlWithoutQueryHash2 {
    fn name(&self) -> &str {
        "url_without_query_hash2"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.url_without_query_hash()[1],
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleHash1;
impl NumericalField for TitleHash1 {
    fn name(&self) -> &str {
        "title_hash1"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.title_hash()[0],
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleHash2;
impl NumericalField for TitleHash2 {
    fn name(&self) -> &str {
        "title_hash2"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.title_hash()[1],
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlHash1;
impl NumericalField for UrlHash1 {
    fn name(&self) -> &str {
        "url_hash1"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(index.schema_ref()), cache.url_hash()[0]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlHash2;
impl NumericalField for UrlHash2 {
    fn name(&self) -> &str {
        "url_hash2"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(self.tantivy_field(index.schema_ref()), cache.url_hash()[1]);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainHash1;
impl NumericalField for DomainHash1 {
    fn name(&self) -> &str {
        "domain_hash1"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.domain_hash()[0],
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainHash2;
impl NumericalField for DomainHash2 {
    fn name(&self) -> &str {
        "domain_hash2"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.domain_hash()[1],
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutTldHash1;
impl NumericalField for UrlWithoutTldHash1 {
    fn name(&self) -> &str {
        "url_without_tld_hash1"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.url_without_tld_hash()[0],
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UrlWithoutTldHash2;
impl NumericalField for UrlWithoutTldHash2 {
    fn name(&self) -> &str {
        "url_without_tld_hash2"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.url_without_tld_hash()[1],
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PreComputedScore;
impl NumericalField for PreComputedScore {
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
        _index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        Ok(())
    }

    fn data_type(&self) -> DataType {
        DataType::F64
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_f64(
            self.tantivy_field(index.schema_ref()),
            webpage.pre_computed_score,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostNodeID;
impl NumericalField for HostNodeID {
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
        _index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        match &webpage.node_id {
            Some(node_id) => {
                doc.add_u64(self.tantivy_field(index.schema_ref()), node_id.as_u64());
            }
            None => {
                doc.add_u64(self.tantivy_field(index.schema_ref()), u64::MAX);
            }
        }

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SimHash;
impl NumericalField for SimHash {
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
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        let clean_text = cache.pretokenize_clean_text();

        let hash = if !clean_text.text.is_empty() {
            simhash::hash(&clean_text.text)
        } else {
            0
        };
        doc.add_u64(self.tantivy_field(index.schema_ref()), hash);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumFlattenedSchemaTokens;
impl NumericalField for NumFlattenedSchemaTokens {
    fn name(&self) -> &str {
        "num_flattened_schema_tokens"
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_u64(
            self.tantivy_field(index.schema_ref()),
            cache.pretokenized_schema_json().tokens.len() as u64,
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumPathAndQuerySlashes;
impl NumericalField for NumPathAndQuerySlashes {
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
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        let num_slashes = html
            .url()
            .path_segments()
            .map(|segments| segments.count())
            .unwrap_or(0);

        doc.add_u64(self.tantivy_field(index.schema_ref()), num_slashes as u64);

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumPathAndQueryDigits;
impl NumericalField for NumPathAndQueryDigits {
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
        index: &crate::inverted_index::InvertedIndex,
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

        doc.add_u64(self.tantivy_field(index.schema_ref()), num_digits as u64);

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LikelyHasAds;
impl NumericalField for LikelyHasAds {
    fn name(&self) -> &str {
        "likely_has_ads"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn data_type(&self) -> DataType {
        DataType::Bool
    }

    fn add_html_tantivy(
        &self,
        html: &Html,
        _cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_bool(
            self.tantivy_field(index.schema_ref()),
            html.likely_has_ads(),
        );

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LikelyHasPaywall;
impl NumericalField for LikelyHasPaywall {
    fn name(&self) -> &str {
        "likely_has_paywall"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn data_type(&self) -> DataType {
        DataType::Bool
    }

    fn add_html_tantivy(
        &self,
        html: &Html,
        _cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_bool(
            self.tantivy_field(index.schema_ref()),
            html.likely_has_paywall(),
        );

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LinkDensity;
impl NumericalField for LinkDensity {
    fn name(&self) -> &str {
        "link_density"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn data_type(&self) -> DataType {
        DataType::F64
    }

    fn add_html_tantivy(
        &self,
        html: &Html,
        _cache: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        doc.add_f64(self.tantivy_field(index.schema_ref()), html.link_density());

        Ok(())
    }

    fn orientation(&self) -> Orientation {
        Orientation::ROW
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TitleEmbeddings;
impl NumericalField for TitleEmbeddings {
    fn name(&self) -> &str {
        "title_embeddings"
    }

    fn data_type(&self) -> DataType {
        DataType::Bytes
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        if let Some(emb) = &webpage.title_embedding {
            let mut serialized = Vec::new();
            emb.write_bytes(&mut serialized)?;

            doc.add_bytes(self.tantivy_field(index.schema_ref()), &serialized);
        } else {
            doc.add_bytes(self.tantivy_field(index.schema_ref()), &[]);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeywordEmbeddings;
impl NumericalField for KeywordEmbeddings {
    fn name(&self) -> &str {
        "keyword_embeddings"
    }

    fn data_type(&self) -> DataType {
        DataType::Bytes
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn add_html_tantivy(
        &self,
        _html: &Html,
        _cache: &mut FnCache,
        _doc: &mut TantivyDocument,
        _index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        Ok(())
    }

    fn add_webpage_tantivy(
        &self,
        webpage: &Webpage,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        if let Some(emb) = &webpage.keyword_embedding {
            let mut serialized = Vec::new();
            emb.write_bytes(&mut serialized)?;

            doc.add_bytes(self.tantivy_field(index.schema_ref()), &serialized);
        } else {
            doc.add_bytes(self.tantivy_field(index.schema_ref()), &[]);
        }

        Ok(())
    }
}

static SUFFIX_ID: std::sync::LazyLock<FxHashMap<String, u32>> = std::sync::LazyLock::new(|| {
    include_str!("../../public_suffix_list.dat")
        .lines()
        .filter(|l| !l.starts_with("//") && !l.chars().all(|c| c.is_whitespace()) && !l.is_empty())
        .enumerate()
        .map(|(i, l)| (l.to_string(), i as u32))
        .collect()
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SuffixId;
impl NumericalField for SuffixId {
    fn name(&self) -> &str {
        "suffix_id"
    }

    fn is_stored(&self) -> bool {
        true
    }

    fn add_html_tantivy(
        &self,
        html: &Html,
        _: &mut FnCache,
        doc: &mut TantivyDocument,
        index: &crate::inverted_index::InvertedIndex,
    ) -> Result<()> {
        let tld = html.url().tld().map(|s| s.to_string()).unwrap_or_default();
        let suffix_id = SUFFIX_ID.get(&tld).copied().unwrap_or(u32::MAX);
        doc.add_u64(self.tantivy_field(index.schema_ref()), suffix_id as u64);

        Ok(())
    }
}
