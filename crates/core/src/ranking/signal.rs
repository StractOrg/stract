// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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

use crate::enum_map::InsertEnumMapKey;
use crate::fastfield_reader::FieldReader;
use crate::query::optic::AsSearchableRule;
use crate::query::Query;
use crate::schema::text_field::TextField;
use crate::schema::{self, Field};
use crate::{enum_dispatch_from_discriminant, Result};
use crate::{
    enum_map::EnumMap,
    fastfield_reader,
    schema::{FastFieldEnum, TextFieldEnum},
    webgraph::NodeID,
    webpage::Webpage,
};
use enum_dispatch::enum_dispatch;
use itertools::Itertools;
use optics::ast::RankingTarget;
use optics::Optic;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::str::FromStr;
use std::sync::Arc;
use strum::{EnumDiscriminants, VariantArray};
use tantivy::fieldnorm::FieldNormReader;
use tantivy::postings::SegmentPostings;
use tantivy::query::{Query as _, Scorer};
use tantivy::tokenizer::Tokenizer as _;
use thiserror::Error;
use utoipa::ToSchema;

use tantivy::DocSet;
use tantivy::{DocId, Postings};

use crate::{schema::FLOAT_SCALING, webpage::region::RegionCount};

use super::bm25::MultiBm25Weight;
use super::models::linear::LinearRegression;
use super::{inbound_similarity, query_centrality};

#[derive(Debug, Error)]
pub enum Error {
    #[error("unknown signal: {0}")]
    UnknownSignal(#[from] serde_json::Error),
}

#[enum_dispatch]
pub trait Signal:
    Clone + Copy + std::fmt::Debug + PartialEq + Eq + std::hash::Hash + Into<SignalEnum>
{
    fn default_coefficient(&self) -> f64;
    fn as_field(&self) -> Option<Field>;
    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64>;

    fn precompute(self, _webpage: &Webpage, _signal_aggregator: &SignalAggregator) -> Option<f64> {
        None
    }

    fn as_textfield(&self) -> Option<TextFieldEnum> {
        self.as_field().and_then(|field| field.as_text())
    }

    fn as_fastfield(&self) -> Option<FastFieldEnum> {
        self.as_field().and_then(|field| field.as_fast())
    }
}

#[enum_dispatch(Signal)]
#[derive(
    Debug, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq, Hash, EnumDiscriminants,
)]
#[strum_discriminants(derive(VariantArray, serde::Serialize, serde::Deserialize, Hash))]
#[strum_discriminants(serde(rename_all = "snake_case"))]
pub enum SignalEnum {
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
}

enum_dispatch_from_discriminant!(SignalEnumDiscriminants => SignalEnum,
[
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
]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Bm25Title;
impl Signal for Bm25Title {
    fn default_coefficient(&self) -> f64 {
        0.0063
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::Title.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Bm25TitleBigrams;
impl Signal for Bm25TitleBigrams {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::TitleBigrams.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Bm25TitleTrigrams;
impl Signal for Bm25TitleTrigrams {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::TitleTrigrams.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Bm25CleanBody;
impl Signal for Bm25CleanBody {
    fn default_coefficient(&self) -> f64 {
        0.0063
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::CleanBody.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Bm25CleanBodyBigrams;
impl Signal for Bm25CleanBodyBigrams {
    fn default_coefficient(&self) -> f64 {
        0.005
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::CleanBodyBigrams.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Bm25CleanBodyTrigrams;
impl Signal for Bm25CleanBodyTrigrams {
    fn default_coefficient(&self) -> f64 {
        0.005
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::CleanBodyTrigrams.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Bm25StemmedTitle;
impl Signal for Bm25StemmedTitle {
    fn default_coefficient(&self) -> f64 {
        0.003
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::StemmedTitle.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Bm25StemmedCleanBody;
impl Signal for Bm25StemmedCleanBody {
    fn default_coefficient(&self) -> f64 {
        0.001
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::StemmedCleanBody.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Bm25AllBody;
impl Signal for Bm25AllBody {
    fn default_coefficient(&self) -> f64 {
        0.0
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::AllBody.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Bm25Keywords;
impl Signal for Bm25Keywords {
    fn default_coefficient(&self) -> f64 {
        0.001
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::Keywords.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Bm25BacklinkText;
impl Signal for Bm25BacklinkText {
    fn default_coefficient(&self) -> f64 {
        0.003
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::BacklinkText.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IdfSumUrl;
impl Signal for IdfSumUrl {
    fn default_coefficient(&self) -> f64 {
        0.0003
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::Url.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IdfSumSite;
impl Signal for IdfSumSite {
    fn default_coefficient(&self) -> f64 {
        0.00015
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::SiteWithout.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IdfSumDomain;
impl Signal for IdfSumDomain {
    fn default_coefficient(&self) -> f64 {
        0.0003
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::Domain.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IdfSumSiteNoTokenizer;
impl Signal for IdfSumSiteNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.00015
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::SiteNoTokenizer.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IdfSumDomainNoTokenizer;
impl Signal for IdfSumDomainNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.0002
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::DomainNoTokenizer.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IdfSumDomainNameNoTokenizer;
impl Signal for IdfSumDomainNameNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.0002
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(
            schema::text_field::DomainNameNoTokenizer.into(),
        ))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IdfSumDomainIfHomepage;
impl Signal for IdfSumDomainIfHomepage {
    fn default_coefficient(&self) -> f64 {
        0.0004
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::DomainIfHomepage.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IdfSumDomainNameIfHomepageNoTokenizer;
impl Signal for IdfSumDomainNameIfHomepageNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.0036
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(
            schema::text_field::DomainNameIfHomepageNoTokenizer.into(),
        ))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IdfSumDomainIfHomepageNoTokenizer;
impl Signal for IdfSumDomainIfHomepageNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.0036
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(
            schema::text_field::DomainIfHomepageNoTokenizer.into(),
        ))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IdfSumTitleIfHomepage;
impl Signal for IdfSumTitleIfHomepage {
    fn default_coefficient(&self) -> f64 {
        0.00022
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::TitleIfHomepage.into()))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();

        seg_reader
            .text_fields
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CrossEncoderSnippet;
impl Signal for CrossEncoderSnippet {
    fn default_coefficient(&self) -> f64 {
        0.17
    }

    fn as_field(&self) -> Option<Field> {
        None
    }

    fn compute(&self, _: DocId, _: &SignalAggregator) -> Option<f64> {
        None // computed in later ranking stage
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CrossEncoderTitle;
impl Signal for CrossEncoderTitle {
    fn default_coefficient(&self) -> f64 {
        0.17
    }

    fn as_field(&self) -> Option<Field> {
        None
    }

    fn compute(&self, _: DocId, _: &SignalAggregator) -> Option<f64> {
        None // computed in later ranking stage
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct HostCentrality;
impl Signal for HostCentrality {
    fn default_coefficient(&self) -> f64 {
        0.5
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::HostCentrality.into()))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalAggregator) -> Option<f64> {
        Some(webpage.host_centrality)
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);

        let val = fastfield_reader
            .get(self.as_fastfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        Some(val as f64 / FLOAT_SCALING as f64)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct HostCentralityRank;
impl Signal for HostCentralityRank {
    fn default_coefficient(&self) -> f64 {
        0.0
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::HostCentralityRank.into()))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalAggregator) -> Option<f64> {
        Some(score_rank(webpage.host_centrality_rank as f64))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);

        let val = fastfield_reader
            .get(self.as_fastfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        Some(score_rank(val as f64))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PageCentrality;
impl Signal for PageCentrality {
    fn default_coefficient(&self) -> f64 {
        0.25
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::PageCentrality.into()))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalAggregator) -> Option<f64> {
        Some(webpage.page_centrality)
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);

        let val = fastfield_reader
            .get(self.as_fastfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        Some(val as f64 / FLOAT_SCALING as f64)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PageCentralityRank;
impl Signal for PageCentralityRank {
    fn default_coefficient(&self) -> f64 {
        0.0
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::PageCentralityRank.into()))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalAggregator) -> Option<f64> {
        Some(score_rank(webpage.page_centrality_rank as f64))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);

        let val = fastfield_reader
            .get(self.as_fastfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        Some(score_rank(val as f64))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IsHomepage;
impl Signal for IsHomepage {
    fn default_coefficient(&self) -> f64 {
        0.0005
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::IsHomepage.into()))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalAggregator) -> Option<f64> {
        Some(webpage.html.is_homepage().into())
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);

        let val = fastfield_reader
            .get(self.as_fastfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        Some(val as f64)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FetchTimeMs;
impl Signal for FetchTimeMs {
    fn default_coefficient(&self) -> f64 {
        0.001
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::FetchTimeMs.into()))
    }

    fn precompute(self, webpage: &Webpage, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let fetch_time_ms = webpage.fetch_time_ms as usize;
        if fetch_time_ms >= signal_aggregator.fetch_time_ms_cache.len() {
            Some(0.0)
        } else {
            Some(signal_aggregator.fetch_time_ms_cache[fetch_time_ms])
        }
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);

        let fetch_time_ms = fastfield_reader
            .get(self.as_fastfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap() as usize;

        if fetch_time_ms >= signal_aggregator.fetch_time_ms_cache.len() {
            Some(0.0)
        } else {
            Some(signal_aggregator.fetch_time_ms_cache[fetch_time_ms])
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct UpdateTimestamp;
impl Signal for UpdateTimestamp {
    fn default_coefficient(&self) -> f64 {
        0.001
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::LastUpdated.into()))
    }

    fn precompute(self, webpage: &Webpage, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let update_timestamp = webpage
            .html
            .updated_time()
            .map(|date| date.timestamp().max(0))
            .unwrap_or(0) as usize;

        Some(score_timestamp(update_timestamp, signal_aggregator))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);

        let val = fastfield_reader
            .get(self.as_fastfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap() as usize;

        Some(score_timestamp(val, signal_aggregator))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TrackerScore;
impl Signal for TrackerScore {
    fn default_coefficient(&self) -> f64 {
        0.05
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::TrackerScore.into()))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalAggregator) -> Option<f64> {
        let num_trackers = webpage.html.trackers().len() as f64;
        Some(score_trackers(num_trackers))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);

        let val = fastfield_reader
            .get(self.as_fastfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        Some(score_trackers(val as f64))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Region;
impl Signal for Region {
    fn default_coefficient(&self) -> f64 {
        0.15
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::Region.into()))
    }

    fn precompute(self, webpage: &Webpage, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let region =
            crate::webpage::Region::guess_from(webpage).unwrap_or(crate::webpage::Region::All);
        Some(score_region(region, signal_aggregator))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);

        let val = fastfield_reader
            .get(self.as_fastfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        let region = crate::webpage::Region::from_id(val);
        Some(score_region(region, signal_aggregator))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct QueryCentrality;
impl Signal for QueryCentrality {
    fn default_coefficient(&self) -> f64 {
        0.0
    }

    fn as_field(&self) -> Option<Field> {
        None
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);
        let host_id = host_id(&fastfield_reader);

        host_id.and_then(|host_id| signal_aggregator.query_centrality(host_id))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct InboundSimilarity;
impl Signal for InboundSimilarity {
    fn default_coefficient(&self) -> f64 {
        0.25
    }

    fn as_field(&self) -> Option<Field> {
        None
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);
        let host_id = host_id(&fastfield_reader);

        host_id.map(|host_id| signal_aggregator.inbound_similarity(host_id))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct LambdaMart;
impl Signal for LambdaMart {
    fn default_coefficient(&self) -> f64 {
        10.0
    }

    fn as_field(&self) -> Option<Field> {
        None
    }

    fn compute(&self, _: DocId, _: &SignalAggregator) -> Option<f64> {
        None // computed in later ranking stage
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct UrlDigits;
impl Signal for UrlDigits {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(
            schema::fast_field::NumPathAndQueryDigits.into(),
        ))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalAggregator) -> Option<f64> {
        let num_digits = (webpage
            .html
            .url()
            .path()
            .chars()
            .filter(|c| c.is_ascii_digit())
            .count()
            + webpage
                .html
                .url()
                .query()
                .unwrap_or_default()
                .chars()
                .filter(|c| c.is_ascii_digit())
                .count()) as f64;

        Some(score_digits(num_digits))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);

        let val = fastfield_reader
            .get(self.as_fastfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        Some(score_digits(val as f64))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct UrlSlashes;
impl Signal for UrlSlashes {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(
            schema::fast_field::NumPathAndQuerySlashes.into(),
        ))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalAggregator) -> Option<f64> {
        let num_slashes = webpage
            .html
            .url()
            .path()
            .chars()
            .filter(|c| c == &'/')
            .count() as f64;
        Some(score_slashes(num_slashes))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);

        let val = fastfield_reader
            .get(self.as_fastfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        Some(score_slashes(val as f64))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct LinkDensity;
impl Signal for LinkDensity {
    fn default_coefficient(&self) -> f64 {
        0.0
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::LinkDensity.into()))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalAggregator) -> Option<f64> {
        let link_density = webpage.html.link_density();
        Some(score_link_density(link_density))
    }

    fn compute(&self, doc: DocId, signal_aggregator: &SignalAggregator) -> Option<f64> {
        let seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(doc);

        let val = fastfield_reader
            .get(self.as_fastfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        Some(score_link_density(val as f64 / FLOAT_SCALING as f64))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TitleEmbeddingSimilarity;
impl Signal for TitleEmbeddingSimilarity {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::TitleEmbeddings.into()))
    }

    fn compute(&self, _: DocId, _: &SignalAggregator) -> Option<f64> {
        None // computed in later ranking stage
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct KeywordEmbeddingSimilarity;
impl Signal for KeywordEmbeddingSimilarity {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::KeywordEmbeddings.into()))
    }

    fn compute(&self, _: DocId, _: &SignalAggregator) -> Option<f64> {
        None // computed in later ranking stage
    }
}

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

fn score_timestamp(timestamp: usize, signal_aggregator: &SignalAggregator) -> f64 {
    if timestamp >= signal_aggregator.current_timestamp.unwrap_or(0) {
        return 0.0;
    }

    let hours_since_update =
        (signal_aggregator.current_timestamp.unwrap() - timestamp).max(1) / 3600;

    if hours_since_update < signal_aggregator.update_time_cache.len() {
        signal_aggregator.update_time_cache[hours_since_update]
    } else {
        0.0
    }
}

#[inline]
fn score_rank(rank: f64) -> f64 {
    1.0 / (rank + 1.0)
}

#[inline]
fn score_trackers(num_trackers: f64) -> f64 {
    1.0 / (num_trackers + 1.0)
}

#[inline]
fn score_digits(num_digits: f64) -> f64 {
    1.0 / (num_digits + 1.0)
}

#[inline]
fn score_slashes(num_slashes: f64) -> f64 {
    1.0 / (num_slashes + 1.0)
}

#[inline]
fn score_link_density(link_density: f64) -> f64 {
    if link_density > 0.5 {
        0.0
    } else {
        1.0 - link_density
    }
}

fn score_region(webpage_region: crate::webpage::Region, aggregator: &SignalAggregator) -> f64 {
    match aggregator.region_count.as_ref() {
        Some(region_count) => {
            let boost = aggregator
                .query_data
                .as_ref()
                .and_then(|q| q.selected_region)
                .map_or(0.0, |region| {
                    if region != crate::webpage::Region::All && region == webpage_region {
                        50.0
                    } else {
                        0.0
                    }
                });

            boost + region_count.score(&webpage_region)
        }
        None => 0.0,
    }
}

fn bm25(field: &mut TextFieldData, doc: DocId) -> f64 {
    if field.postings.is_empty() {
        return 0.0;
    }

    let fieldnorm_id = field.fieldnorm_reader.fieldnorm_id(doc);

    field
        .weight
        .score(field.postings.iter_mut().map(move |posting| {
            if posting.doc() == doc || (posting.doc() < doc && posting.seek(doc) == doc) {
                (fieldnorm_id, posting.term_freq())
            } else {
                (fieldnorm_id, 0)
            }
        })) as f64
}

fn idf_sum(field: &mut TextFieldData, doc: DocId) -> f64 {
    if field.postings.is_empty() {
        return 0.0;
    }

    field
        .postings
        .iter_mut()
        .zip_eq(field.weight.idf())
        .filter_map(|(posting, idf)| {
            if posting.doc() == doc || (posting.doc() < doc && posting.seek(doc) == doc) {
                Some(idf)
            } else {
                None
            }
        })
        .sum::<f32>() as f64
}

fn host_id<'a>(fastfield_reader: &FieldReader<'a>) -> Option<NodeID> {
    let node_id = fastfield_reader
        .get(schema::fast_field::HostNodeID.into())
        .and_then(|n| n.as_u64())
        .unwrap();

    if node_id == u64::MAX {
        None
    } else {
        Some(node_id.into())
    }
}

impl FromStr for SignalEnumDiscriminants {
    type Err = Error;

    fn from_str(name: &str) -> std::result::Result<Self, Self::Err> {
        let s = "\"".to_string() + name + "\"";
        let signal = serde_json::from_str(&s)?;
        Ok(signal)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SignalCoefficient {
    map: EnumMap<SignalEnum, f64>,
}

impl SignalCoefficient {
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

    pub fn from_optic(optic: &Optic) -> Self {
        SignalCoefficient::new(optic.rankings.iter().filter_map(|coeff| {
            match &coeff.target {
                RankingTarget::Signal(signal) => SignalEnumDiscriminants::from_str(signal)
                    .ok()
                    .map(|signal| (signal.into(), coeff.value)),
            }
        }))
    }

    pub fn merge_into(&mut self, coeffs: SignalCoefficient) {
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
}

#[derive(Clone)]
struct TextFieldData {
    postings: Vec<SegmentPostings>,
    weight: MultiBm25Weight,
    fieldnorm_reader: FieldNormReader,
}

struct RuleBoost {
    docset: Box<dyn Scorer>,
    boost: f64,
}

struct OpticBoosts {
    rules: Vec<RuleBoost>,
}

struct SegmentReader {
    text_fields: EnumMap<TextFieldEnum, TextFieldData>,
    optic_boosts: OpticBoosts,
    fastfield_reader: Arc<fastfield_reader::SegmentReader>,
}

#[derive(Clone)]
struct QueryData {
    simple_terms: Vec<String>,
    optic_rules: Vec<optics::Rule>,
    selected_region: Option<crate::webpage::Region>,
}

pub struct SignalAggregator {
    query_data: Option<QueryData>,
    query_signal_coefficients: Option<SignalCoefficient>,
    segment_reader: Option<RefCell<SegmentReader>>,
    inbound_similarity: Option<RefCell<inbound_similarity::Scorer>>,
    fetch_time_ms_cache: Vec<f64>,
    update_time_cache: Vec<f64>,
    query_centrality: Option<RefCell<query_centrality::Scorer>>,
    region_count: Option<Arc<RegionCount>>,
    current_timestamp: Option<usize>,
    linear_regression: Option<Arc<LinearRegression>>,
    order: SignalOrder,
}

impl Clone for SignalAggregator {
    fn clone(&self) -> Self {
        let inbound_similarity = self
            .inbound_similarity
            .as_ref()
            .map(|scorer| RefCell::new(scorer.borrow().clone()));

        let query_centrality = self
            .query_centrality
            .as_ref()
            .map(|scorer| RefCell::new(scorer.borrow().clone()));

        Self {
            query_data: self.query_data.clone(),
            query_signal_coefficients: self.query_signal_coefficients.clone(),
            segment_reader: None,
            inbound_similarity,
            fetch_time_ms_cache: self.fetch_time_ms_cache.clone(),
            update_time_cache: self.update_time_cache.clone(),
            query_centrality,
            region_count: self.region_count.clone(),
            current_timestamp: self.current_timestamp,
            linear_regression: self.linear_regression.clone(),
            order: self.order.clone(),
        }
    }
}

impl std::fmt::Debug for SignalAggregator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignalAggregator")
            .field(
                "query",
                &self
                    .query_data
                    .as_ref()
                    .map(|q| q.simple_terms.clone())
                    .unwrap_or_default(),
            )
            .finish()
    }
}

impl SignalAggregator {
    pub fn new(query: Option<&Query>) -> Self {
        let query_signal_coefficients = query.as_ref().and_then(|q| q.signal_coefficients());

        let fetch_time_ms_cache: Vec<_> = (0..1000)
            .map(|fetch_time| 1.0 / (fetch_time as f64 + 1.0))
            .collect();

        let update_time_cache = (0..(3 * 365 * 24))
            .map(|hours_since_update| 1.0 / ((hours_since_update as f64 + 1.0).log2()))
            .collect();

        let query = query.as_ref().map(|q| QueryData {
            simple_terms: q.simple_terms().to_vec(),
            optic_rules: q
                .optics()
                .iter()
                .flat_map(|o| o.rules.iter())
                .filter(|rule| match rule.action {
                    optics::Action::Downrank(b) | optics::Action::Boost(b) => b != 0,
                    optics::Action::Discard => false,
                })
                .cloned()
                .collect(),
            selected_region: q.region().cloned(),
        });

        let mut s = Self {
            segment_reader: None,
            inbound_similarity: None,
            query_signal_coefficients,
            fetch_time_ms_cache,
            update_time_cache,
            query_centrality: None,
            region_count: None,
            current_timestamp: None,
            linear_regression: None,
            query_data: query,
            order: SignalOrder::empty(),
        };

        s.order = SignalOrder::new(&s);
        s.set_current_timestamp(chrono::Utc::now().timestamp() as usize);

        s
    }

    fn prepare_textfields(
        &self,
        tv_searcher: &tantivy::Searcher,
        segment_reader: &tantivy::SegmentReader,
    ) -> Result<EnumMap<TextFieldEnum, TextFieldData>> {
        let mut text_fields = EnumMap::new();
        let schema = tv_searcher.schema();

        if let Some(query) = &self.query_data {
            if !query.simple_terms.is_empty() {
                for signal in SignalEnum::all() {
                    if let Some(text_field) = signal.as_textfield() {
                        let tv_field = schema.get_field(text_field.name()).unwrap();
                        let simple_query = itertools::intersperse(
                            query.simple_terms.iter().map(|s| s.as_str()),
                            " ",
                        )
                        .collect::<String>();

                        let mut terms = Vec::new();
                        let mut tokenizer = text_field.indexing_tokenizer();
                        let mut stream = tokenizer.token_stream(&simple_query);

                        while let Some(token) = stream.next() {
                            let term = tantivy::Term::from_field_text(tv_field, &token.text);
                            terms.push(term);
                        }

                        if terms.is_empty() {
                            continue;
                        }

                        let fieldnorm_reader = segment_reader.get_fieldnorms_reader(tv_field)?;
                        let inverted_index = segment_reader.inverted_index(tv_field)?;

                        let mut matching_terms = Vec::with_capacity(terms.len());
                        let mut postings = Vec::with_capacity(terms.len());
                        for term in &terms {
                            if let Some(p) =
                                inverted_index.read_postings(term, text_field.record_option())?
                            {
                                postings.push(p);
                                matching_terms.push(term.clone());
                            }
                        }
                        let weight = MultiBm25Weight::for_terms(tv_searcher, &matching_terms)?;

                        text_fields.insert(
                            text_field,
                            TextFieldData {
                                postings,
                                weight,
                                fieldnorm_reader,
                            },
                        );
                    }
                }
            }
        }

        Ok(text_fields)
    }

    fn prepare_optic(
        &self,
        tv_searcher: &tantivy::Searcher,
        segment_reader: &tantivy::SegmentReader,
        fastfield_reader: &fastfield_reader::FastFieldReader,
    ) -> Vec<RuleBoost> {
        let mut optic_rule_boosts = Vec::new();

        if let Some(query) = &self.query_data {
            optic_rule_boosts = query
                .optic_rules
                .iter()
                .filter_map(|rule| rule.as_searchable_rule(tv_searcher.schema(), fastfield_reader))
                .map(|(_, rule)| RuleBoost {
                    docset: rule
                        .query
                        .weight(tantivy::query::EnableScoring::Enabled {
                            searcher: tv_searcher,
                            statistics_provider: tv_searcher,
                        })
                        .unwrap()
                        .scorer(segment_reader, 0.0)
                        .unwrap(),
                    boost: rule.boost,
                })
                .collect();
        }

        optic_rule_boosts
    }

    pub fn register_segment(
        &mut self,
        tv_searcher: &tantivy::Searcher,
        segment_reader: &tantivy::SegmentReader,
        fastfield_reader: &fastfield_reader::FastFieldReader,
    ) -> Result<()> {
        let fastfield_segment_reader = fastfield_reader.get_segment(&segment_reader.segment_id());
        let text_fields = self.prepare_textfields(tv_searcher, segment_reader)?;
        let optic_rule_boosts = self.prepare_optic(tv_searcher, segment_reader, fastfield_reader);

        self.segment_reader = Some(RefCell::new(SegmentReader {
            text_fields,
            fastfield_reader: fastfield_segment_reader,
            optic_boosts: OpticBoosts {
                rules: optic_rule_boosts,
            },
        }));

        Ok(())
    }

    pub fn set_query_centrality(&mut self, query_centrality: query_centrality::Scorer) {
        self.query_centrality = Some(RefCell::new(query_centrality));
    }

    pub fn set_inbound_similarity(&mut self, scorer: inbound_similarity::Scorer) {
        let mut scorer = scorer;
        scorer.set_default_if_precalculated(true);

        self.inbound_similarity = Some(RefCell::new(scorer));
    }

    pub fn set_region_count(&mut self, region_count: RegionCount) {
        self.region_count = Some(Arc::new(region_count));
    }

    pub fn set_current_timestamp(&mut self, current_timestamp: usize) {
        self.current_timestamp = Some(current_timestamp);
    }

    pub fn set_linear_model(&mut self, linear_model: Arc<LinearRegression>) {
        self.linear_regression = Some(linear_model);
    }

    pub fn query_centrality(&self, host_id: NodeID) -> Option<f64> {
        self.query_centrality
            .as_ref()
            .map(|scorer| scorer.borrow_mut().score(host_id))
    }

    pub fn inbound_similarity(&self, host_id: NodeID) -> f64 {
        self.inbound_similarity
            .as_ref()
            .map(|scorer| scorer.borrow_mut().score(&host_id))
            .unwrap_or_default()
    }

    /// Computes the scored signals for a given document.
    ///
    /// Important: This function assues that the docs a scored in ascending order of docid
    /// within their segment. If this invariant is not upheld, the documents will not have
    /// scores calculated for their text related signals. The wrong ranking will most likely
    /// be returned.
    /// This function also assumes that the segment reader has been set.
    pub fn compute_signals(&self, doc: DocId) -> impl Iterator<Item = Option<ComputedSignal>> + '_ {
        self.order.compute(doc, self)
    }

    pub fn boosts(&mut self, doc: DocId) -> Option<f64> {
        self.segment_reader.as_ref().map(|segment_reader| {
            let mut downrank = 0.0;
            let mut boost = 0.0;

            for rule in &mut segment_reader.borrow_mut().optic_boosts.rules {
                if rule.docset.doc() > doc {
                    continue;
                }

                if rule.docset.doc() == doc || rule.docset.seek(doc) == doc {
                    if rule.boost < 0.0 {
                        downrank += rule.boost.abs();
                    } else {
                        boost += rule.boost;
                    }
                }
            }

            if downrank > boost {
                let diff = downrank - boost;
                1.0 / (1.0 + diff)
            } else {
                boost - downrank + 1.0
            }
        })
    }

    pub fn precompute_score(&self, webpage: &Webpage) -> f64 {
        SignalEnum::all()
            .filter_map(|signal| {
                signal
                    .precompute(webpage, self)
                    .map(|value| ComputedSignal {
                        signal,
                        score: SignalScore {
                            coefficient: self.coefficient(&signal),
                            value,
                        },
                    })
            })
            .map(|computed| computed.score.coefficient * computed.score.value)
            .sum()
    }

    pub fn coefficient(&self, signal: &SignalEnum) -> f64 {
        self.query_signal_coefficients
            .as_ref()
            .map(|coefficients| coefficients.get(signal))
            .or_else(|| {
                self.linear_regression
                    .as_ref()
                    .and_then(|model| model.weights.get(*signal).copied())
            })
            .unwrap_or(signal.default_coefficient())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ComputedSignal {
    pub signal: SignalEnum,
    pub score: SignalScore,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignalScore {
    pub coefficient: f64,
    pub value: f64,
}

#[derive(Clone)]
pub struct SignalOrder {
    text_signals: EnumMap<TextFieldEnum, NGramSignalOrder>,
    other_signals: Vec<SignalEnum>,
}

impl SignalOrder {
    pub fn empty() -> Self {
        Self {
            text_signals: EnumMap::new(),
            other_signals: Vec::new(),
        }
    }

    pub fn new(signal_aggregator: &SignalAggregator) -> Self {
        let mut text_signals = EnumMap::new();
        let mut other_signals = Vec::new();

        for signal in SignalEnum::all() {
            if signal_aggregator.coefficient(&signal) == 0.0 {
                continue;
            }

            if let Some(text_field) = signal.as_textfield() {
                let mono = text_field.monogram_field();

                if !text_signals.contains_key(mono) {
                    text_signals.insert(mono, NGramSignalOrder::default());
                }

                let ngram = text_field.ngram_size();
                text_signals.get_mut(mono).unwrap().push(signal, ngram);
            } else {
                other_signals.push(signal);
            }
        }

        Self {
            text_signals,
            other_signals,
        }
    }

    fn compute<'a>(
        &'a self,
        doc: DocId,
        signal_aggregator: &'a SignalAggregator,
    ) -> impl Iterator<Item = Option<ComputedSignal>> + 'a {
        self.text_signals
            .values()
            .flat_map(move |ngram| ngram.compute(doc, signal_aggregator))
            .map(Some)
            .chain(
                self.other_signals
                    .iter()
                    .filter_map(|signal| {
                        let coefficient = signal_aggregator.coefficient(signal);

                        if coefficient > 0.0 {
                            Some((signal, coefficient))
                        } else {
                            None
                        }
                    })
                    .map(move |(signal, coefficient)| {
                        signal
                            .compute(doc, signal_aggregator)
                            .map(|value| ComputedSignal {
                                signal: *signal,
                                score: SignalScore { coefficient, value },
                            })
                    }),
            )
    }
}

/// If an ngram of size n matches the query for a given document in a given field,
/// the score of all ngrams where n' < n is dampened by NGRAM_DAMPENING.
///
/// A dampening factor of 0.0 means that we ignore all ngrams where n' < n. A dampening factor of 1.0
/// does not dampen any ngrams.
const NGRAM_DAMPENING: f64 = 0.4;

#[derive(Debug, Default, Clone)]
pub struct NGramSignalOrder {
    /// ordered by descending ngram size. e.g. [title_bm25_trigram, title_bm25_bigram, title_bm25]
    signals: Vec<(usize, SignalEnum)>,
}

impl NGramSignalOrder {
    fn push(&mut self, signal: SignalEnum, ngram: usize) {
        self.signals.push((ngram, signal));
        self.signals.sort_unstable_by(|(a, _), (b, _)| b.cmp(a));
    }

    fn compute<'a>(
        &'a self,
        doc: DocId,
        signal_aggregator: &'a SignalAggregator,
    ) -> impl Iterator<Item = ComputedSignal> + 'a {
        let mut hits = 0;

        self.signals
            .iter()
            .map(|(_, s)| s)
            .filter_map(move |signal| {
                signal
                    .compute(doc, signal_aggregator)
                    .map(|value| {
                        let coefficient = signal_aggregator.coefficient(signal);

                        ComputedSignal {
                            signal: *signal,
                            score: SignalScore { coefficient, value },
                        }
                    })
                    .map(|mut c| {
                        c.score.coefficient *= NGRAM_DAMPENING.powi(hits);

                        if c.score.value > 0.0 {
                            hits += 1;
                        }

                        c
                    })
            })
    }
}
