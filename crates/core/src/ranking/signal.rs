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

use crate::query::optic::AsSearchableRule;
use crate::query::Query;
use crate::Result;
use crate::{
    enum_map::EnumMap,
    fastfield_reader,
    schema::{FastField, TextField},
    webgraph::NodeID,
    webpage::Webpage,
};
use optics::ast::RankingTarget;
use optics::Optic;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::str::FromStr;
use std::sync::Arc;
use tantivy::fieldnorm::FieldNormReader;
use tantivy::postings::SegmentPostings;
use tantivy::query::{Query as _, Scorer};
use tantivy::tokenizer::Tokenizer;
use thiserror::Error;
use utoipa::ToSchema;

use tantivy::DocSet;
use tantivy::{DocId, Postings};

use crate::{
    schema::FLOAT_SCALING,
    webpage::region::{Region, RegionCount},
};

use super::bm25::Bm25Weight;
use super::models::linear::LinearRegression;
use super::{inbound_similarity, query_centrality};

#[derive(Debug, Error)]
pub enum Error {
    #[error("unknown signal: {0}")]
    UnknownSignal(#[from] serde_json::Error),
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Signal {
    #[serde(rename = "bm25_title")]
    Bm25Title,
    #[serde(rename = "bm25_title_bigrams")]
    Bm25TitleBigrams,
    #[serde(rename = "bm25_title_trigrams")]
    Bm25TitleTrigrams,
    #[serde(rename = "bm25_clean_body")]
    Bm25CleanBody,
    #[serde(rename = "bm25_clean_body_bigrams")]
    Bm25CleanBodyBigrams,
    #[serde(rename = "bm25_clean_body_trigrams")]
    Bm25CleanBodyTrigrams,
    #[serde(rename = "bm25_stemmed_title")]
    Bm25StemmedTitle,
    #[serde(rename = "bm25_stemmed_clean_body")]
    Bm25StemmedCleanBody,
    #[serde(rename = "bm25_all_body")]
    Bm25AllBody,
    #[serde(rename = "bm25_url")]
    Bm25Url,
    #[serde(rename = "bm25_site")]
    Bm25Site,
    #[serde(rename = "bm25_domain")]
    Bm25Domain,
    #[serde(rename = "bm25_site_no_tokenizer")]
    Bm25SiteNoTokenizer,
    #[serde(rename = "bm25_domain_no_tokenizer")]
    Bm25DomainNoTokenizer,
    #[serde(rename = "bm25_domain_name_no_tokenizer")]
    Bm25DomainNameNoTokenizer,
    #[serde(rename = "bm25_domain_if_homepage")]
    Bm25DomainIfHomepage,
    #[serde(rename = "bm25_domain_name_if_homepage_no_tokenizer")]
    Bm25DomainNameIfHomepageNoTokenizer,
    #[serde(rename = "bm25_domain_if_homepage_no_tokenizer")]
    Bm25DomainIfHomepageNoTokenizer,
    #[serde(rename = "bm25_title_if_homepage")]
    Bm25TitleIfHomepage,
    #[serde(rename = "bm25_backlink_text")]
    Bm25BacklinkText,
    #[serde(rename = "cross_encoder_snippet")]
    CrossEncoderSnippet,
    #[serde(rename = "cross_encoder_title")]
    CrossEncoderTitle,
    #[serde(rename = "host_centrality")]
    HostCentrality,
    #[serde(rename = "host_centrality_rank")]
    HostCentralityRank,
    #[serde(rename = "page_centrality")]
    PageCentrality,
    #[serde(rename = "page_centrality_rank")]
    PageCentralityRank,
    #[serde(rename = "is_homepage")]
    IsHomepage,
    #[serde(rename = "fetch_time_ms")]
    FetchTimeMs,
    #[serde(rename = "update_timestamp")]
    UpdateTimestamp,
    #[serde(rename = "tracker_score")]
    TrackerScore,
    #[serde(rename = "region")]
    Region,
    #[serde(rename = "query_centrality")]
    QueryCentrality,
    #[serde(rename = "inbound_similarity")]
    InboundSimilarity,
    #[serde(rename = "lambda_mart")]
    LambdaMART,
    #[serde(rename = "url_digits")]
    UrlDigits,
    #[serde(rename = "url_slashes")]
    UrlSlashes,
    #[serde(rename = "link_density")]
    LinkDensity,
}

impl From<Signal> for usize {
    fn from(signal: Signal) -> Self {
        signal as usize
    }
}

pub const ALL_SIGNALS: [Signal; 37] = [
    Signal::Bm25Title,
    Signal::Bm25TitleBigrams,
    Signal::Bm25TitleTrigrams,
    Signal::Bm25CleanBody,
    Signal::Bm25CleanBodyBigrams,
    Signal::Bm25CleanBodyTrigrams,
    Signal::Bm25StemmedTitle,
    Signal::Bm25StemmedCleanBody,
    Signal::Bm25AllBody,
    Signal::Bm25Url,
    Signal::Bm25Site,
    Signal::Bm25Domain,
    Signal::Bm25SiteNoTokenizer,
    Signal::Bm25DomainNoTokenizer,
    Signal::Bm25DomainNameNoTokenizer,
    Signal::Bm25DomainIfHomepage,
    Signal::Bm25DomainNameIfHomepageNoTokenizer,
    Signal::Bm25DomainIfHomepageNoTokenizer,
    Signal::Bm25TitleIfHomepage,
    Signal::Bm25BacklinkText,
    Signal::CrossEncoderSnippet,
    Signal::CrossEncoderTitle,
    Signal::HostCentrality,
    Signal::HostCentralityRank,
    Signal::PageCentrality,
    Signal::PageCentralityRank,
    Signal::IsHomepage,
    Signal::FetchTimeMs,
    Signal::UpdateTimestamp,
    Signal::TrackerScore,
    Signal::Region,
    Signal::QueryCentrality,
    Signal::InboundSimilarity,
    Signal::LambdaMART,
    Signal::UrlDigits,
    Signal::UrlSlashes,
    Signal::LinkDensity,
];

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

fn score_region(webpage_region: Region, aggregator: &SignalAggregator) -> f64 {
    match aggregator.region_count.as_ref() {
        Some(region_count) => {
            let boost = aggregator
                .query_data
                .as_ref()
                .and_then(|q| q.selected_region)
                .map_or(0.0, |region| {
                    if region != Region::All && region == webpage_region {
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

    let mut term_freq = 0;
    for posting in &mut field.postings {
        if posting.doc() == doc || (posting.doc() < doc && posting.seek(doc) == doc) {
            term_freq += posting.term_freq();
        }
    }

    if term_freq == 0 {
        return 0.0;
    }

    let fieldnorm_id = field.fieldnorm_reader.fieldnorm_id(doc);
    field.weight.score(fieldnorm_id, term_freq) as f64
}

impl Signal {
    fn is_computable_before_search(&self) -> bool {
        self.as_fastfield().is_some()
    }

    pub fn default_coefficient(&self) -> f64 {
        match self {
            Signal::Bm25Title => 0.0063,
            Signal::Bm25TitleBigrams => 0.01,
            Signal::Bm25TitleTrigrams => 0.01,
            Signal::Bm25CleanBody => 0.0063,
            Signal::Bm25CleanBodyBigrams => 0.005,
            Signal::Bm25CleanBodyTrigrams => 0.005,
            Signal::Bm25StemmedTitle => 0.003,
            Signal::Bm25StemmedCleanBody => 0.001,
            Signal::Bm25AllBody => 0.0,
            Signal::Bm25Url => 0.0003,
            Signal::Bm25Site => 0.00015,
            Signal::Bm25Domain => 0.0003,
            Signal::Bm25SiteNoTokenizer => 0.00015,
            Signal::Bm25DomainNoTokenizer => 0.0002,
            Signal::Bm25DomainNameNoTokenizer => 0.0002,
            Signal::Bm25DomainIfHomepage => 0.0004,
            Signal::Bm25DomainNameIfHomepageNoTokenizer => 0.0036,
            Signal::Bm25DomainIfHomepageNoTokenizer => 0.0036,
            Signal::Bm25TitleIfHomepage => 0.00022,
            Signal::Bm25BacklinkText => 0.003,
            Signal::CrossEncoderSnippet => 0.17,
            Signal::CrossEncoderTitle => 0.17,
            Signal::HostCentrality => 0.5,
            Signal::HostCentralityRank => 0.0,
            Signal::PageCentrality => 0.25,
            Signal::PageCentralityRank => 0.0,
            Signal::QueryCentrality => 0.0,
            Signal::IsHomepage => 0.0005,
            Signal::FetchTimeMs => 0.001,
            Signal::UpdateTimestamp => 0.001,
            Signal::TrackerScore => 0.05,
            Signal::Region => 0.15,
            Signal::InboundSimilarity => 0.25,
            Signal::LambdaMART => 10.0,
            Signal::UrlSlashes => 0.01,
            Signal::UrlDigits => 0.01,
            Signal::LinkDensity => 0.00,
        }
    }

    fn compute(self, signal_aggregator: &SignalAggregator, doc: DocId) -> Option<ComputedSignal> {
        let coefficient = signal_aggregator.coefficient(&self);
        if coefficient == 0.0 {
            return None;
        }

        let mut seg_reader = signal_aggregator
            .segment_reader
            .as_ref()
            .unwrap()
            .borrow_mut();
        let fastfield_reader = seg_reader.fastfield_reader.get_field_reader(&doc);

        let node_id = fastfield_reader.get(&FastField::HostNodeID);
        let host_id: Option<NodeID> = if node_id == u64::MAX {
            None
        } else {
            Some(node_id.into())
        };

        let value: Option<f64> = match self {
            Signal::HostCentrality | Signal::PageCentrality => {
                let val = fastfield_reader.get(&self.as_fastfield().unwrap());
                Some(val as f64 / FLOAT_SCALING as f64)
            }
            Signal::HostCentralityRank | Signal::PageCentralityRank => {
                let val = fastfield_reader.get(&self.as_fastfield().unwrap());
                Some(score_rank(val as f64))
            }
            Signal::IsHomepage => {
                let val = fastfield_reader.get(&self.as_fastfield().unwrap());
                Some(val as f64)
            }
            Signal::LinkDensity => {
                let val = fastfield_reader.get(&self.as_fastfield().unwrap());
                Some(score_link_density(val as f64 / FLOAT_SCALING as f64))
            }
            Signal::FetchTimeMs => {
                let fetch_time_ms = fastfield_reader.get(&self.as_fastfield().unwrap()) as usize;

                if fetch_time_ms >= signal_aggregator.fetch_time_ms_cache.len() {
                    Some(0.0)
                } else {
                    Some(signal_aggregator.fetch_time_ms_cache[fetch_time_ms])
                }
            }
            Signal::UpdateTimestamp => {
                let val = fastfield_reader.get(&self.as_fastfield().unwrap()) as usize;

                Some(score_timestamp(val, signal_aggregator))
            }
            Signal::TrackerScore => {
                let val = fastfield_reader.get(&self.as_fastfield().unwrap());
                Some(score_trackers(val as f64))
            }
            Signal::UrlDigits => {
                let val = fastfield_reader.get(&self.as_fastfield().unwrap());
                Some(score_digits(val as f64))
            }
            Signal::UrlSlashes => {
                let val = fastfield_reader.get(&self.as_fastfield().unwrap());
                Some(score_slashes(val as f64))
            }
            Signal::Region => {
                let val = fastfield_reader.get(&self.as_fastfield().unwrap());
                let region = Region::from_id(val);
                Some(score_region(region, signal_aggregator))
            }
            Signal::QueryCentrality => {
                host_id.and_then(|host_id| signal_aggregator.query_centrality(host_id))
            }
            Signal::InboundSimilarity => {
                host_id.map(|host_id| signal_aggregator.inbound_similarity(host_id))
            }
            Signal::Bm25Title
            | Signal::Bm25TitleBigrams
            | Signal::Bm25TitleTrigrams
            | Signal::Bm25CleanBody
            | Signal::Bm25CleanBodyBigrams
            | Signal::Bm25CleanBodyTrigrams
            | Signal::Bm25StemmedTitle
            | Signal::Bm25StemmedCleanBody
            | Signal::Bm25AllBody
            | Signal::Bm25Url
            | Signal::Bm25Site
            | Signal::Bm25Domain
            | Signal::Bm25SiteNoTokenizer
            | Signal::Bm25DomainNoTokenizer
            | Signal::Bm25DomainNameNoTokenizer
            | Signal::Bm25DomainIfHomepage
            | Signal::Bm25DomainNameIfHomepageNoTokenizer
            | Signal::Bm25DomainIfHomepageNoTokenizer
            | Signal::Bm25TitleIfHomepage
            | Signal::Bm25BacklinkText => seg_reader
                .text_fields
                .get_mut(self.as_textfield().unwrap())
                .map(|field| bm25(field, doc)),

            Signal::CrossEncoderSnippet => None, // this is calculated in a later step
            Signal::CrossEncoderTitle => None,   // this is calculated in a later step
            Signal::LambdaMART => None,
        };

        value.map(|value| ComputedSignal {
            signal: self,
            score: SignalScore { coefficient, value },
        })
    }

    pub fn precompute(
        self,
        signal_aggregator: &SignalAggregator,
        webpage: &Webpage,
    ) -> Option<ComputedSignal> {
        if !self.is_computable_before_search() {
            return None;
        }

        let value = match self {
            Signal::HostCentrality => Some(webpage.host_centrality),
            Signal::HostCentralityRank => Some(webpage.host_centrality_rank),
            Signal::PageCentrality => Some(webpage.page_centrality),
            Signal::PageCentralityRank => Some(webpage.page_centrality_rank),
            Signal::IsHomepage => Some(webpage.html.is_homepage().into()),
            Signal::FetchTimeMs => {
                let fetch_time_ms = webpage.fetch_time_ms as usize;
                if fetch_time_ms >= signal_aggregator.fetch_time_ms_cache.len() {
                    Some(0.0)
                } else {
                    Some(signal_aggregator.fetch_time_ms_cache[fetch_time_ms])
                }
            }
            Signal::UpdateTimestamp => {
                let update_timestamp = webpage
                    .html
                    .updated_time()
                    .map(|date| date.timestamp().max(0))
                    .unwrap_or(0) as usize;

                Some(score_timestamp(update_timestamp, signal_aggregator))
            }
            Signal::TrackerScore => {
                let num_trackers = webpage.html.trackers().len() as f64;
                Some(score_trackers(num_trackers))
            }
            Signal::Region => {
                let region = Region::guess_from(webpage).unwrap_or(Region::All);
                Some(score_region(region, signal_aggregator))
            }
            Signal::UrlDigits => {
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
            Signal::UrlSlashes => {
                let num_slashes = webpage
                    .html
                    .url()
                    .path()
                    .chars()
                    .filter(|c| c == &'/')
                    .count() as f64;
                Some(score_slashes(num_slashes))
            }
            Signal::LinkDensity => {
                let link_density = webpage.html.link_density();
                Some(score_link_density(link_density))
            }
            Signal::Bm25Title
            | Signal::Bm25TitleBigrams
            | Signal::Bm25TitleTrigrams
            | Signal::Bm25CleanBody
            | Signal::Bm25CleanBodyBigrams
            | Signal::Bm25CleanBodyTrigrams
            | Signal::Bm25StemmedTitle
            | Signal::Bm25StemmedCleanBody
            | Signal::Bm25AllBody
            | Signal::Bm25Url
            | Signal::Bm25Site
            | Signal::Bm25Domain
            | Signal::Bm25SiteNoTokenizer
            | Signal::Bm25DomainNoTokenizer
            | Signal::Bm25DomainNameNoTokenizer
            | Signal::Bm25DomainIfHomepage
            | Signal::Bm25DomainNameIfHomepageNoTokenizer
            | Signal::Bm25DomainIfHomepageNoTokenizer
            | Signal::Bm25TitleIfHomepage
            | Signal::Bm25BacklinkText
            | Signal::CrossEncoderSnippet
            | Signal::CrossEncoderTitle
            | Signal::InboundSimilarity
            | Signal::LambdaMART
            | Signal::QueryCentrality => {
                tracing::error!("signal {self:?} cannot be precomputed");
                None
            }
        };

        value.map(|value| ComputedSignal {
            signal: self,
            score: SignalScore {
                coefficient: signal_aggregator.coefficient(&self),
                value,
            },
        })
    }

    fn as_fastfield(&self) -> Option<FastField> {
        match self {
            Signal::HostCentrality => Some(FastField::HostCentrality),
            Signal::HostCentralityRank => Some(FastField::HostCentralityRank),
            Signal::PageCentrality => Some(FastField::PageCentrality),
            Signal::PageCentralityRank => Some(FastField::PageCentralityRank),
            Signal::IsHomepage => Some(FastField::IsHomepage),
            Signal::FetchTimeMs => Some(FastField::FetchTimeMs),
            Signal::UpdateTimestamp => Some(FastField::LastUpdated),
            Signal::TrackerScore => Some(FastField::TrackerScore),
            Signal::Region => Some(FastField::Region),
            Signal::UrlSlashes => Some(FastField::NumPathAndQuerySlashes),
            Signal::UrlDigits => Some(FastField::NumPathAndQueryDigits),
            Signal::LinkDensity => Some(FastField::LinkDensity),
            _ => None,
        }
    }

    fn as_textfield(&self) -> Option<TextField> {
        match self {
            Signal::Bm25Title => Some(TextField::Title),
            Signal::Bm25TitleBigrams => Some(TextField::TitleBigrams),
            Signal::Bm25TitleTrigrams => Some(TextField::TitleTrigrams),
            Signal::Bm25CleanBody => Some(TextField::CleanBody),
            Signal::Bm25CleanBodyBigrams => Some(TextField::CleanBodyBigrams),
            Signal::Bm25CleanBodyTrigrams => Some(TextField::CleanBodyTrigrams),
            Signal::Bm25StemmedTitle => Some(TextField::StemmedTitle),
            Signal::Bm25StemmedCleanBody => Some(TextField::StemmedCleanBody),
            Signal::Bm25AllBody => Some(TextField::AllBody),
            Signal::Bm25Url => Some(TextField::Url),
            Signal::Bm25Site => Some(TextField::SiteWithout),
            Signal::Bm25Domain => Some(TextField::Domain),
            Signal::Bm25SiteNoTokenizer => Some(TextField::SiteNoTokenizer),
            Signal::Bm25DomainNoTokenizer => Some(TextField::DomainNoTokenizer),
            Signal::Bm25DomainNameNoTokenizer => Some(TextField::DomainNameNoTokenizer),
            Signal::Bm25DomainIfHomepage => Some(TextField::DomainIfHomepage),
            Signal::Bm25DomainNameIfHomepageNoTokenizer => {
                Some(TextField::DomainNameIfHomepageNoTokenizer)
            }
            Signal::Bm25TitleIfHomepage => Some(TextField::TitleIfHomepage),
            Signal::Bm25BacklinkText => Some(TextField::BacklinkText),
            Signal::Bm25DomainIfHomepageNoTokenizer => Some(TextField::DomainIfHomepageNoTokenizer),
            _ => None,
        }
    }
}

impl FromStr for Signal {
    type Err = Error;

    fn from_str(name: &str) -> std::result::Result<Self, Self::Err> {
        let s = "\"".to_string() + name + "\"";
        let signal = serde_json::from_str(&s)?;
        Ok(signal)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SignalCoefficient {
    map: EnumMap<Signal, f64>,
}

impl SignalCoefficient {
    pub fn get(&self, signal: &Signal) -> Option<f64> {
        self.map.get(*signal).copied()
    }

    pub fn new(coefficients: impl Iterator<Item = (Signal, f64)>) -> Self {
        let mut map = EnumMap::default();

        for (signal, coefficient) in coefficients {
            map.insert(signal, coefficient);
        }

        Self { map }
    }

    pub fn from_optic(optic: &Optic) -> Self {
        SignalCoefficient::new(optic.rankings.iter().filter_map(|coeff| {
            match &coeff.target {
                RankingTarget::Signal(signal) => Signal::from_str(signal)
                    .ok()
                    .map(|signal| (signal, coeff.value)),
            }
        }))
    }

    pub fn merge_into(&mut self, coeffs: SignalCoefficient) {
        for signal in ALL_SIGNALS {
            if let Some(coeff) = coeffs.get(&signal) {
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
    weight: Bm25Weight,
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
    text_fields: EnumMap<TextField, TextFieldData>,
    optic_boosts: OpticBoosts,
    fastfield_reader: Arc<fastfield_reader::SegmentReader>,
}

#[derive(Clone)]
struct QueryData {
    simple_terms: Vec<String>,
    optic_rules: Vec<optics::Rule>,
    selected_region: Option<Region>,
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
    ) -> Result<EnumMap<TextField, TextFieldData>> {
        let mut text_fields = EnumMap::new();
        let schema = tv_searcher.schema();

        if let Some(query) = &self.query_data {
            if !query.simple_terms.is_empty() {
                for signal in ALL_SIGNALS {
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

                        let weight = Bm25Weight::for_terms(tv_searcher, &terms)?;

                        let fieldnorm_reader = segment_reader.get_fieldnorms_reader(tv_field)?;
                        let inverted_index = segment_reader.inverted_index(tv_field)?;

                        let mut postings = Vec::with_capacity(terms.len());
                        for term in &terms {
                            if let Some(p) =
                                inverted_index.read_postings(term, text_field.index_option())?
                            {
                                postings.push(p);
                            }
                        }

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
        ALL_SIGNALS
            .into_iter()
            .filter_map(|signal| signal.precompute(self, webpage))
            .map(|computed| computed.score.coefficient * computed.score.value)
            .sum()
    }

    pub fn coefficient(&self, signal: &Signal) -> f64 {
        self.query_signal_coefficients
            .as_ref()
            .and_then(|coefficients| coefficients.get(signal))
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
    pub signal: Signal,
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
    text_signals: EnumMap<TextField, NGramSignalOrder>,
    other_signals: Vec<Signal>,
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

        for signal in ALL_SIGNALS {
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
                    .map(move |signal| signal.compute(signal_aggregator, doc)),
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
    signals: Vec<(usize, Signal)>,
}

impl NGramSignalOrder {
    fn push(&mut self, signal: Signal, ngram: usize) {
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
                signal.compute(signal_aggregator, doc).map(|mut c| {
                    c.score.coefficient *= NGRAM_DAMPENING.powi(hits);

                    if c.score.value > 0.0 {
                        hits += 1;
                    }

                    c
                })
            })
    }
}
