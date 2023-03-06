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

use crate::{
    fastfield_reader,
    schema::{FastField, TextField},
    webgraph::{
        centrality::{online_harmonic, topic},
        NodeID,
    },
    webpage::Webpage,
};
use std::{ops::Deref, sync::Arc};

use chrono::Utc;
use tantivy::DocId;

use crate::{
    schema::{Field, FLOAT_SCALING},
    webpage::region::{Region, RegionCount},
};

use super::{inbound_similarity, initial::Score};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Signal {
    Bm25,
    HostCentrality,
    PageCentrality,
    IsHomepage,
    FetchTimeMs,
    UpdateTimestamp,
    TrackerScore,
    Region,
    PersonalCentrality,
    CrawlStability,
    TopicCentrality,
    QueryCentrality,
    InboundSimilarity,
}

pub const ALL_SIGNALS: [Signal; 13] = [
    Signal::Bm25,
    Signal::HostCentrality,
    Signal::PageCentrality,
    Signal::IsHomepage,
    Signal::FetchTimeMs,
    Signal::UpdateTimestamp,
    Signal::TrackerScore,
    Signal::Region,
    Signal::PersonalCentrality,
    Signal::CrawlStability,
    Signal::TopicCentrality,
    Signal::QueryCentrality,
    Signal::InboundSimilarity,
];

struct SignalValue {
    bm25: tantivy::Score,
    fastfield_value: Option<u64>,
    current_timestamp: usize,
    selected_region: Option<Region>,
    personal_centrality: Option<f64>,
    topic_score: Option<f64>,
    query_centrality: Option<f64>,
    inbound_similarity: Option<f64>,
}

impl Signal {
    fn is_computable_before_search(&self) -> bool {
        self.as_fastfield().is_some()
    }

    fn value(
        &self,
        region_count: &impl Deref<Target = RegionCount>,
        aggregator: &SignalAggregator,
        value: SignalValue,
    ) -> f64 {
        match self {
            Signal::Bm25 => value.bm25 as f64,
            Signal::HostCentrality | Signal::PageCentrality => {
                value.fastfield_value.unwrap() as f64 / FLOAT_SCALING as f64
            }
            Signal::IsHomepage => value.fastfield_value.unwrap() as f64,
            Signal::FetchTimeMs => {
                let fetch_time_ms = value.fastfield_value.unwrap() as usize;

                if fetch_time_ms >= aggregator.fetch_time_ms_cache.len() {
                    0.0
                } else {
                    aggregator.fetch_time_ms_cache[fetch_time_ms]
                }
            }
            Signal::UpdateTimestamp => {
                let update_timestamp = value.fastfield_value.unwrap() as i64;

                if value.current_timestamp as i64 - update_timestamp <= 0 {
                    return 0.0;
                }

                let hours_since_update =
                    ((value.current_timestamp as i64 - update_timestamp).max(1) / 3600) as usize;

                if hours_since_update < aggregator.update_time_cache.len() {
                    aggregator.update_time_cache[hours_since_update]
                } else {
                    0.0
                }
            }
            Signal::TrackerScore => {
                let tracker_score = value.fastfield_value.unwrap() as f64;
                1.0 / (tracker_score + 1.0)
            }
            Signal::Region => {
                let webpage_region = Region::from_id(value.fastfield_value.unwrap());

                let boost = value.selected_region.map_or(0.0, |region| {
                    if region == webpage_region {
                        50.0
                    } else {
                        0.0
                    }
                });

                boost + region_count.score(&webpage_region)
            }
            Signal::CrawlStability => value.fastfield_value.unwrap() as f64 / FLOAT_SCALING as f64,
            Signal::TopicCentrality => value.topic_score.unwrap_or_default(),
            Signal::QueryCentrality => value.query_centrality.unwrap_or_default(),
            Signal::PersonalCentrality => value.personal_centrality.unwrap_or_default(),
            Signal::InboundSimilarity => value.inbound_similarity.unwrap_or_default(),
        }
    }

    fn default_coefficient(&self) -> f64 {
        match self {
            Signal::Bm25 => 1.0,
            Signal::HostCentrality => 2_500.0,
            Signal::PageCentrality => 4_500.0,
            Signal::TopicCentrality => 2_500.0,
            Signal::QueryCentrality => 1_000.0,
            Signal::IsHomepage => 0.1,
            Signal::FetchTimeMs => 0.001,
            Signal::UpdateTimestamp => 80.0,
            Signal::TrackerScore => 20.0,
            Signal::Region => 60.0,
            Signal::CrawlStability => 20.0,
            Signal::PersonalCentrality => 5_000.0,
            Signal::InboundSimilarity => 5_000.0,
        }
    }

    pub fn from_string(name: String) -> Option<Signal> {
        match name.as_str() {
            "bm25" => Some(Signal::Bm25),
            "host_centrality" => Some(Signal::HostCentrality),
            "page_centrality" => Some(Signal::PageCentrality),
            "is_homepage" => Some(Signal::IsHomepage),
            "fetch_time_ms" => Some(Signal::FetchTimeMs),
            "update_timestamp" => Some(Signal::UpdateTimestamp),
            "tracker_score" => Some(Signal::TrackerScore),
            "region" => Some(Signal::Region),
            "personal_centrality" => Some(Signal::PersonalCentrality),
            "topic_centrality" => Some(Signal::TopicCentrality),
            "query_centrality" => Some(Signal::QueryCentrality),
            "inbound_similarity" => Some(Signal::InboundSimilarity),
            "crawl_stability" => Some(Signal::CrawlStability),
            _ => None,
        }
    }

    fn as_fastfield(&self) -> Option<FastField> {
        match self {
            Signal::Bm25 => None,
            Signal::HostCentrality => Some(FastField::HostCentrality),
            Signal::PageCentrality => Some(FastField::PageCentrality),
            Signal::IsHomepage => Some(FastField::IsHomepage),
            Signal::FetchTimeMs => Some(FastField::FetchTimeMs),
            Signal::UpdateTimestamp => Some(FastField::LastUpdated),
            Signal::TrackerScore => Some(FastField::TrackerScore),
            Signal::Region => Some(FastField::Region),
            Signal::CrawlStability => Some(FastField::CrawlStability),
            Signal::PersonalCentrality => None,
            Signal::TopicCentrality => None,
            Signal::QueryCentrality => None,
            Signal::InboundSimilarity => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FieldBoost(Vec<Option<f64>>);

#[derive(Debug, Clone)]
pub struct SignalCoefficient(Vec<Option<f64>>);

impl SignalCoefficient {
    pub fn get(&self, signal: &Signal) -> f64 {
        self.0
            .get((*signal) as usize)
            .copied()
            .flatten()
            .unwrap_or_else(|| signal.default_coefficient())
    }

    pub fn new(coefficients: impl Iterator<Item = (Signal, f64)>) -> Self {
        let mut fast_coefficients = Vec::new();

        for (signal, coefficient) in coefficients {
            let idx = signal as usize;

            while idx >= fast_coefficients.len() {
                fast_coefficients.push(None);
            }

            fast_coefficients[idx] = Some(coefficient);
        }

        Self(fast_coefficients)
    }
}

impl FieldBoost {
    pub fn get(&self, field: &TextField) -> f64 {
        self.0
            .get((*field) as usize)
            .copied()
            .flatten()
            .or_else(|| Field::Text(*field).boost().map(|s| s as f64))
            .unwrap_or(1.0)
    }

    pub fn new(scores: impl Iterator<Item = (TextField, f64)>) -> Self {
        let mut fast_scores = Vec::new();

        for (field, score) in scores {
            let idx = field as usize;

            while idx >= fast_scores.len() {
                fast_scores.push(None);
            }

            fast_scores[idx] = Some(score);
        }

        Self(fast_scores)
    }
}

#[derive(Clone)]
pub struct SignalAggregator {
    fastfield_reader: Option<Arc<fastfield_reader::SegmentReader>>,
    signal_coefficients: SignalCoefficient,
    personal_centrality: Option<Arc<online_harmonic::Scorer>>,
    inbound_similariy: Option<Arc<inbound_similarity::Scorer>>,
    field_boost: FieldBoost,
    fetch_time_ms_cache: Vec<f64>,
    update_time_cache: Vec<f64>,
    topic_scorer: Option<topic::Scorer>,
    query_centrality: Option<Arc<online_harmonic::Scorer>>,
}

impl std::fmt::Debug for SignalAggregator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignalAggregator")
            .field("signal_coefficients", &self.signal_coefficients)
            .field("field_boost", &self.field_boost)
            .finish()
    }
}

impl Default for SignalAggregator {
    fn default() -> Self {
        Self::new(Vec::new().into_iter(), Vec::new().into_iter())
    }
}

impl SignalAggregator {
    pub fn new(
        coefficients: impl Iterator<Item = (Signal, f64)>,
        boosts: impl Iterator<Item = (TextField, f64)>,
    ) -> Self {
        let signal_coefficients = SignalCoefficient::new(coefficients);
        let field_boost = FieldBoost::new(boosts);

        let fetch_time_ms_cache: Vec<_> = (0..1000)
            .map(|fetch_time| 1.0 / (fetch_time as f64 + 1.0))
            .collect();

        let update_time_cache = (0..(3 * 365 * 24))
            .map(|hours_since_update| 1.0 / ((hours_since_update as f64 + 1.0).log2()))
            .collect();

        Self {
            fastfield_reader: None,
            personal_centrality: None,
            inbound_similariy: None,
            signal_coefficients,
            field_boost,
            fetch_time_ms_cache,
            update_time_cache,
            topic_scorer: None,
            query_centrality: None,
        }
    }

    pub fn register_segment(&mut self, reader: Arc<fastfield_reader::SegmentReader>) {
        self.fastfield_reader = Some(reader);
    }

    pub fn set_topic_scorer(&mut self, topic_scorer: topic::Scorer) {
        self.topic_scorer = Some(topic_scorer);
    }

    pub fn set_query_centrality(&mut self, query_centrality: online_harmonic::Scorer) {
        self.query_centrality = Some(Arc::new(query_centrality));
    }

    pub fn set_personal_harmonic(&mut self, personal_centrality: online_harmonic::Scorer) {
        self.personal_centrality = Some(Arc::new(personal_centrality));
    }

    pub fn set_inbound_similarity(&mut self, scorer: inbound_similarity::Scorer) {
        self.inbound_similariy = Some(Arc::new(scorer));
    }

    pub fn topic_centrality(&self, host_id: NodeID) -> Option<f64> {
        self.topic_scorer
            .as_ref()
            .map(|scorer| scorer.score(host_id))
    }

    pub fn query_centrality(&self, host_id: NodeID) -> Option<f64> {
        self.query_centrality
            .as_ref()
            .map(|scorer| scorer.score(host_id))
    }

    pub fn personal_centrality(&self, host_id: NodeID) -> f64 {
        self.personal_centrality
            .as_ref()
            .map(|scorer| scorer.score(host_id))
            .unwrap_or_default()
    }

    pub fn inbound_similarity(&self, host_id: NodeID) -> f64 {
        self.inbound_similariy
            .as_ref()
            .map(|scorer| scorer.score(&host_id))
            .unwrap_or_default()
    }

    pub fn score(
        &self,
        doc: DocId,
        bm25: tantivy::Score,
        region_count: &Arc<RegionCount>,
        current_timestamp: usize,
        selected_region: Option<Region>,
    ) -> Score {
        let host_id = self.fastfield_reader.as_ref().and_then(|reader| {
            let node_id: Option<u64> = reader
                .get_field_reader(&FastField::HostNodeID)
                .get(&doc)
                .into();
            let node_id = node_id.unwrap();

            if node_id == u64::MAX {
                None
            } else {
                Some(NodeID::from(node_id))
            }
        });

        let topic_score = host_id.and_then(|host_id| self.topic_centrality(host_id));

        let query_centrality = host_id.and_then(|host_id| self.query_centrality(host_id));

        let personal_centrality = host_id.map(|host_id| self.personal_centrality(host_id));
        let inbound_similarity = host_id.map(|host_id| self.inbound_similarity(host_id));

        let score = ALL_SIGNALS
            .into_iter()
            .map(|signal| {
                let fastfield_value = signal.as_fastfield().and_then(|field| {
                    self.fastfield_reader
                        .as_ref()
                        .and_then(|reader| reader.get_field_reader(&field).get(&doc).into())
                });

                self.coefficients().get(&signal)
                    * signal.value(
                        region_count,
                        self,
                        SignalValue {
                            bm25,
                            fastfield_value,
                            current_timestamp,
                            selected_region,
                            personal_centrality,
                            inbound_similarity,
                            topic_score,
                            query_centrality,
                        },
                    )
            })
            .sum();

        Score { bm25, total: score }
    }

    pub fn precompute_score(&self, webpage: &Webpage, region_count: &RegionCount) -> f64 {
        ALL_SIGNALS
            .into_iter()
            .filter(|signal| signal.is_computable_before_search())
            .map(|signal| {
                let fastfield_value = match &signal {
                    Signal::HostCentrality => {
                        (webpage.host_centrality * (FLOAT_SCALING as f64)) as u64
                    }
                    Signal::PageCentrality => {
                        (webpage.page_centrality * (FLOAT_SCALING as f64)) as u64
                    }
                    Signal::IsHomepage => webpage.html.url().is_homepage().into(),
                    Signal::FetchTimeMs => webpage.fetch_time_ms,
                    Signal::UpdateTimestamp => webpage
                        .html
                        .updated_time()
                        .map(|date| date.timestamp().max(0) as u64)
                        .unwrap_or(0),
                    Signal::TrackerScore => webpage.html.trackers().len() as u64,
                    Signal::Region => Region::guess_from(webpage).unwrap_or(Region::All).id(),
                    Signal::CrawlStability => {
                        (webpage.crawl_stability * (FLOAT_SCALING as f64)) as u64
                    }
                    Signal::Bm25
                    | Signal::PersonalCentrality
                    | Signal::TopicCentrality
                    | Signal::InboundSimilarity
                    | Signal::QueryCentrality => {
                        panic!("signal cannot be determined from webpage")
                    }
                };

                let current_timestamp = Utc::now().timestamp() as usize;

                self.coefficients().get(&signal)
                    * signal.value(
                        &region_count,
                        self,
                        SignalValue {
                            bm25: 0.0,
                            fastfield_value: Some(fastfield_value),
                            current_timestamp,
                            selected_region: None,
                            personal_centrality: None,
                            topic_score: None,
                            query_centrality: None,
                            inbound_similarity: None,
                        },
                    )
            })
            .sum()
    }

    pub fn coefficients(&self) -> &SignalCoefficient {
        &self.signal_coefficients
    }

    pub fn field_boosts(&self) -> &FieldBoost {
        &self.field_boost
    }
}
