// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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
    fastfield_cache,
    schema::{FastField, TextField},
    webgraph::centrality::approximate_harmonic,
    webpage::Webpage,
    Result,
};
use std::{array, convert::TryFrom, ops::Deref, sync::Arc};

use chrono::Utc;
use tantivy::{DocId, Score};

use crate::{
    schema::{Field, CENTRALITY_SCALING},
    webpage::region::{Region, RegionCount},
};

use crate::ranking::goggles::ast::{RawAlteration, Target};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Signal {
    Bm25,
    HostCentrality,
    PageCentrality,
    IsHomepage,
    FetchTimeMs,
    UpdateTimestamp,
    NumTrackers,
    Region,
    PersonalCentrality,
}

pub const ALL_SIGNALS: [Signal; 9] = [
    Signal::Bm25,
    Signal::HostCentrality,
    Signal::PageCentrality,
    Signal::IsHomepage,
    Signal::FetchTimeMs,
    Signal::UpdateTimestamp,
    Signal::NumTrackers,
    Signal::Region,
    Signal::PersonalCentrality,
];

struct SignalValue {
    bm25: Score,
    fastfield_value: Option<u64>,
    current_timestamp: usize,
    selected_region: Option<Region>,
    personal_centrality: f64,
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
                value.fastfield_value.unwrap() as f64 / CENTRALITY_SCALING as f64
            }
            Signal::PersonalCentrality => value.personal_centrality,
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
            Signal::NumTrackers => {
                let num_trackers = value.fastfield_value.unwrap() as f64;
                1.0 / (num_trackers + 1.0)
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
        }
    }

    fn default_coefficient(&self) -> f64 {
        match self {
            Signal::Bm25 => 1.0,
            Signal::HostCentrality => 2048.0,
            Signal::PageCentrality => 4096.0,
            Signal::IsHomepage => 0.1,
            Signal::FetchTimeMs => 0.1,
            Signal::UpdateTimestamp => 80.0,
            Signal::NumTrackers => 20.0,
            Signal::Region => 60.0,
            Signal::PersonalCentrality => 2048.0,
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
            "num_trackers" => Some(Signal::NumTrackers),
            "region" => Some(Signal::Region),
            "personal_centrality" => Some(Signal::PersonalCentrality),
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
            Signal::NumTrackers => Some(FastField::NumTrackers),
            Signal::Region => Some(FastField::Region),
            Signal::PersonalCentrality => None,
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
    fastfield_cache: Option<Arc<fastfield_cache::SegmentCache>>,
    signal_coefficients: SignalCoefficient,
    pub personal_harmonic: Vec<Arc<approximate_harmonic::Scorer>>,
    field_boost: FieldBoost,
    fetch_time_ms_cache: [f64; 1000],
    update_time_cache: Vec<f64>,
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

        let fetch_time_ms_cache = array::from_fn(|fetch_time| 1.0 / (fetch_time as f64 + 1.0));

        let update_time_cache = (0..(3 * 365 * 24))
            .map(|hours_since_update| 1.0 / ((hours_since_update as f64 + 1.0).log2()))
            .collect();

        Self {
            fastfield_cache: None,
            personal_harmonic: Vec::new(),
            signal_coefficients,
            field_boost,
            fetch_time_ms_cache,
            update_time_cache,
        }
    }

    pub fn register_segment(&mut self, cache: Arc<fastfield_cache::SegmentCache>) {
        self.fastfield_cache = Some(cache);
    }

    pub fn score(
        &self,
        doc: DocId,
        bm25: Score,
        region_count: &Arc<RegionCount>,
        current_timestamp: usize,
        selected_region: Option<Region>,
    ) -> f64 {
        ALL_SIGNALS
            .into_iter()
            .map(|signal| {
                let fastfield_value = signal.as_fastfield().and_then(|field| {
                    self.fastfield_cache
                        .as_ref()
                        .and_then(|cache| cache.get_doc_cache(&field).get_u64(&doc))
                });

                let personal_centrality = self
                    .personal_harmonic
                    .iter()
                    .map(|scorer| scorer.score(doc as u64))
                    .sum();

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
                        },
                    )
            })
            .sum()
    }

    pub fn precompute_score(&self, webpage: &Webpage, region_count: &RegionCount) -> f64 {
        ALL_SIGNALS
            .into_iter()
            .filter(|signal| signal.is_computable_before_search())
            .map(|signal| {
                let fastfield_value = match &signal {
                    Signal::HostCentrality => {
                        (webpage.host_centrality * (CENTRALITY_SCALING as f64)) as u64
                    }
                    Signal::PageCentrality => {
                        (webpage.page_centrality * (CENTRALITY_SCALING as f64)) as u64
                    }
                    Signal::IsHomepage => webpage.html.url().is_homepage().into(),
                    Signal::FetchTimeMs => webpage.fetch_time_ms,
                    Signal::UpdateTimestamp => webpage
                        .html
                        .updated_time()
                        .map(|date| date.timestamp().max(0) as u64)
                        .unwrap_or(0),
                    Signal::NumTrackers => webpage.html.trackers().len() as u64,
                    Signal::Region => Region::guess_from(webpage).unwrap_or(Region::All).id(),
                    _ => panic!("signal cannot be determined from webpage"),
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
                            personal_centrality: 0.0,
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
#[derive(Debug, PartialEq)]
pub struct Alteration {
    pub target: Target,
    pub score: f64,
}

impl TryFrom<RawAlteration> for Alteration {
    type Error = crate::Error;

    fn try_from(raw: RawAlteration) -> Result<Self> {
        Ok(Alteration {
            target: raw.target,
            score: raw.score.parse()?,
        })
    }
}
