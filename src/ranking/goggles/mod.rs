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

mod ast;

use std::{array, sync::Arc};

use crate::Result;
use tantivy::{
    fastfield::{Column, DynamicFastFieldReader},
    DocId, Score, SegmentReader,
};

use crate::{
    schema::{Field, ALL_FIELDS, CENTRALITY_SCALING},
    webpage::region::{Region, RegionCount},
};

use self::ast::{Alteration, Instruction, RawGoggle, Target};

pub fn parse(goggle: &str) -> Result<Goggle> {
    let raw_goggle = ast::parse(goggle)?;

    Ok(Goggle::from(raw_goggle))
}

impl From<RawGoggle> for Goggle {
    fn from(raw: RawGoggle) -> Self {
        Self {
            aggregator: SignalAggregator::from(raw.alterations),
            instructions: raw.instructions,
        }
    }
}

pub struct Goggle {
    pub aggregator: SignalAggregator,
    instructions: Vec<Instruction>,
}

impl Goggle {
    pub fn as_tantivy(&self) -> Vec<(tantivy::query::Occur, Box<dyn tantivy::query::Query>)> {
        self.instructions
            .iter()
            .map(|instruction| instruction.as_tantivy())
            .collect()
    }
}

impl Instruction {
    pub fn as_tantivy(&self) -> (tantivy::query::Occur, Box<dyn tantivy::query::Query>) {
        todo!();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Signal {
    Bm25,
    HostCentrality,
    IsHomepage,
    FetchTimeMs,
    UpdateTimestamp,
    NumTrackers,
    Region,
}

const ALL_SIGNALS: [Signal; 7] = [
    Signal::Bm25,
    Signal::HostCentrality,
    Signal::IsHomepage,
    Signal::FetchTimeMs,
    Signal::UpdateTimestamp,
    Signal::NumTrackers,
    Signal::Region,
];

impl Signal {
    fn from_field(field: &Field) -> Option<Self> {
        match field {
            Field::IsHomepage => Some(Signal::IsHomepage),
            Field::Centrality => Some(Signal::HostCentrality),
            Field::FetchTimeMs => Some(Signal::FetchTimeMs),
            Field::LastUpdated => Some(Signal::UpdateTimestamp),
            Field::NumTrackers => Some(Signal::NumTrackers),
            Field::Region => Some(Signal::Region),
            _ => None,
        }
    }

    fn has_fast_reader(&self) -> bool {
        !matches!(self, Signal::Bm25)
    }

    fn value(
        &self,
        doc: DocId,
        bm25: Score,
        fastfield: Option<&DynamicFastFieldReader<u64>>,
        region_count: &Arc<RegionCount>,
        current_timestamp: usize,
        selected_region: Option<Region>,
        aggregator: &SignalAggregator,
    ) -> f64 {
        match self {
            Signal::Bm25 => bm25 as f64,
            Signal::HostCentrality => {
                fastfield.unwrap().get_val(doc as u64) as f64 / CENTRALITY_SCALING as f64
            }
            Signal::IsHomepage => fastfield.unwrap().get_val(doc as u64) as f64,
            Signal::FetchTimeMs => {
                let fetch_time_ms = fastfield.unwrap().get_val(doc as u64) as usize;

                if fetch_time_ms >= aggregator.fetch_time_ms_cache.len() {
                    0.0
                } else {
                    aggregator.fetch_time_ms_cache[fetch_time_ms]
                }
            }
            Signal::UpdateTimestamp => {
                let update_timestamp = fastfield.unwrap().get_val(doc as u64) as usize;
                let hours_since_update = (current_timestamp - update_timestamp).max(1) / 3600;

                if hours_since_update < aggregator.update_time_cache.len() {
                    aggregator.update_time_cache[hours_since_update]
                } else {
                    0.0
                }
            }
            Signal::NumTrackers => {
                let num_trackers = fastfield.unwrap().get_val(doc as u64) as f64;
                1.0 / (num_trackers + 1.0)
            }
            Signal::Region => {
                let webpage_region = Region::from_id(fastfield.unwrap().get_val(doc as u64));

                let boost =
                    selected_region.map_or(
                        0.0,
                        |region| if region == webpage_region { 50.0 } else { 0.0 },
                    );

                boost + region_count.score(&webpage_region)
            }
        }
    }

    fn default_coefficient(&self) -> f64 {
        match self {
            Signal::Bm25 => 3.0,
            Signal::HostCentrality => 3200.0,
            Signal::IsHomepage => 1.0,
            Signal::FetchTimeMs => 1.0,
            Signal::UpdateTimestamp => 1500.0,
            Signal::NumTrackers => 200.0,
            Signal::Region => 25.0,
        }
    }

    fn from_string(name: String) -> Option<Signal> {
        match name.as_str() {
            "bm25" => Some(Signal::Bm25),
            "host_centrality" => Some(Signal::HostCentrality),
            _ => None,
        }
    }
}

fn fastfield_reader(segment_reader: &SegmentReader, field: &Field) -> DynamicFastFieldReader<u64> {
    let tv_field = segment_reader
        .schema()
        .get_field(field.as_str())
        .unwrap_or_else(|| panic!("Faild to load {} field", field.as_str()));

    segment_reader
        .fast_fields()
        .u64(tv_field)
        .unwrap_or_else(|_| panic!("Failed to get {} fast-field reader", field.as_str()))
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

    pub fn new(scores: impl Iterator<Item = (Signal, f64)>) -> Self {
        let mut fast_scores = Vec::new();

        for (signal, score) in scores {
            let idx = signal as usize;

            while idx >= fast_scores.len() {
                fast_scores.push(None);
            }

            fast_scores[idx] = Some(score);
        }

        Self(fast_scores)
    }
}

impl FieldBoost {
    pub fn get(&self, field: &Field) -> f64 {
        self.0
            .get((*field) as usize)
            .copied()
            .flatten()
            .or_else(|| field.boost().map(|s| s as f64))
            .unwrap_or(1.0)
    }

    pub fn new(scores: impl Iterator<Item = (Field, f64)>) -> Self {
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

pub struct SignalAggregator {
    readers: Vec<Option<DynamicFastFieldReader<u64>>>,
    signal_coefficients: SignalCoefficient,
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
        boosts: impl Iterator<Item = (Field, f64)>,
    ) -> Self {
        let signal_coefficients = SignalCoefficient::new(coefficients);
        let field_boost = FieldBoost::new(boosts);

        let fetch_time_ms_cache = array::from_fn(|fetch_time| 1.0 / (fetch_time as f64 + 1.0));

        let update_time_cache = (0..(3 * 365 * 24))
            .map(|hours_since_update| 1.0 / ((hours_since_update as f64 + 1.0).log2()))
            .collect();

        Self {
            readers: Vec::new(),
            signal_coefficients,
            field_boost,
            fetch_time_ms_cache,
            update_time_cache,
        }
    }

    pub fn new_like(other: &SignalAggregator) -> Self {
        Self {
            readers: Vec::new(),
            signal_coefficients: other.signal_coefficients.clone(),
            field_boost: other.field_boost.clone(),
            fetch_time_ms_cache: other.fetch_time_ms_cache,
            update_time_cache: other.update_time_cache.clone(),
        }
    }

    pub fn register_readers(&mut self, segment_reader: &SegmentReader) {
        self.readers.clear();

        for field in &ALL_FIELDS {
            if let Some(signal) = Signal::from_field(field) {
                if signal.has_fast_reader() {
                    let idx = signal as usize;

                    while idx >= self.readers.len() {
                        self.readers.push(None);
                    }

                    self.readers[idx] = Some(fastfield_reader(segment_reader, field))
                }
            }
        }
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
                let reader = match self.readers.get(signal as usize) {
                    Some(Some(reader)) => Some(reader),
                    _ => None,
                };

                self.coefficients().get(&signal)
                    * signal.value(
                        doc,
                        bm25,
                        reader,
                        region_count,
                        current_timestamp,
                        selected_region,
                        self,
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

impl From<Vec<Alteration>> for SignalAggregator {
    fn from(alterations: Vec<Alteration>) -> Self {
        let mut coefficients = Vec::new();
        let mut boosts = Vec::new();

        for alteration in alterations {
            match alteration.target {
                Target::Signal(name) => {
                    if let Some(signal) = Signal::from_string(name) {
                        coefficients.push((signal, alteration.score));
                    }
                }
                Target::Field(name) => {
                    if let Some(field) = Field::from_string(name) {
                        boosts.push((field, alteration.score));
                    }
                }
            }
        }

        Self::new(coefficients.into_iter(), boosts.into_iter())
    }
}
