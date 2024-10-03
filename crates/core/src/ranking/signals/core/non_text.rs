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

use tantivy::DocId;

use crate::ranking::{CoreSignal, SignalCalculation, SignalComputer};
use crate::{
    schema::{self, Field},
    webpage::Webpage,
};

fn score_timestamp(page_timestamp: usize, signal_computer: &SignalComputer) -> f64 {
    if page_timestamp >= signal_computer.current_timestamp().unwrap_or(0) {
        return 0.0;
    }

    let hours_since_update = signal_computer
        .current_timestamp()
        .unwrap()
        .saturating_sub(page_timestamp)
        .max(1)
        / 3600;

    signal_computer
        .update_time_cache()
        .get(hours_since_update)
        .copied()
        .unwrap_or(0.0)
}

pub fn time_cache_calculation(hours_since_update: f64) -> f64 {
    const SMOOTHING_FACTOR: f64 = 24.0 * 3.0; // half life of 3 days
    SMOOTHING_FACTOR / (hours_since_update + SMOOTHING_FACTOR)
}

#[inline]
fn score_rank(rank: f64) -> f64 {
    // 10 groups with log base 8 gives us
    // 1.1 billion ranks we can score without
    // exceeding the groups.

    const NUM_GROUPS: f64 = 10.0;
    const BASE: f64 = 8.0;

    (NUM_GROUPS - (1.0 + rank).log(BASE)).max(0.0)
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

fn score_region(webpage_region: crate::webpage::Region, computer: &SignalComputer) -> f64 {
    match computer.region_count() {
        Some(region_count) => {
            let boost = computer
                .query_data()
                .and_then(|q| q.selected_region())
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

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct HostCentrality;
impl CoreSignal for HostCentrality {
    fn default_coefficient(&self) -> f64 {
        2.0
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Numerical(
            schema::numerical_field::HostCentrality.into(),
        ))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalComputer) -> Option<SignalCalculation> {
        Some(SignalCalculation::new_symmetrical(webpage.host_centrality))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();
        let numericalfield_reader = seg_reader.numericalfield_reader().get_field_reader(doc);

        let val = numericalfield_reader
            .get(self.as_numericalfield().unwrap())
            .and_then(|v| v.as_f64())
            .unwrap();
        SignalCalculation::new_symmetrical(val)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct HostCentralityRank;
impl CoreSignal for HostCentralityRank {
    fn default_coefficient(&self) -> f64 {
        0.02
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Numerical(
            schema::numerical_field::HostCentralityRank.into(),
        ))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalComputer) -> Option<SignalCalculation> {
        Some(SignalCalculation {
            value: webpage.host_centrality_rank as f64,
            score: score_rank(webpage.host_centrality_rank as f64),
        })
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();
        let numericalfield_reader = seg_reader.numericalfield_reader().get_field_reader(doc);

        let val = numericalfield_reader
            .get(self.as_numericalfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        SignalCalculation {
            value: val as f64,
            score: score_rank(val as f64),
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct PageCentrality;
impl CoreSignal for PageCentrality {
    fn default_coefficient(&self) -> f64 {
        2.0
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Numerical(
            schema::numerical_field::PageCentrality.into(),
        ))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalComputer) -> Option<SignalCalculation> {
        Some(SignalCalculation::new_symmetrical(webpage.page_centrality))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();
        let numericalfield_reader = seg_reader.numericalfield_reader().get_field_reader(doc);

        let val = numericalfield_reader
            .get(self.as_numericalfield().unwrap())
            .and_then(|v| v.as_f64())
            .unwrap();
        SignalCalculation::new_symmetrical(val)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct PageCentralityRank;
impl CoreSignal for PageCentralityRank {
    fn default_coefficient(&self) -> f64 {
        0.02
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Numerical(
            schema::numerical_field::PageCentralityRank.into(),
        ))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalComputer) -> Option<SignalCalculation> {
        Some(SignalCalculation {
            value: webpage.page_centrality_rank as f64,
            score: score_rank(webpage.page_centrality_rank as f64),
        })
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();
        let numericalfield_reader = seg_reader.numericalfield_reader().get_field_reader(doc);

        let val = numericalfield_reader
            .get(self.as_numericalfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        SignalCalculation {
            value: val as f64,
            score: score_rank(val as f64),
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct IsHomepage;
impl CoreSignal for IsHomepage {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Numerical(schema::numerical_field::IsHomepage.into()))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalComputer) -> Option<SignalCalculation> {
        Some(SignalCalculation::new_symmetrical(
            webpage.html.is_homepage().into(),
        ))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();
        let numericalfield_reader = seg_reader.numericalfield_reader().get_field_reader(doc);

        let val = numericalfield_reader
            .get(self.as_numericalfield().unwrap())
            .and_then(|v| v.as_bool())
            .unwrap();

        if val {
            SignalCalculation::new_symmetrical(1.0)
        } else {
            SignalCalculation::new_symmetrical(0.0)
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct FetchTimeMs;
impl CoreSignal for FetchTimeMs {
    fn default_coefficient(&self) -> f64 {
        0.001
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Numerical(
            schema::numerical_field::FetchTimeMs.into(),
        ))
    }

    fn precompute(
        self,
        webpage: &Webpage,
        signal_computer: &SignalComputer,
    ) -> Option<SignalCalculation> {
        let fetch_time_ms = webpage.fetch_time_ms as usize;
        let score = if fetch_time_ms >= signal_computer.fetch_time_ms_cache().len() {
            0.0
        } else {
            signal_computer.fetch_time_ms_cache()[fetch_time_ms]
        };

        Some(SignalCalculation {
            value: fetch_time_ms as f64,
            score,
        })
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();
        let numericalfield_reader = seg_reader.numericalfield_reader().get_field_reader(doc);

        let fetch_time_ms = numericalfield_reader
            .get(self.as_numericalfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap() as usize;

        let score = if fetch_time_ms >= signal_computer.fetch_time_ms_cache().len() {
            0.0
        } else {
            signal_computer.fetch_time_ms_cache()[fetch_time_ms]
        };

        SignalCalculation {
            value: fetch_time_ms as f64,
            score,
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct UpdateTimestamp;
impl CoreSignal for UpdateTimestamp {
    fn default_coefficient(&self) -> f64 {
        0.001
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Numerical(
            schema::numerical_field::LastUpdated.into(),
        ))
    }

    fn precompute(
        self,
        webpage: &Webpage,
        signal_computer: &SignalComputer,
    ) -> Option<SignalCalculation> {
        let update_timestamp = webpage
            .html
            .updated_time()
            .map(|date| date.timestamp().max(0))
            .unwrap_or(0) as usize;

        let score = score_timestamp(update_timestamp, signal_computer);

        Some(SignalCalculation {
            value: update_timestamp as f64,
            score,
        })
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();
        let numericalfield_reader = seg_reader.numericalfield_reader().get_field_reader(doc);

        let val = numericalfield_reader
            .get(self.as_numericalfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap() as usize;

        let score = score_timestamp(val, signal_computer);

        SignalCalculation {
            value: val as f64,
            score,
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct TrackerScore;
impl CoreSignal for TrackerScore {
    fn default_coefficient(&self) -> f64 {
        0.1
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Numerical(
            schema::numerical_field::TrackerScore.into(),
        ))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalComputer) -> Option<SignalCalculation> {
        let num_trackers = webpage.html.trackers().len() as f64;
        let score = score_trackers(num_trackers);

        Some(SignalCalculation {
            value: num_trackers,
            score,
        })
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();
        let numericalfield_reader = seg_reader.numericalfield_reader().get_field_reader(doc);

        let val = numericalfield_reader
            .get(self.as_numericalfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        let score = score_trackers(val as f64);

        SignalCalculation {
            value: val as f64,
            score,
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Region;
impl CoreSignal for Region {
    fn default_coefficient(&self) -> f64 {
        0.15
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Numerical(schema::numerical_field::Region.into()))
    }

    fn precompute(
        self,
        webpage: &Webpage,
        signal_computer: &SignalComputer,
    ) -> Option<SignalCalculation> {
        let region =
            crate::webpage::Region::guess_from(webpage).unwrap_or(crate::webpage::Region::All);
        let score = score_region(region, signal_computer);

        Some(SignalCalculation {
            value: region.id() as f64,
            score,
        })
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();
        let numericalfield_reader = seg_reader.numericalfield_reader().get_field_reader(doc);

        let val = numericalfield_reader
            .get(self.as_numericalfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap();
        let region = crate::webpage::Region::from_id(val);
        let score = score_region(region, signal_computer);

        SignalCalculation {
            value: val as f64,
            score,
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct UrlDigits;
impl CoreSignal for UrlDigits {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Numerical(
            schema::numerical_field::NumPathAndQueryDigits.into(),
        ))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalComputer) -> Option<SignalCalculation> {
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

        let score = score_digits(num_digits);

        Some(SignalCalculation {
            value: num_digits,
            score,
        })
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();
        let numericalfield_reader = seg_reader.numericalfield_reader().get_field_reader(doc);

        let val = numericalfield_reader
            .get(self.as_numericalfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap() as f64;
        let score = score_digits(val);

        SignalCalculation { value: val, score }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct UrlSlashes;
impl CoreSignal for UrlSlashes {
    fn default_coefficient(&self) -> f64 {
        0.1
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Numerical(
            schema::numerical_field::NumPathAndQuerySlashes.into(),
        ))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalComputer) -> Option<SignalCalculation> {
        let num_slashes = webpage
            .html
            .url()
            .path()
            .chars()
            .filter(|c| c == &'/')
            .count() as f64;
        let score = score_slashes(num_slashes);

        Some(SignalCalculation {
            value: num_slashes,
            score,
        })
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();
        let numericalfield_reader = seg_reader.numericalfield_reader().get_field_reader(doc);

        let val = numericalfield_reader
            .get(self.as_numericalfield().unwrap())
            .and_then(|v| v.as_u64())
            .unwrap() as f64;
        let score = score_slashes(val);

        SignalCalculation { value: val, score }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct LinkDensity;
impl CoreSignal for LinkDensity {
    fn default_coefficient(&self) -> f64 {
        0.0
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Numerical(
            schema::numerical_field::LinkDensity.into(),
        ))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalComputer) -> Option<SignalCalculation> {
        let link_density = webpage.html.link_density();
        let score = score_link_density(link_density);

        Some(SignalCalculation {
            value: link_density,
            score,
        })
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();
        let numericalfield_reader = seg_reader.numericalfield_reader().get_field_reader(doc);

        let val = numericalfield_reader
            .get(self.as_numericalfield().unwrap())
            .and_then(|v| v.as_f64())
            .unwrap();
        let score = score_link_density(val);

        SignalCalculation { value: val, score }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct HasAds;
impl CoreSignal for HasAds {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Numerical(
            schema::numerical_field::LikelyHasAds.into(),
        ))
    }

    fn precompute(self, webpage: &Webpage, _: &SignalComputer) -> Option<SignalCalculation> {
        let has_ads = webpage.html.likely_has_ads();
        let value: f64 = has_ads.into();
        let score = if !has_ads { 1.0 } else { 0.0 };

        Some(SignalCalculation { value, score })
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();
        let numericalfield_reader = seg_reader.numericalfield_reader().get_field_reader(doc);

        let has_ads = numericalfield_reader
            .get(self.as_numericalfield().unwrap())
            .and_then(|v| v.as_bool())
            .unwrap();

        let value: f64 = has_ads.into();
        let score = if !has_ads { 1.0 } else { 0.0 };

        SignalCalculation { value, score }
    }
}
