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

use std::{collections::HashMap, sync::Arc};

use strum::{EnumIter, IntoEnumIterator};
use tantivy::{fastfield::DynamicFastFieldReader, DocId, Score, SegmentReader};

use crate::{
    schema::{Field, ALL_FIELDS},
    webpage::region::{Region, RegionCount},
};

#[derive(Debug, PartialEq, Eq, Hash, EnumIter)]
enum Signal {
    Bm25,
}

impl Signal {
    fn from_field(field: &Field) -> Option<Self> {
        todo!();
    }

    fn has_fast_reader(&self) -> bool {
        todo!()
    }

    fn value(
        &self,
        doc: DocId,
        bm25: Score,
        fastfield: Option<&DynamicFastFieldReader<u64>>,
        region_count: &Arc<RegionCount>,
        current_timestamp: f64,
        selected_region: Option<Region>,
    ) -> f64 {
        todo!();
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

trait SignalAggregator {
    fn coefficients(&self) -> SignalCoefficient;
    fn field_boosts(&self) -> FieldBoost;

    fn mut_readers(&mut self) -> &mut HashMap<Signal, DynamicFastFieldReader<u64>>;
    fn readers(&self) -> &HashMap<Signal, DynamicFastFieldReader<u64>>;

    fn register_readers(&mut self, segment_reader: &SegmentReader) {
        for field in &ALL_FIELDS {
            if let Some(signal) = Signal::from_field(field) {
                if signal.has_fast_reader() {
                    self.mut_readers()
                        .insert(signal, fastfield_reader(segment_reader, field));
                }
            }
        }
    }

    fn score(
        &self,
        doc: DocId,
        bm25: Score,
        region_count: Arc<RegionCount>,
        current_timestamp: f64,
        selected_region: Option<Region>,
    ) -> f64 {
        Signal::iter()
            .map(|signal| {
                self.coefficients().get(&signal)
                    * signal.value(
                        doc,
                        bm25,
                        self.readers().get(&signal),
                        &region_count,
                        current_timestamp,
                        selected_region,
                    )
            })
            .sum()
    }
}

struct FieldBoost(HashMap<Field, f64>);

struct SignalCoefficient(HashMap<Field, f64>);

struct DefaultSignalAggregator {
    readers: HashMap<Signal, DynamicFastFieldReader<u64>>,
}

impl SignalCoefficient {
    fn get(&self, signal: &Signal) -> f64 {
        todo!()
    }
}

impl FieldBoost {
    fn get(&self, field: &Field) -> f64 {
        todo!()
    }
}
