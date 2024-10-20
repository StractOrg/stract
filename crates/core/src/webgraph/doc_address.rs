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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use tantivy::{DocId, SegmentOrdinal};

use crate::ampc::dht::ShardId;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    bincode::Encode,
    bincode::Decode,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct DocAddress {
    pub shard_id: ShardId,
    pub segment_ord: SegmentOrdinal,
    pub doc_id: DocId,
}

impl DocAddress {
    pub fn new(shard_id: ShardId, segment_ord: SegmentOrdinal, doc_id: DocId) -> Self {
        Self {
            shard_id,
            segment_ord,
            doc_id,
        }
    }
}

impl From<DocAddress> for tantivy::DocAddress {
    fn from(doc_address: DocAddress) -> Self {
        Self::new(doc_address.segment_ord, doc_address.doc_id)
    }
}
