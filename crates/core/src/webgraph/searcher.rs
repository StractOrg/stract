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

use crate::ampc::dht::ShardId;

use super::warmed_column_fields::WarmedColumnFields;

pub struct Searcher {
    tantivy_searcher: tantivy::Searcher,
    shard: ShardId,
    warmed_column_fields: WarmedColumnFields,
}

impl Searcher {
    pub fn new(
        tantivy_searcher: tantivy::Searcher,
        warmed_column_fields: WarmedColumnFields,
        shard: ShardId,
    ) -> Self {
        Self {
            tantivy_searcher,
            warmed_column_fields,
            shard,
        }
    }

    pub fn shard(&self) -> ShardId {
        self.shard
    }

    pub fn tantivy_searcher(&self) -> &tantivy::Searcher {
        &self.tantivy_searcher
    }

    pub fn warmed_column_fields(&self) -> &WarmedColumnFields {
        &self.warmed_column_fields
    }
}
