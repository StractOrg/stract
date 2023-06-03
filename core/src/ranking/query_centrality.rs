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

use crate::webgraph::{centrality::online_harmonic, NodeID};

use super::inbound_similarity;

pub struct Scorer {
    online_harmonic: online_harmonic::Scorer,
    inbound_centrality: inbound_similarity::Scorer,
}

impl Scorer {
    pub fn new(
        online_harmonic: online_harmonic::Scorer,
        inbound_centrality: inbound_similarity::Scorer,
    ) -> Self {
        Self {
            online_harmonic,
            inbound_centrality,
        }
    }

    pub fn score(&self, node: NodeID) -> f64 {
        self.online_harmonic.score(node) + self.inbound_centrality.score(&node)
    }
}
