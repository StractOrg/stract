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

use bloom::U64BloomFilter;
use rustc_hash::FxHashSet;

use crate::webgraph;

const SKETCH_THRESHOLD: usize = 16_384;

#[derive(
    Debug, Clone, bincode::Encode, bincode::Decode, serde::Serialize, serde::Deserialize, PartialEq,
)]
pub enum InnerUpdatedNodes {
    Exact(FxHashSet<webgraph::NodeID>),
    Sketch(U64BloomFilter),
}

impl Default for InnerUpdatedNodes {
    fn default() -> Self {
        InnerUpdatedNodes::Exact(FxHashSet::default())
    }
}

impl InnerUpdatedNodes {
    fn contains(&self, node: webgraph::NodeID) -> bool {
        match self {
            InnerUpdatedNodes::Exact(nodes) => nodes.contains(&node),
            InnerUpdatedNodes::Sketch(sketch) => sketch.contains_u128(node.as_u128()),
        }
    }

    fn union(&self, other: &Self, total_nodes: u64) -> Self {
        match (self, other) {
            (InnerUpdatedNodes::Exact(nodes), InnerUpdatedNodes::Exact(other_nodes)) => {
                let mut new_nodes = nodes.clone();
                new_nodes.extend(other_nodes);
                if new_nodes.len() > SKETCH_THRESHOLD {
                    let mut bloom = U64BloomFilter::new(total_nodes, 0.01);

                    for node in nodes {
                        bloom.insert_u128(node.as_u128());
                    }

                    InnerUpdatedNodes::Sketch(bloom)
                } else {
                    InnerUpdatedNodes::Exact(new_nodes)
                }
            }
            (InnerUpdatedNodes::Sketch(sketch), InnerUpdatedNodes::Sketch(other_sketch)) => {
                let mut new_sketch = sketch.clone();
                new_sketch.union(other_sketch.clone());
                InnerUpdatedNodes::Sketch(new_sketch)
            }
            (InnerUpdatedNodes::Exact(nodes), InnerUpdatedNodes::Sketch(sketch)) => {
                let mut new_sketch = sketch.clone();

                for node in nodes {
                    new_sketch.insert_u128(node.as_u128());
                }

                InnerUpdatedNodes::Sketch(new_sketch)
            }
            (InnerUpdatedNodes::Sketch(sketch), InnerUpdatedNodes::Exact(nodes)) => {
                let mut new_sketch = sketch.clone();

                for node in nodes {
                    new_sketch.insert_u128(node.as_u128());
                }

                InnerUpdatedNodes::Sketch(new_sketch)
            }
        }
    }

    fn add(&mut self, node: webgraph::NodeID, total_nodes: u64) {
        match self {
            InnerUpdatedNodes::Exact(nodes) => {
                nodes.insert(node);

                if nodes.len() > SKETCH_THRESHOLD {
                    let mut bloom = U64BloomFilter::new(total_nodes, 0.01);

                    for node in nodes.iter() {
                        bloom.insert_u128(node.as_u128());
                    }

                    *self = InnerUpdatedNodes::Sketch(bloom);
                }
            }
            InnerUpdatedNodes::Sketch(sketch) => sketch.insert_u128(node.as_u128()),
        }
    }
}

pub enum UpdatedNodesKind {
    Exact,
    Sketch,
}

#[derive(
    Debug, Clone, bincode::Encode, bincode::Decode, serde::Serialize, serde::Deserialize, PartialEq,
)]
pub struct UpdatedNodes {
    inner: InnerUpdatedNodes,
    total_nodes: u64,
}

impl UpdatedNodes {
    pub fn new(total_nodes: u64) -> Self {
        Self {
            inner: InnerUpdatedNodes::default(),
            total_nodes,
        }
    }

    pub fn kind(&self) -> UpdatedNodesKind {
        match self.inner {
            InnerUpdatedNodes::Exact(_) => UpdatedNodesKind::Exact,
            InnerUpdatedNodes::Sketch(_) => UpdatedNodesKind::Sketch,
        }
    }

    pub fn add(&mut self, node: webgraph::NodeID) {
        self.inner.add(node, self.total_nodes);
    }

    pub fn union(&self, other: &Self) -> Self {
        Self {
            inner: self.inner.union(&other.inner, self.total_nodes),
            total_nodes: self.total_nodes,
        }
    }

    pub fn contains(&self, node: webgraph::NodeID) -> bool {
        self.inner.contains(node)
    }

    pub fn empty_from(other: &Self) -> Self {
        Self {
            inner: InnerUpdatedNodes::default(),
            total_nodes: other.total_nodes,
        }
    }

    pub fn as_exact(&self) -> Option<&FxHashSet<webgraph::NodeID>> {
        match &self.inner {
            InnerUpdatedNodes::Exact(nodes) => Some(nodes),
            InnerUpdatedNodes::Sketch(_) => None,
        }
    }
}
