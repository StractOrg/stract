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
// along with this program.  If not, see <https://www.gnu.org/licenses/

use utoipa::ToSchema;

use super::{merge::NodeDatum, FullNodeID, Node, NodeID};

pub const MAX_LABEL_LENGTH: usize = 1024;

pub trait EdgeLabel
where
    Self: Send + Sync + Sized,
{
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>>;
    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self>;
}

impl EdgeLabel for String {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(self.as_bytes().to_vec())
    }

    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(String::from_utf8(bytes.to_vec())?)
    }
}

impl EdgeLabel for () {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn from_bytes(_bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, bincode::Encode, bincode::Decode)]
pub struct Edge<L>
where
    L: EdgeLabel,
{
    pub from: NodeID,
    pub to: NodeID,
    pub label: L,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, bincode::Encode, bincode::Decode)]
pub struct InsertableEdge<L>
where
    L: EdgeLabel,
{
    pub from: FullNodeID,
    pub to: FullNodeID,
    pub label: L,
}

#[cfg(test)]
impl<L> From<InsertableEdge<L>> for Edge<L>
where
    L: EdgeLabel,
{
    fn from(edge: InsertableEdge<L>) -> Self {
        Edge {
            from: edge.from.id,
            to: edge.to.id,
            label: edge.label,
        }
    }
}

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    PartialEq,
    Eq,
    Hash,
    ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct FullEdge {
    pub from: Node,
    pub to: Node,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentEdge<L>
where
    L: EdgeLabel,
{
    pub from: NodeDatum<()>,
    pub to: NodeDatum<()>,
    pub label: L,
}

impl<L> From<SegmentEdge<L>> for Edge<L>
where
    L: EdgeLabel,
{
    fn from(edge: SegmentEdge<L>) -> Self {
        Edge {
            from: edge.from.node(),
            to: edge.to.node(),
            label: edge.label,
        }
    }
}

#[cfg(test)]
impl<L> From<Edge<L>> for SegmentEdge<L>
where
    L: EdgeLabel,
{
    fn from(edge: Edge<L>) -> Self {
        SegmentEdge {
            from: NodeDatum::new(edge.from, 0),
            to: NodeDatum::new(edge.to, 0),
            label: edge.label,
        }
    }
}

#[cfg(test)]
impl<L> From<InsertableEdge<L>> for SegmentEdge<L>
where
    L: EdgeLabel,
{
    fn from(edge: InsertableEdge<L>) -> Self {
        SegmentEdge {
            from: NodeDatum::new(edge.from.id, 0),
            to: NodeDatum::new(edge.to.id, 0),
            label: edge.label,
        }
    }
}
