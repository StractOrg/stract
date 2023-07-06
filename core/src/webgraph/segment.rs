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

use rkyv::Archive;
use std::{
    collections::{BTreeMap, HashSet},
    fmt::Debug,
    path::Path,
};

use super::{open_bin, save_bin, store::Store, Edge, Loaded, NodeID, StoredEdge};

#[derive(
    Debug,
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[archive_attr(derive(Eq, Hash, PartialEq, Debug))]
pub struct SegmentNodeID(u64);

impl From<u64> for SegmentNodeID {
    fn from(val: u64) -> Self {
        Self(val)
    }
}

struct MergePacket {
    next_segment_node_id: SegmentNodeID,
    new_id_mapping: Store<NodeID, SegmentNodeID>,
    new_rev_id_mapping: Store<SegmentNodeID, NodeID>,
    new_adjacency: Store<SegmentNodeID, HashSet<StoredEdge>>,
}

pub struct StoredSegment {
    adjacency: Store<SegmentNodeID, HashSet<StoredEdge>>,
    reversed_adjacency: Store<SegmentNodeID, HashSet<StoredEdge>>,
    id_mapping: Store<NodeID, SegmentNodeID>,
    rev_id_mapping: Store<SegmentNodeID, NodeID>,
    meta: Meta,
    id: String,
    folder_path: String,
}

impl StoredSegment {
    pub fn open<P: AsRef<Path>>(folder_path: P, id: String) -> Self {
        StoredSegment {
            adjacency: Store::open(folder_path.as_ref().join("adjacency")),
            reversed_adjacency: Store::open(folder_path.as_ref().join("reversed_adjacency")),
            id_mapping: Store::open(folder_path.as_ref().join("id_mapping")),
            rev_id_mapping: Store::open(folder_path.as_ref().join("rev_id_mapping")),
            meta: open_bin(folder_path.as_ref().join("meta.bin")),
            folder_path: folder_path
                .as_ref()
                .as_os_str()
                .to_str()
                .unwrap()
                .to_string(),
            id,
        }
    }

    pub fn num_nodes(&self) -> usize {
        self.meta.num_nodes as usize
    }

    fn id_mapping(&self, node: &NodeID) -> Option<SegmentNodeID> {
        self.id_mapping.get(node)
    }

    fn rev_id_mapping(&self, node: &SegmentNodeID) -> Option<NodeID> {
        self.rev_id_mapping.get(node)
    }

    pub fn outgoing_edges(&self, node: &NodeID, load_label: bool) -> Vec<Edge> {
        self.id_mapping(node)
            .and_then(|segment_id| {
                self.adjacency.get(&segment_id).map(|edges| {
                    edges
                        .into_iter()
                        .map(move |edge| {
                            if load_label {
                                Edge {
                                    from: *node,
                                    to: self.rev_id_mapping(&SegmentNodeID(edge.other.0)).unwrap(),
                                    label: Loaded::Some(edge.label),
                                }
                            } else {
                                Edge {
                                    from: *node,
                                    to: self.rev_id_mapping(&SegmentNodeID(edge.other.0)).unwrap(),
                                    label: Loaded::NotYet,
                                }
                            }
                        })
                        .collect()
                })
            })
            .unwrap_or_default()
    }

    pub fn ingoing_edges(&self, node: &NodeID, load_label: bool) -> Vec<Edge> {
        self.id_mapping(node)
            .and_then(|segment_id| {
                self.reversed_adjacency.get(&segment_id).map(|edges| {
                    edges
                        .into_iter()
                        .map(move |edge| {
                            if load_label {
                                Edge {
                                    from: self
                                        .rev_id_mapping(&SegmentNodeID(edge.other.0))
                                        .unwrap(),
                                    to: *node,
                                    label: Loaded::Some(edge.label),
                                }
                            } else {
                                Edge {
                                    from: self
                                        .rev_id_mapping(&SegmentNodeID(edge.other.0))
                                        .unwrap(),
                                    to: *node,
                                    label: Loaded::NotYet,
                                }
                            }
                        })
                        .collect()
                })
            })
            .unwrap_or_default()
    }

    fn flush(&mut self) {
        self.adjacency.flush();
        self.reversed_adjacency.flush();
        self.id_mapping.flush();
        self.rev_id_mapping.flush();

        save_bin(&self.meta, self.path().join("meta.bin"));
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn path(&self) -> &Path {
        Path::new(&self.folder_path)
    }

    pub fn edges(&self) -> impl Iterator<Item = Edge> + '_ {
        self.adjacency.iter().flat_map(move |(node_id, edges)| {
            let from = self.rev_id_mapping(&node_id).unwrap();

            edges.into_iter().map(move |stored_edge| Edge {
                from,
                to: self
                    .rev_id_mapping(&SegmentNodeID(stored_edge.other.0))
                    .unwrap(),
                label: Loaded::NotYet,
            })
        })
    }

    fn merge_adjacency<F1>(edges_fn: F1, packet: &mut MergePacket, segments: &[Self])
    where
        F1: Fn(&Self, &NodeID) -> Option<HashSet<StoredEdge>>,
    {
        if segments.is_empty() {
            return;
        }

        let mut i = 0;

        while i < segments.len() {
            let segment = &segments[i];

            for node_id in segment.id_mapping.keys() {
                let mut node_seen_before = false;
                let mut j = 0;
                while j < i {
                    if segments[j].id_mapping.contains_key(&node_id) {
                        node_seen_before = true;
                        break;
                    }
                    j += 1;
                }

                if !node_seen_before {
                    if !packet.new_id_mapping.contains_key(&node_id) {
                        packet
                            .new_id_mapping
                            .put(&node_id, &packet.next_segment_node_id);
                        packet
                            .new_rev_id_mapping
                            .put(&packet.next_segment_node_id, &node_id);
                        packet.next_segment_node_id.0 += 1;
                    }

                    let mut edges = HashSet::new();

                    let mut j = i;

                    while j < segments.len() {
                        let segment = &segments[j];
                        if let Some(edges_j) = edges_fn(segment, &node_id) {
                            for edge in edges_j {
                                let node = segment.rev_id_mapping(&edge.other).unwrap();

                                if !packet.new_id_mapping.contains_key(&node) {
                                    packet
                                        .new_id_mapping
                                        .put(&node, &packet.next_segment_node_id);
                                    packet
                                        .new_rev_id_mapping
                                        .put(&packet.next_segment_node_id, &node);
                                    packet.next_segment_node_id.0 += 1;
                                }

                                let new_segment_node_id = packet.new_id_mapping.get(&node).unwrap();

                                let new_edge = StoredEdge {
                                    other: new_segment_node_id,
                                    label: edge.label,
                                };

                                edges.insert(new_edge);
                            }
                        }

                        j += 1;
                    }

                    if !edges.is_empty() {
                        let segment_node_id = packet.new_id_mapping.get(&node_id).unwrap();
                        packet.new_adjacency.put(&segment_node_id, &edges);
                    }
                }
            }
            i += 1;
        }
    }

    pub fn merge(segments: Vec<StoredSegment>) -> Self {
        debug_assert!(segments.len() > 1);

        let new_segment_id = uuid::Uuid::new_v4().to_string();

        let new_path = segments[0].path().parent().unwrap().join(&new_segment_id);

        let new_adjacency = Store::open(new_path.join("adjacency"));

        let next_segment_node_id = SegmentNodeID(0);

        let new_id_mapping = Store::open(new_path.join("id_mapping"));
        let new_rev_id_mapping = Store::open(new_path.join("rev_id_mapping"));

        let mut packet = MergePacket {
            next_segment_node_id,
            new_id_mapping,
            new_rev_id_mapping,
            new_adjacency,
        };

        Self::merge_adjacency(
            |segment: &StoredSegment, node_id: &NodeID| {
                segment.id_mapping.get(node_id).map(|segment_node_id| {
                    segment.adjacency.get(&segment_node_id).unwrap_or_default()
                })
            },
            &mut packet,
            &segments,
        );
        let new_adjacency = packet.new_adjacency;

        let new_reversed_adjacency = Store::open(new_path.join("reversed_adjacency"));

        let mut packet = MergePacket {
            next_segment_node_id: packet.next_segment_node_id,
            new_id_mapping: packet.new_id_mapping,
            new_rev_id_mapping: packet.new_rev_id_mapping,
            new_adjacency: new_reversed_adjacency,
        };

        Self::merge_adjacency(
            |segment: &StoredSegment, node_id: &NodeID| {
                segment.id_mapping.get(node_id).map(|segment_node_id| {
                    segment
                        .reversed_adjacency
                        .get(&segment_node_id)
                        .unwrap_or_default()
                })
            },
            &mut packet,
            &segments,
        );

        let mut res = Self {
            adjacency: new_adjacency,
            reversed_adjacency: packet.new_adjacency,
            id_mapping: packet.new_id_mapping,
            rev_id_mapping: packet.new_rev_id_mapping,
            meta: Meta {
                num_nodes: packet.next_segment_node_id.0,
            },
            id: new_segment_id,
            folder_path: new_path.to_str().unwrap().to_string(),
        };

        res.flush();

        res
    }

    pub fn update_id_mapping(&mut self, mapping: Vec<(NodeID, NodeID)>) {
        let mut new_mappings = Vec::with_capacity(mapping.len());

        for (old_id, new_id) in mapping {
            if let Some(segment_id) = self.id_mapping(&old_id) {
                self.id_mapping.remove(&old_id);
                self.rev_id_mapping.remove(&segment_id);

                new_mappings.push((segment_id, new_id));
            }
        }

        self.id_mapping.batch_put(
            new_mappings
                .iter()
                .map(|(segment_id, new_id)| (new_id, segment_id)),
        );
        self.rev_id_mapping.batch_put(
            new_mappings
                .iter()
                .map(|(segment_id, new_id)| (segment_id, new_id)),
        );

        self.flush();
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
struct Meta {
    num_nodes: u64,
}

#[derive(Default)]
pub struct LiveSegment {
    adjacency: BTreeMap<SegmentNodeID, HashSet<StoredEdge>>,
    reversed_adjacency: BTreeMap<SegmentNodeID, HashSet<StoredEdge>>,
    id_mapping: BTreeMap<NodeID, SegmentNodeID>,
    rev_id_mapping: BTreeMap<SegmentNodeID, NodeID>,
    next_id: u64,
}

impl LiveSegment {
    fn get_or_create_id(&mut self, id: NodeID) -> SegmentNodeID {
        if let Some(segment_id) = self.id_mapping.get(&id) {
            *segment_id
        } else {
            let segment_id = SegmentNodeID(self.next_id);
            self.next_id += 1;

            self.id_mapping.insert(id, segment_id);
            self.rev_id_mapping.insert(segment_id, id);

            segment_id
        }
    }

    pub fn insert(&mut self, from: NodeID, to: NodeID, label: String) {
        let from = self.get_or_create_id(from);
        let to = self.get_or_create_id(to);

        self.adjacency.entry(from).or_default().insert(StoredEdge {
            other: to,
            label: label.clone(),
        });

        self.reversed_adjacency
            .entry(to)
            .or_default()
            .insert(StoredEdge { other: from, label });
    }

    pub fn commit<P: AsRef<Path>>(self, folder_path: P) -> StoredSegment {
        let segment_id = uuid::Uuid::new_v4().to_string();
        let path = folder_path.as_ref().join(&segment_id);

        let adjacency = Store::open(path.join("adjacency"));
        adjacency.batch_put(self.adjacency.iter());

        let reversed_adjacency = Store::open(path.join("reversed_adjacency"));

        reversed_adjacency.batch_put(self.reversed_adjacency.iter());

        let id_mapping = Store::open(path.join("id_mapping"));
        id_mapping.batch_put(self.id_mapping.iter());

        let rev_id_mapping = Store::open(path.join("rev_id_mapping"));
        rev_id_mapping.batch_put(self.rev_id_mapping.iter());

        let mut stored_segment = StoredSegment {
            adjacency,
            reversed_adjacency,
            id_mapping,
            rev_id_mapping,
            meta: Meta {
                num_nodes: self.next_id,
            },
            folder_path: path.as_os_str().to_str().unwrap().to_string(),
            id: segment_id,
        };

        stored_segment.flush();

        stored_segment
    }

    pub fn is_empty(&self) -> bool {
        self.id_mapping.is_empty()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple_triangle_graph() {
        //     ┌────┐
        //     │    │
        // ┌───0◄─┐ │
        // │      │ │
        // ▼      │ │
        // 1─────►2◄┘

        let mut store = LiveSegment::default();

        let a = NodeID(0);
        let b = NodeID(1);
        let c = NodeID(2);

        store.insert(a, b, String::new());
        store.insert(b, c, String::new());
        store.insert(c, a, String::new());
        store.insert(a, c, String::new());

        let store: StoredSegment = store.commit(crate::gen_temp_path());

        let mut out: Vec<Edge> = store.outgoing_edges(&a, false);

        out.sort_by(|a, b| a.to.cmp(&b.to));

        assert_eq!(
            out,
            vec![
                Edge {
                    from: a,
                    to: b,
                    label: Loaded::NotYet
                },
                Edge {
                    from: a,
                    to: c,
                    label: Loaded::NotYet
                },
            ]
        );

        let mut out: Vec<Edge> = store.outgoing_edges(&b, false);
        out.sort_by(|a, b| a.to.cmp(&b.to));
        assert_eq!(
            out,
            vec![Edge {
                from: b,
                to: c,
                label: Loaded::NotYet
            },]
        );

        let mut out: Vec<Edge> = store.outgoing_edges(&c, false);
        out.sort_by(|a, b| a.to.cmp(&b.to));
        assert_eq!(
            out,
            vec![Edge {
                from: c,
                to: a,
                label: Loaded::NotYet
            },]
        );

        let out: Vec<Edge> = store.ingoing_edges(&a, false);
        assert_eq!(
            out,
            vec![Edge {
                from: c,
                to: a,
                label: Loaded::NotYet
            },]
        );

        let out: Vec<Edge> = store.ingoing_edges(&b, false);
        assert_eq!(
            out,
            vec![Edge {
                from: a,
                to: b,
                label: Loaded::NotYet
            },]
        );

        let mut out: Vec<Edge> = store.ingoing_edges(&c, false);
        out.sort_by(|a, b| a.from.cmp(&b.from));
        assert_eq!(
            out,
            vec![
                Edge {
                    from: a,
                    to: c,
                    label: Loaded::NotYet
                },
                Edge {
                    from: b,
                    to: c,
                    label: Loaded::NotYet
                },
            ]
        );
    }
}
