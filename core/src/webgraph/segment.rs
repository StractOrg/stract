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

use rkyv::{Archive, Deserialize};
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

pub struct StoredSegment {
    adjacency: Store<SegmentNodeID, HashSet<StoredEdge>>,
    reversed_adjacency: Store<SegmentNodeID, HashSet<StoredEdge>>,
    id_mapping: BTreeMap<NodeID, SegmentNodeID>,
    rev_id_mapping: BTreeMap<SegmentNodeID, NodeID>,
    meta: Meta,
    id: String,
    folder_path: String,
}

impl StoredSegment {
    pub fn open<P: AsRef<Path>>(folder_path: P, id: String) -> Self {
        StoredSegment {
            adjacency: Store::open(folder_path.as_ref().join("adjacency"), &id).unwrap(),
            reversed_adjacency: Store::open(folder_path.as_ref().join("reversed_adjacency"), &id)
                .unwrap(),
            id_mapping: open_bin(folder_path.as_ref().join("id_mapping.bin")),
            rev_id_mapping: open_bin(folder_path.as_ref().join("rev_id_mapping.bin")),
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
        self.id_mapping.get(node).copied()
    }

    fn rev_id_mapping(&self, node: &SegmentNodeID) -> Option<NodeID> {
        self.rev_id_mapping.get(node).copied()
    }

    pub fn outgoing_edges(&self, node: &NodeID, load_label: bool) -> Vec<Edge> {
        self.id_mapping(node)
            .and_then(|segment_id| {
                self.adjacency.get(&segment_id).map(|edges| {
                    edges
                        .iter()
                        .map(move |edge| {
                            if load_label {
                                Edge {
                                    from: *node,
                                    to: self.rev_id_mapping(&SegmentNodeID(edge.other.0)).unwrap(),
                                    label: Loaded::Some(
                                        edge.label.deserialize(&mut rkyv::Infallible).unwrap(),
                                    ),
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
                        .iter()
                        .map(move |edge| {
                            if load_label {
                                Edge {
                                    from: self
                                        .rev_id_mapping(&SegmentNodeID(edge.other.0))
                                        .unwrap(),
                                    to: *node,
                                    label: Loaded::Some(
                                        edge.label.deserialize(&mut rkyv::Infallible).unwrap(),
                                    ),
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
        self.adjacency.flush().unwrap();
        self.reversed_adjacency.flush().unwrap();
        save_bin(&self.id_mapping, self.path().join("id_mapping.bin"));
        save_bin(&self.rev_id_mapping, self.path().join("rev_id_mapping.bin"));
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
            let from = self.rev_id_mapping(node_id).unwrap();

            edges.iter().map(move |stored_edge| Edge {
                from,
                to: self
                    .rev_id_mapping(&SegmentNodeID(stored_edge.other.0))
                    .unwrap(),
                label: Loaded::NotYet,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_adjacency<F1, F2>(
        &self,
        self_edges_fn: F1,
        other: &Self,
        other_edges_fn: F2,
        next_segment_id: &mut SegmentNodeID,
        new_id_mapping: &mut BTreeMap<NodeID, SegmentNodeID>,
        new_rev_id_mapping: &mut BTreeMap<SegmentNodeID, NodeID>,
        new_adjacency: &mut Store<SegmentNodeID, HashSet<StoredEdge>>,
    ) where
        F1: Fn(&Self, &SegmentNodeID) -> HashSet<StoredEdge>,
        F2: Fn(&Self, &SegmentNodeID) -> HashSet<StoredEdge>,
    {
        for (node_id, segment_id) in &self.id_mapping {
            let mut edges = self_edges_fn(self, segment_id);

            if let Some(other_segment_id) = other.id_mapping.get(node_id) {
                let other_edges = other_edges_fn(other, other_segment_id);

                for other_edge in other_edges {
                    let node = other.rev_id_mapping(&other_edge.other).unwrap();

                    if let Some(segment_id) = new_id_mapping.get(&node).copied() {
                        edges.insert(StoredEdge {
                            other: segment_id,
                            label: other_edge.label,
                        });
                    } else {
                        let r = new_id_mapping.insert(node, *next_segment_id);
                        debug_assert!(r.is_none());
                        let r = new_rev_id_mapping.insert(*next_segment_id, node);
                        debug_assert!(r.is_none());

                        edges.insert(StoredEdge {
                            other: *next_segment_id,
                            label: other_edge.label,
                        });

                        *next_segment_id = SegmentNodeID(next_segment_id.0 + 1);
                    }
                }
            }

            if !edges.is_empty() {
                let segment_id = new_id_mapping.get(node_id).copied().unwrap();
                println!("{:?} -> {:?}", node_id, segment_id);
                new_adjacency.insert(segment_id, &edges).unwrap();
            }
        }

        for (node_id, segment_id) in &other.id_mapping {
            if self.id_mapping.get(node_id).is_none() {
                let this_segment_id = if let Some(segment_id) = new_id_mapping.get(node_id) {
                    *segment_id
                } else {
                    let r = new_id_mapping.insert(*node_id, *next_segment_id);
                    debug_assert!(r.is_none());

                    let r = new_rev_id_mapping.insert(*next_segment_id, *node_id);
                    debug_assert!(r.is_none());
                    let this = *next_segment_id;

                    *next_segment_id = SegmentNodeID(next_segment_id.0 + 1);

                    this
                };

                let edges = other_edges_fn(other, segment_id);

                let edges: HashSet<_> = edges
                    .into_iter()
                    .map(|edge| {
                        let other_node = other.rev_id_mapping(&edge.other).unwrap();
                        if let Some(segment_id) = new_id_mapping.get(&other_node) {
                            StoredEdge {
                                other: *segment_id,
                                label: edge.label,
                            }
                        } else {
                            let new_segment_id = *next_segment_id;
                            *next_segment_id = SegmentNodeID(next_segment_id.0 + 1);

                            new_id_mapping.insert(other_node, new_segment_id);
                            new_rev_id_mapping.insert(new_segment_id, other_node);

                            StoredEdge {
                                other: new_segment_id,
                                label: edge.label,
                            }
                        }
                    })
                    .collect();

                if !edges.is_empty() {
                    new_adjacency.insert(this_segment_id, &edges).unwrap();
                }
            }
        }
    }

    pub fn merge(self, other: StoredSegment) -> Self {
        let new_segment_id = uuid::Uuid::new_v4().to_string();

        let mut new_adjacency = Store::open(
            self.path().join(&new_segment_id).join("adjacency"),
            &new_segment_id,
        )
        .unwrap();

        let mut next_segment_id = SegmentNodeID(self.meta.num_nodes);

        let mut new_id_mapping = self.id_mapping.clone();
        let mut new_rev_id_mapping = self.rev_id_mapping.clone();

        self.merge_adjacency(
            |_self: &StoredSegment, segment_id: &SegmentNodeID| {
                _self
                    .adjacency
                    .get(segment_id)
                    .map(|edges| edges.deserialize(&mut rkyv::Infallible).unwrap())
                    .unwrap_or_default()
            },
            &other,
            |_other: &StoredSegment, segment_id: &SegmentNodeID| {
                _other
                    .adjacency
                    .get(segment_id)
                    .map(|edges| edges.deserialize(&mut rkyv::Infallible).unwrap())
                    .unwrap_or_default()
            },
            &mut next_segment_id,
            &mut new_id_mapping,
            &mut new_rev_id_mapping,
            &mut new_adjacency,
        );

        let mut new_reversed_adjacency = Store::open(
            self.path().join(&new_segment_id).join("reversed_adjacency"),
            &new_segment_id,
        )
        .unwrap();

        self.merge_adjacency(
            |_self: &StoredSegment, segment_id: &SegmentNodeID| {
                _self
                    .reversed_adjacency
                    .get(segment_id)
                    .map(|edges| edges.deserialize(&mut rkyv::Infallible).unwrap())
                    .unwrap_or_default()
            },
            &other,
            |_other: &StoredSegment, segment_id: &SegmentNodeID| {
                _other
                    .reversed_adjacency
                    .get(segment_id)
                    .map(|edges| edges.deserialize(&mut rkyv::Infallible).unwrap())
                    .unwrap_or_default()
            },
            &mut next_segment_id,
            &mut new_id_mapping,
            &mut new_rev_id_mapping,
            &mut new_reversed_adjacency,
        );

        let mut res = Self {
            adjacency: new_adjacency,
            reversed_adjacency: new_reversed_adjacency,
            id_mapping: new_id_mapping,
            rev_id_mapping: new_rev_id_mapping,
            meta: Meta {
                num_nodes: next_segment_id.0,
            },
            id: new_segment_id,
            folder_path: self.folder_path,
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

        for (segment_id, new_id) in new_mappings {
            self.id_mapping.insert(new_id, segment_id);
            self.rev_id_mapping.insert(segment_id, new_id);
        }

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

        let mut adjacency = Store::open(path.join("adjacency"), &segment_id).unwrap();

        for (from, edges) in self.adjacency {
            adjacency.insert(from, &edges).unwrap();
        }

        let mut reversed_adjacency =
            Store::open(path.join("reversed_adjacency"), &segment_id).unwrap();

        for (to, edges) in self.reversed_adjacency {
            reversed_adjacency.insert(to, &edges).unwrap();
        }

        let mut stored_segment = StoredSegment {
            adjacency,
            reversed_adjacency,
            id_mapping: self.id_mapping,
            rev_id_mapping: self.rev_id_mapping,
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
