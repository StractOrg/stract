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

use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
};

use super::{store::Store, Edge, FullStoredEdge, Loaded, NodeID, SmallStoredEdge};

const FULL_ADJACENCY_STORE: &str = "full_adjacency";
const FULL_REVERSED_ADJACENCY_STORE: &str = "full_reversed_adjacency";
const SMALL_ADJACENCY_STORE: &str = "small_adjacency";
const SMALL_REVERSED_ADJACENCY_STORE: &str = "small_reversed_adjacency";

struct MergePacket {
    new_full_adjacency: Store<NodeID, HashSet<FullStoredEdge>>,
    new_small_adjacency: Store<NodeID, HashSet<SmallStoredEdge>>,
}

pub struct StoredSegment {
    full_adjacency: Store<NodeID, HashSet<FullStoredEdge>>,
    full_reversed_adjacency: Store<NodeID, HashSet<FullStoredEdge>>,
    small_adjacency: Store<NodeID, HashSet<SmallStoredEdge>>,
    small_reversed_adjacency: Store<NodeID, HashSet<SmallStoredEdge>>,
    id: String,
    folder_path: String,
}

impl StoredSegment {
    pub fn open<P: AsRef<Path>>(folder_path: P, id: String) -> Self {
        StoredSegment {
            full_adjacency: Store::open(folder_path.as_ref().join(FULL_ADJACENCY_STORE)),
            full_reversed_adjacency: Store::open(
                folder_path.as_ref().join(FULL_REVERSED_ADJACENCY_STORE),
            ),
            small_adjacency: Store::open(folder_path.as_ref().join(SMALL_ADJACENCY_STORE)),
            small_reversed_adjacency: Store::open(
                folder_path.as_ref().join(SMALL_REVERSED_ADJACENCY_STORE),
            ),
            folder_path: folder_path
                .as_ref()
                .as_os_str()
                .to_str()
                .unwrap()
                .to_string(),
            id,
        }
    }

    pub fn estimate_num_nodes(&self) -> usize {
        self.small_adjacency.estimate_len()
    }

    pub fn outgoing_edges(&self, node: &NodeID, load_label: bool) -> Vec<Edge> {
        if load_label {
            self.full_adjacency
                .get(node)
                .map(|edges| {
                    edges
                        .into_iter()
                        .map(move |edge| Edge {
                            from: *node,
                            to: edge.other,
                            label: Loaded::Some(edge.label),
                        })
                        .collect()
                })
                .unwrap_or_default()
        } else {
            self.small_adjacency
                .get(node)
                .map(|edges| {
                    edges
                        .into_iter()
                        .map(move |edge| Edge {
                            from: *node,
                            to: edge.other,
                            label: Loaded::NotYet,
                        })
                        .collect()
                })
                .unwrap_or_default()
        }
    }

    pub fn ingoing_edges(&self, node: &NodeID, load_label: bool) -> Vec<Edge> {
        if load_label {
            self.full_reversed_adjacency
                .get(node)
                .map(|edges| {
                    edges
                        .into_iter()
                        .map(move |edge| Edge {
                            from: edge.other,
                            to: *node,
                            label: Loaded::Some(edge.label),
                        })
                        .collect()
                })
                .unwrap_or_default()
        } else {
            self.small_reversed_adjacency
                .get(node)
                .map(|edges| {
                    edges
                        .into_iter()
                        .map(move |edge| Edge {
                            from: edge.other,
                            to: *node,
                            label: Loaded::NotYet,
                        })
                        .collect()
                })
                .unwrap_or_default()
        }
    }

    fn flush(&mut self) {
        self.full_adjacency.flush();
        self.full_reversed_adjacency.flush();
        self.small_adjacency.flush();
        self.small_reversed_adjacency.flush();
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn path(&self) -> &Path {
        Path::new(&self.folder_path)
    }

    pub fn edges(&self) -> impl Iterator<Item = Edge> + '_ {
        self.small_adjacency.iter().flat_map(move |(from, edges)| {
            edges.into_iter().map(move |stored_edge| Edge {
                from,
                to: stored_edge.other,
                label: Loaded::NotYet,
            })
        })
    }

    fn merge_adjacency<F1>(edges_fn: F1, packet: &mut MergePacket, segments: &[Self])
    where
        F1: Fn(&Self, &NodeID) -> Option<HashSet<FullStoredEdge>>,
    {
        if segments.is_empty() {
            return;
        }

        for segment in segments {
            for node_id in segment
                .small_adjacency
                .keys()
                .chain(segment.small_reversed_adjacency.keys())
            {
                let mut existing = packet.new_full_adjacency.get(&node_id).unwrap_or_default();

                if let Some(edges) = edges_fn(segment, &node_id) {
                    existing.extend(edges);
                }

                packet.new_full_adjacency.put(&node_id, &existing);
                packet.new_small_adjacency.put(
                    &node_id,
                    &existing
                        .into_iter()
                        .map(|edge| SmallStoredEdge { other: edge.other })
                        .collect::<HashSet<_>>(),
                );
            }
        }
    }

    pub fn merge(segments: Vec<StoredSegment>) -> Self {
        debug_assert!(segments.len() > 1);

        let new_segment_id = uuid::Uuid::new_v4().to_string();

        let new_path = segments[0].path().parent().unwrap().join(&new_segment_id);

        let new_full_adjacency = Store::open(new_path.join(FULL_ADJACENCY_STORE));
        let new_small_adjacency = Store::open(new_path.join(SMALL_ADJACENCY_STORE));

        let mut packet = MergePacket {
            new_full_adjacency,
            new_small_adjacency,
        };

        Self::merge_adjacency(
            |segment: &StoredSegment, node_id: &NodeID| segment.full_adjacency.get(node_id),
            &mut packet,
            &segments,
        );
        let new_adjacency = packet.new_full_adjacency;
        let new_small_adjacency = packet.new_small_adjacency;

        let new_full_reversed_adjacency = Store::open(new_path.join(FULL_REVERSED_ADJACENCY_STORE));
        let new_small_reversed_adjacency =
            Store::open(new_path.join(SMALL_REVERSED_ADJACENCY_STORE));

        let mut packet = MergePacket {
            new_full_adjacency: new_full_reversed_adjacency,
            new_small_adjacency: new_small_reversed_adjacency,
        };

        Self::merge_adjacency(
            |segment: &StoredSegment, node_id: &NodeID| {
                segment.full_reversed_adjacency.get(node_id)
            },
            &mut packet,
            &segments,
        );

        let mut res = Self {
            full_adjacency: new_adjacency,
            small_adjacency: new_small_adjacency,
            full_reversed_adjacency: packet.new_full_adjacency,
            small_reversed_adjacency: packet.new_small_adjacency,
            id: new_segment_id,
            folder_path: new_path.to_str().unwrap().to_string(),
        };

        res.flush();

        res
    }
}

#[derive(Default)]
pub struct LiveSegment {
    adjacency: BTreeMap<NodeID, HashSet<FullStoredEdge>>,
    reversed_adjacency: BTreeMap<NodeID, HashSet<FullStoredEdge>>,
}

impl LiveSegment {
    pub fn insert(&mut self, from: NodeID, to: NodeID, label: String) {
        self.adjacency
            .entry(from)
            .or_default()
            .insert(FullStoredEdge {
                other: to,
                label: label.clone(),
            });

        self.reversed_adjacency
            .entry(to)
            .or_default()
            .insert(FullStoredEdge { other: from, label });
    }

    pub fn commit<P: AsRef<Path>>(self, folder_path: P) -> StoredSegment {
        let segment_id = uuid::Uuid::new_v4().to_string();
        let path = folder_path.as_ref().join(&segment_id);

        let small_adjacency: BTreeMap<NodeID, HashSet<SmallStoredEdge>> = self
            .adjacency
            .clone()
            .into_iter()
            .map(|(segment_node_id, edges)| {
                let edges = edges
                    .into_iter()
                    .map(|edge| SmallStoredEdge { other: edge.other })
                    .collect();

                (segment_node_id, edges)
            })
            .collect();

        let small_rev_adjacency: BTreeMap<NodeID, HashSet<SmallStoredEdge>> = self
            .reversed_adjacency
            .clone()
            .into_iter()
            .map(|(segment_node_id, edges)| {
                let edges = edges
                    .into_iter()
                    .map(|edge| SmallStoredEdge { other: edge.other })
                    .collect();

                (segment_node_id, edges)
            })
            .collect();

        let full_adjacency = Store::open(path.join(FULL_ADJACENCY_STORE));
        full_adjacency.batch_put(self.adjacency.iter());

        let full_reversed_adjacency = Store::open(path.join(FULL_REVERSED_ADJACENCY_STORE));
        full_reversed_adjacency.batch_put(self.reversed_adjacency.iter());

        let small_adjacency_store = Store::open(path.join(SMALL_ADJACENCY_STORE));
        small_adjacency_store.batch_put(small_adjacency.iter());

        let small_rev_adjacency_store = Store::open(path.join(SMALL_REVERSED_ADJACENCY_STORE));
        small_rev_adjacency_store.batch_put(small_rev_adjacency.iter());

        let mut stored_segment = StoredSegment {
            small_adjacency: small_adjacency_store,
            small_reversed_adjacency: small_rev_adjacency_store,
            full_adjacency,
            full_reversed_adjacency,
            folder_path: path.as_os_str().to_str().unwrap().to_string(),
            id: segment_id,
        };

        stored_segment.flush();

        stored_segment
    }

    pub fn is_empty(&self) -> bool {
        self.adjacency.is_empty()
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
