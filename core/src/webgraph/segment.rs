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

use std::path::{Path, PathBuf};

use super::{store::EdgeStore, Deduplication, Edge, NodeID};

const ADJACENCY_STORE: &str = "adjacency";
const REVERSED_ADJACENCY_STORE: &str = "reversed_adjacency";

pub struct StoredSegment {
    full_adjacency: EdgeStore,
    full_reversed_adjacency: EdgeStore,
    id: String,
    folder_path: String,
}

impl StoredSegment {
    pub fn open<P: AsRef<Path>>(folder_path: P, id: String, dedup: Deduplication) -> Self {
        let dedup_insert = match dedup {
            Deduplication::OnlyQuery => false,
            Deduplication::QueryAndInserts => true,
        };

        StoredSegment {
            full_adjacency: EdgeStore::open(
                folder_path.as_ref().join(&id).join(ADJACENCY_STORE),
                false,
                dedup_insert,
            ),
            full_reversed_adjacency: EdgeStore::open(
                folder_path
                    .as_ref()
                    .join(&id)
                    .join(REVERSED_ADJACENCY_STORE),
                true,
                dedup_insert,
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
        self.full_adjacency.estimate_len()
    }

    pub fn outgoing_edges_with_label(
        &self,
        node: &NodeID,
    ) -> impl Iterator<Item = Edge<String>> + '_ {
        self.full_adjacency.get(*node)
    }

    pub fn outgoing_edges(&self, node: &NodeID) -> impl Iterator<Item = Edge<()>> + '_ {
        self.full_adjacency.get(*node)
    }

    pub fn ingoing_edges_with_label(
        &self,
        node: &NodeID,
    ) -> impl Iterator<Item = Edge<String>> + '_ {
        self.full_reversed_adjacency.get(*node)
    }

    pub fn ingoing_edges(&self, node: &NodeID) -> impl Iterator<Item = Edge<()>> + '_ {
        self.full_reversed_adjacency.get(*node)
    }

    pub fn flush(&mut self) {
        self.full_adjacency.flush();
        self.full_reversed_adjacency.flush();
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn path(&self) -> PathBuf {
        Path::new(&self.folder_path).join(&self.id)
    }

    pub fn edges(&self) -> impl Iterator<Item = Edge<()>> + '_ + Send + Sync {
        self.full_adjacency.iter()
    }

    fn merge_with(&self, other: &StoredSegment) {
        self.full_adjacency.merge_with(&other.full_adjacency);
        self.full_reversed_adjacency
            .merge_with(&other.full_reversed_adjacency);
    }

    pub fn merge(mut segments: Vec<StoredSegment>) -> Self {
        debug_assert!(!segments.is_empty());

        if segments.len() == 1 {
            let mut segments = segments;
            return segments.pop().unwrap();
        }

        let segment = segments.remove(0);

        for other in segments {
            segment.merge_with(&other);
        }

        segment
    }

    pub fn insert(&mut self, edges: &[Edge<String>]) {
        self.full_adjacency.put(edges.iter());
        self.full_reversed_adjacency.put(edges.iter());
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

        let mut store = StoredSegment::open(
            crate::gen_temp_path(),
            "test".to_string(),
            Deduplication::OnlyQuery,
        );

        let mut edges = Vec::new();

        let a = NodeID(0);
        let b = NodeID(1);
        let c = NodeID(2);

        edges.push(Edge {
            from: a,
            to: b,
            label: String::new(),
        });
        edges.push(Edge {
            from: b,
            to: c,
            label: String::new(),
        });
        edges.push(Edge {
            from: c,
            to: a,
            label: String::new(),
        });
        edges.push(Edge {
            from: a,
            to: c,
            label: String::new(),
        });

        store.insert(&edges);
        store.flush();

        let mut out: Vec<_> = store.outgoing_edges(&a).collect();

        out.sort_by(|a, b| a.to.cmp(&b.to));

        assert_eq!(
            out,
            vec![
                Edge {
                    from: a,
                    to: b,
                    label: ()
                },
                Edge {
                    from: a,
                    to: c,
                    label: ()
                },
            ]
        );

        let mut out: Vec<_> = store.outgoing_edges(&b).collect();
        out.sort_by(|a, b| a.to.cmp(&b.to));
        assert_eq!(
            out,
            vec![Edge {
                from: b,
                to: c,
                label: ()
            },]
        );

        let mut out: Vec<_> = store.outgoing_edges(&c).collect();
        out.sort_by(|a, b| a.to.cmp(&b.to));
        assert_eq!(
            out,
            vec![Edge {
                from: c,
                to: a,
                label: ()
            },]
        );

        let out: Vec<_> = store.ingoing_edges(&a).collect();
        assert_eq!(
            out,
            vec![Edge {
                from: c,
                to: a,
                label: ()
            },]
        );

        let out: Vec<_> = store.ingoing_edges(&b).collect();
        assert_eq!(
            out,
            vec![Edge {
                from: a,
                to: b,
                label: ()
            },]
        );

        let mut out: Vec<_> = store.ingoing_edges(&c).collect();
        out.sort_by(|a, b| a.from.cmp(&b.from));
        assert_eq!(
            out,
            vec![
                Edge {
                    from: a,
                    to: c,
                    label: ()
                },
                Edge {
                    from: b,
                    to: c,
                    label: ()
                },
            ]
        );
    }
}
