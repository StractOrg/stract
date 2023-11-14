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

use super::{
    store::{EdgeStore, EdgeStoreWriter},
    Compression, Edge, InnerEdge, NodeID,
};

const ADJACENCY_STORE: &str = "adjacency";
const REVERSED_ADJACENCY_STORE: &str = "reversed_adjacency";

pub struct SegmentWriter {
    full_adjacency: EdgeStoreWriter,
    full_reversed_adjacency: EdgeStoreWriter,
    id: String,
    folder_path: String,
}

impl SegmentWriter {
    pub fn open<P: AsRef<Path>>(folder_path: P, id: String, compression: Compression) -> Self {
        SegmentWriter {
            full_adjacency: EdgeStoreWriter::open(
                folder_path.as_ref().join(&id).join(ADJACENCY_STORE),
                compression,
                false,
            ),
            full_reversed_adjacency: EdgeStoreWriter::open(
                folder_path
                    .as_ref()
                    .join(&id)
                    .join(REVERSED_ADJACENCY_STORE),
                compression,
                true,
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

    pub fn finalize(mut self) -> Segment {
        self.flush();

        Segment {
            full_adjacency: self.full_adjacency.finalize(),
            full_reversed_adjacency: self.full_reversed_adjacency.finalize(),
            folder_path: self.folder_path,
            id: self.id,
        }
    }

    pub fn flush(&mut self) {
        self.full_adjacency.flush();
        self.full_reversed_adjacency.flush();
    }

    pub fn insert(&mut self, edges: &[InnerEdge<String>]) {
        self.full_adjacency.put(edges.iter());
        self.full_reversed_adjacency.put(edges.iter());
    }
}

pub struct Segment {
    full_adjacency: EdgeStore,
    full_reversed_adjacency: EdgeStore,
    id: String,
    folder_path: String,
}

impl Segment {
    pub fn open<P: AsRef<Path>>(folder_path: P, id: String, compression: Compression) -> Self {
        Segment {
            full_adjacency: EdgeStore::open(
                folder_path.as_ref().join(&id).join(ADJACENCY_STORE),
                false,
                compression,
            ),
            full_reversed_adjacency: EdgeStore::open(
                folder_path
                    .as_ref()
                    .join(&id)
                    .join(REVERSED_ADJACENCY_STORE),
                true,
                compression,
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

    pub fn outgoing_edges_with_label(&self, node: &NodeID) -> Vec<Edge<String>> {
        self.full_adjacency.get_with_label(node)
    }

    pub fn outgoing_edges(&self, node: &NodeID) -> Vec<Edge<()>> {
        self.full_adjacency.get_without_label(node)
    }

    pub fn ingoing_edges_with_label(&self, node: &NodeID) -> Vec<Edge<String>> {
        self.full_reversed_adjacency.get_with_label(node)
    }

    pub fn ingoing_edges(&self, node: &NodeID) -> Vec<Edge<()>> {
        self.full_reversed_adjacency.get_without_label(node)
    }

    pub fn ingoing_edges_by_host(&self, host_node: &NodeID) -> Vec<Edge<()>> {
        self.full_reversed_adjacency
            .nodes_by_prefix(host_node)
            .into_iter()
            .flat_map(|node| self.ingoing_edges(&node))
            .collect()
    }

    pub fn pages_by_host(&self, host_node: &NodeID) -> Vec<NodeID> {
        self.full_reversed_adjacency.nodes_by_prefix(host_node)
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn path(&self) -> PathBuf {
        Path::new(&self.folder_path).join(&self.id)
    }

    pub fn edges(&self) -> impl Iterator<Item = Edge<()>> + '_ + Send + Sync {
        self.full_adjacency.iter_without_label()
    }
}

#[cfg(test)]
mod test {
    use crate::webgraph::FullNodeID;

    use super::*;

    #[test]
    fn simple_triangle_graph() {
        //     ┌────┐
        //     │    │
        // ┌───0◄─┐ │
        // │      │ │
        // ▼      │ │
        // 1─────►2◄┘

        let mut writer = SegmentWriter::open(
            crate::gen_temp_path(),
            "test".to_string(),
            Compression::default(),
        );

        let mut edges = Vec::new();

        let a = FullNodeID {
            id: NodeID(0),
            prefix: NodeID(0),
        };
        let b = FullNodeID {
            id: NodeID(1),
            prefix: NodeID(0),
        };
        let c = FullNodeID {
            id: NodeID(2),
            prefix: NodeID(0),
        };

        edges.push(InnerEdge {
            from: a.clone(),
            to: b.clone(),
            label: String::new(),
        });
        edges.push(InnerEdge {
            from: b.clone(),
            to: c.clone(),
            label: String::new(),
        });
        edges.push(InnerEdge {
            from: c.clone(),
            to: a.clone(),
            label: String::new(),
        });
        edges.push(InnerEdge {
            from: a.clone(),
            to: c.clone(),
            label: String::new(),
        });

        writer.insert(&edges);
        let segment = writer.finalize();

        let mut out: Vec<_> = segment.outgoing_edges(&a.id);

        out.sort_by(|a, b| a.to.cmp(&b.to));

        assert_eq!(
            out,
            vec![
                Edge {
                    from: a.id,
                    to: b.id,
                    label: ()
                },
                Edge {
                    from: a.id,
                    to: c.id,
                    label: ()
                },
            ]
        );

        let mut out: Vec<_> = segment.outgoing_edges(&b.id);
        out.sort_by(|a, b| a.to.cmp(&b.to));
        assert_eq!(
            out,
            vec![Edge {
                from: b.id,
                to: c.id,
                label: ()
            },]
        );

        let mut out: Vec<_> = segment.outgoing_edges(&c.id);
        out.sort_by(|a, b| a.to.cmp(&b.to));
        assert_eq!(
            out,
            vec![Edge {
                from: c.id,
                to: a.id,
                label: ()
            },]
        );

        let out: Vec<_> = segment.ingoing_edges(&a.id);
        assert_eq!(
            out,
            vec![Edge {
                from: c.id,
                to: a.id,
                label: ()
            },]
        );

        let out: Vec<_> = segment.ingoing_edges(&b.id);
        assert_eq!(
            out,
            vec![Edge {
                from: a.id,
                to: b.id,
                label: ()
            },]
        );

        let mut out: Vec<_> = segment.ingoing_edges(&c.id);
        out.sort_by(|a, b| a.from.cmp(&b.from));
        assert_eq!(
            out,
            vec![
                Edge {
                    from: a.id,
                    to: c.id,
                    label: ()
                },
                Edge {
                    from: b.id,
                    to: c.id,
                    label: ()
                },
            ]
        );
    }
}
