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

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use super::{
    store::EdgeStore, store_writer::EdgeStoreWriter, Compression, EdgeLimit, InsertableEdge,
    NodeID, SegmentEdge,
};
use crate::Result;

const ADJACENCY_STORE: &str = "adjacency";
const REVERSED_ADJACENCY_STORE: &str = "reversed_adjacency";

pub struct SegmentWriter {
    adjacency: EdgeStoreWriter,
    reversed_adjacency: EdgeStoreWriter,
    id: String,
    folder_path: String,
}

impl SegmentWriter {
    pub fn open<P: AsRef<Path>>(
        folder_path: P,
        id: String,
        compression: Compression,
        host_centrality_rank_store: Option<Arc<speedy_kv::Db<NodeID, u64>>>,
    ) -> Self {
        SegmentWriter {
            adjacency: EdgeStoreWriter::new(
                folder_path.as_ref().join(&id).join(ADJACENCY_STORE),
                compression,
                false,
                host_centrality_rank_store.clone(),
            ),
            reversed_adjacency: EdgeStoreWriter::new(
                folder_path
                    .as_ref()
                    .join(&id)
                    .join(REVERSED_ADJACENCY_STORE),
                compression,
                true,
                host_centrality_rank_store.clone(),
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

    pub fn finalize(self) -> Segment {
        Segment {
            adjacency: self.adjacency.finalize(),
            reversed_adjacency: self.reversed_adjacency.finalize(),
            folder_path: self.folder_path,
            id: self.id,
        }
    }

    pub fn insert(&mut self, edge: InsertableEdge<String>) {
        self.adjacency.put(edge.clone());
        self.reversed_adjacency.put(edge);
    }
}

pub struct Segment {
    adjacency: EdgeStore,
    reversed_adjacency: EdgeStore,
    id: String,
    folder_path: String,
}

impl Segment {
    pub fn open<P: AsRef<Path>>(folder_path: P, id: String) -> Self {
        Segment {
            adjacency: EdgeStore::open(folder_path.as_ref().join(&id).join(ADJACENCY_STORE), false),
            reversed_adjacency: EdgeStore::open(
                folder_path
                    .as_ref()
                    .join(&id)
                    .join(REVERSED_ADJACENCY_STORE),
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

    pub fn merge<P: AsRef<Path>>(
        segments: Vec<Self>,
        label_compression: Compression,
        folder: P,
        id: String,
    ) -> Result<()> {
        if segments.is_empty() {
            return Ok(());
        }

        if segments.len() == 1 {
            let segment = &segments[0];
            std::fs::rename(segment.path(), folder.as_ref().join(&id))?;
            return Ok(());
        }

        let old_paths = segments.iter().map(|s| s.path()).collect::<Vec<_>>();

        let (adjacency, reversed_adjacency) = segments
            .into_iter()
            .map(|s| (s.adjacency, s.reversed_adjacency))
            .unzip();

        let adjacency_path = folder.as_ref().join(&id).join(ADJACENCY_STORE);
        let adjacency =
            thread::spawn(move || EdgeStore::merge(adjacency, label_compression, adjacency_path));

        let reversed_adjacency_path = folder.as_ref().join(&id).join(REVERSED_ADJACENCY_STORE);
        let reversed_adjacency = thread::spawn(move || {
            EdgeStore::merge(
                reversed_adjacency,
                label_compression,
                reversed_adjacency_path,
            )
        });

        adjacency.join().unwrap()?;
        reversed_adjacency.join().unwrap()?;

        for path in old_paths {
            if Path::new(&path).exists() {
                std::fs::remove_dir_all(path)?;
            }
        }

        Ok(())
    }

    pub fn outgoing_edges_with_label(
        &self,
        node: &NodeID,
        limit: &EdgeLimit,
    ) -> Vec<SegmentEdge<String>> {
        self.adjacency.get_with_label(node, limit)
    }

    pub fn outgoing_edges(&self, node: &NodeID, limit: &EdgeLimit) -> Vec<SegmentEdge<()>> {
        self.adjacency.get_without_label(node, limit)
    }

    pub fn ingoing_edges_with_label(
        &self,
        node: &NodeID,
        limit: &EdgeLimit,
    ) -> Vec<SegmentEdge<String>> {
        self.reversed_adjacency.get_with_label(node, limit)
    }

    pub fn ingoing_edges(&self, node: &NodeID, limit: &EdgeLimit) -> Vec<SegmentEdge<()>> {
        self.reversed_adjacency.get_without_label(node, limit)
    }

    pub fn pages_by_host(&self, host_node: &NodeID) -> Vec<NodeID> {
        self.reversed_adjacency.nodes_by_host(host_node)
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn path(&self) -> PathBuf {
        Path::new(&self.folder_path).join(&self.id)
    }

    pub fn edges(&self) -> impl Iterator<Item = SegmentEdge<()>> + '_ + Send + Sync {
        self.adjacency.iter_without_label()
    }

    pub fn optimize_read(&mut self) {
        self.adjacency.optimize_read();
        self.reversed_adjacency.optimize_read();
    }
}

#[cfg(test)]
mod test {
    use crate::{
        webgraph::{Edge, FullNodeID, NodeDatum},
        webpage::html::links::RelFlags,
    };

    use super::*;

    #[test]
    #[allow(clippy::too_many_lines)]
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
            None,
        );

        let mut edges = Vec::new();

        let a = FullNodeID {
            id: NodeID::from(0_u64),
            host: NodeID::from(0_u64),
        };
        let b = FullNodeID {
            id: NodeID::from(1_u64),
            host: NodeID::from(0_u64),
        };
        let c = FullNodeID {
            id: NodeID::from(2_u64),
            host: NodeID::from(0_u64),
        };

        edges.push(InsertableEdge {
            from: a.clone(),
            to: b.clone(),
            label: String::new(),
            rel: RelFlags::default(),
        });
        edges.push(InsertableEdge {
            from: b.clone(),
            to: c.clone(),
            label: String::new(),
            rel: RelFlags::default(),
        });
        edges.push(InsertableEdge {
            from: c.clone(),
            to: a.clone(),
            label: String::new(),
            rel: RelFlags::default(),
        });
        edges.push(InsertableEdge {
            from: a.clone(),
            to: c.clone(),
            label: String::new(),
            rel: RelFlags::default(),
        });

        for edge in &edges {
            writer.insert(edge.clone());
        }
        let segment = writer.finalize();

        let mut out: Vec<_> = segment.outgoing_edges(&a.id, &EdgeLimit::Unlimited);

        out.sort_by(|a, b| a.to.node().cmp(&b.to.node()));

        assert_eq!(
            out,
            vec![
                Edge {
                    from: NodeDatum::new(a.id, u64::MAX),
                    to: NodeDatum::new(b.id, u64::MAX),
                    label: (),
                    rel: RelFlags::default(),
                }
                .into(),
                Edge {
                    from: NodeDatum::new(a.id, u64::MAX),
                    to: NodeDatum::new(c.id, u64::MAX),
                    label: (),
                    rel: RelFlags::default(),
                }
                .into(),
            ]
        );

        let mut out: Vec<_> = segment.outgoing_edges(&b.id, &EdgeLimit::Unlimited);
        out.sort_by(|a, b| a.to.node().cmp(&b.to.node()));
        assert_eq!(
            out,
            vec![Edge {
                from: NodeDatum::new(b.id, u64::MAX),
                to: NodeDatum::new(c.id, u64::MAX),
                label: (),
                rel: RelFlags::default(),
            }
            .into(),]
        );

        let mut out: Vec<_> = segment.outgoing_edges(&c.id, &EdgeLimit::Unlimited);
        out.sort_by(|a, b| a.to.node().cmp(&b.to.node()));
        assert_eq!(
            out,
            vec![Edge {
                from: NodeDatum::new(c.id, u64::MAX),
                to: NodeDatum::new(a.id, u64::MAX),
                label: (),
                rel: RelFlags::default(),
            }
            .into(),]
        );

        let out: Vec<_> = segment.ingoing_edges(&a.id, &EdgeLimit::Unlimited);
        assert_eq!(
            out,
            vec![Edge {
                from: NodeDatum::new(c.id, u64::MAX),
                to: NodeDatum::new(a.id, u64::MAX),
                label: (),
                rel: RelFlags::default(),
            }
            .into(),]
        );

        let out: Vec<_> = segment.ingoing_edges(&b.id, &EdgeLimit::Unlimited);
        assert_eq!(
            out,
            vec![Edge {
                from: NodeDatum::new(a.id, u64::MAX),
                to: NodeDatum::new(b.id, u64::MAX),
                label: (),
                rel: RelFlags::default(),
            }
            .into(),]
        );

        let mut out: Vec<_> = segment.ingoing_edges(&c.id, &EdgeLimit::Unlimited);
        out.sort_by(|a, b| a.from.node().cmp(&b.from.node()));
        assert_eq!(
            out,
            vec![
                Edge {
                    from: NodeDatum::new(a.id, u64::MAX),
                    to: NodeDatum::new(c.id, u64::MAX),
                    label: (),
                    rel: RelFlags::default(),
                }
                .into(),
                Edge {
                    from: NodeDatum::new(b.id, u64::MAX),
                    to: NodeDatum::new(c.id, u64::MAX),
                    label: (),
                    rel: RelFlags::default(),
                }
                .into(),
            ]
        );
    }
}
