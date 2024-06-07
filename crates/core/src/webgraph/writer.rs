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
// along with this program.  If not, see <https://www.gnu.org/licenses

use std::{fs, path::Path, sync::Arc};

use crate::executor::Executor;

use super::{
    id_node_db::Id2NodeDb, segment::SegmentWriter, Compression, FullNodeID, InsertableEdge, Meta,
    Node, NodeID, Webgraph, MAX_LABEL_LENGTH,
};

pub struct WebgraphWriter {
    pub path: String,
    segment: SegmentWriter,
    id2node: Id2NodeDb,
    executor: Executor,
    meta: Meta,
}

impl WebgraphWriter {
    fn meta<P: AsRef<Path>>(path: P) -> Meta {
        let meta_path = path.as_ref().join("metadata.json");
        Meta::open(meta_path)
    }

    fn save_metadata(&mut self) {
        let path = Path::new(&self.path).join("metadata.json");
        self.meta.save(path);
    }

    pub fn new<P: AsRef<Path>>(
        path: P,
        executor: Executor,
        compression: Compression,
        host_centrality_rank_store: Option<Arc<speedy_kv::Db<NodeID, u64>>>,
    ) -> Self {
        fs::create_dir_all(&path).unwrap();
        let mut meta = Self::meta(&path);
        meta.comitted_segments.clear();

        fs::create_dir_all(path.as_ref().join("segments")).unwrap();

        let id = uuid::Uuid::new_v4().to_string();
        let segment = SegmentWriter::open(
            path.as_ref().join("segments"),
            id.clone(),
            compression,
            host_centrality_rank_store,
        );

        meta.comitted_segments.push(id);

        Self {
            path: path.as_ref().as_os_str().to_str().unwrap().to_string(),
            segment,
            id2node: Id2NodeDb::open(path.as_ref().join("id2node")),
            executor,
            meta,
        }
    }

    pub fn id2node(&self, id: &NodeID) -> Option<Node> {
        self.id2node.get(id)
    }

    fn id_or_assign(&mut self, node: Node) -> FullNodeID {
        let id = FullNodeID::from(node.clone());

        self.id2node.put(&id.id, &node);

        id
    }

    pub fn insert(&mut self, from: Node, to: Node, label: String) {
        if from == to {
            return;
        }

        let (from_id, to_id) = (
            self.id_or_assign(from.clone()),
            self.id_or_assign(to.clone()),
        );

        let edge = InsertableEdge {
            from: from_id,
            to: to_id,
            label: label.chars().take(MAX_LABEL_LENGTH).collect(),
        };

        self.segment.insert(edge);
    }

    pub fn commit(&mut self) {
        self.save_metadata();
        self.id2node.flush();
    }

    pub fn finalize(mut self) -> Webgraph {
        self.commit();

        Webgraph {
            path: self.path,
            segments: vec![self.segment.finalize()],
            executor: self.executor.into(),
            id2node: self.id2node,
            meta: self.meta,
        }
    }
}
