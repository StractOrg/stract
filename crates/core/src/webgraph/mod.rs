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
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::{fs, io};

use itertools::Itertools;
use rand::seq::SliceRandom;
use rayon::prelude::*;

use self::id_node_db::Id2NodeDb;
use self::segment::Segment;
use crate::executor::Executor;

pub use builder::WebgraphBuilder;
pub use compression::Compression;
pub use edge::*;
pub use node::*;
pub use shortest_path::ShortestPaths;
pub use writer::WebgraphWriter;

mod builder;
pub mod centrality;
mod compression;
mod edge;
mod id_node_db;
mod node;
pub mod remote;
mod segment;
mod shortest_path;
mod store;
mod store_writer;
mod writer;

type SegmentID = String;

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Default)]
struct Meta {
    comitted_segments: Vec<SegmentID>,
}

impl Meta {
    fn open<P: AsRef<Path>>(path: P) -> Self {
        let mut reader = BufReader::new(
            File::options()
                .create(true)
                .truncate(false)
                .write(true)
                .read(true)
                .open(path)
                .unwrap(),
        );
        let mut buf = String::new();
        reader.read_to_string(&mut buf).unwrap();
        serde_json::from_str(&buf).unwrap_or_default()
    }

    fn save<P: AsRef<Path>>(&self, path: P) {
        let mut writer = BufWriter::new(
            File::options()
                .create(true)
                .write(true)
                .read(true)
                .truncate(true)
                .open(path)
                .unwrap(),
        );

        let json = serde_json::to_string_pretty(&self).unwrap();
        writer.write_all(json.as_bytes()).unwrap();
    }
}

pub struct Webgraph {
    path: String,
    segments: Vec<Segment>,
    executor: Arc<Executor>,
    id2node: Id2NodeDb,
    meta: Meta,
    compression: Compression,
}

impl Webgraph {
    pub fn builder<P: AsRef<Path>>(path: P) -> WebgraphBuilder {
        WebgraphBuilder::new(path)
    }

    fn meta<P: AsRef<Path>>(path: P) -> Meta {
        let meta_path = path.as_ref().join("metadata.json");
        Meta::open(meta_path)
    }

    fn save_metadata(&mut self) {
        let path = Path::new(&self.path).join("metadata.json");
        self.meta.save(path);
    }

    fn open<P: AsRef<Path>>(path: P, executor: Executor, compression: Compression) -> Self {
        fs::create_dir_all(&path).unwrap();
        let meta = Self::meta(&path);

        fs::create_dir_all(path.as_ref().join("segments")).unwrap();

        let mut segments = Vec::new();
        for segment in &meta.comitted_segments {
            segments.push(Segment::open(
                path.as_ref().join("segments"),
                segment.clone(),
                compression,
            ));
        }

        Self {
            path: path.as_ref().as_os_str().to_str().unwrap().to_string(),
            segments,
            executor: Arc::new(executor),
            id2node: Id2NodeDb::open(path.as_ref().join("id2node")),
            meta,
            compression,
        }
    }

    pub fn merge(&mut self, other: Webgraph) -> io::Result<()> {
        let other_folder = other.path.clone();
        self.id2node.merge(other.id2node);
        self.id2node.flush();

        for segment in other.segments {
            let id = segment.id();
            let new_path = Path::new(&self.path).join("segments");
            std::fs::rename(segment.path(), &new_path.join(segment.id())).unwrap();

            self.meta.comitted_segments.push(segment.id());
            drop(segment);
            self.segments
                .push(Segment::open(new_path, id, self.compression));
        }

        fs::remove_dir_all(other_folder)?;

        self.save_metadata();

        Ok(())
    }

    pub fn optimize_read(&mut self) {
        self.executor
            .map(|s| s.optimize_read(), self.segments.iter_mut())
            .unwrap();

        self.id2node.optimize_read();
    }

    pub fn ingoing_edges(&self, node: Node) -> Vec<FullEdge> {
        let dedup = |edges: &mut Vec<Edge<String>>| {
            edges.sort_by_key(|e| e.from);
            edges.dedup_by_key(|e| e.from);
        };

        self.inner_edges(
            |segment| segment.ingoing_edges_with_label(&node.id()),
            dedup,
        )
        .into_iter()
        .map(|e| FullEdge {
            from: self.id2node(&e.from).unwrap(),
            to: self.id2node(&e.to).unwrap(),
            label: e.label,
        })
        .collect()
    }

    pub fn raw_ingoing_edges_by_host(&self, host_node: &NodeID) -> Vec<Edge<()>> {
        let dedup = |edges: &mut Vec<Edge<()>>| {
            edges.sort_by_key(|e| e.from);
            edges.dedup_by_key(|e| e.from);
        };

        self.inner_edges(|segment| segment.ingoing_edges_by_host(host_node), dedup)
    }

    pub fn pages_by_host(&self, host_node: &NodeID) -> Vec<NodeID> {
        let mut pages: Vec<_> = self
            .executor
            .map(
                |segment| segment.pages_by_host(host_node),
                self.segments.iter(),
            )
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        pages.sort();
        pages.dedup();

        pages
    }

    pub fn raw_ingoing_edges(&self, node: &NodeID) -> Vec<Edge<()>> {
        let dedup = |edges: &mut Vec<Edge<()>>| {
            edges.sort_by_key(|e| e.from);
            edges.dedup_by_key(|e| e.from);
        };

        self.inner_edges(|segment| segment.ingoing_edges(node), dedup)
    }

    pub fn raw_ingoing_edges_with_labels(&self, node: &NodeID) -> Vec<Edge<String>> {
        let dedup = |edges: &mut Vec<Edge<String>>| {
            edges.sort_by_key(|e| e.from);
            edges.dedup_by_key(|e| e.from);
        };

        self.inner_edges(|segment| segment.ingoing_edges_with_label(node), dedup)
    }

    pub fn raw_outgoing_edges_with_labels(&self, node: &NodeID) -> Vec<Edge<String>> {
        let dedup = |edges: &mut Vec<Edge<String>>| {
            edges.sort_by_key(|e| e.from);
            edges.dedup_by_key(|e| e.from);
        };

        self.inner_edges(|segment| segment.outgoing_edges_with_label(node), dedup)
    }

    pub fn outgoing_edges(&self, node: Node) -> Vec<FullEdge> {
        let dedup = |edges: &mut Vec<Edge<String>>| {
            edges.sort_by_key(|e| e.to);
            edges.dedup_by_key(|e| e.to);
        };

        self.inner_edges(
            |segment| segment.outgoing_edges_with_label(&node.id()),
            dedup,
        )
        .into_iter()
        .map(|e| FullEdge {
            from: self.id2node(&e.from).unwrap(),
            to: self.id2node(&e.to).unwrap(),
            label: e.label,
        })
        .collect()
    }

    pub fn raw_outgoing_edges(&self, node: &NodeID) -> Vec<Edge<()>> {
        let dedup = |edges: &mut Vec<Edge<()>>| {
            edges.sort_by_key(|e| e.to);
            edges.dedup_by_key(|e| e.to);
        };

        self.inner_edges(|segment| segment.outgoing_edges(node), dedup)
    }

    fn inner_edges<F1, F2, L>(&self, loader: F1, dedup: F2) -> Vec<Edge<L>>
    where
        L: EdgeLabel,
        F1: Sized + Sync + Fn(&Segment) -> Vec<Edge<L>>,
        F2: Fn(&mut Vec<Edge<L>>),
    {
        let mut edges: Vec<_> = self
            .executor
            .map(loader, self.segments.iter())
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        dedup(&mut edges);

        edges
    }

    pub fn id2node(&self, id: &NodeID) -> Option<Node> {
        self.id2node.get(id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = NodeID> + '_ {
        self.id2node.keys()
    }

    pub fn random_nodes_with_outgoing(&self, num: usize) -> Vec<NodeID> {
        let mut rng = rand::thread_rng();
        let mut nodes = self
            .edges()
            .map(|e| e.from)
            .unique()
            .take(num)
            .collect::<Vec<_>>();
        nodes.shuffle(&mut rng);
        nodes.into_iter().take(num).collect()
    }

    pub fn par_nodes(&self) -> impl ParallelIterator<Item = NodeID> + '_ {
        self.id2node.keys().par_bridge()
    }

    pub fn node_ids(&self) -> impl Iterator<Item = (Node, NodeID)> + '_ {
        self.id2node.iter().map(|(id, node)| (node, id))
    }

    pub fn estimate_num_nodes(&self) -> usize {
        self.id2node.estimate_num_keys()
    }

    /// Iterate all edges in the graph at least once.
    /// Some edges may be returned multiple times.
    /// This happens if they are present in more than one segment.
    pub fn edges(&self) -> impl Iterator<Item = Edge<()>> + '_ {
        self.segments.iter().flat_map(|segment| segment.edges())
    }

    pub fn par_edges(&self) -> impl ParallelIterator<Item = Edge<()>> + '_ {
        self.segments
            .par_iter()
            .flat_map(|segment| segment.edges().par_bridge())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    fn test_edges() -> Vec<(Node, Node, String)> {
        vec![
            (Node::from("A"), Node::from("B"), String::new()),
            (Node::from("B"), Node::from("C"), String::new()),
            (Node::from("A"), Node::from("C"), String::new()),
            (Node::from("C"), Node::from("A"), String::new()),
            (Node::from("D"), Node::from("C"), String::new()),
        ]
    }

    pub fn test_graph() -> Webgraph {
        //     ┌------┐
        //     │      │
        // ┌───A◄─┐  │
        // │       │  │
        // ▼      │  │
        // B─────►C◄┘
        //        ▲
        //        │
        //        │
        //        D

        let mut graph = WebgraphWriter::new(
            crate::gen_temp_path(),
            Executor::single_thread(),
            Compression::default(),
        );

        for (from, to, label) in test_edges() {
            graph.insert(from, to, label);
        }

        graph.commit();

        graph.finalize()
    }

    #[test]
    fn distance_calculation() {
        let graph = test_graph();

        let distances = graph.distances(Node::from("D"));

        assert_eq!(distances.get(&Node::from("C")), Some(&1));
        assert_eq!(distances.get(&Node::from("A")), Some(&2));
        assert_eq!(distances.get(&Node::from("B")), Some(&3));
    }

    #[test]
    fn nonexisting_node() {
        let graph = test_graph();
        assert_eq!(graph.distances(Node::from("E")).len(), 0);
        assert_eq!(graph.reversed_distances(Node::from("E")).len(), 0);
    }

    #[test]
    fn reversed_distance_calculation() {
        let graph = test_graph();

        let distances = graph.reversed_distances(Node::from("D"));

        assert_eq!(distances.get(&Node::from("C")), None);
        assert_eq!(distances.get(&Node::from("A")), None);
        assert_eq!(distances.get(&Node::from("B")), None);

        let distances = graph.reversed_distances(Node::from("A"));

        assert_eq!(distances.get(&Node::from("C")), Some(&1));
        assert_eq!(distances.get(&Node::from("D")), Some(&2));
        assert_eq!(distances.get(&Node::from("B")), Some(&2));
    }

    #[test]
    fn merge() {
        let mut graphs = Vec::new();
        for (from, to, label) in &[
            (Node::from("A"), Node::from("B"), String::new()),
            (Node::from("B"), Node::from("C"), String::new()),
            (Node::from("C"), Node::from("D"), String::new()),
            (Node::from("D"), Node::from("E"), String::new()),
            (Node::from("E"), Node::from("F"), String::new()),
            (Node::from("F"), Node::from("G"), String::new()),
            (Node::from("G"), Node::from("H"), String::new()),
        ] {
            let mut wrt = WebgraphWriter::new(
                crate::gen_temp_path(),
                Executor::single_thread(),
                Compression::default(),
            );
            wrt.insert(from.clone(), to.clone(), label.clone());
            graphs.push(wrt.finalize());
        }

        let mut graph = graphs.pop().unwrap();

        for other in graphs {
            graph.merge(other).unwrap();
        }

        assert_eq!(
            graph.distances(Node::from("A")).get(&Node::from("H")),
            Some(&7)
        );
    }
    #[test]
    fn merge_cycle() {
        let mut graphs = Vec::new();
        for (from, to, label) in &[
            (Node::from("A"), Node::from("B"), String::new()),
            (Node::from("B"), Node::from("B"), String::new()),
            (Node::from("B"), Node::from("C"), String::new()),
            (Node::from("C"), Node::from("A"), String::new()),
        ] {
            let mut wrt = WebgraphWriter::new(
                crate::gen_temp_path(),
                Executor::single_thread(),
                Compression::default(),
            );
            wrt.insert(from.clone(), to.clone(), label.clone());
            graphs.push(wrt.finalize());
        }

        let mut graph = graphs.pop().unwrap();

        for other in graphs {
            graph.merge(other).unwrap();
        }

        assert_eq!(
            graph.distances(Node::from("A")).get(&Node::from("C")),
            Some(&2)
        );
    }

    #[test]
    fn node_lowercase_name() {
        let n = Node::from("TEST".to_string());
        assert_eq!(n.as_str(), "test");
    }

    #[test]
    fn host_node_cleanup() {
        let n = Node::from("https://www.example.com?test").into_host();
        assert_eq!(n.as_str(), "example.com");
    }

    #[test]
    fn remove_protocol() {
        let n = Node::from("https://www.example.com/?test");

        assert_eq!(n.as_str(), "example.com/?test=");
    }

    #[test]
    fn cap_label_length() {
        let mut writer = WebgraphWriter::new(
            crate::gen_temp_path(),
            Executor::single_thread(),
            Compression::default(),
        );

        writer.insert(
            Node::from("A"),
            Node::from("B"),
            "a".repeat(MAX_LABEL_LENGTH + 1),
        );

        let graph = writer.finalize();

        assert_eq!(graph.segments.len(), 1);
        assert_eq!(
            graph.outgoing_edges(Node::from("A"))[0].label,
            "a".repeat(MAX_LABEL_LENGTH)
        );
    }

    #[test]
    fn edges_by_host() {
        let mut writer = WebgraphWriter::new(
            crate::gen_temp_path(),
            Executor::single_thread(),
            Compression::default(),
        );

        writer.insert(
            Node::from("http://a.com/first"),
            Node::from("http://b.com/first"),
            String::new(),
        );
        writer.insert(
            Node::from("http://c.com/first"),
            Node::from("http://b.com/second"),
            String::new(),
        );

        let graph = writer.finalize();

        let mut res = graph
            .raw_ingoing_edges_by_host(&Node::from("b.com").id())
            .into_iter()
            .map(|e| e.to)
            .map(|id| graph.id2node(&id).unwrap())
            .collect::<Vec<_>>();
        res.sort();

        assert_eq!(
            res,
            vec![
                Node::from("http://b.com/first"),
                Node::from("http://b.com/second")
            ]
        );
    }

    #[test]
    fn test_node_normalized() {
        let n = Node::from("http://www.example.com/abc");
        assert_eq!(n.as_str(), "example.com/abc");

        let n = Node::from("http://www.example.com/abc#123");
        assert_eq!(n.as_str(), "example.com/abc");
    }
}
