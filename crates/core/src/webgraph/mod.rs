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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

use itertools::Itertools;
use rand::seq::{IteratorRandom, SliceRandom};
use rayon::prelude::*;
use uuid::Uuid;

use self::id_node_db::Id2NodeDb;
use self::segment::Segment;
use crate::executor::Executor;

use crate::Result;
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
mod merge;
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

#[derive(
    Debug, Clone, Copy, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub enum EdgeLimit {
    Unlimited,
    Limit(usize),
}

impl EdgeLimit {
    pub fn apply<'a, T>(
        &self,
        it: impl Iterator<Item = T> + 'a,
    ) -> Box<dyn Iterator<Item = T> + 'a> {
        match self {
            EdgeLimit::Unlimited => Box::new(it),
            EdgeLimit::Limit(limit) => Box::new(it.take(*limit)),
        }
    }
}

pub struct Webgraph {
    path: String,
    segments: Vec<Segment>,
    executor: Arc<Executor>,
    id2node: Id2NodeDb,
    meta: Meta,
}

impl Webgraph {
    pub fn builder<P: AsRef<Path>>(path: P) -> WebgraphBuilder {
        WebgraphBuilder::new(path)
    }

    pub fn path(&self) -> PathBuf {
        PathBuf::from(&self.path)
    }

    fn meta<P: AsRef<Path>>(path: P) -> Meta {
        let meta_path = path.as_ref().join("metadata.json");
        Meta::open(meta_path)
    }

    fn save_metadata(&mut self) {
        let path = Path::new(&self.path).join("metadata.json");
        self.meta.save(path);
    }

    fn open<P: AsRef<Path>>(path: P, executor: Executor) -> Self {
        fs::create_dir_all(&path).unwrap();
        let meta = Self::meta(&path);

        fs::create_dir_all(path.as_ref().join("segments")).unwrap();

        let mut segments = Vec::new();
        for segment in &meta.comitted_segments {
            segments.push(Segment::open(
                path.as_ref().join("segments"),
                segment.clone(),
            ));
        }

        Self {
            path: path.as_ref().as_os_str().to_str().unwrap().to_string(),
            segments,
            executor: Arc::new(executor),
            id2node: Id2NodeDb::open(path.as_ref().join("id2node")),
            meta,
        }
    }

    pub fn merge(&mut self, other: Webgraph) -> io::Result<()> {
        let other_folder = other.path.clone();
        self.id2node.merge(other.id2node);
        self.id2node.flush();

        for segment in other.segments {
            let id = segment.id();
            let new_path = Path::new(&self.path).join("segments");
            std::fs::rename(segment.path(), new_path.join(segment.id())).unwrap();

            self.meta.comitted_segments.push(segment.id());
            drop(segment);
            self.segments.push(Segment::open(new_path, id));
        }

        fs::remove_dir_all(other_folder)?;

        self.save_metadata();

        Ok(())
    }

    pub fn merge_all_segments(&mut self, compression: Compression) -> Result<()> {
        let segments = std::mem::take(&mut self.segments);

        let id = Uuid::new_v4().to_string();
        let path = Path::new(&self.path).join("segments");

        Segment::merge(segments, compression, &path, id.clone())?;
        let new_segment = Segment::open(path, id.clone());

        self.segments.push(new_segment);
        self.meta.comitted_segments = vec![id];

        self.save_metadata();

        Ok(())
    }

    pub fn optimize_read(&mut self) {
        self.executor
            .map(|s| s.optimize_read(), self.segments.iter_mut())
            .unwrap();

        self.id2node.optimize_read();
    }

    pub fn ingoing_edges(&self, node: Node, limit: EdgeLimit) -> Vec<FullEdge> {
        self.ingoing_edges_by_id(&node.id(), limit)
    }

    pub fn ingoing_edges_by_id(&self, node_id: &NodeID, limit: EdgeLimit) -> Vec<FullEdge> {
        let dedup = |edges: &mut Vec<SegmentEdge<String>>| {
            edges.sort_by_key(|e| e.from.node());
            edges.dedup_by_key(|e| e.from.node());
        };

        let mut edges = self.inner_edges(
            |segment| segment.ingoing_edges_with_label(node_id, &limit),
            dedup,
        );
        edges.sort_by(|a, b| a.from.host_rank().cmp(&b.from.host_rank()));

        limit
            .apply(edges.into_iter())
            .map(|e| FullEdge {
                from: self.id2node(&e.from.node()).unwrap(),
                to: self.id2node(&e.to.node()).unwrap(),
                label: e.label,
            })
            .collect()
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

    pub fn raw_ingoing_edges(&self, node: &NodeID, limit: EdgeLimit) -> Vec<Edge<()>> {
        let dedup = |edges: &mut Vec<SegmentEdge<()>>| {
            edges.sort_by_key(|e| e.from.node());
            edges.dedup_by_key(|e| e.from.node());
        };

        let mut edges = self.inner_edges(|segment| segment.ingoing_edges(node, &limit), dedup);
        edges.sort_by(|a, b| a.from.host_rank().cmp(&b.from.host_rank()));

        limit
            .apply(edges.into_iter())
            .map(|e| Edge {
                from: e.from,
                to: e.to,
                label: e.label,
                rel: e.rel,
            })
            .collect()
    }

    pub fn raw_ingoing_edges_with_labels(
        &self,
        node: &NodeID,
        limit: EdgeLimit,
    ) -> Vec<Edge<String>> {
        let dedup = |edges: &mut Vec<SegmentEdge<String>>| {
            edges.sort_by_key(|e| e.from.node());
            edges.dedup_by_key(|e| e.from.node());
        };

        let mut edges = self.inner_edges(
            |segment| segment.ingoing_edges_with_label(node, &limit),
            dedup,
        );
        edges.sort_by(|a, b| a.from.host_rank().cmp(&b.from.host_rank()));

        limit
            .apply(edges.into_iter())
            .map(|e| Edge {
                from: e.from,
                to: e.to,
                label: e.label,
                rel: e.rel,
            })
            .collect()
    }

    pub fn raw_outgoing_edges_with_labels(
        &self,
        node: &NodeID,
        limit: EdgeLimit,
    ) -> Vec<Edge<String>> {
        let dedup = |edges: &mut Vec<SegmentEdge<String>>| {
            edges.sort_by_key(|e| e.to.node());
            edges.dedup_by_key(|e| e.to.node());
        };

        let mut edges = self.inner_edges(
            |segment| segment.outgoing_edges_with_label(node, &limit),
            dedup,
        );

        edges.sort_by(|a, b| a.to.host_rank().cmp(&b.to.host_rank()));

        limit
            .apply(edges.into_iter())
            .map(|e| Edge {
                from: e.from,
                to: e.to,
                label: e.label,
                rel: e.rel,
            })
            .collect()
    }

    pub fn out_degree_upper_bound(&self, node: &NodeID) -> u64 {
        self.segments
            .iter()
            .map(|segment| segment.out_degree(node))
            .sum()
    }

    pub fn in_degree_upper_bound(&self, node: &NodeID) -> u64 {
        self.segments
            .iter()
            .map(|segment| segment.in_degree(node))
            .sum()
    }

    pub fn outgoing_edges(&self, node: Node, limit: EdgeLimit) -> Vec<FullEdge> {
        let dedup = |edges: &mut Vec<SegmentEdge<String>>| {
            edges.sort_by_key(|e| e.to.node());
            edges.dedup_by_key(|e| e.to.node());
        };

        let mut edges = self.inner_edges(
            |segment| segment.outgoing_edges_with_label(&node.id(), &limit),
            dedup,
        );
        edges.sort_by(|a, b| a.to.host_rank().cmp(&b.to.host_rank()));

        limit
            .apply(edges.into_iter())
            .map(|e| FullEdge {
                from: self.id2node(&e.from.node()).unwrap(),
                to: self.id2node(&e.to.node()).unwrap(),
                label: e.label,
            })
            .collect()
    }

    pub fn raw_outgoing_edges(&self, node: &NodeID, limit: EdgeLimit) -> Vec<Edge<()>> {
        let dedup = |edges: &mut Vec<SegmentEdge<()>>| {
            edges.sort_by_key(|e| e.to.node());
            edges.dedup_by_key(|e| e.to.node());
        };

        let mut edges = self.inner_edges(|segment| segment.outgoing_edges(node, &limit), dedup);
        edges.sort_by(|a, b| a.to.host_rank().cmp(&b.to.host_rank()));

        limit
            .apply(edges.into_iter())
            .map(|e| Edge {
                from: e.from,
                to: e.to,
                label: e.label,
                rel: e.rel,
            })
            .collect()
    }

    fn inner_edges<F1, F2, L>(&self, loader: F1, dedup: F2) -> Vec<SegmentEdge<L>>
    where
        L: EdgeLabel,
        F1: Sized + Sync + Fn(&Segment) -> Vec<SegmentEdge<L>>,
        F2: Fn(&mut Vec<SegmentEdge<L>>),
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
            .map(|e| e.from.node())
            .unique()
            .choose_multiple(&mut rng, num);
        nodes.shuffle(&mut rng);
        nodes
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
        self.segments
            .iter()
            .flat_map(|segment| segment.edges().map(|e| e.into()))
    }

    pub fn par_edges(&self) -> impl ParallelIterator<Item = Edge<()>> + '_ {
        self.segments
            .par_iter()
            .flat_map(|segment| segment.edges().par_bridge().map(|e| e.into()))
    }
}

#[cfg(test)]
pub mod tests {
    use crate::webpage::html::links::RelFlags;

    use super::*;
    use file_store::temp::TempDir;
    use proptest::prelude::*;

    pub fn test_edges() -> Vec<(Node, Node, String)> {
        vec![
            (Node::from("A"), Node::from("B"), String::new()),
            (Node::from("B"), Node::from("C"), String::new()),
            (Node::from("A"), Node::from("C"), String::new()),
            (Node::from("C"), Node::from("A"), String::new()),
            (Node::from("D"), Node::from("C"), String::new()),
        ]
    }

    pub fn test_graph() -> (Webgraph, TempDir) {
        //     ┌-----┐
        //     │     │
        // ┌───A◄─┐  │
        // │      │  │
        // ▼      │  │
        // B─────►C◄-┘
        //        ▲
        //        │
        //        │
        //        D

        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut graph = WebgraphWriter::new(
            &temp_dir,
            Executor::single_thread(),
            Compression::default(),
            None,
        );

        for (from, to, label) in test_edges() {
            graph.insert(from, to, label, RelFlags::default());
        }

        graph.commit();

        (graph.finalize(), temp_dir)
    }

    #[test]
    fn distance_calculation() {
        let (graph, _temp_dir) = test_graph();

        let distances = graph.distances(Node::from("D"));

        assert_eq!(distances.get(&Node::from("C")), Some(&1));
        assert_eq!(distances.get(&Node::from("A")), Some(&2));
        assert_eq!(distances.get(&Node::from("B")), Some(&3));
    }

    #[test]
    fn nonexisting_node() {
        let (graph, _temp_dir) = test_graph();
        assert_eq!(graph.distances(Node::from("E")).len(), 0);
        assert_eq!(graph.reversed_distances(Node::from("E")).len(), 0);
    }

    #[test]
    fn reversed_distance_calculation() {
        let (graph, _temp_dir) = test_graph();

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
    fn merge_path() {
        let mut graphs = Vec::new();
        let temp_dir = crate::gen_temp_dir().unwrap();
        for (i, (from, to, label)) in (0..).zip(&[
            (Node::from("A"), Node::from("B"), String::new()),
            (Node::from("B"), Node::from("C"), String::new()),
            (Node::from("C"), Node::from("D"), String::new()),
            (Node::from("D"), Node::from("E"), String::new()),
            (Node::from("E"), Node::from("F"), String::new()),
            (Node::from("F"), Node::from("G"), String::new()),
            (Node::from("G"), Node::from("H"), String::new()),
        ]) {
            let mut wrt = WebgraphWriter::new(
                &temp_dir.as_ref().join(format!("test_{}", i)),
                Executor::single_thread(),
                Compression::default(),
                None,
            );
            wrt.insert(from.clone(), to.clone(), label.clone(), RelFlags::default());
            graphs.push(wrt.finalize());
        }

        let mut graph = graphs.pop().unwrap();

        for other in graphs {
            graph.merge(other).unwrap();
        }

        graph.merge_all_segments(Compression::default()).unwrap();
        graph.optimize_read();

        assert_eq!(
            graph.distances(Node::from("A")).get(&Node::from("H")),
            Some(&7)
        );

        assert_eq!(
            graph
                .reversed_distances(Node::from("H"))
                .get(&Node::from("A")),
            Some(&7)
        );
    }

    #[test]
    fn merge_simple() {
        let mut graphs = Vec::new();
        let temp_dir = crate::gen_temp_dir().unwrap();
        for (i, (from, to, label)) in (0..).zip(&test_edges()) {
            let mut wrt = WebgraphWriter::new(
                &temp_dir.as_ref().join(format!("test_{}", i)),
                Executor::single_thread(),
                Compression::default(),
                None,
            );
            wrt.insert(from.clone(), to.clone(), label.clone(), RelFlags::default());
            graphs.push(wrt.finalize());
        }

        let mut graph = graphs.pop().unwrap();

        for other in graphs {
            graph.merge(other).unwrap();
        }

        graph.merge_all_segments(Compression::default()).unwrap();
        graph.optimize_read();

        let mut res = graph.outgoing_edges(Node::from("A"), EdgeLimit::Unlimited);
        res.sort_by(|a, b| a.to.cmp(&b.to));
        assert_eq!(res.len(), 2);

        assert_eq!(res[0].to, Node::from("B"));
        assert_eq!(res[1].to, Node::from("C"));

        let mut res = graph.outgoing_edges(Node::from("B"), EdgeLimit::Unlimited);
        res.sort_by(|a, b| a.to.cmp(&b.to));

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].to, Node::from("C"));

        let mut res = graph.outgoing_edges(Node::from("C"), EdgeLimit::Unlimited);
        res.sort_by(|a, b| a.to.cmp(&b.to));

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].to, Node::from("A"));

        let mut res = graph.outgoing_edges(Node::from("D"), EdgeLimit::Unlimited);
        res.sort_by(|a, b| a.to.cmp(&b.to));

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].to, Node::from("C"));

        let mut res = graph.ingoing_edges(Node::from("A"), EdgeLimit::Unlimited);
        res.sort_by(|a, b| a.from.cmp(&b.from));

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].from, Node::from("C"));

        let mut res = graph.ingoing_edges(Node::from("B"), EdgeLimit::Unlimited);
        res.sort_by(|a, b| a.from.cmp(&b.from));

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].from, Node::from("A"));

        let mut res = graph.ingoing_edges(Node::from("C"), EdgeLimit::Unlimited);
        res.sort_by(|a, b| a.from.cmp(&b.from));

        assert_eq!(res.len(), 3);
        assert_eq!(res[0].from, Node::from("A"));
        assert_eq!(res[1].from, Node::from("B"));
        assert_eq!(res[2].from, Node::from("D"));
    }

    #[test]
    fn merge_cycle() {
        let mut graphs = Vec::new();
        let temp_dir = crate::gen_temp_dir().unwrap();
        for (i, (from, to, label)) in (0..).zip(&[
            (Node::from("A"), Node::from("B"), String::new()),
            (Node::from("B"), Node::from("B"), String::new()),
            (Node::from("B"), Node::from("C"), String::new()),
            (Node::from("C"), Node::from("A"), String::new()),
        ]) {
            let mut wrt = WebgraphWriter::new(
                &temp_dir.as_ref().join(format!("test_{}", i)),
                Executor::single_thread(),
                Compression::default(),
                None,
            );
            wrt.insert(from.clone(), to.clone(), label.clone(), RelFlags::default());
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

        graph.merge_all_segments(Compression::default()).unwrap();

        assert_eq!(
            graph.distances(Node::from("A")).get(&Node::from("C")),
            Some(&2)
        );

        assert_eq!(
            graph
                .outgoing_edges(Node::from("A"), EdgeLimit::Unlimited)
                .len(),
            1
        );
        assert_eq!(
            graph
                .outgoing_edges(Node::from("B"), EdgeLimit::Unlimited)
                .len(),
            1
        );
        assert_eq!(
            graph
                .outgoing_edges(Node::from("C"), EdgeLimit::Unlimited)
                .len(),
            1
        );
    }

    #[test]
    fn merge_star() {
        let mut graphs = Vec::new();
        let temp_dir = crate::gen_temp_dir().unwrap();
        for (i, (from, to, label)) in (0..).zip(&[
            (Node::from("A"), Node::from("B"), String::new()),
            (Node::from("A"), Node::from("C"), String::new()),
            (Node::from("A"), Node::from("D"), String::new()),
            (Node::from("A"), Node::from("E"), String::new()),
        ]) {
            let mut wrt = WebgraphWriter::new(
                &temp_dir.as_ref().join(format!("test_{}", i)),
                Executor::single_thread(),
                Compression::default(),
                None,
            );
            wrt.insert(from.clone(), to.clone(), label.clone(), RelFlags::default());
            graphs.push(wrt.finalize());
        }

        let mut graph = graphs.pop().unwrap();

        for other in graphs {
            graph.merge(other).unwrap();
        }

        graph.merge_all_segments(Compression::default()).unwrap();
        graph.optimize_read();

        assert_eq!(
            graph
                .outgoing_edges(Node::from("A"), EdgeLimit::Unlimited)
                .len(),
            4
        );

        assert_eq!(
            graph
                .ingoing_edges(Node::from("B"), EdgeLimit::Unlimited)
                .len(),
            1
        );

        assert_eq!(
            graph
                .ingoing_edges(Node::from("C"), EdgeLimit::Unlimited)
                .len(),
            1
        );

        assert_eq!(
            graph
                .ingoing_edges(Node::from("D"), EdgeLimit::Unlimited)
                .len(),
            1
        );
    }

    #[test]
    fn merge_reverse_star() {
        let mut graphs = Vec::new();
        let temp_dir = crate::gen_temp_dir().unwrap();
        for (i, (from, to, label)) in (0..).zip(&[
            (Node::from("B"), Node::from("A"), String::new()),
            (Node::from("C"), Node::from("A"), String::new()),
            (Node::from("D"), Node::from("A"), String::new()),
            (Node::from("E"), Node::from("A"), String::new()),
        ]) {
            let mut wrt = WebgraphWriter::new(
                &temp_dir.as_ref().join(format!("test_{}", i)),
                Executor::single_thread(),
                Compression::default(),
                None,
            );
            wrt.insert(from.clone(), to.clone(), label.clone(), RelFlags::default());
            graphs.push(wrt.finalize());
        }

        let mut graph = graphs.pop().unwrap();

        for other in graphs {
            graph.merge(other).unwrap();
        }

        graph.merge_all_segments(Compression::default()).unwrap();
        graph.optimize_read();

        assert_eq!(
            graph
                .ingoing_edges(Node::from("A"), EdgeLimit::Unlimited)
                .len(),
            4
        );

        assert_eq!(
            graph
                .outgoing_edges(Node::from("B"), EdgeLimit::Unlimited)
                .len(),
            1
        );

        assert_eq!(
            graph
                .outgoing_edges(Node::from("C"), EdgeLimit::Unlimited)
                .len(),
            1
        );

        assert_eq!(
            graph
                .outgoing_edges(Node::from("D"), EdgeLimit::Unlimited)
                .len(),
            1
        );
    }

    proptest! {
        #[test]
        fn prop_merge(
            nodes in
            proptest::collection::vec(
                ("[a-z]", "[a-z]"), 0..100
            )
        ) {
            let mut graphs = Vec::new();
            let temp_dir = crate::gen_temp_dir().unwrap();
            let mut wrt = WebgraphWriter::new(
                temp_dir.as_ref().join(uuid::Uuid::new_v4().to_string()),
                Executor::single_thread(),
                Compression::default(),
                None,
            );
            for (from, to) in nodes.clone() {
                wrt.insert(Node::new_for_test(from.as_str()), Node::new_for_test(to.as_str()), String::new(), RelFlags::default());

                if rand::random::<usize>() % 10 == 0 {
                    graphs.push(wrt.finalize());
                    wrt = WebgraphWriter::new(
                        temp_dir.as_ref().join(uuid::Uuid::new_v4().to_string()),
                        Executor::single_thread(),
                        Compression::default(),
                        None,
                    );
                }
            }

            if graphs.is_empty() {
                return Ok(());
            }

            graphs.push(wrt.finalize());

            let mut graph = graphs.pop().unwrap();


            for other in graphs {
                graph.merge(other).unwrap();
            }

            graph.merge_all_segments(Compression::default()).unwrap();

            for (from, to) in nodes {
                if from == to {
                    continue;
                }

                let from = Node::new_for_test(from.as_str());
                let to = Node::new_for_test(to.as_str());

                let outgoing = graph.outgoing_edges(from.clone(), EdgeLimit::Unlimited);
                let ingoing = graph.ingoing_edges(to.clone(), EdgeLimit::Unlimited);

                prop_assert!(outgoing.iter().any(|e| e.to == to));
                prop_assert!(ingoing.iter().any(|e| e.from == from));
            }
        }
    }

    fn proptest_case(nodes: &[(&str, &str)]) {
        let mut graphs = Vec::new();
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut wrt = WebgraphWriter::new(
            temp_dir.as_ref().join(uuid::Uuid::new_v4().to_string()),
            Executor::single_thread(),
            Compression::default(),
            None,
        );

        for (i, (from, to)) in nodes.iter().enumerate() {
            wrt.insert(
                Node::new_for_test(from),
                Node::new_for_test(to),
                String::new(),
                RelFlags::default(),
            );

            if i % 2 == 0 {
                graphs.push(wrt.finalize());
                wrt = WebgraphWriter::new(
                    temp_dir.as_ref().join(uuid::Uuid::new_v4().to_string()),
                    Executor::single_thread(),
                    Compression::default(),
                    None,
                );
            }
        }

        graphs.push(wrt.finalize());

        let mut graph = graphs.pop().unwrap();

        for other in graphs {
            graph.merge(other).unwrap();
        }

        graph.merge_all_segments(Compression::default()).unwrap();

        for (from, to) in nodes {
            if from == to {
                continue;
            }

            let from = Node::new_for_test(from);
            let to = Node::new_for_test(to);

            let outgoing = graph.outgoing_edges(from.clone(), EdgeLimit::Unlimited);
            let ingoing = graph.ingoing_edges(to.clone(), EdgeLimit::Unlimited);

            assert!(outgoing.iter().any(|e| e.to == to));
            assert!(ingoing.iter().any(|e| e.from == from));
        }
    }

    #[test]
    fn merge_proptest_case1() {
        let nodes = [("k", "d"), ("k", "t"), ("y", "m")];
        proptest_case(&nodes);
    }

    #[test]
    fn merge_proptest_case2() {
        let nodes = [("i", "k"), ("k", "g"), ("y", "m"), ("q", "r"), ("e", "g")];

        proptest_case(&nodes);
    }

    #[test]
    fn merge_proptest_case3() {
        let nodes = [("h", "c"), ("r", "r")];

        proptest_case(&nodes);
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
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut writer = WebgraphWriter::new(
            temp_dir.as_ref().join(uuid::Uuid::new_v4().to_string()),
            Executor::single_thread(),
            Compression::default(),
            None,
        );

        writer.insert(
            Node::from("A"),
            Node::from("B"),
            "a".repeat(MAX_LABEL_LENGTH + 1),
            RelFlags::default(),
        );

        let graph = writer.finalize();

        assert_eq!(graph.segments.len(), 1);
        assert_eq!(
            graph.outgoing_edges(Node::from("A"), EdgeLimit::Unlimited)[0].label,
            "a".repeat(MAX_LABEL_LENGTH)
        );
    }

    #[test]
    fn test_edge_limits() {
        let (graph, temp_dir) = test_graph();

        assert_eq!(
            graph
                .outgoing_edges(Node::from("A"), EdgeLimit::Unlimited)
                .len(),
            2
        );

        assert_eq!(
            graph
                .outgoing_edges(Node::from("A"), EdgeLimit::Limit(1))
                .len(),
            1
        );

        let mut graphs = Vec::new();
        for (from, to, label) in &[
            (Node::from("A"), Node::from("B"), String::new()),
            (Node::from("A"), Node::from("C"), String::new()),
        ] {
            let mut wrt = WebgraphWriter::new(
                temp_dir.as_ref().join(uuid::Uuid::new_v4().to_string()),
                Executor::single_thread(),
                Compression::default(),
                None,
            );
            wrt.insert(from.clone(), to.clone(), label.clone(), RelFlags::default());
            graphs.push(wrt.finalize());
        }

        let mut graph = graphs.pop().unwrap();

        for other in graphs {
            graph.merge(other).unwrap();
        }

        assert_eq!(
            graph
                .outgoing_edges(Node::from("A"), EdgeLimit::Unlimited)
                .len(),
            2
        );

        assert_eq!(
            graph
                .outgoing_edges(Node::from("A"), EdgeLimit::Limit(1))
                .len(),
            1
        );

        graph.optimize_read();

        assert_eq!(
            graph
                .outgoing_edges(Node::from("A"), EdgeLimit::Unlimited)
                .len(),
            2
        );

        assert_eq!(
            graph
                .outgoing_edges(Node::from("A"), EdgeLimit::Limit(1))
                .len(),
            1
        );
    }

    #[test]
    fn test_node_normalized() {
        let n = Node::from("http://www.example.com/abc");
        assert_eq!(n.as_str(), "example.com/abc");

        let n = Node::from("http://www.example.com/abc#123");
        assert_eq!(n.as_str(), "example.com/abc");
    }

    #[test]
    fn test_rel_flags() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut writer = WebgraphWriter::new(
            &temp_dir,
            Executor::single_thread(),
            Compression::default(),
            None,
        );

        writer.insert(
            Node::from("A"),
            Node::from("B"),
            String::new(),
            RelFlags::IS_IN_FOOTER | RelFlags::TAG,
        );

        let graph = writer.finalize();

        assert_eq!(
            graph.raw_outgoing_edges(&Node::from("A").id(), EdgeLimit::Unlimited)[0].rel,
            RelFlags::IS_IN_FOOTER | RelFlags::TAG,
        );
    }
}
