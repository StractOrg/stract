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
mod segment;

use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::{cmp, fs};

use itertools::Itertools;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::executor::Executor;
use crate::intmap;

pub mod centrality;
mod store;
use self::segment::StoredSegment;

pub const MAX_LABEL_LENGTH: usize = 1024;
const MAX_BATCH_SIZE: usize = 50_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeID(u128);

impl NodeID {
    pub fn bit_64(self) -> u64 {
        self.0 as u64
    }

    pub fn bit_128(self) -> u128 {
        self.0
    }
}

impl From<u128> for NodeID {
    fn from(val: u128) -> Self {
        NodeID(val)
    }
}

impl intmap::Key for NodeID {
    const BIG_PRIME: Self = NodeID(335579573203413586826293107669396558523);

    fn wrapping_mul(self, rhs: Self) -> Self {
        NodeID(self.0.wrapping_mul(rhs.0))
    }

    fn bit_and(self, rhs: Self) -> Self {
        NodeID(self.0 & rhs.0)
    }

    fn from_usize(val: usize) -> Self {
        NodeID(val as u128)
    }

    fn as_usize(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct Edge<L>
where
    L: Send + Sync,
{
    pub from: NodeID,
    pub to: NodeID,
    pub label: L,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct FullEdge {
    pub from: Node,
    pub to: Node,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
}

impl Node {
    pub fn into_host(self) -> Node {
        let url = if self.name.contains("://") {
            Url::parse(&self.name)
        } else {
            Url::parse(&("http://".to_string() + self.name.as_str()))
        };

        match url {
            Ok(url) => {
                let host = url.host_str().unwrap_or_default().to_string();
                Node { name: host }
            }
            Err(_) => Node {
                name: String::new(),
            },
        }
    }

    pub fn id(&self) -> NodeID {
        let digest = md5::compute(self.name.as_bytes());
        NodeID(u128::from_be_bytes(*digest))
    }
}

impl From<String> for Node {
    fn from(name: String) -> Self {
        let url = if name.contains("://") {
            Url::parse(&name).unwrap()
        } else {
            Url::parse(&("http://".to_string() + name.as_str())).unwrap()
        };

        Node::from(&url)
    }
}

impl From<&Url> for Node {
    fn from(url: &Url) -> Self {
        let normalized = normalize_url(url);
        Node { name: normalized }
    }
}

impl From<&str> for Node {
    fn from(name: &str) -> Self {
        name.to_string().into()
    }
}

impl From<Url> for Node {
    fn from(url: Url) -> Self {
        Self::from(&url)
    }
}

pub fn normalize_url(url: &Url) -> String {
    let mut url = url.clone();
    let allowed_queries: Vec<_> = url
        .query_pairs()
        .filter(|(key, _)| {
            !key.starts_with("utm_")
                && !key.starts_with("fbclid")
                && !key.starts_with("gclid")
                && !key.starts_with("msclkid")
        })
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();

    {
        let mut queries = url.query_pairs_mut();
        queries.clear();

        if !allowed_queries.is_empty() {
            queries.extend_pairs(allowed_queries);
        }
    }

    if url.query().unwrap_or_default().is_empty() {
        url.set_query(None);
    }

    let scheme = url.scheme();
    let mut normalized = url
        .as_str()
        .strip_prefix(scheme)
        .unwrap_or_default()
        .strip_prefix("://")
        .unwrap_or_default()
        .to_string();

    if let Some(stripped) = normalized.strip_prefix("www.") {
        normalized = stripped.to_string();
    }

    if let Some(prefix) = normalized.strip_suffix('/') {
        normalized = prefix.to_string();
    }

    normalized
}

pub struct WebgraphBuilder {
    path: Box<Path>,
    executor: Executor,
}

impl WebgraphBuilder {
    #[cfg(test)]
    pub fn new_memory() -> Self {
        use crate::gen_temp_path;

        let path = gen_temp_path();
        Self::new(path)
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().into(),
            executor: Executor::multi_thread("webgraph").unwrap(),
        }
    }

    pub fn single_threaded(mut self) -> Self {
        self.executor = Executor::single_thread();
        self
    }

    pub fn open(self) -> Webgraph {
        Webgraph::open(self.path, self.executor)
    }
}

pub trait ShortestPaths {
    fn distances(&self, source: Node) -> BTreeMap<Node, u8>;
    fn raw_distances(&self, source: Node) -> BTreeMap<NodeID, u8>;
    fn raw_reversed_distances(&self, source: Node) -> BTreeMap<NodeID, u8>;
    fn reversed_distances(&self, source: Node) -> BTreeMap<Node, u8>;
}

fn dijkstra<F1, F2, L>(source: Node, node_edges: F1, edge_node: F2) -> BTreeMap<NodeID, u8>
where
    L: Send + Sync,
    F1: Fn(NodeID) -> Vec<Edge<L>>,
    F2: Fn(&Edge<L>) -> NodeID,
{
    let source_id = source.id();
    let mut distances: BTreeMap<NodeID, u8> = BTreeMap::default();

    let mut queue = BinaryHeap::new();

    queue.push(cmp::Reverse((0, source_id)));
    distances.insert(source_id, 0);

    while let Some(state) = queue.pop() {
        let (cost, v) = state.0;

        let current_dist = distances.get(&v).unwrap_or(&u8::MAX);

        if cost > *current_dist {
            continue;
        }

        for edge in node_edges(v) {
            if cost + 1 < *distances.get(&edge_node(&edge)).unwrap_or(&u8::MAX) {
                let d = cost + 1;

                let next = cmp::Reverse((d, edge_node(&edge)));
                queue.push(next);
                distances.insert(edge_node(&edge), d);
            }
        }
    }

    distances
}

impl ShortestPaths for Webgraph {
    fn distances(&self, source: Node) -> BTreeMap<Node, u8> {
        self.raw_distances(source)
            .into_iter()
            .filter_map(|(id, dist)| self.id2node(&id).map(|node| (node, dist)))
            .collect()
    }

    fn raw_distances(&self, source: Node) -> BTreeMap<NodeID, u8> {
        dijkstra(
            source,
            |node| self.raw_outgoing_edges(&node),
            |edge| edge.to,
        )
    }

    fn raw_reversed_distances(&self, source: Node) -> BTreeMap<NodeID, u8> {
        dijkstra(
            source,
            |node| self.raw_ingoing_edges(&node),
            |edge| edge.from,
        )
    }

    fn reversed_distances(&self, source: Node) -> BTreeMap<Node, u8> {
        self.raw_reversed_distances(source)
            .into_iter()
            .filter_map(|(id, dist)| self.id2node(&id).map(|node| (node, dist)))
            .collect()
    }
}

type SegmentID = String;

#[derive(Serialize, Deserialize, Default)]
struct Meta {
    comitted_segments: Vec<SegmentID>,
}

struct SegmentMergeCandidate {
    segment: StoredSegment,
    merges: Vec<StoredSegment>,
}

struct Id2NodeDb {
    db: rocksdb::DB,
}

impl Id2NodeDb {
    fn open<P: AsRef<Path>>(path: P) -> Self {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.optimize_for_point_lookup(512);

        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_ribbon_filter(5.0);

        opts.set_block_based_table_factory(&block_opts);

        let db = rocksdb::DB::open(&opts, path).unwrap();

        Self { db }
    }

    fn put(&mut self, id: &NodeID, node: &Node) {
        let mut opts = rocksdb::WriteOptions::default();
        opts.disable_wal(true);

        self.db
            .put_opt(
                id.bit_128().to_be_bytes(),
                bincode::serialize(node).unwrap(),
                &opts,
            )
            .unwrap();
    }

    fn get(&self, id: &NodeID) -> Option<Node> {
        self.db
            .get(id.bit_128().to_be_bytes())
            .unwrap()
            .map(|bytes| bincode::deserialize(&bytes).unwrap())
    }

    fn keys(&self) -> impl Iterator<Item = NodeID> + '_ {
        self.db
            .iterator(rocksdb::IteratorMode::Start)
            .filter_map(|r| {
                let (key, _) = r.ok()?;
                Some(NodeID(u128::from_be_bytes((*key).try_into().unwrap())))
            })
    }

    fn iter(&self) -> impl Iterator<Item = (NodeID, Node)> + '_ {
        self.db
            .iterator(rocksdb::IteratorMode::Start)
            .filter_map(|r| {
                let (key, value) = r.ok()?;

                Some((
                    NodeID(u128::from_be_bytes((*key).try_into().unwrap())),
                    bincode::deserialize(&value).unwrap(),
                ))
            })
    }

    fn batch_put(&mut self, iter: impl Iterator<Item = (NodeID, Node)>) {
        let mut batch = rocksdb::WriteBatch::default();

        for (id, node) in iter {
            batch.put(
                id.bit_128().to_be_bytes(),
                bincode::serialize(&node).unwrap(),
            );
        }

        self.db.write(batch).unwrap();
    }

    fn flush(&self) {
        self.db.flush().unwrap();
    }
}

pub struct Webgraph {
    pub path: String,
    segments: Vec<StoredSegment>,
    executor: Arc<Executor>,
    insert_batch: Vec<Edge<String>>,
    id2node: Id2NodeDb,
    meta: Meta,
}

impl Webgraph {
    fn meta<P: AsRef<Path>>(path: P) -> Meta {
        let meta_path = path.as_ref().join("metadata.json");
        let mut reader = BufReader::new(
            File::options()
                .create(true)
                .write(true)
                .read(true)
                .open(meta_path)
                .unwrap(),
        );
        let mut buf = String::new();
        reader.read_to_string(&mut buf).unwrap();
        serde_json::from_str(&buf).unwrap_or_default()
    }

    fn save_metadata(&mut self) {
        let path = Path::new(&self.path).join("metadata.json");
        let mut writer = BufWriter::new(
            File::options()
                .create(true)
                .write(true)
                .read(true)
                .truncate(true)
                .open(path)
                .unwrap(),
        );

        let json = serde_json::to_string_pretty(&self.meta).unwrap();
        writer.write_all(json.as_bytes()).unwrap();
    }

    fn open<P: AsRef<Path>>(path: P, executor: Executor) -> Self {
        fs::create_dir_all(&path).unwrap();
        let mut meta = Self::meta(&path);

        fs::create_dir_all(path.as_ref().join("segments")).unwrap();
        let mut segments = Vec::new();
        for segment in &meta.comitted_segments {
            segments.push(StoredSegment::open(
                path.as_ref().join("segments"),
                segment.clone(),
            ));
        }

        if segments.is_empty() {
            segments.push(StoredSegment::open(
                path.as_ref().join("segments"),
                uuid::Uuid::new_v4().to_string(),
            ));

            meta.comitted_segments.push(segments[0].id());
        }

        Self {
            path: path.as_ref().as_os_str().to_str().unwrap().to_string(),
            segments,
            executor: Arc::new(executor),
            id2node: Id2NodeDb::open(path.as_ref().join("id2node")),
            insert_batch: Vec::with_capacity(MAX_BATCH_SIZE),
            meta,
        }
    }

    fn id_or_assign(&mut self, node: &Node) -> NodeID {
        let id = node.id();

        if self.id2node(&id).is_none() {
            self.id2node.put(&id, node);
        }

        id
    }

    pub fn insert(&mut self, from: Node, to: Node, label: String) {
        let (from_id, to_id) = (self.id_or_assign(&from), self.id_or_assign(&to));

        let edge = Edge {
            from: from_id,
            to: to_id,
            label: label.chars().take(MAX_LABEL_LENGTH).collect(),
        };

        self.insert_batch.push(edge);

        if self.insert_batch.len() >= MAX_BATCH_SIZE {
            self.commit();
        }
    }

    pub fn merge(&mut self, mut other: Webgraph) {
        other.commit();

        self.id2node.batch_put(other.id2node.iter());

        for segment in other.segments {
            let id = segment.id();
            let new_path = Path::new(&self.path).join("segments");
            std::fs::rename(segment.path(), &new_path.join(segment.id())).unwrap();

            self.meta.comitted_segments.push(segment.id());
            drop(segment);
            self.segments.push(StoredSegment::open(new_path, id));
        }

        self.commit();
    }

    pub fn commit(&mut self) {
        if !self.insert_batch.is_empty() {
            let seg = self.segments.last_mut().unwrap();
            seg.insert(&self.insert_batch);
            seg.flush();
            self.insert_batch.clear();
        }

        self.save_metadata();
        self.id2node.flush();

        if self.segments.len() > 2 * num_cpus::get() {
            self.merge_segments(num_cpus::get());
        }
    }

    pub fn ingoing_edges(&self, node: Node) -> Vec<FullEdge> {
        self.inner_edges(
            |segment| segment.ingoing_edges_with_label(&node.id()).collect_vec(),
            |edges| {
                edges.sort_by_key(|e| e.from);
                edges.dedup_by_key(|e| e.from);
            },
        )
        .into_iter()
        .map(|e| FullEdge {
            from: self.id2node(&e.from).unwrap(),
            to: self.id2node(&e.to).unwrap(),
            label: e.label,
        })
        .collect()
    }

    pub fn raw_ingoing_edges(&self, node: &NodeID) -> Vec<Edge<()>> {
        self.inner_edges(
            |segment| segment.ingoing_edges(node).collect_vec(),
            |edges| {
                edges.sort_by_key(|e| e.from);
                edges.dedup_by_key(|e| e.from);
            },
        )
    }

    pub fn raw_ingoing_edges_with_labels(&self, node: &NodeID) -> Vec<Edge<String>> {
        self.inner_edges(
            |segment| segment.ingoing_edges_with_label(node).collect_vec(),
            |edges| {
                edges.sort_by_key(|e| e.from);
                edges.dedup_by_key(|e| e.from);
            },
        )
    }

    pub fn outgoing_edges(&self, node: Node) -> Vec<FullEdge> {
        self.inner_edges(
            |segment| segment.outgoing_edges_with_label(&node.id()).collect_vec(),
            |edges| {
                edges.sort_by_key(|e| e.to);
                edges.dedup_by_key(|e| e.to);
            },
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
        self.inner_edges(
            |segment| segment.outgoing_edges(node).collect_vec(),
            |edges| {
                edges.sort_by_key(|e| e.to);
                edges.dedup_by_key(|e| e.to);
            },
        )
    }

    fn inner_edges<F1, F2, L>(&self, loader: F1, dedup: F2) -> Vec<Edge<L>>
    where
        L: Send + Sync,
        F1: Sized + Sync + Fn(&StoredSegment) -> Vec<Edge<L>>,
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

    pub fn node_ids(&self) -> impl Iterator<Item = (Node, NodeID)> + '_ {
        self.id2node.iter().map(|(id, node)| (node, id))
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

    pub fn merge_segments(&mut self, num_segments: usize) {
        if num_segments >= self.segments.len() {
            return;
        }

        self.segments
            .sort_by_key(|segment| std::cmp::Reverse(segment.estimate_num_nodes()));

        let mut candidates = Vec::with_capacity(num_segments);

        for segment in self.segments.drain(0..num_segments) {
            candidates.push(SegmentMergeCandidate {
                segment,
                merges: Vec::new(),
            });
        }

        let num_candidates = candidates.len();

        for (next_candidate, segment) in self.segments.drain(0..).enumerate() {
            candidates[next_candidate % num_candidates]
                .merges
                .push(segment);
        }

        self.segments = self
            .executor
            .map(
                |mut candidate| {
                    let mut segments = vec![candidate.segment];
                    segments.append(&mut candidate.merges);

                    StoredSegment::merge(segments)
                },
                candidates.into_iter(),
            )
            .unwrap();

        self.meta.comitted_segments = self.segments.iter().map(|segment| segment.id()).collect();
        self.save_metadata();

        self.garbage_collect();
    }

    fn garbage_collect(&self) {
        let path = Path::new(&self.path).join("segments");
        let segments: HashSet<_> = self.meta.comitted_segments.iter().cloned().collect();

        for path in fs::read_dir(path).unwrap() {
            let path = path.unwrap();
            let file_name = path.file_name();
            let name = file_name.as_os_str().to_str().unwrap();

            if !segments.contains(name) {
                fs::remove_dir_all(path.path()).unwrap();
            }
        }
    }
}

#[cfg(test)]
mod test {
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

    fn test_graph() -> Webgraph {
        //     ┌────┐
        //     │    │
        // ┌───A◄─┐ │
        // │      │ │
        // ▼      │ │
        // B─────►C◄┘
        //        ▲
        //        │
        //        │
        //        D

        let mut graph = WebgraphBuilder::new_memory().open();

        for (from, to, label) in test_edges() {
            graph.insert(from, to, label);
        }

        graph.commit();

        graph
    }

    fn verify_graph(graph: &Webgraph) {
        let mut res = graph.outgoing_edges(Node::from("A"));
        res.sort_by(|a, b| a.to.name.cmp(&b.to.name));

        assert_eq!(
            res,
            vec![
                FullEdge {
                    from: Node::from("A"),
                    to: Node::from("B"),
                    label: String::new()
                },
                FullEdge {
                    from: Node::from("A"),
                    to: Node::from("C"),
                    label: String::new()
                }
            ]
        );

        let mut res = graph.ingoing_edges(Node::from("C"));
        res.sort_by(|a, b| a.from.name.cmp(&b.from.name));

        assert_eq!(
            res,
            vec![
                FullEdge {
                    from: Node::from("A"),
                    to: Node::from("C"),
                    label: String::new()
                },
                FullEdge {
                    from: Node::from("B"),
                    to: Node::from("C"),
                    label: String::new()
                },
                FullEdge {
                    from: Node::from("D"),
                    to: Node::from("C"),
                    label: String::new()
                },
            ]
        );

        let res = graph.outgoing_edges(Node::from("D"));

        assert_eq!(
            res,
            vec![FullEdge {
                from: Node::from("D"),
                to: Node::from("C"),
                label: String::new()
            },]
        );

        let distances = graph.distances(Node::from("D"));

        assert_eq!(distances.get(&Node::from("C")), Some(&1));
        assert_eq!(distances.get(&Node::from("A")), Some(&2));
        assert_eq!(distances.get(&Node::from("B")), Some(&3));
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
            let mut graph = WebgraphBuilder::new_memory().open();
            graph.insert(from.clone(), to.clone(), label.clone());
            graph.commit();
            graphs.push(graph);
        }

        let mut graph = graphs.pop().unwrap();

        for other in graphs {
            graph.merge(other);
        }

        assert_eq!(
            graph.distances(Node::from("A")).get(&Node::from("H")),
            Some(&7)
        );

        graph.merge_segments(1);
        assert_eq!(
            graph.distances(Node::from("A")).get(&Node::from("H")),
            Some(&7)
        )
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
            let mut graph = WebgraphBuilder::new_memory().open();
            graph.insert(from.clone(), to.clone(), label.clone());
            graph.commit();
            graphs.push(graph);
        }

        let mut graph = graphs.pop().unwrap();

        for other in graphs {
            graph.merge(other);
        }

        assert_eq!(
            graph.distances(Node::from("A")).get(&Node::from("C")),
            Some(&2)
        );

        graph.merge_segments(1);
        assert_eq!(
            graph.distances(Node::from("A")).get(&Node::from("C")),
            Some(&2)
        )
    }

    #[test]
    fn node_lowercase_name() {
        let n = Node::from("TEST".to_string());
        assert_eq!(&n.name, "test");
    }

    #[test]
    fn host_node_cleanup() {
        let n = Node::from("https://www.example.com?test").into_host();
        assert_eq!(&n.name, "example.com");
    }

    #[test]
    fn remove_protocol() {
        let n = Node::from("https://www.example.com/?test");

        assert_eq!(&n.name, "example.com/?test=");
    }

    #[test]
    fn merge_segments() {
        let mut graph = WebgraphBuilder::new_memory().open();

        let edges = test_edges();
        let num_edges = edges.len();

        for (from, to, label) in test_edges() {
            let mut tmp_graph = WebgraphBuilder::new_memory().open();
            tmp_graph.insert(from, to, label);
            tmp_graph.commit();
            graph.merge(tmp_graph);
        }

        graph.commit();

        assert!(graph.segments.len() >= num_edges);
        verify_graph(&graph);

        graph.merge_segments(2);
        assert_eq!(graph.segments.len(), 2);

        verify_graph(&graph);
    }

    #[test]
    fn cap_label_length() {
        let mut graph = WebgraphBuilder::new_memory().open();

        graph.insert(
            Node::from("A"),
            Node::from("B"),
            "a".repeat(MAX_LABEL_LENGTH + 1),
        );

        graph.commit();

        assert_eq!(graph.segments.len(), 1);
        assert_eq!(
            graph.outgoing_edges(Node::from("A"))[0].label,
            "a".repeat(MAX_LABEL_LENGTH)
        );
    }
}
