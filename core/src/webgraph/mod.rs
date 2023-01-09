// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::{cmp, fs};
use uuid::Uuid;

use segment::Segment;

use crate::directory::{self, DirEntry};
use crate::webpage::Url;

use crate::kv::rocksdb_store::RocksDbStore;

pub mod centrality;
mod executor;
use self::executor::Executor;
use self::segment::{CachedStore, SegmentNodeID};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeID(pub u64);

impl From<u64> for NodeID {
    fn from(val: u64) -> Self {
        Self(val)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct StoredEdge {
    other: SegmentNodeID,
    label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
}

impl Node {
    pub fn into_host(self) -> Node {
        let url = Url::from(self.name);

        let host = url.host_without_specific_subdomains_and_query();

        Node {
            name: host.to_lowercase(),
        }
    }

    pub fn from_url(url: &Url) -> Self {
        Node::from(url.full())
    }

    fn remove_protocol(&mut self) {
        let url = Url::from(self.name.clone());
        self.name = url.without_protocol().to_string();
    }
}

impl From<String> for Node {
    fn from(name: String) -> Self {
        Self {
            name: name.to_lowercase(),
        }
    }
}

impl From<&Url> for Node {
    fn from(url: &Url) -> Self {
        Self {
            name: url.without_protocol().to_lowercase(),
        }
    }
}

impl From<&str> for Node {
    fn from(name: &str) -> Self {
        Self::from(name.to_lowercase())
    }
}

impl From<Url> for Node {
    fn from(url: Url) -> Self {
        Self::from(&url)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Edge {
    from: NodeID,
    to: NodeID,
    label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullEdge {
    pub from: Node,
    pub to: Node,
    pub label: String,
}

pub struct WebgraphBuilder {
    path: Box<Path>,
    read_only: bool,
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
            read_only: false,
        }
    }

    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;

        self
    }

    pub fn open(self) -> Webgraph {
        if self.read_only {
            Webgraph::open_read_only(self.path)
        } else {
            Webgraph::open(self.path)
        }
    }
}

pub trait Store
where
    Self: Sized + Send + Sync,
{
    fn open<P: AsRef<Path>>(path: P, id: String) -> Segment<Self>;
    fn open_read_only<P: AsRef<Path>>(path: P, id: String) -> Segment<Self>;

    fn temporary() -> Segment<Self> {
        Self::open(crate::gen_temp_path(), "id123".to_string())
    }
}

pub trait ShortestPaths {
    fn distances(&self, source: Node) -> HashMap<Node, usize>;
    fn raw_distances(&self, source: Node) -> HashMap<NodeID, usize>;
    fn raw_reversed_distances(&self, source: Node) -> HashMap<NodeID, usize>;
    fn reversed_distances(&self, source: Node) -> HashMap<Node, usize>;
}

fn dijkstra<F1, F2>(
    source: Node,
    node_edges: F1,
    edge_node: F2,
    graph: &Webgraph,
) -> HashMap<NodeID, usize>
where
    F1: Fn(NodeID) -> Vec<Edge>,
    F2: Fn(&Edge) -> NodeID,
{
    const MAX_DIST: usize = 3;
    let source_id = graph.node2id(&source);
    if source_id.is_none() {
        return HashMap::new();
    }

    let source_id = source_id.unwrap();
    let mut distances: HashMap<NodeID, usize> = HashMap::default();

    let mut queue = BinaryHeap::new();

    queue.push(cmp::Reverse((0_usize, source_id)));
    distances.insert(source_id, 0);

    while let Some(state) = queue.pop() {
        let (cost, v) = state.0;

        if cost >= MAX_DIST {
            continue;
        }

        let current_dist = distances.get(&v).unwrap_or(&usize::MAX);

        if cost > *current_dist {
            continue;
        }

        for edge in node_edges(v) {
            if cost + 1 < *distances.get(&edge_node(&edge)).unwrap_or(&usize::MAX) {
                let d = cost + 1;

                if d > MAX_DIST {
                    continue;
                }

                let next = cmp::Reverse((d, edge_node(&edge)));
                queue.push(next);
                distances.insert(edge_node(&edge), d);
            }
        }
    }

    distances
}

impl ShortestPaths for Webgraph {
    fn distances(&self, source: Node) -> HashMap<Node, usize> {
        self.raw_distances(source)
            .into_iter()
            .map(|(id, dist)| (self.id2node(&id).expect("unknown node"), dist))
            .collect()
    }

    fn raw_distances(&self, source: Node) -> HashMap<NodeID, usize> {
        dijkstra(
            source,
            |node| self.raw_outgoing_edges(&node),
            |edge| edge.to,
            self,
        )
    }

    fn raw_reversed_distances(&self, source: Node) -> HashMap<NodeID, usize> {
        dijkstra(
            source,
            |node| self.raw_ingoing_edges(&node),
            |edge| edge.from,
            self,
        )
    }

    fn reversed_distances(&self, source: Node) -> HashMap<Node, usize> {
        self.raw_reversed_distances(source)
            .into_iter()
            .map(|(id, dist)| (self.id2node(&id).expect("unknown node"), dist))
            .collect()
    }
}

type SegmentID = String;

#[derive(Serialize, Deserialize, Default)]
struct Meta {
    comitted_segments: Vec<SegmentID>,
    next_node_id: u64,
}

struct SegmentMergeCandidate<S: Store> {
    segment: Segment<S>,
    merges: Vec<Segment<S>>,
}

pub struct Webgraph<S: Store = RocksDbStore> {
    pub path: String,
    live_segment: Option<Segment<S>>,
    segments: Vec<Segment<S>>,
    executor: Arc<Executor>,
    node2id: CachedStore<Node, NodeID>,
    id2node: CachedStore<NodeID, Node>,
    meta: Meta,
}

impl<S: Store> Webgraph<S> {
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
                .open(path)
                .unwrap(),
        );

        let json = serde_json::to_string_pretty(&self.meta).unwrap();
        writer.write_all(json.as_bytes()).unwrap();
    }

    fn open<P: AsRef<Path>>(path: P) -> Self {
        fs::create_dir_all(&path).unwrap();
        let meta = Self::meta(&path);

        fs::create_dir_all(path.as_ref().join("segments")).unwrap();
        let mut segments = Vec::new();
        for segment in &meta.comitted_segments {
            segments.push(Segment::open(
                path.as_ref().join("segments").join(segment),
                segment.clone(),
            ));
        }

        Self {
            path: path.as_ref().as_os_str().to_str().unwrap().to_string(),
            live_segment: None,
            segments,
            // executor: Arc::new(Executor::multi_thread("webgraph").unwrap()),
            executor: Arc::new(Executor::single_thread()),
            node2id: CachedStore::new(RocksDbStore::open(path.as_ref().join("node2id")), 100_000),
            id2node: CachedStore::new(RocksDbStore::open(path.as_ref().join("id2node")), 100_000),
            meta,
        }
    }

    fn open_read_only<P: AsRef<Path>>(path: P) -> Self {
        fs::create_dir_all(&path).unwrap();
        let meta = Self::meta(&path);

        fs::create_dir_all(path.as_ref().join("segments")).unwrap();
        let mut segments = Vec::new();
        for segment in &meta.comitted_segments {
            segments.push(Segment::open(
                path.as_ref().join("segments").join(segment),
                segment.clone(),
            ));
        }

        Self {
            path: path.as_ref().as_os_str().to_str().unwrap().to_string(),
            live_segment: None,
            segments,
            executor: Arc::new(Executor::multi_thread("webgraph").unwrap()),
            node2id: CachedStore::new(
                RocksDbStore::open_read_only(path.as_ref().join("node2id")),
                100_000,
            ),
            id2node: CachedStore::new(
                RocksDbStore::open_read_only(path.as_ref().join("id2node")),
                100_000,
            ),
            meta,
        }
    }

    fn id_and_increment(&mut self) -> NodeID {
        let id = self.meta.next_node_id.into();
        self.meta.next_node_id += 1;
        id
    }

    fn id_or_assign(&mut self, node: &Node) -> NodeID {
        match self.node2id(node) {
            Some(id) => id,
            None => {
                let id = self.id_and_increment();

                self.node2id.insert(node.clone(), id);
                self.id2node.insert(id, node.clone());

                id
            }
        }
    }

    pub fn insert(&mut self, mut from: Node, mut to: Node, label: String) {
        from.remove_protocol();
        to.remove_protocol();

        let (from, to) = (from.into_host(), to.into_host());
        let (from_id, to_id) = (self.id_or_assign(&from), self.id_or_assign(&to));

        match &mut self.live_segment {
            Some(segment) => segment.insert(from_id, to_id, label),
            None => {
                let segment_id = Uuid::new_v4().to_string();
                let path = Path::new(&self.path).join("segments").join(&segment_id);
                let mut segment: Segment<S> = Segment::open(path, segment_id);

                segment.insert(from_id, to_id, label);

                self.live_segment = Some(segment);
            }
        }
    }

    pub fn merge(&mut self, mut other: Webgraph<S>) {
        other.commit();
        let mut mapping = Vec::new();

        for (node, other_id) in other.node2id.iter() {
            match self.node2id(&node) {
                Some(this_id) => mapping.push((other_id, this_id)),
                None => {
                    let new_id = self.id_or_assign(&node);
                    mapping.push((other_id, new_id));
                }
            }
        }

        self.executor
            .map(
                |segment| segment.update_id_mapping(mapping.clone()),
                other.segments.iter_mut(),
            )
            .expect("failed to merge webgraphs");

        for mut segment in other.segments {
            segment.flush();
            let id = segment.id();
            let new_path = Path::new(&self.path).join("segments").join(segment.id());
            std::fs::rename(segment.path(), &new_path).unwrap();

            self.meta.comitted_segments.push(segment.id());
            drop(segment);
            self.segments.push(Segment::open(new_path, id));
        }

        self.commit();
    }

    pub fn commit(&mut self) {
        if let Some(mut segment) = self.live_segment.take() {
            segment.flush();
            self.meta.comitted_segments.push(segment.id());
            self.segments.push(segment);
        }

        self.save_metadata();
        self.node2id.flush();
        self.id2node.flush();
    }

    pub fn ingoing_edges(&self, node: Node) -> Vec<FullEdge> {
        if let Some(node_id) = self.node2id(&node) {
            self.raw_ingoing_edges(&node_id)
                .into_iter()
                .map(|edge| FullEdge {
                    from: self.id2node(&edge.from).unwrap(),
                    to: self.id2node(&edge.to).unwrap(),
                    label: edge.label,
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn raw_ingoing_edges(&self, node: &NodeID) -> Vec<Edge> {
        self.executor
            .map(|segment| segment.ingoing_edges(node), self.segments.iter())
            .unwrap()
            .into_iter()
            .flatten()
            .collect()
    }

    pub fn raw_outgoing_edges(&self, node: &NodeID) -> Vec<Edge> {
        self.executor
            .map(|segment| segment.outgoing_edges(node), self.segments.iter())
            .unwrap()
            .into_iter()
            .flatten()
            .collect()
    }

    pub fn outgoing_edges(&self, node: Node) -> Vec<FullEdge> {
        if let Some(node_id) = self.node2id(&node) {
            self.raw_outgoing_edges(&node_id)
                .into_iter()
                .map(|edge| FullEdge {
                    from: self.id2node(&edge.from).unwrap(),
                    to: self.id2node(&edge.to).unwrap(),
                    label: edge.label,
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn node2id(&self, node: &Node) -> Option<NodeID> {
        self.node2id
            .get(node)
            .map(|lock| lock.read().unwrap().0.into())
    }

    pub fn id2node(&self, id: &NodeID) -> Option<Node> {
        self.id2node
            .get(id)
            .map(|lock| lock.read().unwrap().clone())
    }

    pub fn nodes(&self) -> impl Iterator<Item = NodeID> + '_ {
        self.node2id.iter().map(|(_, id)| id)
    }

    pub fn edges(&self) -> impl Iterator<Item = Edge> + '_ {
        self.segments.iter().flat_map(|segment| segment.edges())
    }

    pub fn merge_segments(&mut self, num_segments: usize) {
        if num_segments >= self.segments.len() {
            return;
        }

        self.segments
            .sort_by_key(|b| std::cmp::Reverse(b.num_nodes()));

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
                    for other in candidate.merges {
                        candidate.segment.merge(other);
                    }

                    candidate.segment
                },
                candidates.into_iter(),
            )
            .unwrap();

        self.meta.comitted_segments = self.segments.iter().map(|segment| segment.id()).collect();

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

impl From<FrozenWebgraph> for Webgraph {
    fn from(frozen: FrozenWebgraph) -> Self {
        let path = match &frozen.root {
            DirEntry::Folder { name, entries: _ } => name.clone(),
            DirEntry::File {
                name: _,
                content: _,
            } => {
                panic!("Cannot open webgraph from a file - must be directory.")
            }
        };

        if Path::new(&path).exists() {
            fs::remove_dir_all(&path).unwrap();
        }

        directory::recreate_folder(&frozen.root).unwrap();

        WebgraphBuilder::new(path).open()
    }
}

impl From<Webgraph> for FrozenWebgraph {
    fn from(mut graph: Webgraph) -> Self {
        graph.commit();
        let path = graph.path.clone();
        drop(graph);
        let root = directory::scan_folder(path).unwrap();

        Self { root }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FrozenWebgraph {
    pub root: DirEntry,
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
        let mut graph1 = WebgraphBuilder::new_memory().open();

        graph1.insert(Node::from("A"), Node::from("B"), String::new());
        graph1.commit();

        let mut graph2 = WebgraphBuilder::new_memory().open();
        graph2.insert(Node::from("B"), Node::from("C"), String::new());
        graph2.commit();

        graph1.merge(graph2);

        assert_eq!(
            graph1.distances(Node::from("A")).get(&Node::from("C")),
            Some(&2)
        )
    }

    #[test]
    fn serialize_deserialize_bincode() {
        let graph = test_graph();

        let path = graph.path.clone();
        let frozen: FrozenWebgraph = graph.into();
        let bytes = bincode::serialize(&frozen).unwrap();

        std::fs::remove_dir_all(path).unwrap();

        let deserialized_frozen: FrozenWebgraph = bincode::deserialize(&bytes).unwrap();
        let graph: Webgraph = deserialized_frozen.into();

        let distances = graph.distances(Node::from("D"));

        assert_eq!(distances.get(&Node::from("C")), Some(&1));
        assert_eq!(distances.get(&Node::from("A")), Some(&2));
        assert_eq!(distances.get(&Node::from("B")), Some(&3));
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
        let mut n = Node::from("https://www.example.com?test");
        n.remove_protocol();

        assert_eq!(&n.name, "www.example.com?test");

        let mut graph = WebgraphBuilder::new_memory().open();

        graph.insert(
            Node::from("http://A"),
            Node::from("https://B"),
            String::new(),
        );

        graph.commit();

        let distances = graph.distances(Node::from("A"));
        assert_eq!(distances.get(&Node::from("B")), Some(&1));

        let distances = graph.distances(Node::from("http://A"));
        assert!(distances.is_empty());
    }

    #[test]
    fn merge_segments() {
        let mut graph = WebgraphBuilder::new_memory().open();

        let edges = test_edges();
        let num_edges = edges.len();

        for (from, to, label) in test_edges() {
            graph.insert(from, to, label);
            graph.commit();
        }

        graph.commit();

        assert_eq!(num_edges, graph.segments.len());

        let distances = graph.distances(Node::from("D"));

        assert_eq!(distances.get(&Node::from("C")), Some(&1));
        assert_eq!(distances.get(&Node::from("A")), Some(&2));
        assert_eq!(distances.get(&Node::from("B")), Some(&3));

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

        graph.merge_segments(2);
        assert_eq!(graph.segments.len(), 2);

        let distances = graph.distances(Node::from("D"));

        assert_eq!(distances.get(&Node::from("C")), Some(&1));
        assert_eq!(distances.get(&Node::from("A")), Some(&2));
        assert_eq!(distances.get(&Node::from("B")), Some(&3));

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
    }
}
