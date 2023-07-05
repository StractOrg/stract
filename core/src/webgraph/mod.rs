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

use rkyv::Archive;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::{cmp, fs};

use crate::directory::{self, DirEntry};
use crate::executor::Executor;
use crate::webpage::Url;

pub mod centrality;
mod store;
use self::segment::{LiveSegment, SegmentNodeID, StoredSegment};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeID(pub u64);

impl From<u64> for NodeID {
    fn from(val: u64) -> Self {
        Self(val)
    }
}

#[derive(Debug, Clone, Archive, rkyv::Serialize, rkyv::Deserialize, PartialEq, Eq, Hash)]
#[archive_attr(derive(Eq, Hash, PartialEq, Debug))]
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

    pub fn remove_protocol(&mut self) {
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

#[derive(Debug, PartialEq, Eq)]
pub struct Edge {
    pub from: NodeID,
    pub to: NodeID,
    pub label: Loaded<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Loaded<T> {
    Some(T),
    NotYet,
}
impl<T> Loaded<T> {
    fn loaded(self) -> Option<T> {
        match self {
            Loaded::Some(t) => Some(t),
            Loaded::NotYet => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullEdge {
    pub from: Node,
    pub to: Node,
    pub label: String,
}

pub struct WebgraphBuilder {
    path: Box<Path>,
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
        }
    }

    pub fn open(self) -> Webgraph {
        Webgraph::open(self.path)
    }
}

pub trait ShortestPaths {
    fn distances(&self, source: Node) -> BTreeMap<Node, u8>;
    fn raw_distances(&self, source: Node) -> BTreeMap<NodeID, u8>;
    fn raw_reversed_distances(&self, source: Node) -> BTreeMap<NodeID, u8>;
    fn reversed_distances(&self, source: Node) -> BTreeMap<Node, u8>;
}

fn dijkstra<F1, F2>(
    source: Node,
    node_edges: F1,
    edge_node: F2,
    graph: &Webgraph,
) -> BTreeMap<NodeID, u8>
where
    F1: Fn(NodeID) -> Vec<Edge>,
    F2: Fn(&Edge) -> NodeID,
{
    let source_id = graph.node2id(&source);
    if source_id.is_none() {
        return BTreeMap::new();
    }

    let source_id = source_id.unwrap();
    let mut distances: BTreeMap<NodeID, u8> = BTreeMap::default();

    let mut queue = BinaryHeap::new();

    queue.push(cmp::Reverse((0, *source_id)));
    distances.insert(*source_id, 0);

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
            .map(|(id, dist)| (self.id2node(&id).expect("unknown node").clone(), dist))
            .collect()
    }

    fn raw_distances(&self, source: Node) -> BTreeMap<NodeID, u8> {
        dijkstra(
            source,
            |node| self.raw_outgoing_edges(&node),
            |edge| edge.to,
            self,
        )
    }

    fn raw_reversed_distances(&self, source: Node) -> BTreeMap<NodeID, u8> {
        dijkstra(
            source,
            |node| self.raw_ingoing_edges(&node),
            |edge| edge.from,
            self,
        )
    }

    fn reversed_distances(&self, source: Node) -> BTreeMap<Node, u8> {
        self.raw_reversed_distances(source)
            .into_iter()
            .map(|(id, dist)| (self.id2node(&id).expect("unknown node").clone(), dist))
            .collect()
    }
}

type SegmentID = String;

#[derive(Serialize, Deserialize, Default)]
struct Meta {
    comitted_segments: Vec<SegmentID>,
    next_node_id: u64,
}

struct SegmentMergeCandidate {
    segment: StoredSegment,
    merges: Vec<StoredSegment>,
}

fn open_bin<T, P>(path: P) -> T
where
    P: AsRef<Path>,
    T: DeserializeOwned + Default,
{
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
        .unwrap();

    let reader = BufReader::new(f);

    bincode::deserialize_from(reader).unwrap_or_default()
}

fn save_bin<T, P>(val: &T, path: P)
where
    P: AsRef<Path>,
    T: Serialize,
{
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
        .unwrap();

    bincode::serialize_into(f, val).unwrap();
}
pub struct Webgraph {
    pub path: String,
    live_segment: LiveSegment,
    segments: Vec<StoredSegment>,
    executor: Arc<Executor>,
    node2id: BTreeMap<Node, NodeID>,
    id2node: BTreeMap<NodeID, Node>,
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

    fn open<P: AsRef<Path>>(path: P) -> Self {
        fs::create_dir_all(&path).unwrap();
        let meta = Self::meta(&path);

        fs::create_dir_all(path.as_ref().join("segments")).unwrap();
        let mut segments = Vec::new();
        for segment in &meta.comitted_segments {
            segments.push(StoredSegment::open(
                path.as_ref().join("segments").join(segment),
                segment.clone(),
            ));
        }

        Self {
            path: path.as_ref().as_os_str().to_str().unwrap().to_string(),
            live_segment: LiveSegment::default(),
            segments,
            executor: Arc::new(Executor::multi_thread("webgraph").unwrap()),
            node2id: open_bin(path.as_ref().join("node2id.bin")),
            id2node: open_bin(path.as_ref().join("id2node.bin")),
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
            Some(id) => *id,
            None => {
                let id = self.id_and_increment();

                self.node2id.insert(node.clone(), id);
                self.id2node.insert(id, node.clone());

                id
            }
        }
    }

    pub fn insert(&mut self, from: Node, to: Node, label: String) {
        let (from_id, to_id) = (self.id_or_assign(&from), self.id_or_assign(&to));
        self.live_segment.insert(from_id, to_id, label);
    }

    pub fn merge(&mut self, mut other: Webgraph) {
        other.commit();
        let mut mapping = Vec::new();

        for (node, other_id) in other.node2id.iter() {
            match self.node2id(node) {
                Some(this_id) => mapping.push((*other_id, *this_id)),
                None => {
                    let new_id = self.id_or_assign(node);
                    mapping.push((*other_id, new_id));
                }
            }
        }

        self.executor
            .map(
                |segment| segment.update_id_mapping(mapping.clone()),
                other.segments.iter_mut(),
            )
            .expect("failed to merge webgraphs");

        for segment in other.segments {
            let id = segment.id();
            let new_path = Path::new(&self.path).join("segments").join(segment.id());
            std::fs::rename(segment.path(), &new_path).unwrap();

            self.meta.comitted_segments.push(segment.id());
            drop(segment);
            self.segments.push(StoredSegment::open(new_path, id));
        }

        self.commit();
    }

    pub fn commit(&mut self) {
        if !self.live_segment.is_empty() {
            let live_segment = std::mem::take(&mut self.live_segment);
            let segment = live_segment.commit(Path::new(&self.path).join("segments"));

            self.meta.comitted_segments.push(segment.id());
            self.segments.push(segment);
        }

        self.save_metadata();
        save_bin(&self.node2id, Path::new(&self.path).join("node2id.bin"));
        save_bin(&self.id2node, Path::new(&self.path).join("id2node.bin"));

        if self.segments.len() > 2 * num_cpus::get() {
            self.merge_segments(num_cpus::get());
        }
    }

    pub fn ingoing_edges(&self, node: Node) -> Vec<FullEdge> {
        if let Some(node_id) = self.node2id(&node) {
            self.inner_ingoing_edges(node_id, true)
                .into_iter()
                .map(|edge| FullEdge {
                    from: self.id2node(&edge.from).unwrap().clone(),
                    to: self.id2node(&edge.to).unwrap().clone(),
                    label: edge.label.loaded().unwrap(),
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn raw_ingoing_edges(&self, node: &NodeID) -> Vec<Edge> {
        self.inner_ingoing_edges(node, false)
    }

    fn inner_ingoing_edges(&self, node: &NodeID, load_labels: bool) -> Vec<Edge> {
        self.executor
            .map(
                |segment| segment.ingoing_edges(node, load_labels),
                self.segments.iter(),
            )
            .unwrap()
            .into_iter()
            .flatten()
            .collect()
    }

    pub fn raw_outgoing_edges(&self, node: &NodeID) -> Vec<Edge> {
        self.inner_outgoing_edges(node, false)
    }

    fn inner_outgoing_edges(&self, node: &NodeID, load_labels: bool) -> Vec<Edge> {
        self.executor
            .map(
                |segment| segment.outgoing_edges(node, load_labels),
                self.segments.iter(),
            )
            .unwrap()
            .into_iter()
            .flatten()
            .collect()
    }

    pub fn outgoing_edges(&self, node: Node) -> Vec<FullEdge> {
        if let Some(node_id) = self.node2id(&node) {
            self.inner_outgoing_edges(node_id, true)
                .into_iter()
                .map(|edge| FullEdge {
                    from: self.id2node(&edge.from).unwrap().clone(),
                    to: self.id2node(&edge.to).unwrap().clone(),
                    label: edge.label.loaded().unwrap(),
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn node2id(&self, node: &Node) -> Option<&NodeID> {
        self.node2id.get(node)
    }

    pub fn id2node(&self, id: &NodeID) -> Option<&Node> {
        self.id2node.get(id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &NodeID> + '_ {
        self.node2id.values()
    }

    pub fn node_ids(&self) -> impl Iterator<Item = (&Node, &NodeID)> + '_ {
        self.node2id.iter()
    }

    pub fn edges(&self) -> impl Iterator<Item = Edge> + '_ {
        self.segments.iter().flat_map(|segment| segment.edges())
    }

    pub fn merge_segments(&mut self, num_segments: usize) {
        if num_segments >= self.segments.len() {
            return;
        }

        self.segments
            .sort_by_key(|segment| std::cmp::Reverse(segment.num_nodes()));

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
}
