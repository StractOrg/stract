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

use std::collections::{BTreeMap, BinaryHeap};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::{cmp, fs};

use rand::seq::SliceRandom;
use rayon::prelude::*;
use url::Url;
use utoipa::ToSchema;

use crate::executor::Executor;
use crate::intmap;
use crate::webpage::url_ext::UrlExt;

pub mod centrality;
mod store;
use self::segment::{Segment, SegmentWriter};

pub const MAX_LABEL_LENGTH: usize = 1024;

#[derive(
    Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
pub struct NodeID(u64);

impl NodeID {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u128> for NodeID {
    fn from(val: u128) -> Self {
        NodeID(val as u64)
    }
}

impl From<u64> for NodeID {
    fn from(val: u64) -> Self {
        NodeID(val)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FullNodeID {
    pub prefix: NodeID,
    pub id: NodeID,
}

impl From<Node> for FullNodeID {
    fn from(value: Node) -> Self {
        let id = value.id();
        let prefix = value.into_host().id();

        FullNodeID { prefix, id }
    }
}

impl intmap::Key for NodeID {
    const BIG_PRIME: Self = NodeID(11400714819323198549);

    fn wrapping_mul(self, rhs: Self) -> Self {
        NodeID(self.0.wrapping_mul(rhs.0))
    }

    fn as_usize(self) -> usize {
        self.0 as usize
    }

    fn modulus_usize(self, rhs: usize) -> usize {
        (self.0 % (rhs as u64)) as usize
    }
}

pub trait EdgeLabel
where
    Self: Send + Sync + Sized,
{
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>>;
    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self>;
}

impl EdgeLabel for String {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(self.as_bytes().to_vec())
    }

    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(String::from_utf8(bytes.to_vec())?)
    }
}

impl EdgeLabel for () {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn from_bytes(_bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Edge<L>
where
    L: EdgeLabel,
{
    pub from: NodeID,
    pub to: NodeID,
    pub label: L,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InnerEdge<L>
where
    L: EdgeLabel,
{
    pub from: FullNodeID,
    pub to: FullNodeID,
    pub label: L,
}

impl<L> From<InnerEdge<L>> for Edge<L>
where
    L: EdgeLabel,
{
    fn from(edge: InnerEdge<L>) -> Self {
        Edge {
            from: edge.from.id,
            to: edge.to.id,
            label: edge.label,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FullEdge {
    pub from: Node,
    pub to: Node,
    pub label: String,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    ToSchema,
)]
#[serde(rename_all = "camelCase")]
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
                let host = url.normalized_host().unwrap_or_default().to_string();
                Node { name: host }
            }
            Err(_) => Node {
                name: String::new(),
            },
        }
    }

    pub fn id(&self) -> NodeID {
        let digest = md5::compute(self.name.as_bytes());
        u128::from_le_bytes(*digest).into()
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

#[derive(Default, Debug, Clone, Copy)]
pub enum Compression {
    None,
    #[default]
    Lz4,
}

impl Compression {
    pub fn compress(&self, bytes: &[u8]) -> Vec<u8> {
        match self {
            Compression::None => bytes.to_vec(),
            Compression::Lz4 => lz4_flex::compress_prepend_size(bytes),
        }
    }

    pub fn decompress(&self, bytes: &[u8]) -> Vec<u8> {
        match self {
            Compression::None => bytes.to_vec(),
            Compression::Lz4 => lz4_flex::decompress_size_prepended(bytes).unwrap(),
        }
    }
}

pub struct WebgraphBuilder {
    path: Box<Path>,
    executor: Executor,
    compression: Compression,
}

impl WebgraphBuilder {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().into(),
            executor: Executor::multi_thread("webgraph").unwrap(),
            compression: Compression::default(),
        }
    }

    pub fn single_threaded(mut self) -> Self {
        self.executor = Executor::single_thread();
        self
    }

    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    pub fn open(self) -> Webgraph {
        Webgraph::open(self.path, self.executor, self.compression)
    }
}

pub trait ShortestPaths {
    fn distances(&self, source: Node) -> BTreeMap<Node, u8>;
    fn raw_distances(&self, source: NodeID) -> BTreeMap<NodeID, u8>;
    fn raw_reversed_distances(&self, source: NodeID) -> BTreeMap<NodeID, u8>;
    fn reversed_distances(&self, source: Node) -> BTreeMap<Node, u8>;
}

fn dijkstra_multi<F1, F2, L>(
    sources: &[NodeID],
    node_edges: F1,
    edge_node: F2,
) -> BTreeMap<NodeID, u8>
where
    L: EdgeLabel,
    F1: Fn(NodeID) -> Vec<Edge<L>>,
    F2: Fn(&Edge<L>) -> NodeID,
{
    let mut distances: BTreeMap<NodeID, u8> = BTreeMap::default();

    let mut queue = BinaryHeap::new();

    for source_id in sources.iter().copied() {
        queue.push(cmp::Reverse((0, source_id)));
        distances.insert(source_id, 0);
    }

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
        self.raw_distances(source.id())
            .into_iter()
            .filter_map(|(id, dist)| self.id2node(&id).map(|node| (node, dist)))
            .collect()
    }

    fn raw_distances(&self, source: NodeID) -> BTreeMap<NodeID, u8> {
        dijkstra_multi(
            &[source],
            |node| self.raw_outgoing_edges(&node),
            |edge| edge.to,
        )
    }

    fn raw_reversed_distances(&self, source: NodeID) -> BTreeMap<NodeID, u8> {
        dijkstra_multi(
            &[source],
            |node| self.raw_ingoing_edges(&node),
            |edge| edge.from,
        )
    }

    fn reversed_distances(&self, source: Node) -> BTreeMap<Node, u8> {
        self.raw_reversed_distances(source.id())
            .into_iter()
            .filter_map(|(id, dist)| self.id2node(&id).map(|node| (node, dist)))
            .collect()
    }
}

type SegmentID = String;

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct Meta {
    comitted_segments: Vec<SegmentID>,
}

impl Meta {
    fn open<P: AsRef<Path>>(path: P) -> Self {
        let mut reader = BufReader::new(
            File::options()
                .create(true)
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

struct Id2NodeDb {
    db: rocksdb::DB,
}

impl Id2NodeDb {
    fn open<P: AsRef<Path>>(path: P) -> Self {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.optimize_for_point_lookup(512);

        opts.set_allow_mmap_reads(true);
        opts.set_allow_mmap_writes(true);
        opts.set_write_buffer_size(128 * 1024 * 1024); // 128 MB
        opts.set_target_file_size_base(512 * 1024 * 1024); // 512 MB
        opts.set_target_file_size_multiplier(10);

        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_ribbon_filter(5.0);

        // some recommended settings (https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
        opts.set_level_compaction_dynamic_level_bytes(true);
        opts.set_bytes_per_sync(1048576);
        block_opts.set_block_size(16 * 1024);
        block_opts.set_format_version(5);
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);

        opts.set_block_based_table_factory(&block_opts);
        opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

        let db = rocksdb::DB::open(&opts, path).unwrap();

        Self { db }
    }

    fn put(&mut self, id: &NodeID, node: &Node) {
        let mut opts = rocksdb::WriteOptions::default();
        opts.disable_wal(true);

        self.db
            .put_opt(
                id.as_u64().to_le_bytes(),
                bincode::serialize(node).unwrap(),
                &opts,
            )
            .unwrap();
    }

    fn get(&self, id: &NodeID) -> Option<Node> {
        self.db
            .get(id.as_u64().to_le_bytes())
            .unwrap()
            .map(|bytes| bincode::deserialize(&bytes).unwrap())
    }

    fn keys(&self) -> impl Iterator<Item = NodeID> + '_ {
        self.db
            .iterator(rocksdb::IteratorMode::Start)
            .filter_map(|r| {
                let (key, _) = r.ok()?;
                Some(NodeID(u64::from_le_bytes((*key).try_into().unwrap())))
            })
    }

    fn iter(&self) -> impl Iterator<Item = (NodeID, Node)> + '_ {
        self.db
            .iterator(rocksdb::IteratorMode::Start)
            .filter_map(|r| {
                let (key, value) = r.ok()?;

                Some((
                    NodeID(u64::from_le_bytes((*key).try_into().unwrap())),
                    bincode::deserialize(&value).unwrap(),
                ))
            })
    }

    fn batch_put(&mut self, iter: impl Iterator<Item = (NodeID, Node)>) {
        let mut batch = rocksdb::WriteBatch::default();

        for (id, node) in iter {
            batch.put(
                id.as_u64().to_le_bytes(),
                bincode::serialize(&node).unwrap(),
            );
        }

        self.db.write(batch).unwrap();
    }

    fn flush(&self) {
        self.db.flush().unwrap();
    }
}

pub struct WebgraphWriter {
    pub path: String,
    segment: SegmentWriter,
    insert_batch: Vec<InnerEdge<String>>,
    id2node: Id2NodeDb,
    executor: Executor,
    meta: Meta,
    compression: Compression,
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

    pub fn new<P: AsRef<Path>>(path: P, executor: Executor, compression: Compression) -> Self {
        fs::create_dir_all(&path).unwrap();
        let mut meta = Self::meta(&path);
        meta.comitted_segments.clear();

        fs::create_dir_all(path.as_ref().join("segments")).unwrap();

        let id = uuid::Uuid::new_v4().to_string();
        let segment = SegmentWriter::open(path.as_ref().join("segments"), id.clone(), compression);

        meta.comitted_segments.push(id);

        Self {
            path: path.as_ref().as_os_str().to_str().unwrap().to_string(),
            segment,
            id2node: Id2NodeDb::open(path.as_ref().join("id2node")),
            insert_batch: Vec::with_capacity(store::MAX_BATCH_SIZE),
            executor,
            meta,
            compression,
        }
    }

    pub fn id2node(&self, id: &NodeID) -> Option<Node> {
        self.id2node.get(id)
    }

    fn id_or_assign(&mut self, node: Node) -> FullNodeID {
        let id = FullNodeID::from(node.clone());

        if self.id2node(&id.id).is_none() {
            self.id2node.put(&id.id, &node);
        }

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

        let edge = InnerEdge {
            from: from_id,
            to: to_id,
            label: label.chars().take(MAX_LABEL_LENGTH).collect(),
        };

        self.insert_batch.push(edge);

        if self.insert_batch.len() >= store::MAX_BATCH_SIZE {
            self.commit();
        }
    }

    pub fn commit(&mut self) {
        if !self.insert_batch.is_empty() {
            self.segment.insert(&self.insert_batch);
            self.segment.flush();
            self.insert_batch.clear();
        }

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
            compression: self.compression,
        }
    }
}

pub struct Webgraph {
    pub path: String,
    segments: Vec<Segment>,
    executor: Arc<Executor>,
    id2node: Id2NodeDb,
    meta: Meta,
    compression: Compression,
}

impl Webgraph {
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

    pub fn merge(&mut self, other: Webgraph) {
        self.id2node.batch_put(other.id2node.iter());

        for segment in other.segments {
            let id = segment.id();
            let new_path = Path::new(&self.path).join("segments");
            std::fs::rename(segment.path(), &new_path.join(segment.id())).unwrap();

            self.meta.comitted_segments.push(segment.id());
            drop(segment);
            self.segments
                .push(Segment::open(new_path, id, self.compression));
        }

        self.save_metadata();
        self.id2node.flush();
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

    pub fn random_nodes(&self, num: usize) -> Vec<NodeID> {
        let mut rng = rand::thread_rng();
        let mut nodes = self.nodes().collect::<Vec<_>>();
        nodes.shuffle(&mut rng);
        nodes.into_iter().take(num).collect()
    }

    pub fn par_nodes(&self) -> impl ParallelIterator<Item = NodeID> + '_ {
        self.id2node.keys().par_bridge()
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
            graph.merge(other);
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
            graph.merge(other);
        }

        assert_eq!(
            graph.distances(Node::from("A")).get(&Node::from("C")),
            Some(&2)
        );
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
}
