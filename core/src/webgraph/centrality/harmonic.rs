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

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Mutex},
};

use anyhow::Result;
use std::sync::atomic::Ordering;

use bitvec::vec::BitVec;
use tracing::info;

use crate::{
    hyperloglog::HyperLogLog,
    kahan_sum::KahanSum,
    webgraph::{NodeID, Webgraph},
};

const HYPERLOGLOG_COUNTERS: usize = 64;

#[derive(Clone)]
struct JankyBloomFilter {
    bit_vec: BitVec,
    num_bits: u64,
}

impl JankyBloomFilter {
    pub fn new(estimated_items: u64, fp: f64) -> Self {
        let num_bits = Self::num_bits(estimated_items, fp);
        Self {
            bit_vec: BitVec::repeat(false, num_bits as usize),
            num_bits,
        }
    }

    fn num_bits(estimated_items: u64, fp: f64) -> u64 {
        ((estimated_items as f64) * fp.ln() / (-8.0 * 2.0_f64.ln().powi(2))).ceil() as u64
    }

    fn hash(item: &u64) -> usize {
        item.wrapping_mul(11400714819323198549) as usize
    }

    pub fn insert(&mut self, item: u64) {
        let h = Self::hash(&item);
        self.bit_vec.set(h % self.num_bits as usize, true);
    }

    pub fn contains(&self, item: &u64) -> bool {
        let h = Self::hash(item);
        self.bit_vec[h % self.num_bits as usize]
    }

    pub fn estimate_card(&self) -> u64 {
        let num_ones = self.bit_vec.count_ones() as u64;

        if num_ones == 0 || self.num_bits == 0 {
            return 0;
        }

        if num_ones == self.num_bits {
            return u64::MAX;
        }

        (-(self.num_bits as i64) * (1.0 - (num_ones as f64) / (self.num_bits as f64)).ln() as i64)
            .try_into()
            .unwrap_or_default()
    }
}

struct Db<K, V>
where
    K: serde::Serialize + serde::de::DeserializeOwned,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    db: rocksdb::DB,
    path: PathBuf,
    _cache: rocksdb::Cache,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Db<K, V>
where
    K: serde::Serialize + serde::de::DeserializeOwned,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        if !path.as_ref().exists() {
            fs::create_dir_all(path.as_ref())?;
        }

        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);

        options.set_max_background_jobs(8);
        options.increase_parallelism(8);
        options.set_max_subcompactions(8);

        let cache = rocksdb::Cache::new_lru_cache(20 * 1024 * 1024 * 1024); // 20 gb

        // some recommended settings (https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
        options.set_level_compaction_dynamic_level_bytes(true);
        options.set_bytes_per_sync(1048576);
        let mut block_options = rocksdb::BlockBasedOptions::default();
        block_options.set_block_size(16 * 1024);
        block_options.set_format_version(5);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);

        block_options.set_ribbon_filter(10.0);
        block_options.set_block_cache(&cache);

        options.set_block_based_table_factory(&block_options);
        options.set_compression_type(rocksdb::DBCompressionType::None);

        options.optimize_for_point_lookup(512); // 512 MB

        let db = rocksdb::DB::open(&options, &path)?;

        Ok(Self {
            db,
            path: path.as_ref().to_path_buf(),
            _cache: cache,
            _phantom: std::marker::PhantomData,
        })
    }

    fn get(&self, key: &K) -> Option<V> {
        let key_bytes = bincode::serialize(key).expect("failed to serialize key");

        self.db
            .get(key_bytes)
            .expect("failed to retrieve key")
            .map(|bytes| bincode::deserialize(&bytes).expect("failed to deserialize stored value"))
    }

    fn insert(&self, key: &K, value: &V) {
        let key = bincode::serialize(key).expect("failed to serialize key");
        let value = bincode::serialize(value).expect("failed to serialize value");

        self.db.put(key, value).expect("failed to insert key");
    }

    fn iter(&self) -> impl Iterator<Item = (K, V)> + '_ {
        self.db.iterator(rocksdb::IteratorMode::Start).map(|s| {
            let (key, value) = s.expect("failed to iterate over db");
            (
                bincode::deserialize(&key).expect("failed to deserialize key"),
                bincode::deserialize(&value).expect("failed to deserialize value"),
            )
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

fn calculate_centrality<P: AsRef<Path>>(
    graph: &Webgraph,
    calculation_path: P,
) -> BTreeMap<NodeID, f64> {
    let mut num_nodes = 0;

    let mut counters: Db<NodeID, HyperLogLog<HYPERLOGLOG_COUNTERS>> = Db::open(
        calculation_path
            .as_ref()
            .join(uuid::Uuid::new_v4().to_string()),
    )
    .unwrap();

    let mut centralities: BTreeMap<NodeID, KahanSum> = BTreeMap::new();

    for node in graph.nodes() {
        let mut counter = HyperLogLog::default();
        counter.add(node.bit_64());

        counters.insert(&node, &counter);
        centralities.insert(node, KahanSum::default());

        num_nodes += 1;
    }

    let mut changed_nodes = JankyBloomFilter::new(num_nodes as u64, 0.05);

    for node in graph.nodes() {
        changed_nodes.insert(node.bit_64());
    }

    info!("Found {} nodes in the graph", num_nodes);
    let exact_counting_threshold = (num_nodes as f64).sqrt().max(0.0).round() as u64;
    let norm_factor = (num_nodes - 1) as f64;

    let mut exact_counting = false;
    let has_changes = AtomicBool::new(true);
    let mut t = 0;

    let mut exact_changed_nodes: BTreeSet<NodeID> = BTreeSet::default();

    loop {
        if !has_changes.load(Ordering::Relaxed) {
            break;
        }

        let new_counters: Db<_, _> = Db::open(
            calculation_path
                .as_ref()
                .join(uuid::Uuid::new_v4().to_string()),
        )
        .unwrap();

        for (node, counter) in counters.iter() {
            new_counters.insert(&node, &counter);
        }

        has_changes.store(false, Ordering::Relaxed);
        let new_changed_nodes = Mutex::new(JankyBloomFilter::new(num_nodes as u64, 0.05));

        if !exact_changed_nodes.is_empty()
            && exact_changed_nodes.len() as u64 <= exact_counting_threshold
        {
            let mut new_exact_changed_nodes = BTreeSet::default();

            exact_changed_nodes.iter().for_each(|changed_node| {
                for edge in graph.raw_outgoing_edges(changed_node) {
                    if let (Some(mut counter_to), Some(counter_from)) =
                        (new_counters.get(&edge.to), counters.get(&edge.from))
                    {
                        if counter_to
                            .registers()
                            .iter()
                            .zip(counter_from.registers().iter())
                            .any(|(to, from)| *from > *to)
                        {
                            counter_to.merge(&counter_from);
                            new_counters.insert(&edge.to, &counter_to);
                            new_changed_nodes.lock().unwrap().insert(edge.to.bit_64());

                            new_exact_changed_nodes.insert(edge.to);

                            has_changes.store(true, Ordering::Relaxed);
                        }
                    }
                }
            });

            exact_changed_nodes = new_exact_changed_nodes;
        } else {
            exact_changed_nodes = BTreeSet::default();
            graph.edges().for_each(|edge| {
                if changed_nodes.contains(&edge.from.bit_64()) {
                    if let (Some(mut counter_to), Some(counter_from)) =
                        (new_counters.get(&edge.to), counters.get(&edge.from))
                    {
                        if counter_to
                            .registers()
                            .iter()
                            .zip(counter_from.registers().iter())
                            .any(|(to, from)| *from > *to)
                        {
                            counter_to.merge(&counter_from);
                            new_counters.insert(&edge.to, &counter_to);
                            new_changed_nodes.lock().unwrap().insert(edge.to.bit_64());

                            if exact_counting {
                                exact_changed_nodes.insert(edge.to);
                            }

                            has_changes.store(true, Ordering::Relaxed);
                        }
                    }
                }
            })
        }

        centralities.iter_mut().for_each(|(node, score)| {
            *score += new_counters
                .get(node)
                .map(|counter| counter.size())
                .unwrap_or_default()
                .checked_sub(
                    counters
                        .get(node)
                        .map(|counter| counter.size())
                        .unwrap_or_default(),
                )
                .unwrap_or_default() as f64
                / (t + 1) as f64;
        });

        counters = new_counters;
        let old_path = counters.path().to_path_buf();
        changed_nodes = new_changed_nodes.into_inner().unwrap();
        std::fs::remove_dir_all(old_path).unwrap();
        t += 1;

        if changed_nodes.estimate_card() <= exact_counting_threshold {
            exact_counting = true;
        }
    }

    let res = centralities
        .into_iter()
        .map(|(node_id, sum)| (node_id, f64::from(sum)))
        .filter(|(_, centrality)| *centrality > 0.0)
        .map(|(node_id, centrality)| (node_id, centrality / norm_factor))
        .collect();

    info!("Harmonic centrality calculated");

    res
}

pub struct HarmonicCentrality(BTreeMap<NodeID, f64>);

impl HarmonicCentrality {
    pub fn calculate<P: AsRef<Path>>(graph: &Webgraph, path: P) -> Self {
        Self(calculate_centrality(graph, path))
    }

    pub fn get(&self, node: &NodeID) -> Option<f64> {
        self.0.get(node).copied()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&NodeID, f64)> {
        self.0.iter().map(|(node, centrality)| (node, *centrality))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webgraph::{Node, WebgraphWriter};

    fn test_edges() -> Vec<(Node, Node, String)> {
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
        vec![
            (Node::from("A"), Node::from("B"), String::new()),
            (Node::from("B"), Node::from("C"), String::new()),
            (Node::from("A"), Node::from("C"), String::new()),
            (Node::from("C"), Node::from("A"), String::new()),
            (Node::from("D"), Node::from("C"), String::new()),
        ]
    }

    fn test_graph() -> Webgraph {
        let mut writer = WebgraphWriter::new(
            crate::gen_temp_path(),
            crate::executor::Executor::single_thread(),
            crate::webgraph::Compression::default(),
        );

        for (from, to, label) in test_edges() {
            writer.insert(from, to, label);
        }

        writer.finalize()
    }

    #[test]
    fn host_harmonic_centrality() {
        let mut writer = WebgraphWriter::new(
            crate::gen_temp_path(),
            crate::executor::Executor::single_thread(),
            crate::webgraph::Compression::default(),
        );

        writer.insert(
            Node::from("A.com/1").into_host(),
            Node::from("A.com/2").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/1").into_host(),
            Node::from("A.com/3").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/1").into_host(),
            Node::from("A.com/4").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/2").into_host(),
            Node::from("A.com/1").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/2").into_host(),
            Node::from("A.com/3").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/2").into_host(),
            Node::from("A.com/4").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/3").into_host(),
            Node::from("A.com/1").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/3").into_host(),
            Node::from("A.com/2").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/3").into_host(),
            Node::from("A.com/4").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/4").into_host(),
            Node::from("A.com/1").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/4").into_host(),
            Node::from("A.com/2").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/4").into_host(),
            Node::from("A.com/3").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("C.com").into_host(),
            Node::from("B.com").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("D.com").into_host(),
            Node::from("B.com").into_host(),
            String::new(),
        );

        let graph = writer.finalize();

        let centrality = HarmonicCentrality::calculate(&graph, crate::gen_temp_path());

        assert!(
            centrality.get(&Node::from("B.com").id()).unwrap()
                > centrality.get(&Node::from("A.com").id()).unwrap_or(0.0)
        );
    }

    #[test]
    fn harmonic_centrality() {
        let graph = test_graph();
        let centrality = HarmonicCentrality::calculate(&graph, crate::gen_temp_path());

        assert!(
            centrality.get(&Node::from("C").id()).unwrap()
                > centrality.get(&Node::from("A").id()).unwrap()
        );
        assert!(
            centrality.get(&Node::from("A").id()).unwrap()
                > centrality.get(&Node::from("B").id()).unwrap()
        );
        assert_eq!(centrality.get(&Node::from("D").id()), None);
    }

    #[test]
    fn additional_edges_ignored() {
        let graph = test_graph();
        let centrality = HarmonicCentrality::calculate(&graph, crate::gen_temp_path());

        let mut other = WebgraphWriter::new(
            crate::gen_temp_path(),
            crate::executor::Executor::single_thread(),
            crate::webgraph::Compression::default(),
        );

        for (from, to, label) in test_edges() {
            other.insert(from, to, label);
        }

        other.insert(Node::from("A"), Node::from("B"), "1".to_string());
        other.commit();
        other.insert(Node::from("A"), Node::from("B"), "2".to_string());
        other.commit();
        other.insert(Node::from("A"), Node::from("B"), "3".to_string());
        other.commit();
        other.insert(Node::from("A"), Node::from("B"), "4".to_string());
        other.commit();
        other.insert(Node::from("A"), Node::from("B"), "5".to_string());
        other.commit();
        other.insert(Node::from("A"), Node::from("B"), "6".to_string());
        other.commit();
        other.insert(Node::from("A"), Node::from("B"), "7".to_string());
        other.commit();

        let graph = other.finalize();

        let centrality_extra = HarmonicCentrality::calculate(&graph, crate::gen_temp_path());

        assert_eq!(centrality.0, centrality_extra.0);
    }
}
