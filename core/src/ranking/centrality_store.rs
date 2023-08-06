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

use std::{fs::File, path::Path};

use rocksdb::BlockBasedOptions;
use tracing::debug;
use tracing::log::trace;

use crate::{
    kv::{rocksdb_store::RocksDbStore, Kv},
    webgraph::{centrality::harmonic::HarmonicCentrality, Node, NodeID, Webgraph},
};

use super::inbound_similarity::InboundSimilarity;

pub struct Node2Id {
    db: rocksdb::DB,
    // from rocksdb docs: `Cache must outlive DB instance which uses it.`
    #[allow(dead_code)]
    cache: rocksdb::Cache,
}

impl Node2Id {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.increase_parallelism(8);
        options.set_write_buffer_size(256 * 1024 * 1024); // 256 MB memtable
        options.set_max_write_buffer_number(8);

        let mut block_options = BlockBasedOptions::default();
        block_options.set_ribbon_filter(10.0);

        let cache = rocksdb::Cache::new_lru_cache(256 * 1024 * 1024).unwrap(); // 256 MB cache
        block_options.set_block_cache(&cache);

        options.set_block_based_table_factory(&block_options);

        let db = rocksdb::DB::open(&options, path.as_ref().to_str().unwrap()).unwrap();

        Self { db, cache }
    }

    pub fn get(&self, key: &Node) -> Option<NodeID> {
        let bytes = bincode::serialize(key).unwrap();

        self.db
            .get(bytes)
            .unwrap()
            .map(|bytes| bincode::deserialize(&bytes).unwrap())
    }

    pub fn put(&self, key: &Node, value: &NodeID) {
        let key_bytes = bincode::serialize(key).unwrap();
        let value_bytes = bincode::serialize(value).unwrap();

        self.db.put(key_bytes, value_bytes).unwrap();
    }

    pub fn batch_put(&self, it: impl Iterator<Item = (Node, NodeID)>) {
        let mut batch = rocksdb::WriteBatch::default();

        for (key, value) in it {
            let key_bytes = bincode::serialize(&key).unwrap();
            let value_bytes = bincode::serialize(&value).unwrap();

            batch.put(key_bytes, value_bytes);
        }

        self.db.write(batch).unwrap();
    }

    pub fn contains(&self, key: &Node) -> bool {
        let bytes = bincode::serialize(key).unwrap();

        self.db.get(bytes).unwrap().is_some()
    }

    pub fn nodes(&self) -> impl Iterator<Item = Node> + '_ {
        let mut read_opts = rocksdb::ReadOptions::default();

        read_opts.set_readahead_size(4_194_304); // 4 MB

        self.db
            .iterator_opt(rocksdb::IteratorMode::Start, read_opts)
            .map(|res| {
                let (key, _) = res.unwrap();
                bincode::deserialize(&key).unwrap()
            })
    }

    pub fn ids(&self) -> impl Iterator<Item = NodeID> + '_ {
        let mut read_opts = rocksdb::ReadOptions::default();

        read_opts.set_readahead_size(4_194_304); // 4 MB

        self.db
            .iterator_opt(rocksdb::IteratorMode::Start, read_opts)
            .map(|res| {
                let (_, val) = res.unwrap();
                bincode::deserialize(&val).unwrap()
            })
    }

    pub fn iter(&self) -> impl Iterator<Item = (Node, NodeID)> + '_ {
        let mut read_opts = rocksdb::ReadOptions::default();

        read_opts.set_readahead_size(4_194_304); // 4 MB

        self.db
            .iterator_opt(rocksdb::IteratorMode::Start, read_opts)
            .map(|res| {
                let (key, val) = res.unwrap();
                (
                    bincode::deserialize(&key).unwrap(),
                    bincode::deserialize(&val).unwrap(),
                )
            })
    }

    pub fn flush(&self) {
        self.db.flush().unwrap();
    }
}

pub type HarmonicCentralityStore = Box<dyn Kv<NodeID, f64> + Send + Sync>;

pub struct IndexerCentralityStore {
    pub harmonic: HarmonicCentralityStore,
    pub node2id: Node2Id,
}

impl IndexerCentralityStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        Self {
            harmonic: RocksDbStore::open(path.as_ref().join("harmonic")),
            node2id: Node2Id::open(path.as_ref().join("node2id")),
        }
    }
}

impl From<CentralityStore> for IndexerCentralityStore {
    fn from(store: CentralityStore) -> Self {
        Self {
            node2id: store.node2id,
            harmonic: store.harmonic,
        }
    }
}

pub struct SearchCentralityStore {
    pub inbound_similarity: InboundSimilarity,
    pub node2id: Node2Id,
}

impl SearchCentralityStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        Self {
            inbound_similarity: InboundSimilarity::open(path.as_ref().join("inbound_similarity"))
                .unwrap(),
            node2id: Node2Id::open(path.as_ref().join("node2id")),
        }
    }
}

impl From<CentralityStore> for SearchCentralityStore {
    fn from(store: CentralityStore) -> Self {
        Self {
            inbound_similarity: store.inbound_similarity,
            node2id: store.node2id,
        }
    }
}

pub struct CentralityStore {
    pub harmonic: HarmonicCentralityStore,
    pub inbound_similarity: InboundSimilarity,
    pub node2id: Node2Id,
    pub base_path: String,
}

impl CentralityStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        Self {
            harmonic: RocksDbStore::open(path.as_ref().join("harmonic")),
            inbound_similarity: InboundSimilarity::open(path.as_ref().join("inbound_similarity"))
                .ok()
                .unwrap_or_default(),
            node2id: Node2Id::open(path.as_ref().join("node2id")),
            base_path: path.as_ref().to_str().unwrap().to_string(),
        }
    }

    fn store_harmonic<P: AsRef<Path>>(
        output_path: P,
        store: &mut CentralityStore,
        harmonic_centrality: HarmonicCentrality,
        graph: &Webgraph,
    ) {
        let csv_file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_path.as_ref().join("harmonic.csv"))
            .unwrap();

        for (node_id, centrality) in harmonic_centrality.iter() {
            store.harmonic.insert(*node_id, centrality);
        }
        store.harmonic.flush();

        let mut harmonic: Vec<_> = harmonic_centrality
            .iter()
            .map(|(node, centrality)| (*node, centrality))
            .take(1_000_000)
            .collect();

        harmonic.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let mut wtr = csv::Writer::from_writer(csv_file);
        for (node_id, centrality) in harmonic {
            let node = graph.id2node(&node_id).unwrap();

            wtr.write_record(&[node.name, centrality.to_string()])
                .unwrap();
        }
        wtr.flush().unwrap();
    }

    pub fn build<P: AsRef<Path>>(graph: &Webgraph, output_path: P) -> Self {
        Self::build_harmonic(graph, &output_path);
        Self::build_similarity(graph, &output_path)
    }

    pub fn build_harmonic<P: AsRef<Path>>(graph: &Webgraph, output_path: P) -> Self {
        let mut store = CentralityStore::open(output_path.as_ref());

        store.node2id.batch_put(graph.node_ids());
        let harmonic_centrality = HarmonicCentrality::calculate(graph);
        Self::store_harmonic(&output_path, &mut store, harmonic_centrality, graph);

        store.flush();
        store
    }

    pub fn build_similarity<P: AsRef<Path>>(graph: &Webgraph, output_path: P) -> Self {
        let mut store = CentralityStore::open(output_path.as_ref());

        debug!("Begin inbound similarity index construction");
        store.inbound_similarity = InboundSimilarity::build(graph, &store.harmonic);

        store.flush();
        store
    }

    pub fn flush(&self) {
        trace!("flushing");
        self.harmonic.flush();

        trace!("saving inbound similarity");
        self.inbound_similarity
            .save(Path::new(&self.base_path).join("inbound_similarity"))
            .unwrap();

        trace!("saving node2id");
        self.node2id.flush();
    }
}
