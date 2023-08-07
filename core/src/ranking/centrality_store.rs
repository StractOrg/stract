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

use tracing::debug;
use tracing::log::trace;

use crate::{
    kv::{rocksdb_store::RocksDbStore, Kv},
    webgraph::{centrality::harmonic::HarmonicCentrality, NodeID, Webgraph},
};

use super::inbound_similarity::InboundSimilarity;

pub type HarmonicCentralityStore = Box<dyn Kv<NodeID, f64> + Send + Sync>;

pub struct IndexerCentralityStore {
    pub harmonic: HarmonicCentralityStore,
}

impl IndexerCentralityStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        Self {
            harmonic: RocksDbStore::open(path.as_ref().join("harmonic")),
        }
    }
}

impl From<CentralityStore> for IndexerCentralityStore {
    fn from(store: CentralityStore) -> Self {
        Self {
            harmonic: store.harmonic,
        }
    }
}

pub struct SearchCentralityStore {
    pub inbound_similarity: InboundSimilarity,
}

impl SearchCentralityStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        Self {
            inbound_similarity: InboundSimilarity::open(path.as_ref().join("inbound_similarity"))
                .unwrap(),
        }
    }
}

impl From<CentralityStore> for SearchCentralityStore {
    fn from(store: CentralityStore) -> Self {
        Self {
            inbound_similarity: store.inbound_similarity,
        }
    }
}

pub struct CentralityStore {
    pub harmonic: HarmonicCentralityStore,
    pub inbound_similarity: InboundSimilarity,
    pub base_path: String,
}

impl CentralityStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        Self {
            harmonic: RocksDbStore::open(path.as_ref().join("harmonic")),
            inbound_similarity: InboundSimilarity::open(path.as_ref().join("inbound_similarity"))
                .ok()
                .unwrap_or_default(),
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
    }
}
