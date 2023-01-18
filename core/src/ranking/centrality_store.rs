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

use std::{fs::File, path::Path};

use crate::{
    kv::{rocksdb_store::RocksDbStore, Kv},
    webgraph::{
        centrality::{harmonic::HarmonicCentrality, online_harmonic::OnlineHarmonicCentrality},
        Webgraph,
    },
};

pub struct HarmonicCentralityStore {
    pub host: Box<dyn Kv<String, f64>>,
    pub full: Box<dyn Kv<String, f64>>,
}

impl HarmonicCentralityStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        Self {
            host: RocksDbStore::open(path.as_ref().join("host")),
            full: RocksDbStore::open(path.as_ref().join("full")),
        }
    }

    fn flush(&self) {
        self.host.flush();
        self.full.flush();
    }
}

pub struct CentralityStore {
    pub harmonic: HarmonicCentralityStore,
    pub online_harmonic: OnlineHarmonicCentrality,
    pub base_path: String,
}

impl CentralityStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        Self {
            harmonic: HarmonicCentralityStore::open(path.as_ref().join("harmonic")),
            online_harmonic: OnlineHarmonicCentrality::open(path.as_ref().join("online_harmonic"))
                .ok()
                .unwrap_or_default(),
            base_path: path.as_ref().to_str().unwrap().to_string(),
        }
    }

    fn store_host<P: AsRef<Path>>(
        output_path: P,
        store: &mut CentralityStore,
        harmonic_centrality: &HarmonicCentrality,
    ) {
        let csv_file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_path.as_ref().join("harmonic_host.csv"))
            .unwrap();

        let mut host: Vec<_> = harmonic_centrality.host.clone().into_iter().collect();
        host.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let mut wtr = csv::Writer::from_writer(csv_file);
        for (node, centrality) in host {
            store.harmonic.host.insert(node.name.clone(), centrality);
            wtr.write_record(&[node.name, centrality.to_string()])
                .unwrap();
        }
        wtr.flush().unwrap();
    }

    pub fn build<P: AsRef<Path>>(graph: &Webgraph, output_path: P) -> Self {
        let mut store = CentralityStore::open(output_path.as_ref());

        let harmonic_centrality = HarmonicCentrality::calculate(graph);

        Self::store_host(&output_path, &mut store, &harmonic_centrality);
        store.online_harmonic = OnlineHarmonicCentrality::new(graph, &harmonic_centrality);

        store.flush();

        store
    }

    pub fn flush(&self) {
        self.harmonic.flush();

        self.online_harmonic
            .save(Path::new(&self.base_path).join("online_harmonic"))
            .unwrap();
    }
}
