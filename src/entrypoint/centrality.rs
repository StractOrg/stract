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

use std::{collections::HashMap, fs::File, path::Path};

use crate::{
    ranking::centrality_store::CentralityStore,
    webgraph::{centrality::harmonic::HarmonicCentrality, WebgraphBuilder},
};

pub struct Centrality {}

impl Centrality {
    fn save<P: AsRef<Path>>(centrality: HashMap<String, f64>, output_path: P) {
        let mut centrality_store = CentralityStore::new(output_path.as_ref());

        centrality_store.append(centrality.clone().into_iter());

        let mut centralities: Vec<_> = centrality.into_iter().collect();
        centralities
            .sort_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));

        let csv_file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_path.as_ref().join("data.csv"))
            .unwrap();
        let mut wtr = csv::Writer::from_writer(csv_file);

        for (host, centrality) in centralities {
            wtr.write_record(&[host, centrality.to_string()]).unwrap();
        }

        wtr.flush().unwrap();
    }

    fn host<P: AsRef<Path>>(centrality: &HarmonicCentrality, output_path: P) {
        Self::save(
            centrality
                .host
                .clone()
                .into_iter()
                .map(|(node, centrality)| (node.name, centrality))
                .collect(),
            output_path,
        );
    }

    fn full<P: AsRef<Path>>(centrality: &HarmonicCentrality, output_path: P) {
        Self::save(
            centrality
                .full
                .clone()
                .into_iter()
                .map(|(node, centrality)| (node.name, centrality))
                .collect(),
            output_path,
        );
    }

    pub fn run<P: AsRef<Path>>(webgraph_path: P, output_path: P) {
        let graph = WebgraphBuilder::new(webgraph_path)
            .with_host_graph()
            .with_full_graph()
            .open();

        let centrality = HarmonicCentrality::calculate(&graph);

        Self::host(&centrality, output_path.as_ref().join("host"));
        Self::full(&centrality, output_path.as_ref().join("full"));
    }
}
