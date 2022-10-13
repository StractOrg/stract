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
    ranking::centrality_store::CentralityStore,
    webgraph::{
        centrality::{
            approximate_harmonic::ApproximatedHarmonicCentrality, harmonic::HarmonicCentrality,
        },
        WebgraphBuilder,
    },
};

pub struct Centrality {}

impl Centrality {
    pub fn run<P: AsRef<Path>>(webgraph_path: P, output_path: P) {
        let graph = WebgraphBuilder::new(webgraph_path)
            .read_only(true)
            .with_host_graph()
            .with_full_graph()
            .open();

        let mut store = CentralityStore::open(output_path.as_ref());

        store.approx_harmonic = ApproximatedHarmonicCentrality::new(&graph);

        let harmonic_centrality = HarmonicCentrality::calculate(&graph);

        let csv_file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_path.as_ref().join("harmonic_full.csv"))
            .unwrap();
        let mut wtr = csv::Writer::from_writer(csv_file);

        for (node, centrality) in harmonic_centrality.full {
            store.harmonic.full.insert(node.name.clone(), centrality);
            wtr.write_record(&[node.name, centrality.to_string()])
                .unwrap();
        }
        wtr.flush().unwrap();

        let csv_file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_path.as_ref().join("harmonic_host.csv"))
            .unwrap();
        let mut wtr = csv::Writer::from_writer(csv_file);
        for (node, centrality) in harmonic_centrality.host {
            store.harmonic.host.insert(node.name.clone(), centrality);
            wtr.write_record(&[node.name, centrality.to_string()])
                .unwrap();
        }
        wtr.flush().unwrap();

        store.flush();
    }
}
