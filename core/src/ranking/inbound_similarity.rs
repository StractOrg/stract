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

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::{
    intmap::IntMap,
    webgraph::{centrality::harmonic::HarmonicCentrality, Webgraph},
    Result,
};

use super::bitvec_similarity;
const HARMONIC_CENTRALITY_THRESHOLD: f64 = 0.025;

#[derive(Serialize, Deserialize, Default)]
pub struct InboundSimilarity {
    vectors: IntMap<bitvec_similarity::BitVec>,
}

impl InboundSimilarity {
    pub fn build(graph: &Webgraph, harmonic: &HarmonicCentrality) -> Self {
        let mut vectors = IntMap::new();
        let nodes: Vec<_> = graph.nodes().collect();

        if let Some(max_node) = nodes.iter().max().copied() {
            for node_id in nodes {
                let mut buf = vec![false; max_node.0 as usize];

                for edge in graph
                    .raw_ingoing_edges(&node_id)
                    .into_iter()
                    .filter(|edge| match graph.id2node(&edge.from) {
                        Some(node) => {
                            let score = *harmonic.host.get(&node.into_host()).unwrap_or(&0.0);
                            score >= HARMONIC_CENTRALITY_THRESHOLD
                        }
                        None => false,
                    })
                {
                    buf[edge.from.0 as usize] = true;
                }

                vectors.insert(node_id.0, bitvec_similarity::BitVec::new(buf));
            }
        }

        Self { vectors }
    }

    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut file = BufWriter::new(
            File::options()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)?,
        );

        bincode::serialize_into(&mut file, &self)?;

        Ok(())
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;

        Ok(bincode::deserialize(&buf)?)
    }
}
