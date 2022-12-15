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

use std::path::Path;

use crate::{ranking::centrality_store::CentralityStore, webgraph::WebgraphBuilder};

pub struct Centrality {}

impl Centrality {
    pub fn run<P: AsRef<Path>>(webgraph_path: P, output_path: P) {
        let graph = WebgraphBuilder::new(webgraph_path)
            .read_only(true)
            .with_host_graph()
            .open();

        CentralityStore::build(&graph, output_path);
    }
}
