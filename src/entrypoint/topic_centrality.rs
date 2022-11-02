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

use crate::{
    human_website_annotations,
    index::Index,
    webgraph::{
        centrality::{
            approximate_harmonic::ApproximatedHarmonicCentrality, topic::TopicCentrality,
        },
        WebgraphBuilder,
    },
};

pub fn run(
    index_path: String,
    topics_path: String,
    webgraph_path: String,
    approximate_harmonic_path: String,
    output_path: String,
) {
    let index = Index::open(index_path).unwrap();
    let topics = human_website_annotations::Mapper::open(topics_path).unwrap();
    let webgraph = WebgraphBuilder::new(webgraph_path).with_host_graph().open();
    let approx = ApproximatedHarmonicCentrality::open(approximate_harmonic_path).unwrap();

    let centrality: TopicCentrality<50> = TopicCentrality::build(index, topics, webgraph, approx);

    centrality.save(output_path).unwrap();
}
