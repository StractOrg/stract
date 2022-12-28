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
        centrality::{online_harmonic::OnlineHarmonicCentrality, topic::TopicCentrality},
        WebgraphBuilder,
    },
};

pub fn run(
    index_path: String,
    topics_path: String,
    webgraph_path: String,
    online_harmonic_path: String,
    output_path: String,
) {
    let index = Index::open(index_path).unwrap();
    let topics = human_website_annotations::Mapper::open(topics_path).unwrap();
    let webgraph = WebgraphBuilder::new(webgraph_path).open();
    let harmonic = OnlineHarmonicCentrality::open(online_harmonic_path).unwrap();

    let centrality: TopicCentrality = TopicCentrality::build(&index, topics, webgraph, harmonic);

    centrality.save(output_path).unwrap();
}
