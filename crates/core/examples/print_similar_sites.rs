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

use stract_core::{
    ranking::inbound_similarity::InboundSimilarity,
    similar_sites::{ScoredNode, SimilarSitesFinder},
};
use webgraph::WebgraphBuilder;

fn print_nodes(nodes: &[ScoredNode]) {
    for (i, node) in nodes.iter().enumerate() {
        println!("{i} \t {:.3} \t {} ", node.score, node.node.name);
    }
}

fn print_top_nodes(liked_sites: &[&str], top_n: usize, similarity_finder: &SimilarSitesFinder) {
    let sites: Vec<_> = liked_sites.iter().map(|s| s.to_string()).collect();
    let top_nodes = similarity_finder.find_similar_sites(&sites, top_n);

    println!("top {top_n} sites for these liked sites {liked_sites:?}");
    print_nodes(&top_nodes);

    println!();
    println!();
}

pub fn main() {
    const TOP_N: usize = 50;
    let graph = WebgraphBuilder::new("data/webgraph".as_ref()).open();
    let inbound_similarity =
        InboundSimilarity::open("data/centrality/inbound_similarity".as_ref()).unwrap();

    let similarity_finder = SimilarSitesFinder::new(
        graph.into(),
        inbound_similarity,
        stract_config::defaults::WebgraphServer::max_similar_sites(),
    );

    print_top_nodes(
        &[
            "dr.dk",
            "berlingske.dk",
            "ekstrabladet.dk",
            "politikken.dk",
            "tv2.dk",
            "seoghoer.dk",
            "zetland.dk",
        ],
        TOP_N,
        &similarity_finder,
    );

    print_top_nodes(
        &[
            "stackoverflow.com",
            "github.com",
            "arxiv.org",
            "news.ycombinator.com",
        ],
        TOP_N,
        &similarity_finder,
    );

    print_top_nodes(&["nature.com", "who.int"], TOP_N, &similarity_finder);

    print_top_nodes(&["webmd.com", "medlineplus.gov"], TOP_N, &similarity_finder);
}
