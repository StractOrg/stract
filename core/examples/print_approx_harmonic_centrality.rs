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
    cmp,
    collections::{BinaryHeap, HashMap},
};

use cuely::{
    ranking::centrality_store::CentralityStore,
    webgraph::{centrality::online_harmonic::Scorer, Node, NodeID, WebgraphBuilder},
    webpage::Url,
};

struct ScoredNode {
    score: f64,
    id: u64,
}

impl PartialOrd for ScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.score.partial_cmp(&other.score)
    }
}

impl PartialEq for ScoredNode {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Ord for ScoredNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl Eq for ScoredNode {}

fn get_top_nodes(scorer: Scorer, top_n: usize, nodes: &[NodeID]) -> Vec<ScoredNode> {
    let mut top_nodes = BinaryHeap::with_capacity(top_n);

    for node in nodes {
        top_nodes.push(cmp::Reverse(ScoredNode {
            score: scorer.score(*node),
            id: node.0,
        }));

        while top_nodes.len() > top_n {
            top_nodes.pop();
        }
    }

    let mut nodes = Vec::new();
    while let Some(node) = top_nodes.pop() {
        nodes.push(node.0);
    }

    nodes
}

fn print_nodes(nodes: &[ScoredNode], id2node: &HashMap<NodeID, Node>) {
    for (i, node) in nodes.iter().rev().enumerate() {
        println!(
            "{i} \t {:.3} \t {} ",
            node.score,
            id2node.get(&(node.id.into())).unwrap().name
        );
    }
}

fn print_top_nodes(
    liked_sites: &[&str],
    top_n: usize,
    store: &CentralityStore,
    id2node: &HashMap<NodeID, Node>,
    nodes: &[NodeID],
) {
    let liked_nodes: Vec<_> = liked_sites
        .iter()
        .map(|host| Node::from_url(&Url::from(host.to_string())))
        .collect();

    let scorer = store.online_harmonic.scorer(&liked_nodes, &[]);
    let top_nodes = get_top_nodes(scorer, top_n, nodes);
    println!(
        "top {} sites for these liked sites {:?}",
        top_n, liked_sites
    );
    print_nodes(&top_nodes, id2node);

    println!();
    println!();
}

pub fn main() {
    const TOP_N: usize = 50;
    let graph = WebgraphBuilder::new("data/webgraph").read_only(true).open();
    let nodes: Vec<_> = graph.nodes().collect();
    let id2node: HashMap<_, _> = nodes
        .iter()
        .map(|id| (*id, graph.id2node(id).unwrap()))
        .collect();

    let store = CentralityStore::open("data/centrality");

    let mut proxy_nodes = Vec::new();
    for node in &store.online_harmonic.proxy_nodes {
        proxy_nodes.push(id2node.get(&node.id).unwrap().name.clone());
    }

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
        &store,
        &id2node,
        &nodes,
    );

    print_top_nodes(
        &[
            "stackoverflow.com",
            "github.com",
            "arxiv.org",
            "news.ycombinator.com",
        ],
        TOP_N,
        &store,
        &id2node,
        &nodes,
    );

    print_top_nodes(&["nature.com", "who.int"], TOP_N, &store, &id2node, &nodes);
    print_top_nodes(
        &["webmd.com", "medlineplus.gov"],
        TOP_N,
        &store,
        &id2node,
        &nodes,
    );
}
