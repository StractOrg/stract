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

use stract::{
    ranking::inbound_similarity::InboundSimilarity,
    webgraph::{Node, WebgraphBuilder},
};

pub fn main() {
    let graph = WebgraphBuilder::new("data/webgraph").open();
    let inbound = InboundSimilarity::open("data/centrality/inbound_similarity").unwrap();

    for host in ["www.homedepot.com"] {
        println!("{host}:");

        let node = Node::from(host).into_host();
        let node_id = graph.node2id(&node).unwrap();
        let inbound_vec = inbound.get(&node_id).unwrap();
        println!("{:?}", inbound_vec);

        for edge in graph.ingoing_edges(node) {
            println!("{} -> {} ({})", edge.from.name, edge.to.name, edge.label);
        }

        println!();
    }
}
