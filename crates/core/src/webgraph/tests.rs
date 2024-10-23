// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

use super::query;

use super::*;

impl Webgraph {
    pub fn ingoing_edges(&self, node: Node, limit: EdgeLimit) -> Vec<Edge> {
        self.search(&query::FullBacklinksQuery::new(node).with_limit(limit))
            .unwrap_or_default()
    }

    pub fn raw_ingoing_edges(&self, node: &NodeID, limit: EdgeLimit) -> Vec<SmallEdge> {
        self.search(&query::BacklinksQuery::new(*node).with_limit(limit))
            .unwrap_or_default()
    }

    pub fn raw_ingoing_edges_with_labels(
        &self,
        node: &NodeID,
        limit: EdgeLimit,
    ) -> Vec<SmallEdgeWithLabel> {
        self.search(&query::BacklinksWithLabelsQuery::new(*node).with_limit(limit))
            .unwrap_or_default()
    }

    pub fn outgoing_edges(&self, node: Node, limit: EdgeLimit) -> Vec<Edge> {
        self.search(&query::FullForwardlinksQuery::new(node).with_limit(limit))
            .unwrap_or_default()
    }

    pub fn raw_outgoing_edges(&self, node: &NodeID, limit: EdgeLimit) -> Vec<SmallEdge> {
        self.search(&query::ForwardlinksQuery::new(*node).with_limit(limit))
            .unwrap_or_default()
    }
}

use crate::config::WebgraphGranularity;
use crate::webpage::html::links::RelFlags;

use file_store::temp::TempDir;
use proptest::prelude::*;

pub fn test_edges() -> Vec<(Node, Node, String)> {
    vec![
        (Node::from("A"), Node::from("B"), String::new()),
        (Node::from("B"), Node::from("C"), String::new()),
        (Node::from("A"), Node::from("C"), String::new()),
        (Node::from("C"), Node::from("A"), String::new()),
        (Node::from("D"), Node::from("C"), String::new()),
    ]
}

pub fn test_graph() -> (Webgraph, TempDir) {
    //     ┌-----┐
    //     │     │
    // ┌───A◄─┐  │
    // │      │  │
    // ▼      │  │
    // B─────►C◄-┘
    //        ▲
    //        │
    //        │
    //        D

    let temp_dir = crate::gen_temp_dir().unwrap();
    let mut graph = Webgraph::builder(&temp_dir, 0u64.into()).open().unwrap();

    for (from, to, label) in test_edges() {
        graph
            .insert(Edge {
                from,
                to,
                rel_flags: RelFlags::default(),
                label,
                sort_score: 0.0,
            })
            .unwrap();
    }

    graph.commit().unwrap();

    (graph, temp_dir)
}

#[test]
fn distance_calculation() {
    let (graph, _temp_dir) = test_graph();

    let distances = graph.distances(Node::from("D"), WebgraphGranularity::Page);

    assert_eq!(distances.get(&Node::from("C")), Some(&1));
    assert_eq!(distances.get(&Node::from("A")), Some(&2));
    assert_eq!(distances.get(&Node::from("B")), Some(&3));
}

#[test]
fn nonexisting_node() {
    let (graph, _temp_dir) = test_graph();
    assert_eq!(
        graph
            .distances(Node::from("E"), WebgraphGranularity::Page)
            .len(),
        0
    );
    assert_eq!(
        graph
            .reversed_distances(Node::from("E"), WebgraphGranularity::Page)
            .len(),
        0
    );
}

#[test]
fn reversed_distance_calculation() {
    let (graph, _temp_dir) = test_graph();

    let distances = graph.reversed_distances(Node::from("D"), WebgraphGranularity::Page);

    assert_eq!(distances.get(&Node::from("C")), None);
    assert_eq!(distances.get(&Node::from("A")), None);
    assert_eq!(distances.get(&Node::from("B")), None);

    let distances = graph.reversed_distances(Node::from("A"), WebgraphGranularity::Page);

    assert_eq!(distances.get(&Node::from("C")), Some(&1));
    assert_eq!(distances.get(&Node::from("D")), Some(&2));
    assert_eq!(distances.get(&Node::from("B")), Some(&2));
}

#[test]
fn merge_path() {
    let mut graphs = Vec::new();
    let temp_dir = crate::gen_temp_dir().unwrap();
    for (i, (from, to, label)) in (0..).zip([
        (Node::from("A"), Node::from("B"), String::new()),
        (Node::from("B"), Node::from("C"), String::new()),
        (Node::from("C"), Node::from("D"), String::new()),
        (Node::from("D"), Node::from("E"), String::new()),
        (Node::from("E"), Node::from("F"), String::new()),
        (Node::from("F"), Node::from("G"), String::new()),
        (Node::from("G"), Node::from("H"), String::new()),
    ]) {
        let mut graph =
            Webgraph::builder(&temp_dir.as_ref().join(format!("test_{}", i)), 0u64.into())
                .open()
                .unwrap();
        graph
            .insert(Edge {
                from,
                to,
                rel_flags: RelFlags::default(),
                label,
                sort_score: 0.0,
            })
            .unwrap();
        graph.commit().unwrap();
        graphs.push(graph);
    }

    let mut graph = graphs.pop().unwrap();

    for other in graphs {
        graph.merge(other).unwrap();
    }

    graph.optimize_read().unwrap();

    assert_eq!(
        graph
            .distances(Node::from("A"), WebgraphGranularity::Page)
            .get(&Node::from("H")),
        Some(&7)
    );

    assert_eq!(
        graph
            .reversed_distances(Node::from("H"), WebgraphGranularity::Page)
            .get(&Node::from("A")),
        Some(&7)
    );
}

#[test]
fn merge_simple() {
    let mut graphs = Vec::new();
    let temp_dir = crate::gen_temp_dir().unwrap();
    for (i, (from, to, label)) in (0..).zip(test_edges()) {
        let mut graph =
            Webgraph::builder(&temp_dir.as_ref().join(format!("test_{}", i)), 0u64.into())
                .open()
                .unwrap();
        graph
            .insert(Edge {
                from,
                to,
                rel_flags: RelFlags::default(),
                label,
                sort_score: 0.0,
            })
            .unwrap();
        graph.commit().unwrap();
        graphs.push(graph);
    }

    let mut graph = graphs.pop().unwrap();

    for other in graphs {
        graph.merge(other).unwrap();
    }

    graph.optimize_read().unwrap();

    let mut res = graph.outgoing_edges(Node::from("A"), EdgeLimit::Unlimited);
    res.sort_by(|a, b| a.to.cmp(&b.to));
    assert_eq!(res.len(), 2);

    assert_eq!(res[0].to, Node::from("B"));
    assert_eq!(res[1].to, Node::from("C"));

    let mut res = graph.outgoing_edges(Node::from("B"), EdgeLimit::Unlimited);
    res.sort_by(|a, b| a.to.cmp(&b.to));

    assert_eq!(res.len(), 1);
    assert_eq!(res[0].to, Node::from("C"));

    let mut res = graph.outgoing_edges(Node::from("C"), EdgeLimit::Unlimited);
    res.sort_by(|a, b| a.to.cmp(&b.to));

    assert_eq!(res.len(), 1);
    assert_eq!(res[0].to, Node::from("A"));

    let mut res = graph.outgoing_edges(Node::from("D"), EdgeLimit::Unlimited);
    res.sort_by(|a, b| a.to.cmp(&b.to));

    assert_eq!(res.len(), 1);
    assert_eq!(res[0].to, Node::from("C"));

    let mut res = graph.ingoing_edges(Node::from("A"), EdgeLimit::Unlimited);
    res.sort_by(|a, b| a.from.cmp(&b.from));

    assert_eq!(res.len(), 1);
    assert_eq!(res[0].from, Node::from("C"));

    let mut res = graph.ingoing_edges(Node::from("B"), EdgeLimit::Unlimited);
    res.sort_by(|a, b| a.from.cmp(&b.from));

    assert_eq!(res.len(), 1);
    assert_eq!(res[0].from, Node::from("A"));

    let mut res = graph.ingoing_edges(Node::from("C"), EdgeLimit::Unlimited);
    res.sort_by(|a, b| a.from.cmp(&b.from));

    assert_eq!(res.len(), 3);
    assert_eq!(res[0].from, Node::from("A"));
    assert_eq!(res[1].from, Node::from("B"));
    assert_eq!(res[2].from, Node::from("D"));
}

#[test]
fn merge_cycle() {
    let mut graphs = Vec::new();
    let temp_dir = crate::gen_temp_dir().unwrap();
    for (i, (from, to, label)) in (0..).zip([
        (Node::from("A"), Node::from("B"), String::new()),
        (Node::from("B"), Node::from("B"), String::new()),
        (Node::from("B"), Node::from("C"), String::new()),
        (Node::from("C"), Node::from("A"), String::new()),
    ]) {
        let mut graph =
            Webgraph::builder(&temp_dir.as_ref().join(format!("test_{}", i)), 0u64.into())
                .open()
                .unwrap();
        graph
            .insert(Edge {
                from,
                to,
                rel_flags: RelFlags::default(),
                label,
                sort_score: 0.0,
            })
            .unwrap();
        graph.commit().unwrap();
        graphs.push(graph);
    }

    let mut graph = graphs.pop().unwrap();

    for other in graphs {
        graph.merge(other).unwrap();
    }

    assert_eq!(
        graph
            .distances(Node::from("A"), WebgraphGranularity::Page)
            .get(&Node::from("C")),
        Some(&2)
    );

    graph.optimize_read().unwrap();

    assert_eq!(
        graph
            .distances(Node::from("A"), WebgraphGranularity::Page)
            .get(&Node::from("C")),
        Some(&2)
    );

    assert_eq!(
        graph
            .outgoing_edges(Node::from("A"), EdgeLimit::Unlimited)
            .len(),
        1
    );
    assert_eq!(
        graph
            .outgoing_edges(Node::from("B"), EdgeLimit::Unlimited)
            .len(),
        2
    );
    assert_eq!(
        graph
            .outgoing_edges(Node::from("C"), EdgeLimit::Unlimited)
            .len(),
        1
    );
}

#[test]
fn merge_star() {
    let mut graphs = Vec::new();
    let temp_dir = crate::gen_temp_dir().unwrap();
    for (i, (from, to, label)) in (0..).zip([
        (Node::from("A"), Node::from("B"), String::new()),
        (Node::from("A"), Node::from("C"), String::new()),
        (Node::from("A"), Node::from("D"), String::new()),
        (Node::from("A"), Node::from("E"), String::new()),
    ]) {
        let mut graph =
            Webgraph::builder(&temp_dir.as_ref().join(format!("test_{}", i)), 0u64.into())
                .open()
                .unwrap();
        graph
            .insert(Edge {
                from,
                to,
                rel_flags: RelFlags::default(),
                label,
                sort_score: 0.0,
            })
            .unwrap();
        graph.commit().unwrap();
        graphs.push(graph);
    }

    let mut graph = graphs.pop().unwrap();

    for other in graphs {
        graph.merge(other).unwrap();
    }

    graph.optimize_read().unwrap();

    assert_eq!(
        graph
            .outgoing_edges(Node::from("A"), EdgeLimit::Unlimited)
            .len(),
        4
    );

    assert_eq!(
        graph
            .ingoing_edges(Node::from("B"), EdgeLimit::Unlimited)
            .len(),
        1
    );

    assert_eq!(
        graph
            .ingoing_edges(Node::from("C"), EdgeLimit::Unlimited)
            .len(),
        1
    );

    assert_eq!(
        graph
            .ingoing_edges(Node::from("D"), EdgeLimit::Unlimited)
            .len(),
        1
    );
}

#[test]
fn merge_reverse_star() {
    let mut graphs = Vec::new();
    let temp_dir = crate::gen_temp_dir().unwrap();
    for (i, (from, to, label)) in (0..).zip([
        (Node::from("B"), Node::from("A"), String::new()),
        (Node::from("C"), Node::from("A"), String::new()),
        (Node::from("D"), Node::from("A"), String::new()),
        (Node::from("E"), Node::from("A"), String::new()),
    ]) {
        let mut graph =
            Webgraph::builder(&temp_dir.as_ref().join(format!("test_{}", i)), 0u64.into())
                .open()
                .unwrap();
        graph
            .insert(Edge {
                from,
                to,
                rel_flags: RelFlags::default(),
                label,
                sort_score: 0.0,
            })
            .unwrap();
        graph.commit().unwrap();
        graphs.push(graph);
    }

    let mut graph = graphs.pop().unwrap();

    for other in graphs {
        graph.merge(other).unwrap();
    }

    graph.optimize_read().unwrap();

    assert_eq!(
        graph
            .ingoing_edges(Node::from("A"), EdgeLimit::Unlimited)
            .len(),
        4
    );

    assert_eq!(
        graph
            .outgoing_edges(Node::from("B"), EdgeLimit::Unlimited)
            .len(),
        1
    );

    assert_eq!(
        graph
            .outgoing_edges(Node::from("C"), EdgeLimit::Unlimited)
            .len(),
        1
    );

    assert_eq!(
        graph
            .outgoing_edges(Node::from("D"), EdgeLimit::Unlimited)
            .len(),
        1
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    #[test]
    fn prop_merge(
        nodes in
        proptest::collection::vec(
            ("[a-z]", "[a-z]"), 0..100
        )
    ) {
        let mut graphs = Vec::new();
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut graph = Webgraph::builder(&temp_dir.as_ref().join(uuid::Uuid::new_v4().to_string()), 0u64.into())
            .open()
            .unwrap();
        for (from, to) in nodes.clone() {
            graph.insert(Edge {
                from: Node::new_for_test(from.as_str()),
                to: Node::new_for_test(to.as_str()),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            }).unwrap();

            if rand::random::<usize>() % 10 == 0 {
                graph.commit().unwrap();
                graphs.push(graph);
                graph = Webgraph::builder(&temp_dir.as_ref().join(uuid::Uuid::new_v4().to_string()), 0u64.into())
                    .open()
                    .unwrap();
            }
        }

        if graphs.is_empty() {
            return Ok(());
        }

        graphs.push(graph);

        let mut graph = graphs.pop().unwrap();


        for other in graphs {
            graph.merge(other).unwrap();
        }


        for (from, to) in nodes {
            if from == to {
                continue;
            }

            let from = Node::new_for_test(from.as_str());
            let to = Node::new_for_test(to.as_str());

            let outgoing = graph.outgoing_edges(from.clone(), EdgeLimit::Unlimited);
            let ingoing = graph.ingoing_edges(to.clone(), EdgeLimit::Unlimited);

            prop_assert!(outgoing.iter().any(|e| e.to == to));
            prop_assert!(ingoing.iter().any(|e| e.from == from));
        }
    }
}

fn proptest_case(nodes: &[(&str, &str)]) {
    let mut graphs = Vec::new();
    let temp_dir = crate::gen_temp_dir().unwrap();
    let mut graph = Webgraph::builder(
        &temp_dir.as_ref().join(uuid::Uuid::new_v4().to_string()),
        0u64.into(),
    )
    .open()
    .unwrap();

    for (i, (from, to)) in nodes.iter().enumerate() {
        graph
            .insert(Edge {
                from: Node::new_for_test(from),
                to: Node::new_for_test(to),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        if i % 2 == 0 {
            graph.commit().unwrap();
            graphs.push(graph);
            graph = Webgraph::builder(
                &temp_dir.as_ref().join(uuid::Uuid::new_v4().to_string()),
                0u64.into(),
            )
            .open()
            .unwrap();
        }
    }

    graphs.push(graph);

    let mut graph = graphs.pop().unwrap();

    for other in graphs {
        graph.merge(other).unwrap();
    }

    for (from, to) in nodes {
        if from == to {
            continue;
        }

        let from = Node::new_for_test(from);
        let to = Node::new_for_test(to);

        let outgoing = graph.outgoing_edges(from.clone(), EdgeLimit::Unlimited);
        let ingoing = graph.ingoing_edges(to.clone(), EdgeLimit::Unlimited);

        assert!(outgoing.iter().any(|e| e.to == to));
        assert!(ingoing.iter().any(|e| e.from == from));
    }
}

#[test]
fn merge_proptest_case1() {
    let nodes = [("k", "d"), ("k", "t"), ("y", "m")];
    proptest_case(&nodes);
}

#[test]
fn merge_proptest_case2() {
    let nodes = [("i", "k"), ("k", "g"), ("y", "m"), ("q", "r"), ("e", "g")];

    proptest_case(&nodes);
}

#[test]
fn merge_proptest_case3() {
    let nodes = [("h", "c"), ("r", "r")];

    proptest_case(&nodes);
}

#[test]
fn node_lowercase_name() {
    let n = Node::from("TEST".to_string());
    assert_eq!(n.as_str(), "test");
}

#[test]
fn host_node_cleanup() {
    let n = Node::from("https://www.example.com?test").into_host();
    assert_eq!(n.as_str(), "example.com");
}

#[test]
fn remove_protocol() {
    let n = Node::from("https://www.example.com/?test");

    assert_eq!(n.as_str(), "example.com/?test=");
}

#[test]
fn cap_label_length() {
    let temp_dir = crate::gen_temp_dir().unwrap();
    let mut graph = Webgraph::builder(
        &temp_dir.as_ref().join(uuid::Uuid::new_v4().to_string()),
        0u64.into(),
    )
    .open()
    .unwrap();

    graph
        .insert(Edge {
            from: Node::from("A"),
            to: Node::from("B"),
            rel_flags: RelFlags::default(),
            label: "a".repeat(MAX_LABEL_LENGTH + 1),
            sort_score: 0.0,
        })
        .unwrap();

    graph.commit().unwrap();

    assert_eq!(
        graph.outgoing_edges(Node::from("A"), EdgeLimit::Unlimited)[0].label,
        "a".repeat(MAX_LABEL_LENGTH)
    );
}

#[test]
fn test_edge_limits() {
    let (graph, temp_dir) = test_graph();

    assert_eq!(
        graph
            .outgoing_edges(Node::from("A"), EdgeLimit::Unlimited)
            .len(),
        2
    );

    assert_eq!(
        graph
            .outgoing_edges(Node::from("A"), EdgeLimit::Limit(1))
            .len(),
        1
    );

    let mut graphs = Vec::new();
    for (from, to, label) in &[
        (Node::from("A"), Node::from("B"), String::new()),
        (Node::from("A"), Node::from("C"), String::new()),
    ] {
        let mut graph = Webgraph::builder(
            &temp_dir.as_ref().join(uuid::Uuid::new_v4().to_string()),
            0u64.into(),
        )
        .open()
        .unwrap();
        graph
            .insert(Edge {
                from: from.clone(),
                to: to.clone(),
                rel_flags: RelFlags::default(),
                label: label.clone(),
                sort_score: 0.0,
            })
            .unwrap();
        graph.commit().unwrap();
        graphs.push(graph);
    }

    let mut graph = graphs.pop().unwrap();

    for other in graphs {
        graph.merge(other).unwrap();
    }

    assert_eq!(
        graph
            .outgoing_edges(Node::from("A"), EdgeLimit::Unlimited)
            .len(),
        2
    );

    assert_eq!(
        graph
            .outgoing_edges(Node::from("A"), EdgeLimit::Limit(1))
            .len(),
        1
    );

    graph.optimize_read().unwrap();

    assert_eq!(
        graph
            .outgoing_edges(Node::from("A"), EdgeLimit::Unlimited)
            .len(),
        2
    );

    assert_eq!(
        graph
            .outgoing_edges(Node::from("A"), EdgeLimit::Limit(1))
            .len(),
        1
    );
}

#[test]
fn test_node_normalized() {
    let n = Node::from("http://www.example.com/abc");
    assert_eq!(n.as_str(), "example.com/abc");

    let n = Node::from("http://www.example.com/abc#123");
    assert_eq!(n.as_str(), "example.com/abc");
}

#[test]
fn test_rel_flags() {
    let temp_dir = crate::gen_temp_dir().unwrap();
    let mut graph = Webgraph::builder(
        &temp_dir.as_ref().join(uuid::Uuid::new_v4().to_string()),
        0u64.into(),
    )
    .open()
    .unwrap();

    graph
        .insert(Edge {
            from: Node::from("A"),
            to: Node::from("B"),
            rel_flags: RelFlags::IS_IN_FOOTER | RelFlags::TAG,
            label: String::new(),
            sort_score: 0.0,
        })
        .unwrap();

    graph.commit().unwrap();

    assert_eq!(
        graph.raw_outgoing_edges(&Node::from("A").id(), EdgeLimit::Unlimited)[0].rel_flags,
        RelFlags::IS_IN_FOOTER | RelFlags::TAG,
    );
}

#[test]
fn test_limit_and_offset() {
    let (graph, _temp_dir) = test_graph();

    let no_offset = graph.raw_outgoing_edges(
        &Node::from("A").id(),
        EdgeLimit::LimitAndOffset {
            limit: 2,
            offset: 0,
        },
    );
    assert_eq!(no_offset.len(), 2);

    let edges = graph.raw_outgoing_edges(
        &Node::from("A").id(),
        EdgeLimit::LimitAndOffset {
            limit: 2,
            offset: 1,
        },
    );
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].to, no_offset[1].to);
}
