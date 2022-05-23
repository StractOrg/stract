use super::{Edge, Node};
use crate::webgraph::GraphStore;
use std::collections::HashMap;

type NodeName = String;
type NodeID = u64;

#[derive(Debug)]
struct InternalEdge {
    to_node: NodeID,
    label: String,
}

#[derive(Default)]
pub struct MemoryStore {
    adjacency: HashMap<NodeID, Vec<InternalEdge>>,
    reversed_adjacency: HashMap<NodeID, Vec<InternalEdge>>,
    node2id: HashMap<NodeName, NodeID>,
    id2node: Vec<NodeName>,
    next_id: NodeID,
}

impl MemoryStore {
    fn node_id_or_insert(&mut self, node: NodeName) -> NodeID {
        match self.node2id.get(&node) {
            Some(id) => *id,
            None => {
                let id = self.next_id;
                self.next_id += 1;

                self.node2id.insert(node.clone(), id);
                self.id2node.push(node);

                id
            }
        }
    }
}

impl GraphStore for MemoryStore {
    type Iter = std::vec::IntoIter<Node>;

    fn outgoing_edges(&self, node: Node) -> Vec<Edge> {
        match self.node2id.get(&node.name) {
            None => Vec::new(),
            Some(id) => self
                .adjacency
                .get(id)
                .unwrap_or(&Vec::new())
                .clone()
                .into_iter()
                .map(|internal_edge| {
                    let to_name = self
                        .id2node
                        .get(internal_edge.to_node as usize)
                        .expect("Fatal error: id2node out of sync")
                        .clone();
                    let to = Node { name: to_name };

                    Edge {
                        from: node.clone(),
                        to,
                        label: internal_edge.label.clone(),
                    }
                })
                .collect(),
        }
    }

    fn nodes(&self) -> Self::Iter {
        let nodes: Vec<Node> = self
            .id2node
            .iter()
            .map(|name| Node { name: name.clone() })
            .collect();

        nodes.into_iter()
    }

    fn insert(&mut self, edge: Edge) {
        let from_id = self.node_id_or_insert(edge.from.name);
        let to_id = self.node_id_or_insert(edge.to.name);

        self.adjacency
            .entry(from_id)
            .or_default()
            .push(InternalEdge {
                to_node: to_id,
                label: edge.label.clone(),
            });

        self.reversed_adjacency
            .entry(to_id)
            .or_default()
            .push(InternalEdge {
                to_node: from_id,
                label: edge.label,
            });
    }

    fn ingoing_edges(&self, node: Node) -> Vec<Edge> {
        match self.node2id.get(&node.name) {
            None => Vec::new(),
            Some(id) => self
                .reversed_adjacency
                .get(id)
                .unwrap_or(&Vec::new())
                .clone()
                .into_iter()
                .map(|internal_edge| {
                    let from_name = self
                        .id2node
                        .get(internal_edge.to_node as usize)
                        .expect("Fatal error: id2node out of sync")
                        .clone();
                    let from = Node { name: from_name };

                    Edge {
                        from,
                        to: node.clone(),
                        label: internal_edge.label.clone(),
                    }
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple_triangle_graph() {
        //     ┌────┐
        //     │    │
        // ┌───A◄─┐ │
        // │      │ │
        // ▼      │ │
        // B─────►C◄┘

        let mut store = MemoryStore::default();

        let a = Node {
            name: "A".to_string(),
        };
        let b = Node {
            name: "B".to_string(),
        };
        let c = Node {
            name: "C".to_string(),
        };

        store.insert(Edge {
            from: a.clone(),
            to: b.clone(),
            label: String::new(),
        });
        store.insert(Edge {
            from: b.clone(),
            to: c.clone(),
            label: String::new(),
        });
        store.insert(Edge {
            from: c.clone(),
            to: a.clone(),
            label: String::new(),
        });
        store.insert(Edge {
            from: a.clone(),
            to: c.clone(),
            label: String::new(),
        });

        let nodes: Vec<Node> = store.nodes().collect();
        assert_eq!(nodes, vec![a.clone(), b.clone(), c.clone()]);
        assert_eq!(
            store.outgoing_edges(a.clone()),
            vec![
                Edge {
                    from: a.clone(),
                    to: b.clone(),
                    label: String::new()
                },
                Edge {
                    from: a.clone(),
                    to: c.clone(),
                    label: String::new()
                },
            ]
        );
        assert_eq!(
            store.outgoing_edges(b.clone()),
            vec![Edge {
                from: b.clone(),
                to: c.clone(),
                label: String::new()
            },]
        );
        assert_eq!(
            store.ingoing_edges(c.clone()),
            vec![
                Edge {
                    from: b.clone(),
                    to: c.clone(),
                    label: String::new()
                },
                Edge {
                    from: a.clone(),
                    to: c.clone(),
                    label: String::new()
                },
            ]
        );
        assert_eq!(
            store.ingoing_edges(a.clone()),
            vec![Edge {
                from: c.clone(),
                to: a.clone(),
                label: String::new()
            },]
        );
        assert_eq!(
            store.ingoing_edges(b.clone()),
            vec![Edge {
                from: a.clone(),
                to: b.clone(),
                label: String::new()
            },]
        );
    }
}
