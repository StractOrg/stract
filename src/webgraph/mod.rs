mod memory_store;
mod sled_store;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Node {
    name: String,
}

pub trait GraphStore {
    type Iter: Iterator<Item = Node>;

    fn outgoing_edges(&self, node: Node) -> Vec<Edge>;
    fn nodes(&self) -> Self::Iter;
    fn insert(&mut self, edge: Edge);
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Edge {
    from: Node,
    to: Node,
    label: String,
}

pub struct WebGraph<S: GraphStore> {
    internal_store: S,
}
