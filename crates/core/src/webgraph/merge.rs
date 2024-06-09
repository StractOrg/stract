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
// along with this program.  If not, see <https://www.gnu.org/license

use std::{cmp::Reverse, collections::BinaryHeap};

use super::{store::NodeRange, NodeID};

pub struct MergeNode<O = ()> {
    node: NodeID,
    range: NodeRange,
    labels: std::ops::Range<u64>,
    ord: O,
}

impl MergeNode {
    pub fn new(node: NodeID, range: NodeRange, labels: std::ops::Range<u64>) -> Self {
        Self {
            node,
            range,
            labels,
            ord: (),
        }
    }
}

impl<O> MergeNode<O> {
    pub fn with_ord<O2>(self, ord: O2) -> MergeNode<O2> {
        MergeNode {
            node: self.node,
            range: self.range,
            labels: self.labels,
            ord,
        }
    }

    pub fn id(&self) -> NodeID {
        self.node
    }

    pub fn range(&self) -> &NodeRange {
        &self.range
    }

    pub fn labels(&self) -> std::ops::Range<u64> {
        self.labels.clone()
    }

    pub fn ord(&self) -> &O {
        &self.ord
    }
}

impl<O> Ord for MergeNode<O> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // nodes in the fst are sorted by their id,
        // not by their sort key
        self.node.cmp(&other.node)
    }
}
impl<O> PartialOrd for MergeNode<O> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<O> PartialEq for MergeNode<O> {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node
    }
}
impl<O> Eq for MergeNode<O> {}

#[derive(Clone, Copy)]
pub struct MergeSegmentOrd(usize);
impl MergeSegmentOrd {
    pub fn new(ord: usize) -> Self {
        Self(ord)
    }

    pub fn as_usize(&self) -> usize {
        self.0
    }
}

type MinHeap<T> = BinaryHeap<Reverse<T>>;

/// Merge multiple iterators of MergeNodes into a single iterator based on the node id
pub struct MergeIter<'a> {
    iters: MinHeap<file_store::Peekable<Box<dyn Iterator<Item = MergeNode<MergeSegmentOrd>> + 'a>>>,
}
impl<'a> MergeIter<'a> {
    pub fn new(iters: Vec<impl Iterator<Item = MergeNode> + 'a>) -> Self {
        let iters = iters
            .into_iter()
            .enumerate()
            .map(|(ord, iter)| {
                let it = Box::new(iter.map(move |node| node.with_ord(MergeSegmentOrd::new(ord))))
                    as Box<dyn Iterator<Item = _>>;

                Reverse(file_store::Peekable::new(it))
            })
            .collect();

        Self { iters }
    }

    pub fn advance(&mut self, buf: &mut Vec<MergeNode<MergeSegmentOrd>>) -> bool {
        buf.clear();

        let next = {
            let item = self.iters.peek_mut();

            if item.is_none() {
                return false;
            }

            let mut item = item.unwrap();

            if item.0.peek().is_none() {
                return false;
            }

            item.0.next().unwrap()
        };

        let node = next.node;
        buf.push(next);

        // advance all iterators that have the same node
        while let Some(mut peek) = self.iters.peek_mut() {
            if peek.0.peek().map(|x| x.node) == Some(node) {
                buf.push(peek.0.next().unwrap());
            } else {
                break;
            }
        }

        true
    }
}

/// Merge multiple iterators of NodeDatum into a single iterator based on the sort key
pub struct NodeDatumMerger<'a, L = String> {
    iters: MinHeap<file_store::Peekable<Box<dyn Iterator<Item = NodeDatum<L>> + 'a>>>,
}

impl<'a, L> NodeDatumMerger<'a, L> {
    pub fn new(iters: Vec<impl Iterator<Item = NodeDatum<L>> + 'a>) -> Self {
        let iters = iters
            .into_iter()
            .map(|iter| {
                let it = Box::new(iter) as Box<dyn Iterator<Item = _>>;
                Reverse(file_store::Peekable::new(it))
            })
            .collect();

        Self { iters }
    }
}

impl<'a, L> Iterator for NodeDatumMerger<'a, L> {
    type Item = NodeDatum<L>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iters.peek_mut().and_then(|mut item| item.0.next());

        if let Some(datum) = &res {
            while let Some(mut peek) = self.iters.peek_mut() {
                if peek.0.peek().map(|x| x.sort_key) == Some(datum.sort_key) {
                    peek.0.next().unwrap();
                } else {
                    break;
                }
            }
        }

        res
    }
}

#[derive(Debug, Clone)]
pub struct NodeDatum<L = ()> {
    id: NodeID,
    sort_key: u64,
    label: L,
}

impl<L> PartialOrd for NodeDatum<L> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<L> Ord for NodeDatum<L> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sort_key
            .cmp(&other.sort_key)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl<L> PartialEq for NodeDatum<L> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.sort_key == other.sort_key
    }
}

impl<L> Eq for NodeDatum<L> {}

impl NodeDatum {
    pub fn new(node: NodeID, sort_key: u64) -> Self {
        Self {
            id: node,
            sort_key,
            label: (),
        }
    }
}

impl<L> NodeDatum<L> {
    #[inline]
    pub fn node(&self) -> NodeID {
        self.id
    }

    #[inline]
    pub fn sort_key(&self) -> u64 {
        self.sort_key
    }

    pub fn with_label<L2>(self, label: L2) -> NodeDatum<L2> {
        NodeDatum {
            id: self.id,
            sort_key: self.sort_key,
            label,
        }
    }

    #[inline]
    pub fn label(&self) -> &L {
        &self.label
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_nodes() {
        let a = vec![
            MergeNode::new(1u64.into(), NodeRange::new(0..10, 1), 0..10),
            MergeNode::new(4u64.into(), NodeRange::new(0..10, 2), 0..10),
            MergeNode::new(5u64.into(), NodeRange::new(0..10, 3), 0..10),
        ];

        let b = vec![
            MergeNode::new(2u64.into(), NodeRange::new(0..10, 4), 0..10),
            MergeNode::new(3u64.into(), NodeRange::new(0..10, 5), 0..10),
            MergeNode::new(5u64.into(), NodeRange::new(0..10, 3), 0..10),
        ];

        let mut merger = MergeIter::new(vec![a.into_iter(), b.into_iter()]);
        let mut buf = Vec::new();

        assert!(merger.advance(&mut buf));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].id(), 1u64.into());

        assert!(merger.advance(&mut buf));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].id(), 2u64.into());

        assert!(merger.advance(&mut buf));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].id(), 3u64.into());

        assert!(merger.advance(&mut buf));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].id(), 4u64.into());

        assert!(merger.advance(&mut buf));
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0].id(), 5u64.into());
        assert_eq!(buf[1].id(), 5u64.into());

        assert!(!merger.advance(&mut buf));
    }

    #[test]
    fn test_datum_merge() {
        let a = vec![
            NodeDatum::new(1u64.into(), 1),
            NodeDatum::new(2u64.into(), 4),
            NodeDatum::new(3u64.into(), 5),
        ];

        let b = vec![
            NodeDatum::new(4u64.into(), 2),
            NodeDatum::new(5u64.into(), 3),
            NodeDatum::new(3u64.into(), 5),
        ];

        let mut merger = NodeDatumMerger::new(vec![a.into_iter(), b.into_iter()]);

        assert_eq!(merger.next().unwrap().sort_key(), 1);
        assert_eq!(merger.next().unwrap().sort_key(), 2);
        assert_eq!(merger.next().unwrap().sort_key(), 3);
        assert_eq!(merger.next().unwrap().sort_key(), 4);
        assert_eq!(merger.next().unwrap().sort_key(), 5);
        assert!(merger.next().is_none());
    }
}
