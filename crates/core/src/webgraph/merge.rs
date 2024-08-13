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

use super::{store::EdgeRange, NodeID, StoredEdge};
use std::hash::Hash;

#[derive(Debug)]
pub struct MergeNode<O = ()> {
    node: NodeID,
    range: EdgeRange,
    labels: std::ops::Range<u64>,
    ord: O,
}

impl MergeNode {
    pub fn new(node: NodeID, range: EdgeRange, labels: std::ops::Range<u64>) -> Self {
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

    pub fn range(&self) -> &EdgeRange {
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

#[derive(Debug, Clone, Copy)]
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
        let mut heap = MinHeap::new();

        for item in iters.into_iter().enumerate().map(|(ord, iter)| {
            let it = Box::new(iter.map(move |node| node.with_ord(MergeSegmentOrd::new(ord))))
                as Box<dyn Iterator<Item = _>>;

            Reverse(file_store::Peekable::new(it))
        }) {
            heap.push(item);
        }

        Self { iters: heap }
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
pub struct EdgeMerger<'a, L = String> {
    iters: MinHeap<file_store::Peekable<Box<dyn Iterator<Item = StoredEdge<L>> + 'a>>>,
}

impl<'a, L> EdgeMerger<'a, L> {
    pub fn new(iters: Vec<impl Iterator<Item = StoredEdge<L>> + 'a>) -> Self {
        let mut heap = MinHeap::new();
        for item in iters.into_iter().map(|iter| {
            let it = Box::new(iter) as Box<dyn Iterator<Item = _>>;
            Reverse(file_store::Peekable::new(it))
        }) {
            heap.push(item);
        }

        Self { iters: heap }
    }
}

impl<'a, L> Iterator for EdgeMerger<'a, L> {
    type Item = StoredEdge<L>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iters.peek_mut().and_then(|mut item| item.0.next());

        if let Some(edge) = &res {
            while let Some(mut peek) = self.iters.peek_mut() {
                if peek.0.peek().map(|x| x.other.node()) == Some(edge.other.node()) {
                    peek.0.next().unwrap();
                } else {
                    break;
                }
            }
        } else {
            debug_assert!(self.iters.iter().all(|p| p.0.peek().is_none()));
        }

        res
    }
}

impl<L> Ord for StoredEdge<L> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.other.cmp(&other.other)
    }
}

impl<L> PartialOrd for StoredEdge<L> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<L> PartialEq for StoredEdge<L> {
    fn eq(&self, other: &Self) -> bool {
        self.other == other.other
    }
}

impl<L> Eq for StoredEdge<L> {}

impl<L> Hash for StoredEdge<L> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.other.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use crate::{webgraph::NodeDatum, webpage::html::links::RelFlags};
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn test_merge_nodes() {
        let a = vec![
            MergeNode::new(1u64.into(), EdgeRange::new(0..10, 1), 0..10),
            MergeNode::new(4u64.into(), EdgeRange::new(0..10, 2), 0..10),
            MergeNode::new(5u64.into(), EdgeRange::new(0..10, 3), 0..10),
        ];

        let b = vec![
            MergeNode::new(2u64.into(), EdgeRange::new(0..10, 4), 0..10),
            MergeNode::new(3u64.into(), EdgeRange::new(0..10, 5), 0..10),
            MergeNode::new(5u64.into(), EdgeRange::new(0..10, 3), 0..10),
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
    fn test_merge_nodes_unequal_len1() {
        let a = vec![
            MergeNode::new(1u64.into(), EdgeRange::new(0..10, 1), 0..10),
            MergeNode::new(4u64.into(), EdgeRange::new(0..10, 2), 0..10),
            MergeNode::new(5u64.into(), EdgeRange::new(0..10, 3), 0..10),
        ];

        let b = vec![MergeNode::new(2u64.into(), EdgeRange::new(0..10, 4), 0..10)];

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
        assert_eq!(buf[0].id(), 4u64.into());

        assert!(merger.advance(&mut buf));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].id(), 5u64.into());

        assert!(!merger.advance(&mut buf));
    }

    #[test]
    fn test_merge_nodes_unequal_len2() {
        let a = vec![MergeNode::new(2u64.into(), EdgeRange::new(0..10, 4), 0..10)];
        let b = vec![
            MergeNode::new(1u64.into(), EdgeRange::new(0..10, 1), 0..10),
            MergeNode::new(4u64.into(), EdgeRange::new(0..10, 2), 0..10),
            MergeNode::new(5u64.into(), EdgeRange::new(0..10, 3), 0..10),
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
        assert_eq!(buf[0].id(), 4u64.into());

        assert!(merger.advance(&mut buf));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].id(), 5u64.into());

        assert!(!merger.advance(&mut buf));
    }

    #[test]
    fn test_datum_merge() {
        let a = vec![
            StoredEdge::new(NodeDatum::new(1u64, 1), RelFlags::default()),
            StoredEdge::new(NodeDatum::new(2u64, 4), RelFlags::default()),
            StoredEdge::new(NodeDatum::new(3u64, 5), RelFlags::default()),
        ];

        let b = vec![
            StoredEdge::new(NodeDatum::new(4u64, 2), RelFlags::default()),
            StoredEdge::new(NodeDatum::new(5u64, 3), RelFlags::default()),
            StoredEdge::new(NodeDatum::new(3u64, 5), RelFlags::default()),
        ];

        let mut merger = EdgeMerger::new(vec![a.into_iter(), b.into_iter()]);

        assert_eq!(merger.next().unwrap().other.host_rank(), 1);
        assert_eq!(merger.next().unwrap().other.host_rank(), 2);
        assert_eq!(merger.next().unwrap().other.host_rank(), 3);
        assert_eq!(merger.next().unwrap().other.host_rank(), 4);
        assert_eq!(merger.next().unwrap().other.host_rank(), 5);
        assert!(merger.next().is_none());
    }

    #[test]
    fn test_datum_merge_unequal_len() {
        let a = vec![
            StoredEdge::new(NodeDatum::new(1u64, 1), RelFlags::default()),
            StoredEdge::new(NodeDatum::new(2u64, 4), RelFlags::default()),
            StoredEdge::new(NodeDatum::new(3u64, 5), RelFlags::default()),
        ];

        let b = vec![StoredEdge::new(
            NodeDatum::new(4u64, 2),
            RelFlags::default(),
        )];

        let mut merger = EdgeMerger::new(vec![a.into_iter(), b.into_iter()]);

        assert_eq!(merger.next().unwrap().other.host_rank(), 1);
        assert_eq!(merger.next().unwrap().other.host_rank(), 2);
        assert_eq!(merger.next().unwrap().other.host_rank(), 4);
        assert_eq!(merger.next().unwrap().other.host_rank(), 5);
        assert!(merger.next().is_none());
    }

    #[test]
    fn test_mergenode_heap_order() {
        let mut heap = MinHeap::new();

        heap.push(Reverse(MergeNode::new(
            15414505588350686497u64.into(),
            EdgeRange::new(0..10, 1),
            0..10,
        )));
        heap.push(Reverse(MergeNode::new(
            470624832116206178u64.into(),
            EdgeRange::new(0..10, 1),
            0..10,
        )));
        heap.push(Reverse(MergeNode::new(
            15414505588350686497u64.into(),
            EdgeRange::new(0..10, 1),
            0..10,
        )));

        assert_eq!(
            heap.into_iter()
                .map(|x| x.0.id().as_u64())
                .collect::<Vec<_>>(),
            vec![
                470624832116206178,
                15414505588350686497,
                15414505588350686497
            ]
        );
    }

    proptest! {
        #[test]
        fn merge(mut ids: Vec<u64>) {
            ids.sort();

            let mut a: Vec<MergeNode> = Vec::new();
            let mut b: Vec<MergeNode> = Vec::new();

            for id in ids.clone() {
                if rand::random() {
                    a.push(MergeNode::new(NodeID::from(id), EdgeRange::new(0..10, 4), 0..10));
                } else {
                    b.push(MergeNode::new(NodeID::from(id), EdgeRange::new(0..10, 4), 0..10));
                }
            }

            let mut merge = MergeIter::new(vec![a.into_iter() , b.into_iter()]);

            let mut merged = Vec::new();
            let mut buf = Vec::new();

            while merge.advance(&mut buf) {
                merged.extend(buf.iter().map(|n| n.id().as_u64()));
            }

            prop_assert_eq!(merged, ids);
        }
    }
}
