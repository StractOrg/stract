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

use itertools::Itertools;
use std::future::Future;

use crate::webgraph::NodeID;

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
struct VeryJankyBloomFilter {
    data: Vec<u64>,
    ones: usize,
}

impl VeryJankyBloomFilter {
    fn new(num_blooms: usize) -> Self {
        Self {
            data: vec![0; num_blooms],
            ones: 0,
        }
    }

    fn hash(&self, item: &u64) -> (usize, u64) {
        (
            (item.wrapping_mul(11400714819323198549) % self.data.len() as u64) as usize,
            item.wrapping_mul(11400714819323198549) % 64,
        )
    }

    fn insert(&mut self, item: u64) {
        let (a, b) = self.hash(&item);

        // check if bit is already set
        if self.data[a] & (1 << b) != 0 {
            return;
        }

        self.data[a] |= 1 << b;
        self.ones += 1;
    }

    #[inline]
    fn ones(&self) -> usize {
        self.ones
    }

    #[inline]
    fn intersect_ones(&self, other: &Self) -> usize {
        self.data
            .iter()
            .zip_eq(other.data.iter())
            .map(|(a, b)| a & b)
            .map(|x| x.count_ones() as usize)
            .sum()
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
struct Posting {
    ranks: Vec<u64>,
}

impl Posting {
    fn new(ranks: Vec<u64>) -> Self {
        Self { ranks }
    }

    fn intersection_size(&self, other: &Self) -> usize {
        let mut i = 0;
        let mut j = 0;

        let mut count = 0;

        while i < self.ranks.len() && j < other.ranks.len() {
            let a = self.ranks[i];
            let b = other.ranks[j];

            match a.cmp(&b) {
                std::cmp::Ordering::Equal => {
                    count += 1;
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => {
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    j += 1;
                }
            }
        }

        count
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct BitVec {
    bloom: VeryJankyBloomFilter,
    posting: Posting,
    sqrt_len: f64,
}

impl Default for BitVec {
    fn default() -> Self {
        Self::new(vec![])
    }
}

pub trait Graph {
    fn batch_ingoing(&self, nodes: &[NodeID]) -> impl Future<Output = Vec<Vec<NodeID>>>;
}

impl BitVec {
    pub async fn batch_new_for<G>(nodes: &[NodeID], graph: &G) -> Vec<Self>
    where
        G: Graph,
    {
        let ingoing = graph.batch_ingoing(nodes).await;

        ingoing
            .into_iter()
            .map(|nodes| Self::new(nodes.into_iter().map(|n| n.as_u64()).collect()))
            .collect()
    }

    pub fn new(mut ranks: Vec<u64>) -> Self {
        ranks.sort();
        ranks.dedup();
        ranks.shrink_to_fit();

        let len = ranks.len();
        let mut bloom = VeryJankyBloomFilter::new(16);

        for rank in &ranks {
            bloom.insert(*rank);
        }

        let posting = Posting::new(ranks);

        Self {
            bloom,
            posting,
            sqrt_len: (len as f64).sqrt(),
        }
    }

    pub fn sim(&self, other: &Self) -> f64 {
        if self.sqrt_len == 0.0 || other.sqrt_len == 0.0 {
            return 0.0;
        }

        let max_bloom_ones = self.bloom.ones().max(other.bloom.ones());
        let intersect_bloom_ones = self.bloom.intersect_ones(&other.bloom);

        if (intersect_bloom_ones as f64) / (max_bloom_ones as f64) < 0.25 {
            return 0.0;
        }

        let intersect = self.posting.intersection_size(&other.posting) as f64;

        intersect / (self.sqrt_len * other.sqrt_len)
    }

    pub fn len(&self) -> usize {
        self.posting.ranks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        executor::Executor,
        webgraph::{Edge, Node, Webgraph},
        webpage::html::links::RelFlags,
    };

    use super::*;
    use std::iter::repeat;

    fn into_ranks(a: &[bool]) -> Vec<u64> {
        a.iter()
            .enumerate()
            .filter(|(_, b)| **b)
            .map(|(i, _)| i as u64)
            .collect()
    }

    fn naive_sim(a: &[bool], b: &[bool]) -> f64 {
        let dot = a
            .iter()
            .zip_eq(b.iter())
            .filter(|(a, b)| **a && **b)
            .count();

        let len_a = a.iter().filter(|a| **a).count();
        let len_b = b.iter().filter(|b| **b).count();

        dot as f64 / ((len_a as f64).sqrt() * (len_b as f64).sqrt())
    }

    #[test]
    fn simple() {
        let a: Vec<_> = repeat(false)
            .take(1000)
            .chain(repeat(true).take(10))
            .collect();

        let b: Vec<_> = repeat(false)
            .take(1000)
            .chain(repeat(true).take(8))
            .chain(repeat(false).take(2))
            .collect();

        let expected = naive_sim(&a, &b);

        let a = BitVec::new(into_ranks(&a));
        let b = BitVec::new(into_ranks(&b));

        let sim = a.sim(&b);

        assert!((expected - sim).abs() < 0.1);
    }

    #[test]
    fn zero_sim() {
        let a: Vec<_> = repeat(false).take(1000).collect();

        let b: Vec<_> = repeat(true).take(1000).collect();

        let a = BitVec::new(into_ranks(&a));
        let b = BitVec::new(into_ranks(&b));

        let sim = a.sim(&b);

        assert_eq!(sim, 0.0);
    }

    #[test]
    fn empty_sim() {
        let a: Vec<_> = Vec::new();
        let b: Vec<_> = Vec::new();

        let a = BitVec::new(into_ranks(&a));
        let b = BitVec::new(into_ranks(&b));

        let sim = a.sim(&b);

        assert_eq!(sim, 0.0);
    }

    #[test]
    fn low_sim() {
        let a: Vec<_> = repeat(false)
            .take(100000)
            .chain(repeat(true).take(10))
            .collect();

        let b: Vec<_> = repeat(true)
            .take(100000)
            .chain(repeat(true).take(8))
            .chain(repeat(false).take(2))
            .collect();

        let expected = naive_sim(&a, &b);

        assert!(expected < 0.01);

        let a = BitVec::new(into_ranks(&a));
        let b = BitVec::new(into_ranks(&b));

        let sim = a.sim(&b);

        assert!((expected - sim).abs() < 0.1);
    }

    #[tokio::test]
    async fn test_ignores_no_follow() {
        let temp_dir = crate::gen_temp_dir().unwrap();

        let mut graph = Webgraph::open(&temp_dir).unwrap();

        let a = Node::from("A");
        let b = Node::from("B");
        let c = Node::from("C");

        graph
            .insert(Edge {
                from: a.clone(),
                to: b.clone(),
                rel_flags: RelFlags::NOFOLLOW,
                label: String::new(),
                combined_centrality: 0.0,
            })
            .unwrap();

        graph
            .insert(Edge {
                from: a.clone(),
                to: c.clone(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                combined_centrality: 0.0,
            })
            .unwrap();

        graph.commit().unwrap();

        let bitvecs = BitVec::batch_new_for(&[b.id(), c.id()], &graph).await;

        assert_eq!(bitvecs.len(), 2);
        assert_eq!(bitvecs[0].sim(&bitvecs[1]), 0.0);
    }
}
