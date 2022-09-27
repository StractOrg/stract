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

use min_max_heap::MinMaxHeap;
use tantivy::{
    collector::{Collector, ScoreSegmentTweaker, ScoreTweaker, SegmentCollector},
    fastfield::MultiValuedFastFieldReader,
    DocId, Score, SegmentOrdinal, SegmentReader,
};

use crate::{
    inverted_index::{DocAddress, WebsitePointer},
    prehashed::{combine_u64s, PrehashMap, Prehashed},
    schema::Field,
};

fn adjust_score(num_taken: usize, original_score: f64) -> f64 {
    const SCALE: f64 = 14.0;
    original_score * (SCALE / (num_taken as f64 + SCALE))
}

#[derive(Clone)]
pub struct MaxDocsConsidered {
    pub total_docs: usize,
    pub segments: usize,
}

pub struct TopDocs {
    top_n: usize,
    offset: usize,
    max_docs: Option<MaxDocsConsidered>,
}

impl TopDocs {
    pub fn with_limit(top_n: usize) -> Self {
        Self {
            top_n,
            offset: 0,
            max_docs: None,
        }
    }

    pub fn and_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    pub fn and_max_docs(mut self, max_docs: MaxDocsConsidered) -> Self {
        self.max_docs = Some(max_docs);

        self
    }

    pub fn tweak_score<TScoreSegmentTweaker, TScoreTweaker>(
        self,
        score_tweaker: TScoreTweaker,
    ) -> impl Collector<Fruit = Vec<WebsitePointer>>
    where
        TScoreSegmentTweaker: ScoreSegmentTweaker<f64> + 'static,
        TScoreTweaker: ScoreTweaker<f64, Child = TScoreSegmentTweaker> + Send + Sync,
    {
        TweakedScoreTopCollector::new(score_tweaker, self)
    }
}

impl Collector for TopDocs {
    type Fruit = Vec<WebsitePointer>;

    type Child = TopSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let key_reader = segment.fast_fields().u64s(
            segment
                .schema()
                .get_field(Field::SiteHash.as_str())
                .unwrap(),
        )?;

        let max_docs = self
            .max_docs
            .as_ref()
            .map(|max_docs| max_docs.total_docs / max_docs.segments);

        Ok(TopSegmentCollector {
            key_reader,
            max_docs,
            num_docs_taken: 0,
            segment_ord: segment_local_id,
            bucket_collector: BucketCollector::new(self.top_n + self.offset),
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut collector = BucketCollector::new(self.top_n + self.offset);

        for docs in segment_fruits {
            for doc in docs {
                collector.insert(doc);
            }
        }

        Ok(collector
            .into_sorted_vec(true)
            .into_iter()
            .skip(self.offset)
            .map(|doc| WebsitePointer {
                score: doc.score,
                site_hash: doc.key,
                address: DocAddress {
                    segment: doc.segment,
                    doc_id: doc.id,
                },
            })
            .collect())
    }
}

pub struct TopSegmentCollector {
    key_reader: MultiValuedFastFieldReader<u64>,
    max_docs: Option<usize>,
    num_docs_taken: usize,
    segment_ord: SegmentOrdinal,
    bucket_collector: BucketCollector,
}

impl SegmentCollector for TopSegmentCollector {
    type Fruit = Vec<Doc>;

    fn collect(&mut self, doc: DocId, score: Score) {
        if let Some(max_docs) = &self.max_docs {
            if self.num_docs_taken >= *max_docs {
                return;
            }

            self.num_docs_taken += 1;
        }
        let mut keys = Vec::new();
        self.key_reader.get_vals(doc, &mut keys);
        debug_assert_eq!(keys.len(), 2);

        let keys = [keys[0], keys[1]];
        let key = combine_u64s(keys);

        self.bucket_collector.insert(Doc {
            key: Prehashed(key),
            id: doc,
            segment: self.segment_ord,
            score: score as f64,
        });
    }

    fn harvest(self) -> Self::Fruit {
        self.bucket_collector.into_sorted_vec(false)
    }
}

struct BucketCollector {
    buckets: PrehashMap<Bucket>,
    heads: MinMaxHeap<BucketHead>,
    top_n: usize,
}

impl BucketCollector {
    pub fn new(top_n: usize) -> Self {
        assert!(top_n > 0);

        Self {
            top_n,
            heads: MinMaxHeap::with_capacity(top_n + 1),
            buckets: PrehashMap::new(),
        }
    }

    pub fn insert(&mut self, doc: Doc) {
        if let Some(bucket) = self.buckets.get_mut(&doc.key) {
            bucket.insert(doc);
        } else {
            let mut bucket = Bucket::new(self.top_n);
            bucket.insert(doc.clone());

            self.buckets.insert(doc.key.clone(), bucket);

            self.heads.push(BucketHead {
                key: doc.key,
                tweaked_score: doc.score,
            });
        }

        if self.buckets.len() > self.top_n + 1 {
            self.prune_buckets()
        }
    }

    fn prune_buckets(&mut self) {
        self.update_worst_head();
        let worst_head = self.heads.pop_min().unwrap();
        self.buckets.remove(&worst_head.key);
    }

    fn update_worst_head(&mut self) {
        loop {
            let mut worst_head = self.heads.peek_min_mut().unwrap();
            let current_score = self
                .buckets
                .get(&worst_head.key)
                .unwrap()
                .get_best()
                .unwrap()
                .tweaked_score;

            if worst_head.tweaked_score != current_score {
                worst_head.tweaked_score = current_score;
            } else {
                break;
            }
        }
    }

    fn build_heads(&self) -> MinMaxHeap<BucketHead> {
        let mut bucket_heads: MinMaxHeap<BucketHead> = MinMaxHeap::with_capacity(self.top_n);

        for (key, bucket) in self.buckets.iter() {
            let best_in_bucket = bucket.get_best().unwrap();
            if bucket_heads.len() >= self.top_n {
                let mut worst_head = bucket_heads.peek_min_mut().unwrap();

                if best_in_bucket.tweaked_score > worst_head.tweaked_score {
                    worst_head.key = key.clone();
                    worst_head.tweaked_score = best_in_bucket.tweaked_score
                }
            } else {
                bucket_heads.push(BucketHead {
                    key: key.clone(),
                    tweaked_score: best_in_bucket.tweaked_score,
                });
            }
        }

        bucket_heads
    }

    pub fn into_sorted_vec(mut self, apply_adjust_score: bool) -> Vec<Doc> {
        let mut res = Vec::new();

        let mut bucket_heads = self.build_heads();

        while let Some(mut head) = bucket_heads.pop_max() {
            let bucket = self.buckets.get_mut(&head.key).unwrap();

            if let Some(mut doc) = bucket.pop_best() {
                if apply_adjust_score {
                    doc.score = adjust_score(bucket.num_taken - 1, doc.score);
                }

                res.push(doc);

                if let Some(new_best) = bucket.get_best() {
                    head.tweaked_score = new_best.tweaked_score;
                }

                bucket_heads.push(head);
            }

            if res.len() == self.top_n {
                break;
            }
        }

        res
    }
}

struct BucketHead {
    key: Prehashed,
    tweaked_score: f64,
}

impl PartialEq for BucketHead {
    fn eq(&self, other: &Self) -> bool {
        self.tweaked_score == other.tweaked_score
    }
}

impl Eq for BucketHead {}

impl PartialOrd for BucketHead {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.tweaked_score.partial_cmp(&other.tweaked_score)
    }
}

impl Ord for BucketHead {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

struct Bucket {
    num_taken: usize,
    docs: MinMaxHeap<Doc>,
    top_n: usize,
}

impl Bucket {
    pub fn new(top_n: usize) -> Self {
        assert!(top_n > 0);

        Self {
            top_n,
            num_taken: 0,
            docs: MinMaxHeap::with_capacity(top_n),
        }
    }

    pub fn pop_best(&mut self) -> Option<Doc> {
        let res = self.docs.pop_max();

        self.num_taken += 1;

        res
    }

    pub fn get_best(&self) -> Option<TweakedDoc<'_>> {
        self.docs.peek_max().map(|doc| TweakedDoc {
            tweaked_score: adjust_score(self.num_taken, doc.score),
            _doc_id: &doc.id,
        })
    }

    pub fn insert(&mut self, doc: Doc) {
        if self.docs.len() >= self.top_n {
            let mut worst = self.docs.peek_min_mut().unwrap();

            worst.id = doc.id;
            worst.score = doc.score;
            worst.segment = doc.segment;
        } else {
            self.docs.push(doc);
        }
    }
}

struct TweakedDoc<'a> {
    tweaked_score: f64,
    _doc_id: &'a DocId,
}

#[derive(Debug, Clone)]
pub struct Doc {
    key: Prehashed,
    id: DocId,
    segment: SegmentOrdinal,
    score: f64,
}

impl PartialEq for Doc {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for Doc {}

impl PartialOrd for Doc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.score.partial_cmp(&other.score)
    }
}

impl Ord for Doc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

pub(crate) struct TweakedScoreTopCollector<TScoreTweaker> {
    score_tweaker: TScoreTweaker,
    collector: TopDocs,
}

impl<TScoreTweaker> TweakedScoreTopCollector<TScoreTweaker> {
    pub fn new(
        score_tweaker: TScoreTweaker,
        collector: TopDocs,
    ) -> TweakedScoreTopCollector<TScoreTweaker> {
        TweakedScoreTopCollector {
            score_tweaker,
            collector,
        }
    }
}

impl<TScoreTweaker> Collector for TweakedScoreTopCollector<TScoreTweaker>
where
    TScoreTweaker: ScoreTweaker<f64> + Send + Sync,
{
    type Fruit = Vec<WebsitePointer>;

    type Child = TopTweakedScoreSegmentCollector<TScoreTweaker::Child>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let segment_scorer = self.score_tweaker.segment_tweaker(segment_reader)?;
        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;
        Ok(TopTweakedScoreSegmentCollector {
            segment_collector,
            segment_scorer,
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        self.collector.merge_fruits(segment_fruits)
    }
}

pub struct TopTweakedScoreSegmentCollector<TSegmentScoreTweaker>
where
    TSegmentScoreTweaker: ScoreSegmentTweaker<f64>,
{
    segment_collector: TopSegmentCollector,
    segment_scorer: TSegmentScoreTweaker,
}

impl<TSegmentScoreTweaker> SegmentCollector
    for TopTweakedScoreSegmentCollector<TSegmentScoreTweaker>
where
    TSegmentScoreTweaker: 'static + ScoreSegmentTweaker<f64>,
{
    type Fruit = Vec<Doc>;

    fn collect(&mut self, doc: DocId, score: Score) {
        let score = self.segment_scorer.score(doc, score);
        self.segment_collector.collect(doc, score as f32);
    }

    fn harvest(self) -> Self::Fruit {
        self.segment_collector.harvest()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test(top_n: usize, docs: &[(u128, DocId, f64)], expected: &[(f64, DocId)]) {
        let mut collector = BucketCollector::new(top_n);

        for doc in docs {
            collector.insert(Doc {
                key: Prehashed(doc.0),
                id: doc.1,
                score: doc.2,
                segment: 0,
            });
        }

        let res: Vec<(f64, DocId)> = collector
            .into_sorted_vec(true)
            .into_iter()
            .map(|doc| (doc.score, doc.id))
            .collect();

        assert_eq!(&res, expected);
    }

    #[test]
    fn all_different() {
        test(
            3,
            &[
                (1, 123, 1.0),
                (2, 124, 2.0),
                (3, 125, 3.0),
                (4, 126, 4.0),
                (5, 127, 5.0),
            ],
            &[(5.0, 127), (4.0, 126), (3.0, 125)],
        );
    }

    #[test]
    fn less_than_topn() {
        test(
            10,
            &[(3, 125, 3.0), (4, 126, 4.0), (5, 127, 5.0)],
            &[(5.0, 127), (4.0, 126), (3.0, 125)],
        );
    }

    #[test]
    fn same_key_de_prioritised() {
        test(
            10,
            &[(1, 125, 3.0), (2, 126, 3.1), (2, 127, 5.0)],
            &[
                (adjust_score(0, 5.0), 127),
                (adjust_score(0, 3.0), 125),
                (adjust_score(1, 3.1), 126),
            ],
        );

        test(
            2,
            &[(1, 125, 3.0), (2, 126, 3.1), (2, 127, 5.0)],
            &[(adjust_score(0, 5.0), 127), (adjust_score(0, 3.0), 125)],
        );
    }
}
