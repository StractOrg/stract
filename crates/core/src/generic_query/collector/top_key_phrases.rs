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

use crate::{inverted_index::ShardId, Result, SortableFloat};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    sync::Arc,
};

use lending_iter::LendingIterator;
use tantivy::collector::SegmentCollector;

use crate::{
    inverted_index::KeyPhrase,
    schema::text_field::{self, TextField},
};
use std::str;

use super::Collector;

const NON_ALPHABETIC_CHAR_THRESHOLD: f64 = 0.25;

pub struct TopKeyPhrasesCollector {
    pub top_n: usize,
    pub shard_id: Option<ShardId>,
}

impl TopKeyPhrasesCollector {
    pub fn new(top_n: usize) -> Self {
        Self {
            top_n,
            shard_id: None,
        }
    }

    pub fn with_shard_id(mut self, shard_id: ShardId) -> Self {
        self.shard_id = Some(shard_id);
        self
    }
}

impl Collector for TopKeyPhrasesCollector {
    type Fruit = Vec<(ShardId, KeyPhrase)>;
    type Child = TopKeyPhrasesSegmentCollector;

    fn for_segment(
        &self,
        _: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> crate::Result<Self::Child> {
        TopKeyPhrasesSegmentCollector::new(self.top_n, self.shard_id.unwrap(), segment)
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let mut phrases = HashMap::new();

        for (shard_id, fruit) in segment_fruits.into_iter().flatten() {
            *phrases
                .entry((shard_id, fruit.text().to_string()))
                .or_default() += fruit.score();
        }

        let mut res: Vec<_> = phrases
            .into_iter()
            .map(|((shard_id, phrase), score)| (shard_id, KeyPhrase::new(phrase, score)))
            .collect();

        res.sort_by(|(_, a), (_, b)| b.score().total_cmp(&a.score()));
        res.truncate(self.top_n);

        Ok(res)
    }

    fn collect_segment(
        &self,
        _: &dyn tantivy::query::Weight,
        segment_ord: u32,
        reader: &tantivy::SegmentReader,
    ) -> crate::Result<<Self::Child as tantivy::collector::SegmentCollector>::Fruit> {
        let child = self.for_segment(segment_ord, reader)?;
        Ok(child.top_phrases())
    }
}

pub struct TopKeyPhrasesSegmentCollector {
    top_n: usize,
    shard_id: ShardId,
    inverted_index: Arc<tantivy::InvertedIndexReader>,
}

impl TopKeyPhrasesSegmentCollector {
    pub fn new(top_n: usize, shard_id: ShardId, reader: &tantivy::SegmentReader) -> Result<Self> {
        let field = reader
            .schema()
            .get_field(text_field::KeyPhrases.name())
            .unwrap();

        let inverted_index = reader.inverted_index(field)?;

        Ok(Self {
            top_n,
            shard_id,
            inverted_index,
        })
    }

    pub fn top_phrases(&self) -> Vec<(ShardId, KeyPhrase)> {
        let mut keywords: BinaryHeap<(Reverse<SortableFloat>, String)> =
            BinaryHeap::with_capacity(self.top_n);

        let mut stream = self.inverted_index.terms().stream().unwrap();
        while let Some((term, info)) = stream.next() {
            let term_str = str::from_utf8(term).unwrap().to_string();
            let num_chars = term_str.chars().count();

            if term_str.chars().filter(|c| !c.is_alphabetic()).count() as f64 / num_chars as f64
                > NON_ALPHABETIC_CHAR_THRESHOLD
            {
                continue;
            }

            let left_paren = term_str.chars().filter(|c| c == &'(').count();
            let right_paren = term_str.chars().filter(|c| c == &')').count();

            if left_paren != right_paren {
                continue;
            }

            let words = term_str.split_whitespace().collect::<Vec<_>>();

            if words.is_empty() {
                continue;
            }

            let score = info.doc_freq as f64;

            if score.is_normal() {
                let term_str = words.join(" ");

                if keywords.len() >= self.top_n {
                    if let Some(mut min) = keywords.peek_mut() {
                        if score > min.0 .0.into() {
                            *min = (Reverse(score.into()), term_str);
                        }
                    }
                } else {
                    keywords.push((Reverse(score.into()), term_str));
                }
            }
        }

        keywords
            .into_iter()
            .map(|(Reverse(score), phrase)| KeyPhrase::new(phrase, score.into()))
            .map(|phrase| (self.shard_id, phrase))
            .collect()
    }
}

impl SegmentCollector for TopKeyPhrasesSegmentCollector {
    type Fruit = Vec<(ShardId, KeyPhrase)>;

    fn collect(&mut self, _: tantivy::DocId, _: tantivy::Score) {
        unimplemented!()
    }

    fn harvest(self) -> Self::Fruit {
        unimplemented!()
    }
}
