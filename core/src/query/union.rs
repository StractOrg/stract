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

use std::{cell::RefCell, cmp::Ordering};

use min_max_heap::MinMaxHeap;
use tantivy::{
    query::{Explanation, Scorer},
    DocId, DocSet, TERMINATED,
};

struct DocsetHead<T> {
    docset: RefCell<T>,
    min_doc: DocId,
}

impl<T> PartialOrd for DocsetHead<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.min_doc.partial_cmp(&other.min_doc)
    }
}

impl<T> Ord for DocsetHead<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl<T> PartialEq for DocsetHead<T> {
    fn eq(&self, other: &Self) -> bool {
        self.min_doc == other.min_doc
    }
}

impl<T> Eq for DocsetHead<T> {}

struct Union<T> {
    docsets: MinMaxHeap<DocsetHead<T>>,
}

impl<T: Scorer> From<Vec<T>> for Union<T> {
    fn from(docsets: Vec<T>) -> Self {
        let mut heap = MinMaxHeap::with_capacity(docsets.len());

        for docset in docsets
            .into_iter()
            .filter(|docset| docset.doc() != TERMINATED)
        {
            heap.push(DocsetHead {
                min_doc: docset.doc(),
                docset: RefCell::new(docset),
            })
        }

        Union { docsets: heap }
    }
}

impl<T: DocSet> DocSet for Union<T> {
    fn advance(&mut self) -> tantivy::DocId {
        let old_min = self.doc();

        if old_min == TERMINATED {
            return TERMINATED;
        }

        {
            loop {
                let head = self.docsets.peek_min().unwrap();

                if head.min_doc != old_min {
                    break;
                }

                let mut head = self.docsets.peek_min_mut().unwrap();

                head.min_doc = head.docset.get_mut().advance();
            }
        }

        self.docsets.peek_min().unwrap().min_doc
    }

    fn seek(&mut self, target: DocId) -> DocId {
        let old_min = self.doc();

        if old_min > target {
            return old_min;
        }

        if old_min == TERMINATED {
            return TERMINATED;
        }

        {
            loop {
                let head = self.docsets.peek_min().unwrap();

                if head.min_doc != old_min || head.min_doc == target {
                    break;
                }

                let mut head = self.docsets.peek_min_mut().unwrap();

                head.min_doc = head.docset.get_mut().seek(target);
            }
        }

        self.docsets.peek_min().unwrap().min_doc
    }

    fn doc(&self) -> tantivy::DocId {
        self.docsets
            .peek_min()
            .map(|head| head.min_doc)
            .unwrap_or(TERMINATED)
    }

    fn size_hint(&self) -> u32 {
        self.docsets
            .iter()
            .map(|head| head.docset.borrow().size_hint())
            .max()
            .unwrap_or(0)
    }
}

impl<T: Scorer> Scorer for Union<T> {
    fn score(&mut self) -> tantivy::Score {
        let cur_doc = self.doc();
        self.docsets
            .iter()
            .filter(|head| head.min_doc == cur_doc)
            .map(|head| head.docset.borrow_mut().score())
            .sum()
    }
}

#[derive(Debug)]
pub struct UnionQuery {
    subqueries: Vec<Box<dyn tantivy::query::Query>>,
}

impl From<Vec<Box<dyn tantivy::query::Query>>> for UnionQuery {
    fn from(subqueries: Vec<Box<dyn tantivy::query::Query>>) -> Self {
        Self { subqueries }
    }
}

impl Clone for UnionQuery {
    fn clone(&self) -> Self {
        self.subqueries
            .iter()
            .map(|subquery| subquery.box_clone())
            .collect::<Vec<_>>()
            .into()
    }
}

impl tantivy::query::Query for UnionQuery {
    fn weight(
        &self,
        enable_scoring: tantivy::query::EnableScoring,
    ) -> tantivy::Result<Box<dyn tantivy::query::Weight>> {
        let mut weights = Vec::new();

        for query in &self.subqueries {
            weights.push(query.weight(enable_scoring)?);
        }

        Ok(Box::new(UnionWeight { weights }))
    }
}

struct UnionWeight {
    weights: Vec<Box<dyn tantivy::query::Weight>>,
}

impl tantivy::query::Weight for UnionWeight {
    fn scorer(
        &self,
        reader: &tantivy::SegmentReader,
        boost: tantivy::Score,
    ) -> tantivy::Result<Box<dyn Scorer>> {
        let mut scorers = Vec::new();

        for weight in &self.weights {
            scorers.push(weight.scorer(reader, boost)?);
        }

        Ok(Box::new(Union::from(scorers)))
    }

    fn explain(&self, reader: &tantivy::SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        let mut explanation = Explanation::new("Union. Sum of ...", scorer.score());

        for weight in &self.weights {
            if let Ok(child_explanation) = weight.explain(reader, doc) {
                explanation.add_detail(child_explanation);
            }
        }

        Ok(explanation)
    }
}
