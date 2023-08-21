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

use tantivy::query::Scorer;
use tantivy::{DocId, DocSet, Score, TERMINATED};

/// Creates a `DocSet` that iterate through the intersection of two or more `DocSet`s.
pub struct Intersection<TDocSet: DocSet = Box<dyn Scorer>> {
    docsets: Vec<TDocSet>,
    old_idx: Vec<usize>,
}

fn go_to_first_doc<TDocSet: DocSet>(docsets: &mut [TDocSet]) -> DocId {
    assert!(!docsets.is_empty());
    let mut candidate = docsets.iter().map(TDocSet::doc).max().unwrap();
    'outer: loop {
        for docset in docsets.iter_mut() {
            let seek_doc = docset.seek(candidate);
            if seek_doc > candidate {
                candidate = docset.doc();
                continue 'outer;
            }
        }
        return candidate;
    }
}

impl<TDocSet: DocSet> Intersection<TDocSet> {
    pub(crate) fn new(mut docsets: Vec<TDocSet>) -> Intersection<TDocSet> {
        if docsets.len() == 1 {
            return Intersection {
                docsets,
                old_idx: vec![0],
            };
        }

        let mut docsets: Vec<(usize, TDocSet)> = docsets.drain(..).enumerate().collect();
        docsets.sort_by_key(|(_, docset)| docset.size_hint());
        let (tmp, mut docsets): (Vec<usize>, Vec<TDocSet>) = docsets.into_iter().unzip();

        let mut old_idx = vec![0; docsets.len()];

        for (new_idx, t) in tmp.into_iter().enumerate() {
            old_idx[t] = new_idx;
        }

        go_to_first_doc(&mut docsets);
        Intersection { docsets, old_idx }
    }
}

impl<TDocSet: DocSet> DocSet for Intersection<TDocSet> {
    fn advance(&mut self) -> DocId {
        if self.docsets.is_empty() {
            return TERMINATED;
        }

        let (first, rest) = self.docsets.split_at_mut(1);
        let rarest_docset = &mut first[0];

        if rest.is_empty() {
            return rarest_docset.advance();
        }

        let mut candidate = rarest_docset.advance();
        'outer: loop {
            for docset in rest.iter_mut() {
                let seek_doc = docset.seek(candidate);
                if seek_doc > candidate {
                    candidate = rarest_docset.seek(seek_doc);
                    continue 'outer;
                }
            }
            debug_assert!(self.docsets.iter().all(|docset| docset.doc() == candidate));
            return candidate;
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        let num_docsets = self.docsets.len();
        match self.docsets.first_mut() {
            Some(docset) => {
                docset.seek(target);

                if num_docsets == 1 {
                    return docset.doc();
                }

                let doc = go_to_first_doc(&mut self.docsets[..]);
                debug_assert!(self.docsets.iter().all(|docset| docset.doc() == doc));
                debug_assert!(doc >= target);
                doc
            }
            None => TERMINATED,
        }
    }

    fn doc(&self) -> DocId {
        self.docsets
            .first()
            .map(|docset| docset.doc())
            .unwrap_or(TERMINATED)
    }

    fn size_hint(&self) -> u32 {
        self.docsets
            .first()
            .map(|docset| docset.size_hint())
            .unwrap_or(0)
    }
}

impl<TScorer> Scorer for Intersection<TScorer>
where
    TScorer: Scorer,
{
    fn score(&mut self) -> Score {
        self.docsets.iter_mut().map(Scorer::score).sum::<Score>()
    }
}

impl<TDocSet: DocSet> Intersection<TDocSet> {
    pub(crate) fn docset_mut_specialized(&mut self, ord: usize) -> &mut TDocSet {
        &mut self.docsets[self.old_idx[ord]]
    }
}
