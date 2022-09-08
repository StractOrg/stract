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

use tantivy::query::Scorer;
use tantivy::{DocId, DocSet, Score};

/// Creates a `DocSet` that iterate through the intersection of two or more `DocSet`s.
pub struct Intersection<TDocSet: DocSet, TOtherDocSet: DocSet = Box<dyn Scorer>> {
    left: TDocSet,
    right: TDocSet,
    others: Vec<TOtherDocSet>,
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

impl<TDocSet: DocSet> Intersection<TDocSet, TDocSet> {
    pub(crate) fn new(mut docsets: Vec<TDocSet>) -> Intersection<TDocSet, TDocSet> {
        let num_docsets = docsets.len();
        assert!(num_docsets >= 2);
        docsets.sort_by_key(|docset| docset.size_hint());
        go_to_first_doc(&mut docsets);
        let left = docsets.remove(0);
        let right = docsets.remove(0);
        Intersection {
            left,
            right,
            others: docsets,
        }
    }
}

impl<TDocSet: DocSet, TOtherDocSet: DocSet> DocSet for Intersection<TDocSet, TOtherDocSet> {
    fn advance(&mut self) -> DocId {
        let (left, right) = (&mut self.left, &mut self.right);
        let mut candidate = left.advance();

        'outer: loop {
            // In the first part we look for a document in the intersection
            // of the two rarest `DocSet` in the intersection.

            loop {
                let right_doc = right.seek(candidate);
                candidate = left.seek(right_doc);
                if candidate == right_doc {
                    break;
                }
            }

            debug_assert_eq!(left.doc(), right.doc());
            // test the remaining scorers;
            for docset in self.others.iter_mut() {
                let seek_doc = docset.seek(candidate);
                if seek_doc > candidate {
                    candidate = left.seek(seek_doc);
                    continue 'outer;
                }
            }
            debug_assert_eq!(candidate, self.left.doc());
            debug_assert_eq!(candidate, self.right.doc());
            debug_assert!(self.others.iter().all(|docset| docset.doc() == candidate));
            return candidate;
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.left.seek(target);
        let mut docsets: Vec<&mut dyn DocSet> = vec![&mut self.left, &mut self.right];
        for docset in &mut self.others {
            docsets.push(docset);
        }
        let doc = go_to_first_doc(&mut docsets[..]);
        debug_assert!(docsets.iter().all(|docset| docset.doc() == doc));
        debug_assert!(doc >= target);
        doc
    }

    fn doc(&self) -> DocId {
        self.left.doc()
    }

    fn size_hint(&self) -> u32 {
        self.left.size_hint()
    }
}

impl<TScorer, TOtherScorer> Scorer for Intersection<TScorer, TOtherScorer>
where
    TScorer: Scorer,
    TOtherScorer: Scorer,
{
    fn score(&mut self) -> Score {
        self.left.score()
            + self.right.score()
            + self.others.iter_mut().map(Scorer::score).sum::<Score>()
    }
}

impl<TDocSet: DocSet> Intersection<TDocSet, TDocSet> {
    pub(crate) fn docset_mut_specialized(&mut self, ord: usize) -> &mut TDocSet {
        match ord {
            0 => &mut self.left,
            1 => &mut self.right,
            n => &mut self.others[n - 2],
        }
    }
}
