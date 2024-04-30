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

use std::sync::Arc;

use tantivy::{
    postings::SegmentPostings, query::Scorer, DocId, DocSet, Postings, Score, TERMINATED,
};

use crate::{
    fastfield_reader::{self, FastFieldReader},
    query::intersection::Intersection,
    schema::FastFieldEnum,
};

use super::SmallPatternPart;

pub enum PatternScorer {
    Normal(NormalPatternScorer),
    FastSiteDomain(Box<FastSiteDomainPatternScorer>),
    Everything(AllScorer),
    EmptyField(EmptyFieldScorer),
}

impl Scorer for PatternScorer {
    fn score(&mut self) -> Score {
        match self {
            PatternScorer::Normal(scorer) => scorer.score(),
            PatternScorer::FastSiteDomain(scorer) => scorer.score(),
            PatternScorer::Everything(scorer) => scorer.score(),
            PatternScorer::EmptyField(scorer) => scorer.score(),
        }
    }
}

impl DocSet for PatternScorer {
    fn advance(&mut self) -> DocId {
        match self {
            PatternScorer::Normal(scorer) => scorer.advance(),
            PatternScorer::FastSiteDomain(scorer) => scorer.advance(),
            PatternScorer::Everything(scorer) => scorer.advance(),
            PatternScorer::EmptyField(scorer) => scorer.advance(),
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        match self {
            PatternScorer::Normal(scorer) => scorer.seek(target),
            PatternScorer::FastSiteDomain(scorer) => scorer.seek(target),
            PatternScorer::Everything(scorer) => scorer.seek(target),
            PatternScorer::EmptyField(scorer) => scorer.seek(target),
        }
    }

    fn doc(&self) -> DocId {
        match self {
            PatternScorer::Normal(scorer) => scorer.doc(),
            PatternScorer::FastSiteDomain(scorer) => scorer.doc(),
            PatternScorer::Everything(scorer) => scorer.doc(),
            PatternScorer::EmptyField(scorer) => scorer.doc(),
        }
    }

    fn size_hint(&self) -> u32 {
        match self {
            PatternScorer::Normal(scorer) => scorer.size_hint(),
            PatternScorer::FastSiteDomain(scorer) => scorer.size_hint(),
            PatternScorer::Everything(scorer) => scorer.size_hint(),
            PatternScorer::EmptyField(scorer) => scorer.size_hint(),
        }
    }
}

pub struct AllScorer {
    pub doc: DocId,
    pub max_doc: DocId,
}

impl DocSet for AllScorer {
    fn advance(&mut self) -> DocId {
        if self.doc + 1 >= self.max_doc {
            self.doc = TERMINATED;
            return TERMINATED;
        }
        self.doc += 1;
        self.doc
    }

    fn seek(&mut self, target: DocId) -> DocId {
        if target >= self.max_doc {
            self.doc = TERMINATED;
            return TERMINATED;
        }
        self.doc = target;
        self.doc
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        self.max_doc
    }
}

impl Scorer for AllScorer {
    fn score(&mut self) -> Score {
        1.0
    }
}

pub struct EmptyFieldScorer {
    pub segment_reader: Arc<fastfield_reader::SegmentReader>,
    pub num_tokens_fastfield: FastFieldEnum,
    pub all_scorer: AllScorer,
}

impl EmptyFieldScorer {
    fn num_tokes(&self, doc: DocId) -> u64 {
        let s: Option<u64> = self
            .segment_reader
            .get_field_reader(doc)
            .get(self.num_tokens_fastfield)
            .and_then(|v| v.as_u64());
        s.unwrap_or_default()
    }
}

impl DocSet for EmptyFieldScorer {
    fn advance(&mut self) -> DocId {
        let mut doc = self.all_scorer.advance();

        while doc != TERMINATED && self.num_tokes(doc) > 0 {
            doc = self.all_scorer.advance();
        }

        doc
    }

    fn doc(&self) -> DocId {
        self.all_scorer.doc()
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.all_scorer.seek(target);

        if self.doc() != TERMINATED && self.num_tokes(self.all_scorer.doc()) > 0 {
            self.advance()
        } else {
            self.doc()
        }
    }

    fn size_hint(&self) -> u32 {
        self.all_scorer.size_hint()
    }
}

impl Scorer for EmptyFieldScorer {
    fn score(&mut self) -> Score {
        1.0
    }
}

pub struct FastSiteDomainPatternScorer {
    pub posting: SegmentPostings,
}

impl Scorer for FastSiteDomainPatternScorer {
    fn score(&mut self) -> Score {
        1.0
    }
}

impl DocSet for FastSiteDomainPatternScorer {
    fn advance(&mut self) -> DocId {
        self.posting.advance()
    }

    fn doc(&self) -> DocId {
        self.posting.doc()
    }

    fn size_hint(&self) -> u32 {
        self.posting.size_hint()
    }
}

pub struct NormalPatternScorer {
    pattern_all_simple: bool,
    intersection_docset: Intersection<SegmentPostings>,
    pattern: Vec<SmallPatternPart>,
    num_query_terms: usize,
    left: Vec<u32>,
    right: Vec<u32>,
    phrase_count: u32,
    num_tokens_field: FastFieldEnum,
    segment_reader: Arc<fastfield_reader::SegmentReader>,
}

impl NormalPatternScorer {
    pub fn new(
        term_postings_list: Vec<SegmentPostings>,
        pattern: Vec<SmallPatternPart>,
        segment: tantivy::SegmentId,
        num_tokens_field: FastFieldEnum,
        fastfield_reader: FastFieldReader,
    ) -> Self {
        let num_query_terms = term_postings_list.len();
        let segment_reader = fastfield_reader.get_segment(&segment);

        let mut s = Self {
            pattern_all_simple: pattern.iter().all(|p| matches!(p, SmallPatternPart::Term)),
            intersection_docset: Intersection::new(term_postings_list),
            num_query_terms,
            pattern,
            left: Vec::with_capacity(100),
            right: Vec::with_capacity(100),
            phrase_count: 0,
            num_tokens_field,
            segment_reader,
        };

        if !s.pattern_match() {
            s.advance();
        }

        s
    }

    fn pattern_match(&mut self) -> bool {
        if self.num_query_terms == 1 && self.pattern_all_simple {
            // speedup for single term patterns
            self.phrase_count = self
                .intersection_docset
                .docset_mut_specialized(0)
                .term_freq();
            return self.phrase_count > 0;
        }

        self.phrase_count = self.perform_pattern_match() as u32;

        self.phrase_count > 0
    }

    fn perform_pattern_match(&mut self) -> usize {
        if self.intersection_docset.doc() == TERMINATED {
            return 0;
        }

        {
            self.intersection_docset
                .docset_mut_specialized(0)
                .positions(&mut self.left);
        }

        let mut intersection_len = self.left.len();
        let mut out = Vec::new();

        let mut current_right_term = 0;
        let mut slop = 1;
        let num_tokens_doc: Option<u64> = self
            .segment_reader
            .get_field_reader(self.doc())
            .get(self.num_tokens_field)
            .and_then(|v| v.as_u64());
        let num_tokens_doc = num_tokens_doc.unwrap();

        for (i, pattern_part) in self.pattern.iter().enumerate() {
            match pattern_part {
                SmallPatternPart::Term => {
                    if current_right_term == 0 {
                        current_right_term = 1;
                        continue;
                    }

                    {
                        self.intersection_docset
                            .docset_mut_specialized(current_right_term)
                            .positions(&mut self.right);
                    }
                    out.resize(self.left.len().max(self.right.len()), 0);
                    intersection_len =
                        intersection_with_slop(&self.left[..], &self.right[..], &mut out, slop);

                    slop = 1;

                    if intersection_len == 0 {
                        return 0;
                    }

                    self.left = out[..intersection_len].to_vec();
                    out = Vec::new();
                    current_right_term += 1;
                }
                SmallPatternPart::Wildcard => {
                    slop = u32::MAX;
                }
                SmallPatternPart::Anchor if i == 0 => {
                    if let Some(pos) = self.left.first() {
                        if *pos != 0 {
                            return 0;
                        }
                    }
                }
                SmallPatternPart::Anchor if i == self.pattern.len() - 1 => {
                    {
                        self.intersection_docset
                            .docset_mut_specialized(self.num_query_terms - 1)
                            .positions(&mut self.right);
                    }

                    if let Some(pos) = self.right.last() {
                        if *pos != (num_tokens_doc - 1) as u32 {
                            return 0;
                        }
                    }
                }
                SmallPatternPart::Anchor => {}
            }
        }

        intersection_len
    }
}

impl Scorer for NormalPatternScorer {
    fn score(&mut self) -> Score {
        1.0
    }
}

impl DocSet for NormalPatternScorer {
    fn advance(&mut self) -> DocId {
        loop {
            let doc = self.intersection_docset.advance();
            if doc == TERMINATED || self.pattern_match() {
                return doc;
            }
        }
    }

    fn doc(&self) -> tantivy::DocId {
        self.intersection_docset.doc()
    }

    fn size_hint(&self) -> u32 {
        self.intersection_docset.size_hint()
    }
}

/// Intersect twos sorted arrays `left` and `right` and outputs the
/// resulting array in `out`. The positions in out are all positions from right where
/// the distance to left_pos <= slop
///
/// Returns the length of the intersection
fn intersection_with_slop(left: &[u32], right: &[u32], out: &mut [u32], slop: u32) -> usize {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut count = 0;
    let left_len = left.len();
    let right_len = right.len();
    while left_index < left_len && right_index < right_len {
        let left_val = left[left_index];
        let right_val = right[right_index];

        // The three conditions are:
        // left_val < right_slop -> left index increment.
        // right_slop <= left_val <= right -> find the best match.
        // left_val > right -> right index increment.
        let right_slop = if right_val >= slop {
            right_val - slop
        } else {
            0
        };

        if left_val < right_slop {
            left_index += 1;
        } else if right_slop <= left_val && left_val <= right_val {
            while left_index + 1 < left_len {
                // there could be a better match
                let next_left_val = left[left_index + 1];
                if next_left_val > right_val {
                    // the next value is outside the range, so current one is the best.
                    break;
                }
                // the next value is better.
                left_index += 1;
            }
            // store the match in left.
            out[count] = right_val;
            count += 1;
            right_index += 1;
        } else if left_val > right_val {
            right_index += 1;
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aux_intersection(left: &[u32], right: &[u32], expected: &[u32], slop: u32) {
        let mut out = vec![0; left.len().max(right.len())];

        let intersection_size = intersection_with_slop(left, right, &mut out, slop);

        assert_eq!(&out[..intersection_size], expected);
    }

    #[test]
    fn test_intersection_with_slop() {
        aux_intersection(&[20, 75, 77], &[18, 21, 60], &[21, 60], u32::MAX);
        aux_intersection(&[21, 60], &[50, 61], &[61], 1);

        aux_intersection(&[1, 2, 3], &[], &[], 1);
        aux_intersection(&[], &[1, 2, 3], &[], 1);

        aux_intersection(&[1, 2, 3], &[4, 5, 6], &[4], 1);
        aux_intersection(&[1, 2, 3], &[4, 5, 6], &[4, 5, 6], u32::MAX);

        aux_intersection(&[20, 75, 77], &[18, 21, 60], &[21, 60], u32::MAX);
        aux_intersection(&[21, 60], &[61, 62], &[61, 62], 2);

        aux_intersection(&[60], &[61, 62], &[61, 62], 2);
    }
}
