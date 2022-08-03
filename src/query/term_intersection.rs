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

use itertools::Itertools;
use tantivy::query::{EmptyScorer, Scorer};
use tantivy::{DocId, DocSet, Postings, Score, TERMINATED};

use super::field_union::FieldUnion;

const MAX_WIDTH: u32 = 45;
const LAMBDA: f32 = 0.55;
const GAMMA: f32 = 0.25;

pub fn intersect_terms(mut scorers: Vec<FieldUnion>) -> Box<dyn Scorer> {
    if scorers.is_empty() {
        return Box::new(EmptyScorer);
    }
    scorers.sort_by_key(|scorer| scorer.size_hint());
    let doc = go_to_first_doc(&mut scorers[..]);
    if doc == TERMINATED {
        return Box::new(EmptyScorer);
    }
    Box::new(TermIntersection { docsets: scorers })
}

pub struct TermIntersection {
    docsets: Vec<FieldUnion>,
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

impl DocSet for TermIntersection {
    fn advance(&mut self) -> DocId {
        let mut candidate = self.docsets[0].advance();

        'outer: loop {
            for docset in self.docsets.iter_mut() {
                let seek_doc = docset.seek(candidate);
                if seek_doc > candidate {
                    candidate = self.docsets[0].seek(seek_doc);
                    continue 'outer;
                }
            }
            debug_assert!(self.docsets.iter().all(|docset| docset.doc() == candidate));
            return candidate;
        }
    }

    fn seek(&mut self, target: DocId) -> DocId {
        self.docsets[0].seek(target);
        let doc = go_to_first_doc(&mut self.docsets);
        debug_assert!(self.docsets.iter().all(|docset| docset.doc() == doc));
        debug_assert!(doc >= target);
        doc
    }

    fn doc(&self) -> DocId {
        self.docsets[0].doc()
    }

    fn size_hint(&self) -> u32 {
        self.docsets[0].size_hint()
    }
}

/// modified bm25 score based on term proximity.
/// more details in the papers "Viewing Term Proximity from a Different Perspective"
/// and "A Short Note on Proximity-based Scoring of Documents with Multiple Fields"
impl Scorer for TermIntersection {
    fn score(&mut self) -> Score {
        if self.doc() == TERMINATED {
            return 0.0;
        }

        let num_docsets = self.docsets.len();

        let postings = self
            .docsets
            .iter_mut()
            .map(|docset| {
                docset
                    .docsets
                    .iter_mut()
                    .map(|docset| (&mut docset.postings, &docset.similarity_weight))
                    .collect_vec()
            })
            .collect_vec();

        debug_assert_eq!(postings.len(), num_docsets);

        let mut fields = Vec::new();
        for term in postings {
            for (i, field) in term.into_iter().enumerate() {
                if i >= fields.len() {
                    fields.push(Vec::new());
                }
                fields[i].push(field);
            }
        }

        debug_assert!(fields.iter().all(|terms| terms.len() == num_docsets));

        let mut positions: Vec<Vec<u32>> = Vec::new();
        for _ in 0..fields.get(0).map(|t| t.len()).unwrap_or(0) {
            positions.push(Vec::new());
        }

        fields
            .into_iter()
            .enumerate()
            .map(|(field_id, terms)| {
                let mut weights = Vec::new();
                for (i, (term, weight)) in terms.into_iter().enumerate() {
                    weights.push(weight);
                    if term.doc() == TERMINATED {
                        for pos in &mut positions[i..] {
                            if !pos.is_empty() {
                                pos.clear();
                            }
                        }
                        break;
                    }

                    term.positions(positions.get_mut(i).unwrap());
                }

                if positions[0].is_empty() {
                    // field has no matches in any/all of the terms.
                    return 0.0;
                }

                let spans: Vec<Span> = SpansIterator::new(positions.clone()).collect();

                positions
                    .iter()
                    .zip_eq(weights.iter())
                    .enumerate()
                    .map(|(term_id, (_pos, weight))| {
                        let rc = spans
                            .iter()
                            .filter(|span| span.has_term(term_id as u32))
                            .map(|span| span.relevance_contribution())
                            .sum::<f32>();

                        weight.score(field_id.try_into().unwrap(), rc)
                    })
                    .sum::<Score>()
            })
            .sum::<Score>()
    }
}

struct SpansIterator {
    num_terms: usize,
    hits: HitsIterator,
    max_width: u32,
    current_hit: Option<Hit>,
    spillover_hit: Option<Hit>,
}

impl SpansIterator {
    pub fn new(positions: Vec<Vec<u32>>) -> Self {
        Self::new_with_max_width(positions, MAX_WIDTH)
    }

    fn new_with_max_width(positions: Vec<Vec<u32>>, max_width: u32) -> Self {
        let num_terms = positions.len();
        let hits = HitsIterator::new(positions);

        Self {
            hits,
            num_terms,
            max_width,
            current_hit: None,
            spillover_hit: None,
        }
    }
}

impl Iterator for SpansIterator {
    type Item = Span;

    fn next(&mut self) -> Option<Self::Item> {
        let mut current_span = Span {
            max_width: self.max_width,
            ..Default::default()
        };
        let mut term_seen_before = vec![None; self.num_terms];

        if let Some(spillover) = self.spillover_hit.take() {
            term_seen_before[spillover.term_id as usize] = Some(spillover.position);
            current_span.terms.push(SpanTermInfo {
                term_id: spillover.term_id,
                position: spillover.position,
            });
        }

        loop {
            match (self.current_hit.take(), self.hits.next()) {
                (None, Some(next_hit)) => self.current_hit = Some(next_hit),
                (Some(current_hit), None) => {
                    term_seen_before[current_hit.term_id as usize] = Some(current_hit.position);

                    current_span.terms.push(SpanTermInfo {
                        term_id: current_hit.term_id,
                        position: current_hit.position,
                    });

                    self.current_hit = None;

                    return Some(current_span);
                }
                (Some(current_hit), Some(next_hit)) => {
                    term_seen_before[current_hit.term_id as usize] = Some(current_hit.position);

                    let dist = next_hit.position - current_hit.position + 1;
                    if dist > self.max_width || current_hit.term_id == next_hit.term_id {
                        // case 1 and 2 from paper
                        current_span.terms.push(SpanTermInfo {
                            term_id: current_hit.term_id,
                            position: current_hit.position,
                        });

                        self.current_hit = Some(next_hit);

                        return Some(current_span);
                    } else if let Some(prev_hit) = term_seen_before[next_hit.term_id as usize] {
                        // case 3 from paper
                        let dist_prev = current_hit.position - prev_hit + 1;
                        let dist_next = next_hit.position - current_hit.position + 1;

                        if dist_prev < dist_next {
                            current_span.terms.push(SpanTermInfo {
                                term_id: current_hit.term_id,
                                position: current_hit.position,
                            });
                        } else {
                            self.spillover_hit = Some(current_hit);
                        }
                        self.current_hit = Some(next_hit);
                        return Some(current_span);
                    } else {
                        // case 4
                        current_span.terms.push(SpanTermInfo {
                            term_id: current_hit.term_id,
                            position: current_hit.position,
                        });
                        self.current_hit = Some(next_hit);
                    }
                }
                (None, None) => return None,
            }
        }
    }
}

#[derive(Debug)]
struct SpanTermInfo {
    term_id: u32,
    position: u32,
}

#[derive(Debug)]
struct Span {
    max_width: u32,
    terms: Vec<SpanTermInfo>,
}

impl Default for Span {
    fn default() -> Self {
        Self {
            max_width: MAX_WIDTH,
            terms: Default::default(),
        }
    }
}

impl Span {
    fn width(&self) -> u32 {
        match (self.terms.first(), self.terms.last()) {
            (Some(first), Some(last)) if first.term_id != last.term_id => {
                (last.position - first.position + 1).min(self.max_width)
            }
            _ => self.max_width,
        }
    }

    fn num_terms(&self) -> u32 {
        self.terms.len() as u32
    }

    fn has_term(&self, term_id: u32) -> bool {
        self.terms.iter().any(|term| term.term_id == term_id)
    }

    fn relevance_contribution(&self) -> f32 {
        (self.num_terms() as f32).powf(LAMBDA) / (self.width() as f32).powf(GAMMA)
    }
}

#[derive(PartialEq, Debug)]
struct Hit {
    position: u32,
    term_id: u32,
}

struct HitsIterator {
    positions: Vec<Vec<u32>>,
    cursors: Vec<usize>,
}

impl HitsIterator {
    fn new(positions: Vec<Vec<u32>>) -> Self {
        let cursors = positions.iter().map(|_| 0).collect();

        Self { positions, cursors }
    }
}

impl Iterator for HitsIterator {
    type Item = Hit;

    fn next(&mut self) -> Option<Self::Item> {
        let mut smallest_idx = None;

        for idx in 0..self.positions.len() {
            let cursor = self.cursors[idx];
            if cursor >= self.positions[idx].len() {
                continue;
            }

            smallest_idx = match smallest_idx {
                Some(s_idx) => {
                    let tmp: &Vec<u32> = &self.positions[s_idx];
                    if self.positions[idx][cursor] < tmp[self.cursors[s_idx]] {
                        Some(idx)
                    } else {
                        smallest_idx
                    }
                }
                None => Some(idx),
            };
        }

        smallest_idx.map(|idx| {
            let res = self.positions[idx][self.cursors[idx]];
            self.cursors[idx] += 1;

            Hit {
                position: res,
                term_id: idx as u32,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hits_iterator() {
        let positions = vec![vec![0, 2, 6], vec![1, 4], vec![3, 5]];

        let mut iter = HitsIterator::new(positions);

        assert_eq!(
            iter.next(),
            Some(Hit {
                position: 0,
                term_id: 0,
            })
        );
        assert_eq!(
            iter.next(),
            Some(Hit {
                position: 1,
                term_id: 1,
            })
        );
        assert_eq!(
            iter.next(),
            Some(Hit {
                position: 2,
                term_id: 0,
            })
        );
        assert_eq!(
            iter.next(),
            Some(Hit {
                position: 3,
                term_id: 2,
            })
        );
        assert_eq!(
            iter.next(),
            Some(Hit {
                position: 4,
                term_id: 1,
            })
        );
        assert_eq!(
            iter.next(),
            Some(Hit {
                position: 5,
                term_id: 2,
            })
        );
        assert_eq!(
            iter.next(),
            Some(Hit {
                position: 6,
                term_id: 0,
            })
        );
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn span_info() {
        let span = Span {
            terms: vec![
                SpanTermInfo {
                    term_id: 1,
                    position: 0,
                },
                SpanTermInfo {
                    term_id: 2,
                    position: 1,
                },
                SpanTermInfo {
                    term_id: 3,
                    position: 3,
                },
            ],
            ..Default::default()
        };

        assert_eq!(span.width(), 4);
        assert_eq!(span.num_terms(), 3);
        assert!(span.has_term(1));
        assert!(span.has_term(2));
        assert!(span.has_term(3));
        assert!(!span.has_term(4));

        let span = Span {
            terms: vec![SpanTermInfo {
                term_id: 1,
                position: 0,
            }],
            ..Default::default()
        };

        assert_eq!(span.width(), MAX_WIDTH);

        let span = Span {
            terms: vec![
                SpanTermInfo {
                    term_id: 1,
                    position: 0,
                },
                SpanTermInfo {
                    term_id: 1,
                    position: MAX_WIDTH + 10,
                },
            ],
            ..Default::default()
        };

        assert_eq!(span.width(), MAX_WIDTH);
    }

    #[test]
    fn hits_to_span() {
        // example from paper
        // positions:
        // sea: 5, 29
        // thousand: 7, 10
        // years: 8, 11

        let positions = vec![vec![5, 29], vec![7, 10], vec![8, 11]];

        let spans: Vec<Span> = SpansIterator::new_with_max_width(positions, 10).collect();

        assert_eq!(spans.len(), 3);
        assert_eq!(spans[0].width(), 4);
        assert_eq!(spans[1].width(), 2);
        assert_eq!(spans[2].width(), 10);

        assert_eq!(spans[0].num_terms(), 3);
        assert_eq!(spans[1].num_terms(), 2);
        assert_eq!(spans[2].num_terms(), 1);
    }
}
