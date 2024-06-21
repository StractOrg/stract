// source: https://github.com/quickwit-oss/tantivy/blob/main/src/query/bm25.rs

use itertools::Itertools;

use tantivy::fieldnorm::FieldNormReader;
use tantivy::{Score, Searcher, Term};

const K1: Score = 1.2;
const B: Score = 0.75;

#[derive(Clone, Copy)]
pub struct Bm25Constants {
    pub k1: Score,
    pub b: Score,
}

impl Default for Bm25Constants {
    fn default() -> Self {
        Bm25Constants { k1: K1, b: B }
    }
}

pub fn idf(doc_freq: u64, doc_count: u64) -> Score {
    assert!(doc_count >= doc_freq, "{doc_count} >= {doc_freq}");
    let x = ((doc_count - doc_freq) as Score + 0.5) / (doc_freq as Score + 0.5);
    (1.0 + x).ln()
}

fn cached_tf_component(
    fieldnorm: u32,
    average_fieldnorm: Score,
    constants: Bm25Constants,
) -> Score {
    constants.k1 * (1.0 - constants.b + constants.b * fieldnorm as Score / average_fieldnorm)
}

pub fn compute_tf_cache(average_fieldnorm: Score, constants: Bm25Constants) -> [Score; 256] {
    let mut cache: [Score; 256] = [0.0; 256];
    for (fieldnorm_id, cache_mut) in cache.iter_mut().enumerate() {
        let fieldnorm = FieldNormReader::id_to_fieldnorm(fieldnorm_id as u8);
        *cache_mut = cached_tf_component(fieldnorm, average_fieldnorm, constants);
    }
    cache
}

#[derive(Clone)]
pub struct MultiBm25Weight {
    weights: Vec<Bm25Weight>,
}

impl MultiBm25Weight {
    pub fn for_terms(
        searcher: &Searcher,
        terms: &[Term],
        constants: Bm25Constants,
    ) -> tantivy::Result<Self> {
        if terms.is_empty() {
            return Ok(Self {
                weights: Vec::new(),
            });
        }

        let field = terms[0].field();
        for term in terms.iter().skip(1) {
            assert_eq!(
                term.field(),
                field,
                "All terms must belong to the same field."
            );
        }

        let mut total_num_tokens = 0u64;
        let mut total_num_docs = 0u64;
        for segment_reader in searcher.segment_readers() {
            let inverted_index = segment_reader.inverted_index(field)?;
            total_num_tokens += inverted_index.total_num_tokens();
            total_num_docs += u64::from(segment_reader.max_doc());
        }
        let average_fieldnorm = total_num_tokens as Score / total_num_docs as Score;

        let mut weights = Vec::new();

        for term in terms {
            let term_doc_freq = searcher.doc_freq(term)?;
            weights.push(Bm25Weight::for_one_term(
                term_doc_freq,
                total_num_docs,
                average_fieldnorm,
                constants,
            ));
        }

        Ok(Self { weights })
    }

    #[inline]
    pub fn score(&self, stats: impl Iterator<Item = (u8, u32)>) -> Score {
        stats
            .zip_eq(self.weights.iter())
            .map(|((fieldnorm_id, term_freq), weight)| weight.score(fieldnorm_id, term_freq))
            .sum()
    }

    pub fn idf(&self) -> impl Iterator<Item = f32> + '_ {
        self.weights.iter().map(|w| w.weight)
    }
}

#[derive(Clone)]
pub struct Bm25Weight {
    weight: Score,
    constants: Bm25Constants,
    cache: [Score; 256],
}

impl Bm25Weight {
    pub fn for_one_term(
        term_doc_freq: u64,
        total_num_docs: u64,
        avg_fieldnorm: Score,
        constants: Bm25Constants,
    ) -> Bm25Weight {
        let idf = idf(term_doc_freq, total_num_docs);
        Bm25Weight::new(idf, avg_fieldnorm, constants)
    }

    pub fn new(weight: Score, average_fieldnorm: Score, constants: Bm25Constants) -> Bm25Weight {
        Bm25Weight {
            weight,
            cache: compute_tf_cache(average_fieldnorm, constants),
            constants,
        }
    }

    #[inline]
    pub fn score(&self, fieldnorm_id: u8, term_freq: u32) -> Score {
        self.weight * self.tf_factor(fieldnorm_id, term_freq)
    }

    #[inline]
    pub fn tf_factor(&self, fieldnorm_id: u8, term_freq: u32) -> Score {
        let term_freq = term_freq as Score;
        let norm = self.cache[fieldnorm_id as usize];
        (term_freq * (self.constants.k1 + 1.0)) / (term_freq + norm)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bm25_idf_scaling() {
        // assume the query is something like 'the end'
        // 'the' appears in almost all docs (98)
        // 'end' appears in a smalle subset (20)
        let constants = Bm25Constants::default();
        let weight = MultiBm25Weight {
            weights: vec![
                Bm25Weight::for_one_term(98, 100, 1.0, constants),
                Bm25Weight::for_one_term(20, 100, 1.0, constants),
            ],
        };

        // if a document has high frequency of 'end'
        // it should have a higher score than a document that
        // has an almost equally high frequency of 'the'
        let high_the = weight.score(vec![(0, 15), (0, 10)].into_iter());
        let high_end = weight.score(vec![(0, 8), (0, 13)].into_iter());
        assert!(high_end > high_the);
    }
}
