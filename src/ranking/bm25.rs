use super::scorer::Scorer;
use crate::search_index::FastWebpage;

struct BM25 {}

impl Scorer for BM25 {
    fn score(&self, webpages: &[FastWebpage]) -> Vec<f64> {
        todo!()
    }
}
