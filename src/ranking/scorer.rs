use crate::search_index::FastWebpage;

pub(crate) trait Scorer {
    fn score(&self, webpages: &[FastWebpage]) -> Vec<f64>;
}
