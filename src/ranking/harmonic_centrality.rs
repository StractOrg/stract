use std::collections::HashMap;

use super::scorer::Scorer;
use crate::{search_index::FastWebpage, webgraph::Node};

struct HarmonicCentrality {}

impl From<HashMap<Node, f64>> for HarmonicCentrality {
    fn from(scores: HashMap<Node, f64>) -> Self {
        todo!()
    }
}

impl Scorer for HarmonicCentrality {
    fn score(&self, webpages: &[FastWebpage]) -> Vec<f64> {
        todo!()
    }
}
