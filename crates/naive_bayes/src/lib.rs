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

use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use std::hash::Hash;
use stdx::intmap::IntMap;

/// Laplace smoothing factor
const ALPHA: f32 = 1.0;

/// Struct for sparse Term Frequency-Inverse Document Frequency (TF-IDF) features.
#[derive(Debug)]
pub struct TfIdf {
    pub term_id: u64,
    pub value: f32,
}

/// Vectorizer to convert text into TF-IDF features
#[derive(serde::Deserialize, serde::Serialize)]
struct TfidfVectorizer {
    vocabulary: HashMap<String, u64>,
    idf: IntMap<u64, f32>,
}

impl TfidfVectorizer {
    pub fn new() -> Self {
        Self {
            vocabulary: HashMap::new(),
            idf: IntMap::new(),
        }
    }

    pub fn fit(&mut self, corpus: &[String]) {
        let doc_freq = self.calculate_document_frequencies(corpus);
        self.build_vocabulary_and_idf(corpus, &doc_freq);
    }

    /// Transforms a document into TF-IDF features
    pub fn transform(&self, doc: &str) -> Vec<TfIdf> {
        let term_frequencies = self.calculate_term_frequencies(doc);
        self.calculate_tfidf(&term_frequencies)
    }

    /// Helper function to calculate document frequencies
    fn calculate_document_frequencies(&self, corpus: &[String]) -> HashMap<String, usize> {
        let mut doc_freq: HashMap<String, usize> = HashMap::new();
        let mut doc_words: HashSet<&str> = HashSet::new();

        for doc in corpus {
            doc_words.clear();
            for word in doc.split_whitespace() {
                if doc_words.insert(word) {
                    *doc_freq.entry(word.to_string()).or_insert(0) += 1;
                }
            }
        }
        doc_freq
    }

    /// Helper function to build vocabulary and idf
    fn build_vocabulary_and_idf(&mut self, corpus: &[String], doc_freq: &HashMap<String, usize>) {
        let mut vocab: Vec<String> = doc_freq.keys().cloned().collect();
        vocab.sort();
        for (term_id, word) in vocab.iter().enumerate() {
            let term_id = term_id as u64;
            self.vocabulary.insert(word.to_owned(), term_id);
            self.idf.insert(
                term_id,
                ((corpus.len() as f32) / (1.0 + doc_freq[word] as f32)).ln() + 1.0,
            );
        }
    }

    /// Helper function to calculate term frequencies for a document
    fn calculate_term_frequencies(&self, doc: &str) -> IntMap<u64, f32> {
        let mut tf: IntMap<u64, f32> = IntMap::new();
        for word in doc.split_whitespace() {
            if let Some(&term_id) = self.vocabulary.get(word) {
                let c = tf.get(&term_id).copied().unwrap_or_default();
                tf.insert(term_id, c + 1.0);
            }
        }
        tf
    }

    /// Helper function to calculate TF-IDF values
    fn calculate_tfidf(&self, term_frequencies: &IntMap<u64, f32>) -> Vec<TfIdf> {
        term_frequencies
            .iter()
            .map(|(term_id, tf)| TfIdf {
                term_id: *term_id,
                value: tf * self.idf.get(term_id).copied().unwrap_or_default(),
            })
            .collect()
    }
}

pub trait Label: Hash + PartialEq + Eq + Clone + PartialOrd + Ord {}

impl Label for String {}

/// Struct to represent a data point for classification
#[derive(Debug)]
pub struct Datapoint<L: Label> {
    pub features: Vec<TfIdf>,
    pub label: L,
}

#[derive(Debug)]
pub struct Prediction<L> {
    pub label: L,
    pub confidence: f32,
}

/// Naive Bayes Classifier
#[derive(serde::Deserialize, serde::Serialize)]
pub struct NaiveBayes<L: Label> {
    classes: Vec<L>,
    class_prior: Vec<f32>,
    feature_log_prob: Vec<IntMap<u64, f32>>,
}

impl Default for NaiveBayes<String> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: Label> NaiveBayes<L> {
    pub fn new() -> Self {
        Self {
            classes: Vec::new(),
            class_prior: Vec::new(),
            feature_log_prob: Vec::new(),
        }
    }

    pub fn fit(&mut self, datapoints: &[Datapoint<L>]) {
        if datapoints.is_empty() {
            return;
        }
        self.classes = self.extract_unique_classes(datapoints);
        let class_counts = self.count_classes(datapoints);
        self.calculate_feature_log_prob(&class_counts, datapoints);
    }

    /// Predicts the class for a given sample
    pub fn predict(&self, sample: &[TfIdf]) -> Prediction<L> {
        let class_log_probs = self.calculate_class_log_probs(sample);

        let mut best_class_index = 0;
        let mut best_class_log_prob = f32::INFINITY;

        let mut s = 0.0;

        for (class_id, class_log_prob) in class_log_probs {
            if class_log_prob < best_class_log_prob {
                best_class_index = class_id;
                best_class_log_prob = class_log_prob;
            }
            s += class_log_prob;
        }

        let label = self.classes[best_class_index].clone();
        let confidence = (best_class_log_prob / s).max(0.0);
        Prediction { label, confidence }
    }

    /// Helper function to extract unique classes from the data points
    fn extract_unique_classes(&self, datapoints: &[Datapoint<L>]) -> Vec<L> {
        datapoints
            .iter()
            .map(|d| &d.label)
            .unique()
            .cloned()
            .sorted()
            .collect()
    }

    /// Helper function to count the number of instances for each class
    fn count_classes(&self, datapoints: &[Datapoint<L>]) -> HashMap<L, usize> {
        let mut class_counts: HashMap<L, usize> = HashMap::new();
        for datapoint in datapoints {
            *class_counts.entry(datapoint.label.clone()).or_insert(0) += 1;
        }
        class_counts
    }

    /// Helper function to calculate feature log probabilities for each class
    fn calculate_feature_log_prob(
        &mut self,
        class_counts: &HashMap<L, usize>,
        datapoints: &[Datapoint<L>],
    ) {
        let num_classes = self.classes.len();
        let mut feature_log_prob = vec![IntMap::new(); num_classes];
        for (class_id, class) in self.classes.iter().enumerate() {
            let mut counts: IntMap<u64, f32> = IntMap::new();
            let mut total_count: f32 = 0.0;
            for datapoint in datapoints {
                if &datapoint.label == class {
                    for feature in &datapoint.features {
                        let count = counts.get(&feature.term_id).copied().unwrap_or_default();
                        counts.insert(feature.term_id, count + feature.value);
                        total_count += feature.value;
                    }
                }
            }
            for (term_id, count) in counts.iter() {
                let log_prob = ((count + ALPHA) / (total_count + ALPHA)).ln();
                feature_log_prob[class_id].insert(*term_id, log_prob);
            }
        }
        self.class_prior = self
            .classes
            .iter()
            .map(|class| class_counts[class] as f32 / datapoints.len() as f32)
            .collect();
        self.feature_log_prob = feature_log_prob;
    }

    /// Helper function to calculate the log probabilities for each class
    fn calculate_class_log_probs<'a>(
        &'a self,
        sample: &'a [TfIdf],
    ) -> impl Iterator<Item = (usize, f32)> + 'a {
        self.classes
            .iter()
            .enumerate()
            .map(|(id, _)| id)
            .map(move |class_id| {
                let mut log_prob = self.class_prior[class_id].ln();
                for feature in sample {
                    if feature.value == 0.0 {
                        continue;
                    }

                    log_prob += feature.value
                        * self.feature_log_prob[class_id]
                            .get(&feature.term_id)
                            .copied()
                            .unwrap_or(ALPHA / (ALPHA + ALPHA)) // Laplace smoothing
                }
                (class_id, log_prob)
            })
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct Pipeline<L: Label> {
    vectorizer: TfidfVectorizer,
    classifier: NaiveBayes<L>,
}

impl Default for Pipeline<String> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: Label> Pipeline<L> {
    pub fn new() -> Self {
        Self {
            vectorizer: TfidfVectorizer::new(),
            classifier: NaiveBayes::new(),
        }
    }

    pub fn fit(&mut self, data: &[(String, L)]) {
        let (corpus, labels): (Vec<_>, Vec<_>) = data.iter().cloned().unzip();

        self.vectorizer.fit(&corpus);
        let datapoints: Vec<Datapoint<_>> = corpus
            .iter()
            .zip_eq(labels)
            .map(|(doc, label)| Datapoint {
                features: self.vectorizer.transform(doc),
                label: label.clone(),
            })
            .collect();
        self.classifier.fit(&datapoints);
    }

    pub fn predict(&self, doc: &str) -> Prediction<L> {
        let features = self.vectorizer.transform(doc);
        self.classifier.predict(&features)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_naive_bayes() {
        let x = vec![
            vec![1.0, 1.0, 0.0],
            vec![1.0, 1.0, 0.0],
            vec![1.0, 1.0, 0.0],
            vec![1.0, 1.0, 0.0],
            vec![1.0, 1.0, 0.0],
            vec![0.0, 1.0, 1.0],
            vec![0.0, 1.0, 1.0],
            vec![0.0, 1.0, 1.0],
            vec![0.0, 1.0, 1.0],
            vec![0.0, 1.0, 1.0],
        ];
        let y = vec![
            "ham".to_owned(),
            "ham".to_owned(),
            "ham".to_owned(),
            "ham".to_owned(),
            "ham".to_owned(),
            "spam".to_owned(),
            "spam".to_owned(),
            "spam".to_owned(),
            "spam".to_owned(),
            "spam".to_owned(),
        ];

        let datapoints: Vec<Datapoint<_>> = x
            .into_iter()
            .zip_eq(y)
            .map(|(x, y)| {
                let features = x
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, v)| {
                        if v == 0.0 {
                            None
                        } else {
                            Some(TfIdf {
                                term_id: i as u64,
                                value: v,
                            })
                        }
                    })
                    .collect();
                Datapoint { features, label: y }
            })
            .collect();

        let mut model = NaiveBayes::new();
        model.fit(&datapoints);
        let pred = model.predict(&[TfIdf {
            term_id: 0,
            value: 1.0,
        }]);
        assert_eq!(pred.label, "ham".to_owned());

        let pred = model.predict(&[TfIdf {
            term_id: 2,
            value: 1.0,
        }]);

        assert_eq!(pred.label, "spam".to_owned());
    }
}
