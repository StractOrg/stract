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

use std::fs::OpenOptions;
use std::path::Path;

use crate::naive_bayes;
use crate::Result;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum Label {
    SFW,
    NSFW,
}

impl naive_bayes::Label for Label {}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Datapoint {
    pub label: Label,
    pub text: String,
}

pub fn load_dataset<P: AsRef<Path>>(path: P) -> Result<Vec<Datapoint>> {
    let mut datapoints = Vec::new();
    let mut reader = csv::Reader::from_path(path)?;
    for result in reader.deserialize() {
        let datapoint: Datapoint = result?;
        datapoints.push(datapoint);
    }
    Ok(datapoints)
}

fn normalize(text: &str) -> String {
    text.to_string()
}

pub fn page_text(page: &crate::webpage::Webpage) -> String {
    page.html.title().unwrap_or_default()
        + " "
        + page.html.clean_text().cloned().unwrap_or_default().as_str()
}

pub struct Evaluation {
    pub accuracy: f64,
    pub precision: f64,
    pub recall: f64,
    pub f1: f64,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct Model {
    pipeline: naive_bayes::Pipeline<Label>,
}

impl Default for Model {
    fn default() -> Self {
        Self::new()
    }
}

impl Model {
    pub fn new() -> Self {
        let pipeline = naive_bayes::Pipeline::new();
        Self { pipeline }
    }

    pub fn fit(&mut self, datapoints: &[Datapoint]) {
        let datapoints: Vec<_> = datapoints
            .iter()
            .map(|datapoint| (normalize(&datapoint.text), datapoint.label))
            .collect();
        self.pipeline.fit(&datapoints);
    }

    pub fn predict_text(&self, text: &str) -> naive_bayes::Prediction<Label> {
        let text = normalize(text);
        self.pipeline.predict(&text)
    }

    pub fn predict(&self, page: &crate::webpage::Webpage) -> naive_bayes::Prediction<Label> {
        let text = normalize(&page_text(page));
        self.predict_text(&text)
    }

    pub fn evaluate(&self, datapoints: &[Datapoint]) -> Evaluation {
        let mut true_positives = 0;
        let mut false_positives = 0;
        let mut true_negatives = 0;
        let mut false_negatives = 0;

        for datapoint in datapoints {
            let pred = self.predict_text(&datapoint.text);

            match (pred.label, datapoint.label) {
                (Label::NSFW, Label::NSFW) => true_positives += 1,
                (Label::NSFW, Label::SFW) => false_positives += 1,
                (Label::SFW, Label::SFW) => true_negatives += 1,
                (Label::SFW, Label::NSFW) => false_negatives += 1,
            }

            if pred.label != datapoint.label {
                tracing::debug!(
                    "got {:?} expected {:?} ({:.2}):",
                    pred.label,
                    datapoint.label,
                    pred.confidence
                );
                tracing::debug!("{}\n", datapoint.text);
            }
        }

        let accuracy = (true_positives + true_negatives) as f64 / datapoints.len() as f64;
        let precision = true_positives as f64 / (true_positives + false_positives) as f64;
        let recall = true_positives as f64 / (true_positives + false_negatives) as f64;
        let f1 = 2.0 * (precision * recall) / (precision + recall);

        Evaluation {
            accuracy,
            precision,
            recall,
            f1,
        }
    }

    pub fn save<P: AsRef<Path>>(self, path: P) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        bincode::serialize_into(file, &self)?;

        Ok(())
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;

        let model = bincode::deserialize_from(file)?;

        Ok(model)
    }
}
