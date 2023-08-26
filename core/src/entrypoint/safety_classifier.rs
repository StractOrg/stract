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

use rand::seq::SliceRandom;
use tracing::info;

use crate::{webpage, Result};
use std::path::Path;

const TEST_SIZE: f64 = 0.2;

pub fn train<P: AsRef<Path>>(dataset: P, output: P) -> Result<()> {
    if !dataset.as_ref().exists() {
        return Err(anyhow::anyhow!(
            "dataset path {:?} does not exist",
            dataset.as_ref()
        ));
    }

    let mut model = webpage::safety_classifier::Model::new();
    let mut dataset = webpage::safety_classifier::load_dataset(dataset)?;

    if dataset.is_empty() {
        return Err(anyhow::anyhow!("dataset is empty"));
    }

    dataset.shuffle(&mut rand::thread_rng());

    let test_size = (dataset.len() as f64 * TEST_SIZE) as usize;
    let test_set = dataset.split_off(dataset.len() - test_size);

    model.fit(&dataset);
    let evaluation = model.evaluate(&test_set);

    info!("accuracy: {}", evaluation.accuracy);
    info!("precision: {}", evaluation.precision);
    info!("recall: {}", evaluation.recall);
    info!("f1: {}", evaluation.f1);

    model.save(output)?;

    Ok(())
}

pub fn predict<P: AsRef<Path>>(model: P, text: &str) -> Result<()> {
    let model = webpage::safety_classifier::Model::open(model)?;
    let pred = model.predict_text(text);

    info!("{:#?}", pred);

    Ok(())
}
