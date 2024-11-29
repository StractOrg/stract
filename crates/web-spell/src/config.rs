// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

fn misspelled_prob() -> f64 {
    0.1
}

fn correction_threshold() -> f64 {
    50.0 // logprob difference
}

fn lm_prob_weight() -> f64 {
    5.77
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
pub struct CorrectionConfig {
    /// The probability that a word is misspelled
    #[serde(default = "misspelled_prob")]
    pub misspelled_prob: f64,

    /// Lambda in eq. 2 (http://static.googleusercontent.com/media/research.google.com/en/us/pubs/archive/36180.pdf)
    #[serde(default = "lm_prob_weight")]
    pub lm_prob_weight: f64,

    /// The threshold that the difference between the log probability of the best
    /// correction and the observed word must be above for the word to be
    /// corrected
    #[serde(default = "correction_threshold")]
    pub correction_threshold: f64,
}

impl Default for CorrectionConfig {
    fn default() -> Self {
        Self {
            misspelled_prob: misspelled_prob(),
            lm_prob_weight: lm_prob_weight(),
            correction_threshold: correction_threshold(),
        }
    }
}

pub fn bincode_config() -> bincode::config::Configuration {
    bincode::config::standard()
}
