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

use serde::{Deserialize, Serialize};

use super::Result;
use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{BufReader, BufWriter},
    path::Path,
};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ErrorType {
    Insertion(char),
    Deletion(char),
    Substitution(char, char),
    Transposition(char, char),
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ErrorSequence(Vec<ErrorType>);

pub fn possible_errors(a: &str, b: &str) -> Option<ErrorSequence> {
    if a == b {
        return None;
    }

    let a_len = a.chars().count();
    let b_len = b.chars().count();
    let mut dp = vec![vec![0; b_len + 1]; a_len + 1];

    for i in 0..=a_len {
        for j in 0..=b_len {
            if i == 0 {
                dp[i][j] = j;
            } else if j == 0 {
                dp[i][j] = i;
            } else {
                let cost = if a.chars().nth(i - 1) == b.chars().nth(j - 1) {
                    0
                } else {
                    1
                };
                dp[i][j] = std::cmp::min(
                    std::cmp::min(dp[i - 1][j] + 1, dp[i][j - 1] + 1),
                    dp[i - 1][j - 1] + cost,
                );
            }
        }
    }

    let mut i = a_len;
    let mut j = b_len;
    let mut errors = Vec::new();

    while i > 0 && j > 0 {
        let cost = if a.chars().nth(i - 1) == b.chars().nth(j - 1) {
            0
        } else {
            1
        };
        if dp[i][j] == dp[i - 1][j - 1] + cost {
            if cost == 1 {
                errors.push(ErrorType::Substitution(
                    a.chars().nth(i - 1).unwrap(),
                    b.chars().nth(j - 1).unwrap(),
                ));
            }
            i -= 1;
            j -= 1;
        } else if dp[i][j] == dp[i - 1][j] + 1 {
            errors.push(ErrorType::Deletion(a.chars().nth(i - 1).unwrap()));
            i -= 1;
        } else {
            errors.push(ErrorType::Insertion(b.chars().nth(j - 1).unwrap()));
            j -= 1;
        }
    }

    while i > 0 {
        errors.push(ErrorType::Deletion(a.chars().nth(i - 1).unwrap()));
        i -= 1;
    }

    while j > 0 {
        errors.push(ErrorType::Insertion(b.chars().nth(j - 1).unwrap()));
        j -= 1;
    }

    if !errors.is_empty() {
        Some(ErrorSequence(errors))
    } else {
        None
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredErrorModel {
    errors: HashMap<String, u64>,
    total: u64,
}

impl From<ErrorModel> for StoredErrorModel {
    fn from(value: ErrorModel) -> Self {
        let stored_errors = value
            .errors
            .into_iter()
            .map(|(error_seq, count)| (serde_json::to_string(&error_seq).unwrap(), count))
            .collect();

        Self {
            errors: stored_errors,
            total: value.total,
        }
    }
}

impl From<StoredErrorModel> for ErrorModel {
    fn from(value: StoredErrorModel) -> Self {
        let errors = value
            .errors
            .into_iter()
            .map(|(error_seq, count)| (serde_json::from_str(&error_seq).unwrap(), count))
            .collect();

        Self {
            errors,
            total: value.total,
        }
    }
}

#[derive(Debug)]
pub struct ErrorModel {
    errors: HashMap<ErrorSequence, u64>,
    total: u64,
}

impl Default for ErrorModel {
    fn default() -> Self {
        Self::new()
    }
}

impl ErrorModel {
    pub fn new() -> Self {
        Self {
            errors: HashMap::new(),
            total: 0,
        }
    }

    pub fn save<P: AsRef<Path>>(self, path: P) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        let wrt = BufWriter::new(file);

        serde_json::to_writer_pretty(wrt, &StoredErrorModel::from(self))?;

        Ok(())
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;

        let rdr = BufReader::new(file);

        let stored: StoredErrorModel = serde_json::from_reader(rdr)?;

        Ok(stored.into())
    }

    pub fn add(&mut self, a: &str, b: &str) {
        if let Some(errors) = possible_errors(a, b) {
            *self.errors.entry(errors).or_insert(0) += 1;
            self.total += 1;
        }
    }

    pub fn prob(&self, error: &ErrorSequence) -> f64 {
        let count = self.errors.get(error).unwrap_or(&0);
        *count as f64 / self.total as f64
    }

    pub fn log_prob(&self, error: &ErrorSequence) -> f64 {
        match self.errors.get(error) {
            Some(count) => (*count as f64).log2() - (self.total as f64).log2(),
            None => 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_possible_errors() {
        assert_eq!(possible_errors("hello", "hello"), None);

        assert_eq!(
            possible_errors("hello", "helo"),
            Some(ErrorSequence(vec![ErrorType::Deletion('l')]))
        );

        assert_eq!(
            possible_errors("hello", "hellol"),
            Some(ErrorSequence(vec![ErrorType::Insertion('l')]))
        );

        assert_eq!(
            possible_errors("hello", "heo"),
            Some(ErrorSequence(vec![
                ErrorType::Deletion('l'),
                ErrorType::Deletion('l')
            ]))
        );

        assert_eq!(
            possible_errors("hello", "helli"),
            Some(ErrorSequence(vec![ErrorType::Substitution('o', 'i')]))
        );
    }
}
