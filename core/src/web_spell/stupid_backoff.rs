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

use super::{tokenize, MergePointer, Result};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::BufWriter,
    path::Path,
};

use fst::{Automaton, IntoStreamer, Streamer};
use serde::{Deserialize, Serialize};

const DISCOUNT: f64 = 0.4;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct Ngram {
    terms: Vec<String>,
}

pub struct StoredNgram {
    combined: String,
}

impl From<Ngram> for StoredNgram {
    fn from(ngram: Ngram) -> Self {
        Self {
            combined: ngram.terms.join(" "),
        }
    }
}

impl AsRef<[u8]> for StoredNgram {
    fn as_ref(&self) -> &[u8] {
        self.combined.as_bytes()
    }
}

pub struct StupidBackoffTrainer {
    max_ngram_size: usize,
    ngrams: BTreeMap<Ngram, u64>,
    rotated_ngrams: BTreeMap<Ngram, u64>,
    n_counts: Vec<u64>,
}

impl StupidBackoffTrainer {
    pub fn new(max_ngram_size: usize) -> Self {
        Self {
            max_ngram_size,
            ngrams: BTreeMap::new(),
            rotated_ngrams: BTreeMap::new(),
            n_counts: vec![0; max_ngram_size],
        }
    }

    pub fn train(&mut self, tokens: &[String]) {
        for window in tokens.windows(self.max_ngram_size) {
            for i in 1..window.len() {
                let ngram = Ngram {
                    terms: window[..i].to_vec(),
                };

                self.ngrams
                    .entry(ngram)
                    .and_modify(|e| *e += 1)
                    .or_insert(1);

                self.n_counts[i - 1] += 1;
            }

            let mut ngram = Ngram {
                terms: window.to_vec(),
            };
            ngram.terms.rotate_left(1);
            self.rotated_ngrams
                .entry(ngram)
                .and_modify(|e| *e += 1)
                .or_insert(1);
        }
    }

    pub fn build<P: AsRef<Path>>(self, path: P) -> Result<()> {
        if !path.as_ref().exists() {
            std::fs::create_dir_all(path.as_ref())?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path.as_ref().join("ngrams.bin"))?;

        let wtr = BufWriter::new(file);

        let mut builder = fst::MapBuilder::new(wtr)?;

        for (ngram, freq) in self.ngrams {
            builder.insert(StoredNgram::from(ngram), freq)?;
        }

        builder.finish()?;

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path.as_ref().join("rotated_ngrams.bin"))?;

        let wtr = BufWriter::new(file);

        let mut builder = fst::MapBuilder::new(wtr)?;

        for (ngram, freq) in self.rotated_ngrams {
            builder.insert(StoredNgram::from(ngram), freq)?;
        }

        builder.finish()?;

        bincode::serialize_into(
            File::create(path.as_ref().join("n_counts.bin"))?,
            &self.n_counts,
        )?;

        Ok(())
    }
}

fn merge_streams(
    mut builder: fst::MapBuilder<BufWriter<File>>,
    streams: Vec<fst::map::Stream<'_, fst::automaton::AlwaysMatch>>,
) -> Result<()> {
    let mut pointers: Vec<_> = streams
        .into_iter()
        .map(|stream| MergePointer {
            term: String::new(),
            value: 0,
            stream,
            is_finished: false,
        })
        .collect();

    for pointer in pointers.iter_mut() {
        pointer.advance();
    }

    while pointers.iter().any(|p| !p.is_finished) {
        let mut min_pointer: Option<&MergePointer<'_>> = None;

        for pointer in pointers.iter() {
            if pointer.is_finished {
                continue;
            }

            if let Some(min) = min_pointer {
                if pointer.term < min.term {
                    min_pointer = Some(pointer);
                }
            } else {
                min_pointer = Some(pointer);
            }
        }

        if let Some(min_pointer) = min_pointer {
            let term = min_pointer.term.clone();
            let mut freq = 0;

            for pointer in pointers.iter_mut() {
                if pointer.is_finished {
                    continue;
                }

                if pointer.term == term {
                    freq += pointer.value;
                    pointer.advance();
                }
            }

            builder.insert(term, freq)?;
        }
    }

    builder.finish()?;

    Ok(())
}

pub struct StupidBackoff {
    ngrams: fst::Map<memmap::Mmap>,
    rotated_ngrams: fst::Map<memmap::Mmap>,
    n_counts: Vec<u64>,
}

impl StupidBackoff {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mmap = unsafe { memmap::Mmap::map(&File::open(path.as_ref().join("ngrams.bin"))?)? };
        let ngrams = fst::Map::new(mmap)?;

        let mmap =
            unsafe { memmap::Mmap::map(&File::open(path.as_ref().join("rotated_ngrams.bin"))?)? };
        let rotated_ngrams = fst::Map::new(mmap)?;

        let n_counts = bincode::deserialize_from(File::open(path.as_ref().join("n_counts.bin"))?)?;

        Ok(Self {
            ngrams,
            rotated_ngrams,
            n_counts,
        })
    }

    pub fn merge<P: AsRef<Path>>(models: Vec<Self>, path: P) -> Result<Self> {
        if !path.as_ref().exists() {
            std::fs::create_dir_all(path.as_ref())?;
        }
        let n_counts = models
            .iter()
            .fold(vec![0; models[0].n_counts.len()], |mut acc, m| {
                for (i, n) in m.n_counts.iter().enumerate() {
                    acc[i] += n;
                }

                acc
            });

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path.as_ref().join("ngrams.bin"))?;

        let wtr = BufWriter::new(file);
        let builder = fst::MapBuilder::new(wtr)?;

        let streams: Vec<_> = models.iter().map(|d| d.ngrams.stream()).collect();

        merge_streams(builder, streams)?;

        let mmap = unsafe { memmap::Mmap::map(&File::open(path.as_ref().join("ngrams.bin"))?)? };
        let ngrams = fst::Map::new(mmap)?;

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path.as_ref().join("rotated_ngrams.bin"))?;

        let wtr = BufWriter::new(file);
        let builder = fst::MapBuilder::new(wtr)?;

        let streams: Vec<_> = models.iter().map(|d| d.ngrams.stream()).collect();

        merge_streams(builder, streams)?;

        let mmap = unsafe { memmap::Mmap::map(&File::open(path.as_ref().join("ngrams.bin"))?)? };
        let rotated_ngrams = fst::Map::new(mmap)?;

        Ok(Self {
            ngrams,
            rotated_ngrams,
            n_counts,
        })
    }

    pub fn freq(&self, words: &[String]) -> Option<u64> {
        if words.len() >= self.ngrams.len() || words.is_empty() {
            return None;
        }

        let ngram = StoredNgram {
            combined: words.join(" "),
        };

        self.ngrams.get(ngram)
    }

    pub fn log_prob<S: NextWordsStrategy>(&self, words: &[String], strat: S) -> f64 {
        if words.len() >= self.ngrams.len() || words.is_empty() {
            return 0.0;
        }

        if let Some(freq) = self.freq(words) {
            (freq as f64 / self.n_counts[words.len() - 1] as f64).log2()
        } else {
            let mut strat = strat;
            DISCOUNT.log2() + self.log_prob(strat.next_words(words), strat)
        }
    }

    pub fn contexts(&self, word: &str) -> Vec<(Vec<String>, u64)> {
        let q = word.to_string() + " ";
        let automaton = fst::automaton::Str::new(&q).starts_with();

        let mut stream = self.rotated_ngrams.search(automaton).into_stream();

        let mut contexts = Vec::new();

        while let Some((ngram, freq)) = stream.next() {
            if let Ok(ngram) = std::str::from_utf8(ngram) {
                let mut ngram = tokenize(ngram);
                ngram.rotate_right(1);
                contexts.push((ngram, freq));
            }
        }

        contexts
    }
}

pub trait NextWordsStrategy {
    fn next_words<'a>(&mut self, words: &'a [String]) -> &'a [String];
}

pub struct LeftToRight;

impl NextWordsStrategy for LeftToRight {
    fn next_words<'a>(&mut self, words: &'a [String]) -> &'a [String] {
        &words[1..]
    }
}

pub struct RightToLeft;

impl NextWordsStrategy for RightToLeft {
    fn next_words<'a>(&mut self, words: &'a [String]) -> &'a [String] {
        &words[..words.len() - 1]
    }
}

#[derive(Default)]
pub struct IntoMiddle {
    last_right: bool,
}

impl NextWordsStrategy for IntoMiddle {
    fn next_words<'a>(&mut self, words: &'a [String]) -> &'a [String] {
        let res = if self.last_right {
            &words[..words.len() - 1]
        } else {
            &words[1..]
        };

        self.last_right = !self.last_right;

        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gen_temp_path;

    #[test]
    fn test_contexts() {
        let mut trainer = StupidBackoffTrainer::new(3);

        trainer.train(&tokenize(
            "a b c d e f g h i j k l m n o p q r s t u v w x y z",
        ));

        let path = gen_temp_path();

        trainer.build(&path).unwrap();

        let model = StupidBackoff::open(&path).unwrap();

        assert_eq!(
            model.contexts("b"),
            vec![(vec!["a".to_string(), "b".to_string(), "c".to_string()], 1)]
        );
    }
}
