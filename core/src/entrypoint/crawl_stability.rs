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

use flate2::bufread::MultiGzDecoder;

use crate::{
    kv::{rocksdb_store::RocksDbStore, Kv},
    Result,
};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

pub struct CrawlStability {
    scores: Box<dyn Kv<String, f64>>,
}

impl CrawlStability {
    pub fn build<P: AsRef<Path>>(host_ranks_paths: Vec<String>, path: P) -> Result<Self> {
        let scores = RocksDbStore::open(path.as_ref());

        let norm: f64 = (0..host_ranks_paths.len())
            .map(|i| 1.0 / (i + 1) as f64)
            .sum();

        for (i, path) in host_ranks_paths.into_iter().enumerate() {
            let file = File::open(path)?;
            let reader = BufReader::new(file);
            let reader = BufReader::new(MultiGzDecoder::new(reader));

            for line in reader.lines().skip(1) {
                let line = line?;
                let rev_host = line.split('\t').last().unwrap();
                let host: String = itertools::intersperse(rev_host.split('.').rev(), ".").collect();
                let current_score: f64 = scores.get(&host).unwrap_or_default();
                scores.insert(host, current_score + ((1.0 / (i + 1) as f64) / norm));
            }
        }

        scores.flush();

        Ok(Self { scores })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let hosts = RocksDbStore::open(path);
        Self { scores: hosts }
    }

    pub fn get(&self, host: &String) -> Option<f64> {
        self.scores.get(host)
    }
}
