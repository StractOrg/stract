// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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
use std::fs::File;
use std::io::{self, BufRead};

use crate::warc::WarcFile;
use crate::webpage::Html;
use crate::{IndexingConfig, Result};

pub struct Indexer {
    warc_paths: Vec<String>,
    config: IndexingConfig,
}

impl From<IndexingConfig> for Indexer {
    fn from(config: IndexingConfig) -> Self {
        let file = File::open(&config.warc_paths_file).unwrap();
        let mut warc_paths = Vec::new();

        for line in io::BufReader::new(file).lines() {
            warc_paths.push(line.unwrap());
        }

        Self { warc_paths, config }
    }
}

impl Indexer {
    pub fn run(self) -> Result<()> {
        self.warc_paths
            .into_iter()
            .map(|warc_path| {
                let source = self.config.warc_source.clone();

                WarcFile::download(source, &warc_path)
            })
            .map(|warc| {
                if warc.is_err() {
                    return;
                }
                let warc = warc.unwrap();

                for record in warc.records().flatten() {
                    let _webpage = Html::parse(&record.response.body, &record.request.url);
                    // println!("TEST: {:?}", webpage.title());
                    // println!();
                }
                // panic!();
            })
            .for_each(drop);

        Ok(())
    }
}
