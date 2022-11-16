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

// #![warn(clippy::pedantic)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::missing_errors_doc)]

use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, BufRead};
use std::num::ParseIntError;
use std::path::PathBuf;
use tantivy::TantivyError;
use thiserror::Error;

pub mod entrypoint;
mod inverted_index;

pub mod mapreduce;

mod autosuggest;
mod bangs;
mod collector;
mod directory;
mod entity_index;
mod exponential_backoff;
mod fastfield_cache;
mod frontend;
mod human_website_annotations;
mod hyperloglog;
mod image_downloader;
mod image_store;
pub mod index;
mod intmap;
mod kahan_sum;
mod kv;
pub mod prehashed;
mod query;
pub mod ranking;
mod schema;
mod search_prettifier;
pub mod searcher;
mod simhash;
mod snippet;
mod sonic;
mod spell;
mod subdomain_count;
mod tokenizer;
#[allow(unused)]
mod ttl_cache;
mod warc;
pub mod webgraph;
pub mod webpage;

#[derive(Debug, Deserialize, Clone)]
pub struct IndexingMasterConfig {
    limit_warc_files: Option<usize>,
    final_num_segments: Option<u32>,
    warc_source: WarcSource,
    workers: Vec<String>,
    batch_size: Option<usize>,
    download_images: Option<bool>,
    host_centrality_threshold: Option<f64>,
    index_base_path: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct IndexingLocalConfig {
    crawl_stability_path: Option<String>,
    limit_warc_files: Option<usize>,
    final_num_segments: Option<u32>,
    warc_source: WarcSource,
    batch_size: Option<usize>,
    webgraph_path: Option<String>,
    output_path: Option<String>,
    download_images: Option<bool>,
    host_centrality_threshold: Option<f64>,
    topics_path: Option<String>,
    centrality_store_path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WebgraphMasterConfig {
    limit_warc_files: Option<usize>,
    warc_source: WarcSource,
    workers: Vec<String>,
    graph_base_path: Option<String>,
    batch_size: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WebgraphLocalConfig {
    limit_warc_files: Option<usize>,
    warc_source: WarcSource,
    graph_base_path: Option<String>,
    batch_size: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum WarcSource {
    HTTP(HttpConfig),
    Local(LocalConfig),
}

impl WarcSource {
    pub fn paths(&self) -> Result<Vec<String>> {
        let mut warc_paths = Vec::new();
        match &self {
            WarcSource::HTTP(config) => {
                let file = File::open(&config.warc_paths_file)?;
                for line in io::BufReader::new(file).lines() {
                    warc_paths.push(line?);
                }
            }
            WarcSource::Local(config) => {
                warc_paths = config.names.clone();
            }
        }

        Ok(warc_paths)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalConfig {
    pub folder: String,
    pub names: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HttpConfig {
    base_url: String,
    warc_paths_file: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FrontendConfig {
    pub queries_csv_path: String,
    pub host: String,
    pub search_servers: Vec<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SearchServerConfig {
    pub index_path: String,
    pub entity_index_path: Option<String>,
    pub bangs_path: Option<String>,
    pub centrality_store_path: Option<String>,
    pub host: String,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to download object from HTTP")]
    HTTPDownloadError(#[from] reqwest::Error),

    #[error("Got an IO error")]
    IOError(#[from] io::Error),

    #[error("Not valid UTF8")]
    FromUTF8(#[from] std::string::FromUtf8Error),

    #[error("Failed to parse WARC file")]
    WarcParse(&'static str),

    #[error("Could not parse string to int")]
    IntParse(#[from] ParseIntError),

    #[error("Encountered a tantivy error")]
    Tantivy(#[from] TantivyError),

    #[error("Encountered an empty required field when converting to tantivy")]
    EmptyField(&'static str),

    #[error("Parsing error")]
    ParsingError(String),

    #[error("Error while serializing/deserializing to/from bytes")]
    Serialization(#[from] bincode::Error),

    #[error("Error executing distributed jobs")]
    MapReduce(#[from] mapreduce::Error),

    #[error("Failed to download warc files after all retries")]
    DownloadFailed,

    #[error("Encountered an error when reading CSV file")]
    Csv(#[from] csv::Error),

    #[error("Encountered an error in the FST crate")]
    Fst(#[from] fst::Error),

    #[error("Image error")]
    Image(#[from] image::ImageError),

    #[error("XML parser error")]
    XML(#[from] quick_xml::Error),

    #[error("Spell dictionary error")]
    Spell(#[from] crate::spell::dictionary::DictionaryError),

    #[error("Parser error")]
    Parse,

    #[error("Query cannot be completely empty")]
    EmptyQuery,

    #[error("Unknown region")]
    UnknownRegion,

    #[error("String is not float")]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("Could not open inverted-index directory")]
    Directory(#[from] tantivy::directory::error::OpenDirectoryError),

    #[error("Could not convert to/from JSON")]
    Json(#[from] serde_json::Error),

    #[error("Unknown CLI option")]
    UnknownCLIOption,
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

// taken from https://docs.rs/sled/0.34.7/src/sled/config.rs.html#445
fn gen_temp_path() -> PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;

    static SALT_COUNTER: AtomicUsize = AtomicUsize::new(0);

    let seed = SALT_COUNTER.fetch_add(1, Ordering::SeqCst) as u128;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        << 48;

    let pid = u128::from(std::process::id());

    let salt = (pid << 16) + now + seed;

    if cfg!(target_os = "linux") {
        // use shared memory for temporary linux files
        format!("/dev/shm/pagecache.tmp.{salt}").into()
    } else {
        std::env::temp_dir().join(format!("pagecache.tmp.{salt}"))
    }
}

#[cfg(test)]
fn rand_words(num_words: usize) -> String {
    use rand::{distributions::Alphanumeric, Rng};
    let mut res = String::new();

    for _ in 0..num_words {
        res.push_str(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .map(char::from)
                .collect::<String>()
                .as_str(),
        );
        res.push(' ');
    }

    res.trim().to_string()
}
