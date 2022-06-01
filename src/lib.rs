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
use serde::Deserialize;
use std::io;
use std::num::ParseIntError;
use tantivy::TantivyError;
use thiserror::Error;

pub mod entrypoint;
mod index;
mod query;
mod ranking;
mod schema;
mod searcher;
mod snippet;
mod tokenizer;
mod warc;
mod webgraph;
mod webpage;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub mode: Mode,
    warc_source: Option<WarcSource>,
    warc_paths_file: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", content = "args")]
pub enum WarcSource {
    S3(S3Config),
    HTTP(HttpConfig),
}

#[derive(Debug, Deserialize, Clone)]
pub enum Mode {
    /// Index warc documents into index
    Indexer,
    /// Create webgraph from warc documents
    Webgraph,
}

#[derive(Debug, Deserialize, Clone)]
pub struct S3Config {
    name: String,
    endpoint: String,
    bucket: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpConfig {
    base_url: String,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to download object from S3")]
    S3DownloadError,

    #[error("Failed to download object from HTTP")]
    HTTPDownloadERror(#[from] reqwest::Error),

    #[error("Failed to get the object from S3")]
    GetObjectError(#[from] rusoto_core::RusotoError<rusoto_s3::GetObjectError>),

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
}

pub(crate) type Result<T> = std::result::Result<T, Error>;
