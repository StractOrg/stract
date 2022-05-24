use serde::Deserialize;
use std::io;
use std::num::ParseIntError;
use thiserror::Error;

mod indexer;
mod warc;
mod webgraph;
mod webpage;

pub use indexer::Indexer;

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
pub enum Mode {
    Indexer,
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
}

pub(crate) type Result<T> = std::result::Result<T, Error>;
