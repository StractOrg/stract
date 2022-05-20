use serde::Deserialize;
use std::io;
use std::num::ParseIntError;
use thiserror::Error;

mod indexer;
mod warc;

pub use indexer::Indexer;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub mode: Mode,
    s3: S3Config,
    warc_paths_file: String,
}

#[derive(Debug, Deserialize)]
pub enum Mode {
    Indexer,
}

#[derive(Debug, Deserialize)]
pub struct S3Config {
    name: String,
    endpoint: String,
    bucket: String,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to download object from S3")]
    S3DownloadError,

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
