use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use serde::Deserialize;
use std::fs::File;
use std::io::{self, BufRead};
use thiserror::Error;
use tokio::io::AsyncReadExt;

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
pub enum CuelyError {
    #[error("Failed to download object from S3")]
    S3DownloadError,

    #[error("Failed to get the object from S3")]
    GetObjectError(#[from] rusoto_core::RusotoError<rusoto_s3::GetObjectError>),

    #[error("Got an IO error")]
    IOError(#[from] io::Error),
}

pub struct Indexer {
    warc_paths: Vec<String>,
    config: Config,
}

type Result<T> = std::result::Result<T, CuelyError>;

impl Indexer {
    pub fn from_config(config: Config) -> Self {
        let file = File::open(&config.warc_paths_file).unwrap();
        let mut warc_paths = Vec::new();

        for line in io::BufReader::new(file).lines() {
            warc_paths.push(line.unwrap());
        }

        Self { warc_paths, config }
    }

    pub async fn run(self) -> Result<()> {
        for warc_s3_path in self.warc_paths {
            println!("{}", warc_s3_path);
            Indexer::download_from_s3(
                warc_s3_path,
                self.config.s3.name.clone(),
                self.config.s3.endpoint.clone(),
                self.config.s3.bucket.clone(),
            )
            .await?;
        }

        Ok(())
    }

    async fn download_from_s3(
        key: String,
        region_name: String,
        region_endpoint: String,
        bucket: String,
    ) -> Result<Vec<u8>> {
        let region = Region::Custom {
            name: region_name,
            endpoint: region_endpoint,
        };

        let client = S3Client::new(region);

        let obj = client
            .get_object(GetObjectRequest {
                bucket,
                key,
                ..Default::default()
            })
            .await?;

        let mut res = Vec::new();
        obj.body
            .ok_or(CuelyError::S3DownloadError)?
            .into_async_read()
            .read_to_end(&mut res)
            .await?;

        Ok(res)
    }
}
