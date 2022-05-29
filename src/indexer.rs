use futures::prelude::*;
use indicatif::{ProgressBar, ProgressIterator, ProgressStyle};
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use std::fs::File;
use std::io::{self, BufRead};
use tokio::io::AsyncReadExt;

use crate::warc::{WarcFile};
use crate::webpage::Html;
use crate::{Config, Error, Result, WarcSource};

pub struct Indexer {
    warc_paths: Vec<String>,
    config: Config,
}

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
        let pb = ProgressBar::new(self.warc_paths.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{wide_bar}] {pos:>7}/{len:7} ({eta})",
                )
                .progress_chars("#>-"),
        );

        stream::iter(self.warc_paths.into_iter().progress_with(pb))
            .map(|warc_path| {
                let download_config = self
                    .config
                    .warc_source
                    .as_ref()
                    .expect("Indexing needs a warc source")
                    .clone();

                tokio::spawn(async move {
                    let raw_object = match download_config {
                        WarcSource::S3(config) => {
                            Indexer::download_from_s3(
                                warc_path.clone(),
                                config.name.clone(),
                                config.endpoint.clone(),
                                config.bucket.clone(),
                            )
                            .await
                        }
                        WarcSource::HTTP(config) => {
                            Indexer::download_from_http(warc_path.clone(), config.base_url.clone())
                                .await
                        }
                    };

                    raw_object
                })
            })
            .buffer_unordered(20)
            .map(|raw_bytes| {
                tokio::task::spawn_blocking(move || {
                    if raw_bytes.is_err() {
                        return;
                    }
                    let raw_bytes = raw_bytes.unwrap();
                    if raw_bytes.is_err() {
                        return;
                    }
                    let raw_bytes = raw_bytes.unwrap();
                    let warc = WarcFile::new(&raw_bytes[..]);
                    for record in warc.flatten() {
                        let _webpage = Html::parse(&record.response.body, &record.request.url);
                        // println!("TEST: {:?}", webpage.title());
                        // println!();
                    }
                    // panic!();
                })
            })
            .buffer_unordered(20)
            .collect::<Vec<_>>()
            .await;

        Ok(())
    }

    async fn download_from_http(warc_path: String, base_url: String) -> Result<Vec<u8>> {
        let mut url = base_url;
        if !url.ends_with('/') {
            url += "/";
        }
        url += &warc_path;

        let client = reqwest::Client::new();
        let res = client.get(url).send().await?;

        Ok(Vec::from(&res.bytes().await?[..]))
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
            .ok_or(Error::S3DownloadError)?
            .into_async_read()
            .read_to_end(&mut res)
            .await?;

        Ok(res)
    }
}
