use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use std::fs::File;
use std::io::{self, BufRead};
use tokio::io::AsyncReadExt;

use crate::warc::WarcFile;
use crate::{Config, Error, Result};

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
        for warc_s3_path in self.warc_paths {
            println!("{}", warc_s3_path);
            let raw_object = Indexer::download_from_s3(
                warc_s3_path,
                self.config.s3.name.clone(),
                self.config.s3.endpoint.clone(),
                self.config.s3.bucket.clone(),
            )
            .await?;

            println!("Downloaded {} bytes", raw_object.len());
            let warc = WarcFile::new(&raw_object[..]);
            for record in warc {
                println!("TEST: {:?}", record);
                panic!();
            }
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
            .ok_or(Error::S3DownloadError)?
            .into_async_read()
            .read_to_end(&mut res)
            .await?;

        Ok(res)
    }
}
