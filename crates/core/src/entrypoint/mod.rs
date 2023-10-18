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

//! The entrypoint module contains all entrypoints that runs the executables.
#[cfg(feature = "with_alice")]
pub mod alice;
pub mod api;
pub mod autosuggest_scrape;
mod centrality;
#[cfg(feature = "dev")]
pub mod configure;
pub mod crawler;
pub mod dmoz_parser;
pub mod indexer;
pub mod safety_classifier;
pub mod search_server;
mod webgraph;
pub mod webgraph_server;

pub use centrality::Centrality;
pub use entity_index::builder::EntityIndexer;
pub use indexer::Indexer;
use tracing::{debug, log::error};
pub use webgraph::Webgraph;

use crate::{config, warc::WarcFile};

fn download_all_warc_files<'a>(
    warc_paths: &'a [String],
    source: &'a config::WarcSource,
) -> impl Iterator<Item = WarcFile> + 'a {
    let warc_paths: Vec<_> = warc_paths
        .iter()
        .map(|warc_path| warc_path.to_string())
        .collect();

    warc_paths.into_iter().filter_map(|warc_path| {
        debug!("downloading warc file {}", &warc_path);
        let res = warc_download::download(source, &warc_path);

        if let Err(err) = res {
            error!("error while downloading: {:?}", err);
            return None;
        }

        debug!("finished downloading");

        Some(res.unwrap())
    })
}

mod warc_download {
    use std::{
        fs::File,
        io::{BufReader, Cursor, Read, Seek, Write},
        path::Path,
        thread::sleep,
        time::Duration,
    };

    use distributed::retry_strategy::ExponentialBackoff;
    use tracing::{debug, trace};

    use crate::{
        config::{S3Config, WarcSource},
        warc::WarcFile,
        Error, Result,
    };

    pub(super) fn download(source: &WarcSource, warc_path: &str) -> Result<WarcFile> {
        let mut cursor = Cursor::new(Vec::new());
        download_into_buf(source, warc_path, &mut cursor)?;
        cursor.rewind()?;

        let mut buf = Vec::new();
        cursor.read_to_end(&mut buf)?;

        Ok(WarcFile::new(buf))
    }

    fn download_into_buf<W: Write + Seek>(
        source: &WarcSource,
        warc_path: &str,
        buf: &mut W,
    ) -> Result<()> {
        for dur in ExponentialBackoff::from_millis(10)
            .with_limit(Duration::from_secs(30))
            .take(35)
        {
            let res = match source.clone() {
                WarcSource::HTTP(config) => download_from_http(warc_path, config.base_url, buf),
                WarcSource::Local(config) => load_from_folder(warc_path, &config.folder, buf),
                WarcSource::S3(config) => download_from_s3(warc_path, &config, buf),
            };

            if res.is_ok() {
                return Ok(());
            } else {
                trace!("Error {:?}", res);
            }

            debug!("warc download failed: {:?}", res.err().unwrap());
            debug!("retrying in {} ms", dur.as_millis());

            sleep(dur);
        }

        Err(Error::DownloadFailed.into())
    }

    fn load_from_folder<W: Write + Seek>(name: &str, folder: &str, buf: &mut W) -> Result<()> {
        let f = File::open(Path::new(folder).join(name))?;
        let mut reader = BufReader::new(f);

        buf.rewind()?;

        std::io::copy(&mut reader, buf)?;

        Ok(())
    }

    fn download_from_http<W: Write + Seek>(
        warc_path: &str,
        base_url: String,
        buf: &mut W,
    ) -> Result<()> {
        let mut url = base_url;
        if !url.ends_with('/') {
            url += "/";
        }
        url += warc_path;

        let client = reqwest::blocking::ClientBuilder::new()
            .tcp_keepalive(None)
            .pool_idle_timeout(Duration::from_secs(30 * 60))
            .timeout(Duration::from_secs(30 * 60))
            .connect_timeout(Duration::from_secs(30 * 60))
            .build()?;
        let res = client.get(url).send()?;

        if res.status().as_u16() != 200 {
            return Err(Error::DownloadFailed.into());
        }

        let bytes = res.bytes()?;

        buf.rewind()?;
        std::io::copy(&mut &bytes[..], buf)?;

        Ok(())
    }

    fn download_from_s3<W: Write + Seek>(
        warc_path: &str,
        config: &S3Config,
        buf: &mut W,
    ) -> Result<()> {
        let bucket = s3::Bucket::new(
            &config.bucket,
            s3::Region::Custom {
                region: "".to_string(),
                endpoint: config.endpoint.clone(),
            },
            s3::creds::Credentials {
                access_key: Some(config.access_key.clone()),
                secret_key: Some(config.secret_key.clone()),
                security_token: None,
                session_token: None,
                expiration: None,
            },
        )?
        .with_path_style()
        .with_request_timeout(Duration::from_secs(30 * 60));

        let res = bucket.get_object_blocking(warc_path)?;

        buf.write_all(res.bytes())?;

        Ok(())
    }
}
