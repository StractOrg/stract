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
mod centrality;
mod entity;
pub mod frontend;
mod indexer;
pub mod search_server;
mod webgraph;

use std::{fs::File, path::Path};

pub use centrality::Centrality;
pub use entity::EntityIndexer;
use futures::StreamExt;
pub use indexer::Indexer;
use tracing::debug;
pub use webgraph::Webgraph;

use crate::{warc::WarcFile, WarcSource};

async fn async_download_all_warc_files(
    warc_paths: &[String],
    source: &WarcSource,
    download_path: &Path,
) {
    futures::stream::iter(
        warc_paths
            .iter()
            .map(|path| (path.split('/').last().unwrap(), path))
            .map(|(name, path)| {
                let file = File::options()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(download_path.join(name))
                    .unwrap();
                (file, path)
            })
            .map(|(mut file, warc_path)| async move {
                debug!("downloading warc file {}", &warc_path);
                let res = WarcFile::download_into_buf(source, warc_path, &mut file)
                    .await
                    .ok();
                debug!("finished downloading");

                res
            }),
    )
    .buffer_unordered(warc_paths.len())
    .collect::<Vec<Option<()>>>()
    .await;
}

fn download_all_warc_files(
    warc_paths: &[String],
    source: &WarcSource,
    base_path: &str,
) -> Vec<String> {
    let download_path = Path::new(base_path).join("warc_files");

    if !download_path.exists() {
        std::fs::create_dir_all(&download_path).unwrap();
    }

    let file_paths = warc_paths
        .iter()
        .map(|warc_path| warc_path.split('/').last().unwrap())
        .map(|name| {
            download_path
                .join(name)
                .as_os_str()
                .to_str()
                .unwrap()
                .to_string()
        })
        .collect();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            async_download_all_warc_files(warc_paths, source, &download_path).await
        });

    file_paths
}
