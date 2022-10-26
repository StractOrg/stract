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
pub mod autosuggest_scrape;
mod centrality;
mod entity;
pub mod frontend;
pub mod indexer;
pub mod search_server;
mod webgraph;

use std::{fs::File, path::Path};

pub use centrality::Centrality;
pub use entity::EntityIndexer;
use futures::{Stream, StreamExt};
pub use indexer::Indexer;
use tracing::debug;
pub use webgraph::Webgraph;

use crate::{warc::WarcFile, WarcSource};

async fn async_download_all_warc_files<'a>(
    warc_paths: &'a [String],
    source: &'a WarcSource,
    base_path: &'a str,
) -> impl Stream<Item = String> + 'a {
    let download_path = Path::new(base_path).join("warc_files");

    if !download_path.exists() {
        std::fs::create_dir_all(&download_path).unwrap();
    }

    let warc_paths: Vec<_> = warc_paths
        .iter()
        .map(|warc_path| warc_path.to_string())
        .collect();

    let num_files = warc_paths.len();

    futures::stream::iter(warc_paths.into_iter().map(|warc_path| async {
        let download_path = Path::new(base_path).join("warc_files");
        let name = warc_path.split('/').last().unwrap();
        let mut file = File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .open(download_path.join(name))
            .unwrap();
        debug!("downloading warc file {}", &warc_path);
        let res = WarcFile::download_into_buf(source, &warc_path, &mut file).await;

        if let Err(err) = res {
            debug!("error while downloading: {:?}", err);
        }

        debug!("finished downloading");

        warc_path
    }))
    .buffer_unordered(num_files)
}
