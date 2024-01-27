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
pub mod api;
pub mod autosuggest_scrape;
mod centrality;
#[cfg(feature = "dev")]
pub mod configure;
pub mod crawler;
pub mod dmoz_parser;
mod entity;
pub mod entity_search_server;
pub mod feed_indexer;
pub mod indexer;
pub mod safety_classifier;
pub mod search_server;
pub mod web_spell;
mod webgraph;
pub mod webgraph_server;

pub use centrality::Centrality;
pub use entity::EntityIndexer;
pub use indexer::Indexer;
use tracing::{debug, log::error};
pub use webgraph::Webgraph;
pub mod live_index;

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
        let res = WarcFile::download(source, &warc_path);

        if let Err(err) = res {
            error!("error while downloading: {:?}", err);
            return None;
        }

        debug!("finished downloading");

        Some(res.unwrap())
    })
}
