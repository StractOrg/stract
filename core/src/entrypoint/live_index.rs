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

use std::path::Path;

use crate::{
    config::{LiveIndexConfig, LiveIndexSchedulerConfig},
    feed::{self, index::FeedIndex},
    kv::rocksdb_store::RocksDbStore,
    live_index::IndexManager,
    webgraph::WebgraphBuilder,
};
use anyhow::Result;

pub async fn serve(config: LiveIndexConfig) -> Result<()> {
    let manager = IndexManager::new(config)?;
    tokio::task::spawn(manager.run());

    todo!("accept search requests");
}

pub fn schedule(config: LiveIndexSchedulerConfig) -> Result<()> {
    let feed_index = FeedIndex::open(config.feed_index_path)?;
    let host_harmonic =
        RocksDbStore::open(Path::new(&config.host_centrality_store_path).join("harmonic"));
    let host_graph = WebgraphBuilder::new(config.host_graph_path).open();

    let schedule =
        feed::scheduler::schedule(&feed_index, &host_harmonic, &host_graph, config.num_splits);
    schedule.save(config.schedule_path)?;

    Ok(())
}
