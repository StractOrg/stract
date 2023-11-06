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

use anyhow::Result;
use hashbrown::{HashMap, HashSet};
use url::Url;

use crate::{
    kv::rocksdb_store::RocksDbStore,
    webgraph::{
        centrality::{top_hosts, TopHosts},
        NodeID, Webgraph,
    },
    webpage::url_ext::UrlExt,
};

use super::{index::FeedIndex, Feed};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct Domain(String);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DomainFeeds {
    pub domain: Domain,
    pub feeds: Vec<Feed>,
}

impl From<&Url> for Domain {
    fn from(url: &Url) -> Self {
        Domain(url.icann_domain().unwrap_or_default().to_string())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct SplitId(uuid::Uuid);

impl SplitId {
    pub fn id(&self) -> uuid::Uuid {
        self.0
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Split {
    pub id: SplitId,
    pub feeds: Vec<DomainFeeds>,
}

impl Split {
    pub fn save<P: AsRef<Path>>(self, path: P) -> Result<()> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())?;
        let writer = std::io::BufWriter::new(file);

        serde_json::to_writer_pretty(writer, &self)
            .map_err(|e| anyhow::anyhow!("Failed to serialize split: {}", e))?;

        Ok(())
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(path.as_ref())?;
        let reader = std::io::BufReader::new(file);

        serde_json::from_reader(reader)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize split: {}", e))
    }
}

pub struct Schedule {
    splits: Vec<Split>,
}
impl Schedule {
    pub fn save<P: AsRef<Path>>(self, path: P) -> Result<()> {
        if !path.as_ref().exists() {
            std::fs::create_dir_all(&path)?;
        }

        for split in self.splits {
            let name = split.id.0.to_string() + ".json";
            split.save(path.as_ref().join(name))?;
        }

        Ok(())
    }
}

pub fn schedule(
    index: &FeedIndex,
    host_centrality: &RocksDbStore<NodeID, f64>,
    host_graph: &Webgraph,
    num_splits: u64,
) -> Schedule {
    let top_hosts = top_hosts(host_centrality, TopHosts::Top(1_000_000));

    let mut all_feeds = HashMap::new();

    for host in top_hosts {
        let host = host_graph.id2node(&host).unwrap();
        let url = Url::parse(&format!("http://{}", host.name));

        if url.is_err() {
            continue;
        }

        let url = url.unwrap();
        let domain = Domain::from(&url);

        all_feeds
            .entry(domain)
            .or_insert(HashSet::new())
            .extend(index.search(&host.name).unwrap().into_iter());
    }

    let mut splits = Vec::new();

    for _ in 0..num_splits {
        splits.push(Split {
            id: SplitId(uuid::Uuid::new_v4()),
            feeds: Vec::new(),
        });
    }

    for (i, (domain, feeds)) in all_feeds.into_iter().enumerate() {
        let split = &mut splits[i % num_splits as usize];

        split.feeds.push(DomainFeeds {
            domain,
            feeds: feeds.into_iter().collect(),
        });
    }

    Schedule { splits }
}
