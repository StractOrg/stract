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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Domain(String);

pub struct DomainFeeds {
    domain: Domain,
    feeds: Vec<Feed>,
}

pub struct Split {
    feeds: Vec<DomainFeeds>,
}

pub struct Schedule {
    splits: Vec<Split>,
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
        let domain = Domain(url.icann_domain().unwrap().to_string());

        all_feeds
            .entry(domain)
            .or_insert(HashSet::new())
            .extend(index.search(&host.name).unwrap().into_iter());
    }

    let mut splits = Vec::new();

    for _ in 0..num_splits {
        splits.push(Split { feeds: Vec::new() });
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
