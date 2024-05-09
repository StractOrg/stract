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

use crate::{
    distributed::{
        cluster::Cluster,
        member::Service,
        sonic::replication::{
            AllShardsSelector, RandomReplicaSelector, RemoteClient, ReplicatedClient, Shard,
            ShardIdentifier, ShardedClient, SpecificShardSelector,
        },
    },
    entrypoint::search_server::{self, SearchService},
    feed::scheduler::SplitId,
    inverted_index::{RetrievedWebpage, WebpagePointer},
    ranking::pipeline::{PrecisionRankingWebpage, RecallRankingWebpage},
};

use std::future::Future;
use std::{collections::HashMap, sync::Arc};

use fnv::FnvHashMap;
use futures::future::join_all;
use itertools::Itertools;

use super::{InitialWebsiteResult, SearchQuery};

#[derive(Clone, Debug)]
pub struct ScoredWebpagePointer {
    pub website: RecallRankingWebpage,
    pub split_id: SplitId,
}

impl ShardIdentifier for SplitId {}

#[derive(Debug)]
pub struct InitialSearchResultSplit {
    pub local_result: InitialWebsiteResult,
    pub split_id: SplitId,
}

pub struct LiveSearcher {
    cluster: Arc<Cluster>,
}

impl LiveSearcher {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        Self { cluster }
    }

    async fn client(&self) -> ShardedClient<SearchService, SplitId> {
        let mut shards = HashMap::new();
        for member in self.cluster.members().await {
            if let Service::LiveIndex { host, split_id } = member.service {
                shards.entry(split_id).or_insert_with(Vec::new).push(host);
            }
        }

        let mut shard_clients = Vec::new();

        for (id, replicas) in shards {
            let replicated =
                ReplicatedClient::new(replicas.into_iter().map(RemoteClient::new).collect());
            let shard = Shard::new(id, replicated);
            shard_clients.push(shard);
        }

        ShardedClient::new(shard_clients)
    }

    async fn retrieve_webpages_from_shard(
        &self,
        split: SplitId,
        client: &ShardedClient<SearchService, SplitId>,
        query: &str,
        pointers: Vec<(usize, WebpagePointer)>,
    ) -> Vec<(usize, RetrievedWebpage)> {
        let (idxs, pointers): (Vec<usize>, Vec<WebpagePointer>) = pointers.into_iter().unzip();

        match client
            .send(
                search_server::RetrieveWebsites {
                    websites: pointers,
                    query: query.to_string(),
                },
                &SpecificShardSelector(split),
                &RandomReplicaSelector,
            )
            .await
        {
            Ok(v) => v
                .into_iter()
                .flat_map(|(_, v)| v.into_iter().map(|(_, v)| v))
                .flatten()
                .flatten()
                .zip_eq(idxs)
                .map(|(v, i)| (i, v))
                .collect(),
            _ => vec![],
        }
    }
}

impl SearchClient for LiveSearcher {
    async fn search_initial(&self, query: &SearchQuery) -> Vec<InitialSearchResultSplit> {
        let client = self.client().await;
        let mut results = Vec::new();

        if let Ok(res) = client
            .send(
                search_server::Search {
                    query: query.clone(),
                },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await
        {
            for (shard_id, mut res) in res {
                if let Some((_, Some(res))) = res.pop() {
                    results.push(InitialSearchResultSplit {
                        local_result: res,
                        split_id: shard_id,
                    });
                }
            }
        }

        results
    }

    async fn retrieve_webpages(
        &self,
        top_websites: &[(usize, ScoredWebpagePointer)],
        query: &str,
    ) -> Vec<(usize, PrecisionRankingWebpage)> {
        let mut rankings = FnvHashMap::default();
        let mut pointers: HashMap<_, Vec<_>> = HashMap::new();

        for (i, pointer) in top_websites {
            pointers
                .entry(pointer.split_id.clone())
                .or_default()
                .push((*i, pointer.website.pointer().clone()));

            rankings.insert(*i, pointer.website.clone());
        }

        let client = self.client().await;
        let mut futures = Vec::new();
        for (shard, pointers) in pointers {
            futures.push(self.retrieve_webpages_from_shard(shard, &client, query, pointers));
        }

        let mut retrieved_webpages = Vec::new();
        for pages in join_all(futures).await {
            for (i, page) in pages {
                retrieved_webpages
                    .push((i, PrecisionRankingWebpage::new(page, rankings[&i].clone())));
            }
        }

        debug_assert_eq!(retrieved_webpages.len(), top_websites.len());

        retrieved_webpages.sort_by(|(a, _), (b, _)| a.cmp(b));

        retrieved_webpages
    }
}

pub trait SearchClient {
    fn search_initial(
        &self,
        query: &SearchQuery,
    ) -> impl Future<Output = Vec<InitialSearchResultSplit>> + Send;
    fn retrieve_webpages(
        &self,
        top_websites: &[(usize, ScoredWebpagePointer)],
        query: &str,
    ) -> impl Future<Output = Vec<(usize, PrecisionRankingWebpage)>> + Send;
}
