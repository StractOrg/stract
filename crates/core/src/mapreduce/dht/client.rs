// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

use rand::seq::SliceRandom;
use std::{collections::BTreeMap, net::SocketAddr};

use crate::{
    distributed::{
        cluster::Cluster,
        member::{Service, ShardId},
    },
    Result,
};

use super::network::api;

struct Node {
    api: api::RemoteClient,
}

impl Node {
    fn new(addr: SocketAddr) -> Self {
        let api = api::RemoteClient::new(addr);

        Self { api }
    }

    async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.api.get(key).await
    }

    async fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.api.set(key, value).await
    }
}

struct Shard {
    nodes: Vec<Node>,
}

impl Shard {
    fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    fn add_node(&mut self, addr: SocketAddr) {
        self.nodes.push(Node::new(addr));
    }

    fn node(&self) -> &Node {
        self.nodes.choose(&mut rand::thread_rng()).unwrap()
    }

    async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.node().get(key).await
    }

    async fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.node().set(key, value).await
    }
}

pub struct Client {
    shards: BTreeMap<ShardId, Shard>,
}

impl Client {
    pub async fn new(cluster: &Cluster) -> Self {
        let dht_members = cluster
            .members()
            .await
            .into_iter()
            .filter_map(|member| {
                if let Service::Dht { shard, host } = member.service {
                    Some((shard, host))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut shards = BTreeMap::new();

        for (shard, host) in dht_members {
            shards
                .entry(shard)
                .or_insert_with(Shard::new)
                .add_node(host);
        }

        Self { shards }
    }

    pub fn add_node(&mut self, shard_id: ShardId, addr: SocketAddr) {
        self.shards
            .entry(shard_id)
            .or_insert_with(Shard::new)
            .add_node(addr);
    }

    fn shard_for_key(&self, key: &[u8]) -> Result<&Shard> {
        let ids = self.shards.keys().collect::<Vec<_>>();

        if ids.is_empty() {
            return Err(anyhow::anyhow!("No shards"));
        }

        let hash = md5::compute(key);
        let hash = u64::from_le_bytes((&hash.0[..(u64::BITS / 8) as usize]).try_into().unwrap());

        let shard_id = ids[hash as usize % ids.len()];
        Ok(self.shards.get(shard_id).unwrap())
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.shard_for_key(&key)?.get(key).await
    }

    pub async fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.shard_for_key(&key)?.set(key, value).await
    }
}
