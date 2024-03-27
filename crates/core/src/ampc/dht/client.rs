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

use super::{
    network::api,
    store::{Key, Table, Value},
};

struct Node {
    api: api::RemoteClient,
}

impl Node {
    fn new(addr: SocketAddr) -> Self {
        let api = api::RemoteClient::new(addr);

        Self { api }
    }

    async fn get(&self, table: Table, key: Key) -> Result<Option<Value>> {
        self.api.get(table, key).await
    }

    async fn set(&self, table: Table, key: Key, value: Value) -> Result<()> {
        self.api.set(table, key, value).await
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

    async fn get(&self, table: Table, key: Key) -> Result<Option<Value>> {
        self.node().get(table, key).await
    }

    async fn set(&self, table: Table, key: Key, value: Value) -> Result<()> {
        self.node().set(table, key, value).await
    }
}

pub struct Client {
    ids: Vec<ShardId>,
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

        let ids = shards.keys().cloned().collect();

        Self { shards, ids }
    }

    pub fn add_node(&mut self, shard_id: ShardId, addr: SocketAddr) {
        self.shards
            .entry(shard_id)
            .or_insert_with(Shard::new)
            .add_node(addr);

        self.ids = self.shards.keys().cloned().collect();
    }

    fn shard_for_key(&self, key: &[u8]) -> Result<&Shard> {
        if self.ids.is_empty() {
            return Err(anyhow::anyhow!("No shards"));
        }

        let hash = md5::compute(key);
        let hash = u64::from_le_bytes((&hash.0[..(u64::BITS / 8) as usize]).try_into().unwrap());

        let shard_id = &self.ids[hash as usize % self.ids.len()];
        Ok(self.shards.get(shard_id).unwrap())
    }

    pub async fn get(&self, table: Table, key: Key) -> Result<Option<Value>> {
        self.shard_for_key(key.as_bytes())?.get(table, key).await
    }

    pub async fn set(&self, table: Table, key: Key, value: Value) -> Result<()> {
        self.shard_for_key(key.as_bytes())?
            .set(table, key, value)
            .await
    }

    pub async fn drop_table(&self, table: Table) -> Result<()> {
        for shard in self.shards.values() {
            for node in &shard.nodes {
                node.api.drop_table(table.clone()).await?;
            }
        }

        Ok(())
    }

    pub async fn create_table(&self, table: Table) -> Result<()> {
        for shard in self.shards.values() {
            for node in &shard.nodes {
                node.api.create_table(table.clone()).await?;
            }
        }

        Ok(())
    }

    pub async fn all_tables(&self) -> Result<Vec<Table>> {
        let mut tables = Vec::new();

        for shard in self.shards.values() {
            for node in &shard.nodes {
                tables.extend(node.api.all_tables().await?);
            }
        }

        tables.sort();
        tables.dedup();

        Ok(tables)
    }
}
