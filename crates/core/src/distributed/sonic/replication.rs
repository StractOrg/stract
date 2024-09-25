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

use futures::future::join_all;
use rand::seq::IteratorRandom;

use super::Result;
use crate::distributed::{cluster::Cluster, retry_strategy::ExponentialBackoff, sonic};
use std::{net::SocketAddr, ops::DerefMut, sync::Arc, time::Duration};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_RETRY: ExponentialBackoff =
    ExponentialBackoff::from_millis(500).with_limit(Duration::from_secs(3));

#[derive(Debug)]
pub struct RemoteClient<S>
where
    S: sonic::service::Service,
{
    addr: SocketAddr,
    pool: sonic::ConnectionPool<sonic::service::Connection<S>>,
    _phantom: std::marker::PhantomData<S>,
}

impl<S> Clone for RemoteClient<S>
where
    S: sonic::service::Service,
{
    fn clone(&self) -> Self {
        Self::create(self.addr)
    }
}

impl<S> RemoteClient<S>
where
    S: sonic::service::Service,
{
    pub fn new(addr: SocketAddr) -> Self {
        Self::create(addr)
    }

    pub fn create(addr: SocketAddr) -> Self {
        Self {
            addr,
            pool: sonic::ConnectionPool::new(addr).unwrap(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl<S> RemoteClient<S>
where
    S: sonic::service::Service,
{
    pub async fn conn(&self) -> Result<impl DerefMut<Target = sonic::service::Connection<S>>> {
        self.pool
            .get()
            .await
            .map_err(|_| crate::distributed::sonic::Error::PoolGet)
    }

    pub async fn send<R: sonic::service::Wrapper<S> + Clone>(&self, req: R) -> Result<R::Response> {
        self.send_with_timeout_retry(req, DEFAULT_TIMEOUT, DEFAULT_RETRY)
            .await
    }

    pub async fn batch_send<R: sonic::service::Wrapper<S> + Clone>(
        &self,
        reqs: &[R],
    ) -> Result<Vec<R::Response>> {
        self.batch_send_with_timeout_retry(reqs, DEFAULT_TIMEOUT, DEFAULT_RETRY)
            .await
    }

    pub async fn send_with_timeout<R: sonic::service::Wrapper<S> + Clone>(
        &self,
        req: R,
        timeout: Duration,
    ) -> Result<R::Response> {
        let mut conn = self.conn().await?;
        conn.send_with_timeout(req, timeout).await
    }

    pub async fn batch_send_with_timeout<R: sonic::service::Wrapper<S> + Clone>(
        &self,
        reqs: &[R],
        timeout: Duration,
    ) -> Result<Vec<R::Response>> {
        let mut conn = self.conn().await?;
        conn.batch_send_with_timeout(reqs, timeout).await
    }

    pub async fn send_with_timeout_retry<R: sonic::service::Wrapper<S> + Clone>(
        &self,
        req: R,
        timeout: Duration,
        retry: impl Iterator<Item = Duration>,
    ) -> Result<R::Response> {
        let mut er = None;
        for backoff in retry {
            match self.send_with_timeout(req.clone(), timeout).await {
                Ok(r) => return Ok(r),
                Err(e) => {
                    tracing::error!("Failed to send request: {:?}", e);
                    er = Some(e);
                    tokio::time::sleep(backoff).await;
                }
            }
        }

        Err(er.unwrap())
    }

    pub async fn batch_send_with_timeout_retry<R: sonic::service::Wrapper<S> + Clone>(
        &self,
        reqs: &[R],
        timeout: Duration,
        retry: impl Iterator<Item = Duration>,
    ) -> Result<Vec<R::Response>> {
        let mut er = None;
        for backoff in retry {
            match self.batch_send_with_timeout(reqs, timeout).await {
                Ok(r) => return Ok(r),
                Err(e) => {
                    tracing::error!("Failed to send request: {:?}", e);
                    er = Some(e);
                    tokio::time::sleep(backoff).await;
                }
            }
        }

        Err(er.unwrap())
    }
}

pub trait ReplicaSelector<S: sonic::service::Service> {
    fn select<'a>(&self, replicas: &'a [RemoteClient<S>]) -> Vec<&'a RemoteClient<S>>;
}

pub struct RandomReplicaSelector;

impl<S> ReplicaSelector<S> for RandomReplicaSelector
where
    S: sonic::service::Service,
{
    fn select<'a>(&self, replicas: &'a [RemoteClient<S>]) -> Vec<&'a RemoteClient<S>> {
        let mut rng = rand::thread_rng();
        replicas.iter().choose_multiple(&mut rng, 1)
    }
}

pub struct AllReplicaSelector;

impl<S> ReplicaSelector<S> for AllReplicaSelector
where
    S: sonic::service::Service,
{
    fn select<'a>(&self, replicas: &'a [RemoteClient<S>]) -> Vec<&'a RemoteClient<S>> {
        replicas.iter().collect()
    }
}

pub struct ReplicatedClient<S: sonic::service::Service> {
    clients: Vec<RemoteClient<S>>,
}

impl<S> ReplicatedClient<S>
where
    S: sonic::service::Service,
{
    pub fn new(clients: Vec<RemoteClient<S>>) -> Self {
        Self { clients }
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    async fn send_single<Req>(
        &self,
        req: Req,
        client: &RemoteClient<S>,
        timeout: Duration,
    ) -> Result<(SocketAddr, Req::Response)>
    where
        Req: sonic::service::Wrapper<S> + Clone,
    {
        Ok((client.addr(), client.send_with_timeout(req, timeout).await?))
    }
    pub async fn send_with_timeout<Req, Sel>(
        &self,
        req: Req,
        selector: &Sel,
        timeout: Duration,
    ) -> Result<Vec<(SocketAddr, Req::Response)>>
    where
        Req: sonic::service::Wrapper<S> + Clone,
        Sel: ReplicaSelector<S>,
    {
        let mut futures = Vec::new();
        for client in selector.select(&self.clients) {
            futures.push(self.send_single(req.clone(), client, timeout));
        }

        let mut results = Vec::new();
        for r in join_all(futures).await {
            match r {
                Ok(r) => results.push(r),
                Err(e) => {
                    tracing::error!("Failed to send request: {:?}", e);
                }
            }
        }

        Ok(results)
    }

    pub async fn send<Req, Sel>(
        &self,
        req: Req,
        selector: &Sel,
    ) -> Result<Vec<(SocketAddr, Req::Response)>>
    where
        Req: sonic::service::Wrapper<S> + Clone,
        Sel: ReplicaSelector<S>,
    {
        self.send_with_timeout(req, selector, DEFAULT_TIMEOUT).await
    }

    async fn batch_send_single<Req>(
        &self,
        reqs: &[Req],
        client: &RemoteClient<S>,
    ) -> Result<(SocketAddr, Vec<Req::Response>)>
    where
        Req: sonic::service::Wrapper<S> + Clone,
    {
        Ok((client.addr(), client.batch_send(reqs).await?))
    }

    pub async fn batch_send<Req, Sel>(
        &self,
        reqs: &[Req],
        selector: &Sel,
    ) -> Result<Vec<(SocketAddr, Vec<Req::Response>)>>
    where
        Req: sonic::service::Wrapper<S> + Clone,
        Sel: ReplicaSelector<S>,
    {
        let mut futures = Vec::new();
        for client in selector.select(&self.clients) {
            futures.push(self.batch_send_single(reqs, client));
        }

        let mut results = Vec::new();
        for r in join_all(futures).await {
            match r {
                Ok(r) => results.push(r),
                Err(e) => {
                    tracing::error!("Failed to send request: {:?}", e);
                }
            }
        }

        Ok(results)
    }
}

pub trait ShardIdentifier: PartialEq + Eq + Clone {}

impl ShardIdentifier for () {}

pub trait ShardSelector<S: sonic::service::Service, Id: ShardIdentifier> {
    fn select<'a>(&self, shards: &'a [Shard<S, Id>]) -> Vec<&'a Shard<S, Id>>;
}

pub struct AllShardsSelector;

impl<S, Id> ShardSelector<S, Id> for AllShardsSelector
where
    S: sonic::service::Service,
    Id: ShardIdentifier,
{
    fn select<'a>(&self, shards: &'a [Shard<S, Id>]) -> Vec<&'a Shard<S, Id>> {
        shards.iter().collect()
    }
}

pub struct RandomShardSelector;

impl<S, Id> ShardSelector<S, Id> for RandomShardSelector
where
    S: sonic::service::Service,
    Id: ShardIdentifier,
{
    fn select<'a>(&self, shards: &'a [Shard<S, Id>]) -> Vec<&'a Shard<S, Id>> {
        let mut rng = rand::thread_rng();
        shards.iter().choose_multiple(&mut rng, 1)
    }
}

pub struct SpecificShardSelector<Id: ShardIdentifier>(pub Id);

impl<S, Id> ShardSelector<S, Id> for SpecificShardSelector<Id>
where
    S: sonic::service::Service,
    Id: ShardIdentifier,
{
    fn select<'a>(&self, shards: &'a [Shard<S, Id>]) -> Vec<&'a Shard<S, Id>> {
        shards.iter().find(|s| s.id == self.0).into_iter().collect()
    }
}

pub struct Shard<S: sonic::service::Service, Id: ShardIdentifier> {
    replicas: ReplicatedClient<S>,
    id: Id,
}

impl<S, Id> Shard<S, Id>
where
    S: sonic::service::Service,
    Id: ShardIdentifier,
{
    pub fn new(id: Id, replicas: ReplicatedClient<S>) -> Self {
        Self { replicas, id }
    }

    pub fn id(&self) -> &Id {
        &self.id
    }
}

pub struct ShardedClient<S: sonic::service::Service, Id: ShardIdentifier> {
    shards: Vec<Shard<S, Id>>,
}

impl<S, Id> ShardedClient<S, Id>
where
    S: sonic::service::Service,
    Id: ShardIdentifier,
{
    pub fn new(shards: Vec<Shard<S, Id>>) -> Self {
        Self { shards }
    }

    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    pub fn shards(&self) -> &[Shard<S, Id>] {
        &self.shards
    }

    async fn send_single<Req, Sel>(
        &self,
        req: Req,
        shard: &Shard<S, Id>,
        replica_selector: &Sel,
        timeout: Duration,
    ) -> Result<(Id, Vec<(SocketAddr, Req::Response)>)>
    where
        Req: sonic::service::Wrapper<S> + Clone,
        Sel: ReplicaSelector<S>,
    {
        Ok((
            shard.id.clone(),
            shard
                .replicas
                .send_with_timeout(req, replica_selector, timeout)
                .await?,
        ))
    }

    pub async fn send_with_timeout<Req, SSel, RSel>(
        &self,
        req: Req,
        shard_selector: &SSel,
        replica_selector: &RSel,
        timeout: Duration,
    ) -> Result<Vec<(Id, Vec<(SocketAddr, Req::Response)>)>>
    where
        Req: sonic::service::Wrapper<S> + Clone,
        SSel: ShardSelector<S, Id>,
        RSel: ReplicaSelector<S>,
    {
        let mut futures = Vec::new();
        for shard in shard_selector.select(&self.shards) {
            futures.push(self.send_single(req.clone(), shard, replica_selector, timeout));
        }

        if futures.is_empty() {
            return Err(anyhow::anyhow!("no shards available").into());
        }

        let mut results = Vec::new();
        for r in join_all(futures).await {
            match r {
                Ok(r) => results.push(r),
                Err(e) => {
                    tracing::error!("Failed to send request: {:?}", e);
                }
            }
        }

        Ok(results)
    }

    pub async fn send<Req, SSel, RSel>(
        &self,
        req: Req,
        shard_selector: &SSel,
        replica_selector: &RSel,
    ) -> Result<Vec<(Id, Vec<(SocketAddr, Req::Response)>)>>
    where
        Req: sonic::service::Wrapper<S> + Clone,
        SSel: ShardSelector<S, Id>,
        RSel: ReplicaSelector<S>,
    {
        self.send_with_timeout(req, shard_selector, replica_selector, DEFAULT_TIMEOUT)
            .await
    }

    async fn batch_send_single<Req, Sel>(
        &self,
        reqs: &[Req],
        shard: &Shard<S, Id>,
        replica_selector: &Sel,
    ) -> Result<(Id, Vec<(SocketAddr, Vec<Req::Response>)>)>
    where
        Req: sonic::service::Wrapper<S> + Clone,
        Sel: ReplicaSelector<S>,
    {
        Ok((
            shard.id.clone(),
            shard.replicas.batch_send(reqs, replica_selector).await?,
        ))
    }

    pub async fn batch_send<Req, SSel, RSel>(
        &self,
        reqs: &[Req],
        shard_selector: &SSel,
        replica_selector: &RSel,
    ) -> Result<Vec<(Id, Vec<(SocketAddr, Vec<Req::Response>)>)>>
    where
        Req: sonic::service::Wrapper<S> + Clone,
        SSel: ShardSelector<S, Id>,
        RSel: ReplicaSelector<S>,
    {
        let mut futures = Vec::new();
        for shard in shard_selector.select(&self.shards) {
            futures.push(self.batch_send_single(reqs, shard, replica_selector));
        }

        let mut results = Vec::new();
        for r in join_all(futures).await {
            match r {
                Ok(r) => results.push(r),
                Err(e) => {
                    tracing::error!("Failed to send request: {:?}", e);
                }
            }
        }

        Ok(results)
    }
}

pub trait ReusableClientManager {
    const CLIENT_REFRESH_INTERVAL: Duration;

    type Service: sonic::service::Service;
    type ShardId: ShardIdentifier;

    fn new_client(
        cluster: &Cluster,
    ) -> impl std::future::Future<Output = ShardedClient<Self::Service, Self::ShardId>>;
}

pub struct ReusableShardedClient<M>
where
    M: ReusableClientManager,
{
    cluster: Arc<Cluster>,
    client: Arc<sonic::replication::ShardedClient<M::Service, M::ShardId>>,
    last_client_update: std::time::Instant,
}

impl<M> ReusableShardedClient<M>
where
    M: ReusableClientManager,
{
    pub async fn new(cluster: Arc<Cluster>) -> Self {
        let client = Arc::new(M::new_client(&cluster).await);
        let last_client_update = std::time::Instant::now();

        Self {
            cluster,
            client,
            last_client_update,
        }
    }

    pub async fn conn(&mut self) -> Arc<sonic::replication::ShardedClient<M::Service, M::ShardId>> {
        if self.client.is_empty() || self.last_client_update.elapsed() > M::CLIENT_REFRESH_INTERVAL
        {
            self.client = Arc::new(M::new_client(&self.cluster).await);
            self.last_client_update = std::time::Instant::now();
        }

        self.client.clone()
    }
}
