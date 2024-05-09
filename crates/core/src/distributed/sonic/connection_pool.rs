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

use std::net::SocketAddr;

use crate::Result;
use deadpool::managed;

use super::service::Service;

pub trait Connection {
    type Manager: managed::Manager;

    fn new_manager(addr: SocketAddr) -> Self::Manager;
}

pub struct ConnectionPool<C>
where
    C: Connection,
{
    addr: SocketAddr,
    pool: managed::Pool<C::Manager>,
}

impl<C> std::fmt::Debug for ConnectionPool<C>
where
    C: Connection,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("addr", &self.addr)
            .finish()
    }
}

impl<C> ConnectionPool<C>
where
    C: Connection,
{
    pub fn new(addr: SocketAddr) -> Result<Self> {
        let manager = C::new_manager(addr);
        let pool = managed::Pool::builder(manager).build()?;

        Ok(Self { addr, pool })
    }

    pub async fn get(&self) -> Result<managed::Object<C::Manager>> {
        self.pool
            .get()
            .await
            .map_err(|_| anyhow::anyhow!("Failed to get connection from pool"))
    }
}

pub struct Manager<Req, Res> {
    addr: SocketAddr,
    _marker: std::marker::PhantomData<(Req, Res)>,
}

impl<Req, Res> Manager<Req, Res> {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Req, Res> Connection for super::Connection<Req, Res>
where
    Req: Send + Sync + bincode::Encode,
    Res: Send + Sync + bincode::Decode,
{
    type Manager = Manager<Req, Res>;

    fn new_manager(addr: SocketAddr) -> Self::Manager {
        Manager::new(addr)
    }
}

impl<Req, Res> managed::Manager for Manager<Req, Res>
where
    Req: Send + Sync + bincode::Encode,
    Res: Send + Sync + bincode::Decode,
{
    type Type = super::Connection<Req, Res>;
    type Error = anyhow::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        Ok(super::Connection::connect(self.addr).await?)
    }

    async fn recycle(
        &self,
        obj: &mut Self::Type,
        _: &managed::Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        if obj.is_closed().await {
            Err(managed::RecycleError::Message(
                "Connection is closed".into(),
            ))
        } else {
            Ok(())
        }
    }
}

pub struct ServiceManager<S> {
    addr: SocketAddr,
    _marker: std::marker::PhantomData<S>,
}

impl<S> ServiceManager<S> {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S> Connection for super::service::Connection<S>
where
    S: Send + Sync + Service,
{
    type Manager = ServiceManager<S>;

    fn new_manager(addr: SocketAddr) -> Self::Manager {
        ServiceManager::new(addr)
    }
}

impl<S> managed::Manager for ServiceManager<S>
where
    S: Send + Sync + Service,
{
    type Type = super::service::Connection<S>;
    type Error = anyhow::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        Ok(super::service::Connection::create(self.addr).await?)
    }

    async fn recycle(
        &self,
        obj: &mut Self::Type,
        _: &managed::Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        if obj.is_closed().await {
            Err(managed::RecycleError::Message(
                "Connection is closed".into(),
            ))
        } else {
            Ok(())
        }
    }
}
