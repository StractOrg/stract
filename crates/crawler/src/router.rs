use anyhow::Result;
use distributed::retry_strategy::ExponentialBackoff;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sonic::{service::Message, sonic_service};
use std::{net::SocketAddr, time::Duration};
use tokio::sync::Mutex;

use super::{
    coordinator::{CoordinatorService, GetJob},
    Job,
};

pub struct RouterService {
    pub router: Router,
}

sonic_service!(RouterService, [NewJob]);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewJob {}

#[async_trait::async_trait]
impl Message<RouterService> for NewJob {
    type Response = Option<Job>;

    async fn handle(self, server: &RouterService) -> sonic::Result<Self::Response> {
        Ok(server.router.sample_job().await?)
    }
}
struct RemoteCoordinator {
    addr: SocketAddr,
}

impl RemoteCoordinator {
    async fn conn(&self) -> Result<sonic::service::ResilientConnection<CoordinatorService>> {
        let retry = ExponentialBackoff::from_millis(1_000).with_limit(Duration::from_secs(10));

        Ok(sonic::service::ResilientConnection::create_with_timeout(
            self.addr,
            Duration::from_secs(60),
            retry,
        )
        .await?)
    }

    async fn sample_job(&self) -> Result<Option<Job>> {
        let conn = self.conn().await?;

        let response = conn
            .send_with_timeout(&GetJob {}, Duration::from_secs(90))
            .await?;

        Ok(response)
    }
}

struct InnerRouter {
    coordinators: Vec<RemoteCoordinator>,
}

impl InnerRouter {
    async fn new(coordinator_addrs: Vec<SocketAddr>) -> Result<Self> {
        Ok(Self {
            coordinators: coordinator_addrs
                .into_iter()
                .map(|addr| RemoteCoordinator { addr })
                .collect(),
        })
    }

    async fn sample_job(&mut self) -> Result<Option<Job>> {
        while !self.coordinators.is_empty() {
            let idx = rand::thread_rng().gen_range(0..self.coordinators.len());
            let res = self.coordinators[idx].sample_job().await?;

            if res.is_some() {
                return Ok(res);
            }

            self.coordinators.remove(idx);
        }

        Ok(None)
    }
}

pub struct Router {
    inner: Mutex<InnerRouter>,
}

impl Router {
    pub async fn new(coordinator_addrs: Vec<SocketAddr>) -> Result<Self> {
        Ok(Self {
            inner: Mutex::new(InnerRouter::new(coordinator_addrs).await?),
        })
    }

    pub async fn sample_job(&self) -> Result<Option<Job>> {
        self.inner.lock().await.sample_job().await
    }
}
