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

use std::{future::Future, net::SocketAddr};

use crate::mapreduce::MapReduceServer;

use super::{Map, Result, Task};
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, info};

#[derive(Default)]
pub struct StatelessWorker {}

pub trait Worker {
    fn run<I, O>(&self, addr: SocketAddr) -> impl Future<Output = Result<()>>
    where
        Self: Sized,
        I: Map<Self, O> + Send + Sync,
        O: Serialize + DeserializeOwned + Send + Sync,
    {
        async move {
            let server = MapReduceServer::<I, O>::bind(addr).await?;
            info!("worker listening on: {:}", addr);

            loop {
                let req = server.accept().await?;
                debug!("received request");
                match req.body() {
                    Task::Job(job) => {
                        debug!("request is a job");
                        let res = job.map(self);
                        req.respond(Some(res)).await?;
                    }
                    Task::AllFinished => {
                        req.respond(None).await?;
                        break;
                    }
                }
            }

            Ok(())
        }
    }
}

impl Worker for StatelessWorker {}
