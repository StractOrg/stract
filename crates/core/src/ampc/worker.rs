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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

use std::net::SocketAddr;

use super::{block_on, CoordReq, CoordResp, DhtConn, Job, JobConn, JobReq, JobResp, Resp, Server};
use crate::Result;
use anyhow::anyhow;
use tokio::net::ToSocketAddrs;

pub trait Worker: Send + Sync {
    type Remote: RemoteWorker<Job = Self::Job>;

    type Request: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync;
    type Response: serde::Serialize + serde::de::DeserializeOwned + Send + Sync;
    type Job: Job<Worker = Self>;

    fn handle(&self, req: Self::Request) -> Self::Response;

    fn bind(self, addr: impl ToSocketAddrs) -> Result<Server<Self>>
    where
        Self: Sized + 'static,
    {
        Server::bind(addr, self)
    }

    fn run(self, addr: impl ToSocketAddrs) -> Result<()>
    where
        Self: Sized + 'static,
    {
        self.bind(addr)?.run()
    }
}

pub trait Message<W: Worker>: std::fmt::Debug + Clone {
    type Response;
    fn handle(self, worker: &W) -> Self::Response;
}

pub trait RemoteWorker
where
    Self: Send + Sync,
{
    type Job: Job;
    fn remote_addr(&self) -> SocketAddr;

    fn schedule_job(&self, job: &Self::Job, mapper: <Self::Job as Job>::Mapper) -> Result<()> {
        self.send(&JobReq::Coordinator(CoordReq::ScheduleJob {
            job: job.clone(),
            mapper,
        }))?;

        Ok(())
    }

    fn send_dht(&self, dht: &DhtConn<<Self::Job as Job>::DhtTables>) -> Result<()> {
        self.send(&JobReq::Coordinator(CoordReq::Setup { dht: dht.clone() }))?;

        Ok(())
    }

    fn current_job(&self) -> Result<Option<Self::Job>> {
        let req = JobReq::Coordinator(CoordReq::CurrentJob);
        let res = self.send(&req)?;

        match res {
            Resp::Coordinator(CoordResp::CurrentJob(job)) => Ok(job),
            _ => Err(anyhow!("unexpected response")),
        }
    }

    fn conn(&self) -> Result<JobConn<Self::Job>> {
        let conn = block_on(JobConn::connect(self.remote_addr()))?;
        Ok(conn)
    }

    fn send(&self, req: &JobReq<Self::Job>) -> Result<JobResp<Self::Job>> {
        let conn = self.conn()?;
        let res = block_on(conn.send(req))?;
        Ok(res)
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub struct WorkerRef(pub usize);

macro_rules! impl_worker {
    ($job:ident , $remote:ident => $worker:ident, [$($req:ident),*$(,)?]) => {
        mod worker_impl__ {
            #![allow(dead_code)]

            use super::{$worker, $remote, $job, $($req),*};

            use $crate::ampc;
            use $crate::ampc::Message;

            #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize)]
            pub enum Request {
                $($req($req),)*
            }
            #[derive(::serde::Serialize, ::serde::Deserialize)]
            pub enum Response {
                $($req(<$req as ampc::Message<$worker>>::Response),)*
            }

            impl ampc::Worker for $worker {
                type Remote = $remote;
                type Request = Request;
                type Response = Response;
                type Job = $job;

                fn handle(&self, req: Self::Request) -> Self::Response {
                    match req {
                        $(Request::$req(req) => Response::$req(req.handle(self)),)*
                    }
                }
            }

        }
    };
}

pub(crate) use impl_worker;
