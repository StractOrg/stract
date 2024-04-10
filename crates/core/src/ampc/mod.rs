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

use self::{job::Job, worker::WorkerRef};
use crate::distributed::sonic;
use std::time::Duration;

mod coordinator;
pub mod dht;
pub mod dht_conn;
mod finisher;
mod job;
mod mapper;
pub mod prelude;
mod server;
mod setup;
mod worker;

use self::prelude::*;

pub use coordinator::Coordinator;
pub use dht_conn::{DefaultDhtTable, DhtConn, DhtTable, DhtTables, Table};
pub use server::Server;
pub use worker::{Message, RequestWrapper, Worker};

static TOKIO_RUNTIME: once_cell::sync::Lazy<tokio::runtime::Runtime> =
    once_cell::sync::Lazy::new(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    });

pub fn block_on<F: std::future::Future>(f: F) -> F::Output {
    TOKIO_RUNTIME.block_on(f)
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum CoordReq<J, M, T> {
    CurrentJob,
    ScheduleJob { job: J, mapper: M },
    Setup { dht: DhtConn<T> },
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum CoordResp<J> {
    CurrentJob(Option<J>),
    ScheduleJob(()),
    Setup(()),
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum Req<J, M, R, T> {
    Coordinator(CoordReq<J, M, T>),
    User(R),
}

type JobReq<J> =
    Req<J, <J as Job>::Mapper, <<J as Job>::Worker as Worker>::Request, <J as Job>::DhtTables>;

type JobResp<J> = Resp<J, <<J as Job>::Worker as Worker>::Response>;

#[derive(serde::Serialize, serde::Deserialize)]
pub enum Resp<J, R> {
    Coordinator(CoordResp<J>),
    User(R),
}

type JobDht<J> = DhtConn<<J as Job>::DhtTables>;

type JobConn<J> = sonic::Connection<JobReq<J>, JobResp<J>>;

#[must_use = "this `JobScheduled` may not have scheduled the job on any worker"]
enum JobScheduled {
    Success(WorkerRef),
    NoAvailableWorkers,
}

struct ExponentialBackoff {
    min: Duration,
    max: Duration,
    factor: f64,
    attempts: usize,
}

impl ExponentialBackoff {
    fn new(min: Duration, max: Duration, factor: f64) -> Self {
        Self {
            min,
            max,
            factor,
            attempts: 0,
        }
    }
}

impl ExponentialBackoff {
    fn next(&mut self) -> Duration {
        let duration = self.min.mul_f64(self.factor.powi(self.attempts as i32));
        self.attempts += 1;
        duration.min(self.max)
    }

    fn success(&mut self) {
        self.attempts = 0;
    }
}
