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

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone)]
pub enum CoordReq<J, M, T> {
    CurrentJob,
    ScheduleJob { job: J, mapper: M },
    Setup { dht: DhtConn<T> },
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub enum CoordResp<J> {
    CurrentJob(Option<J>),
    ScheduleJob(()),
    Setup(()),
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone)]
pub enum Req<J, M, R, T> {
    Coordinator(CoordReq<J, M, T>),
    User(R),
}

type JobReq<J> =
    Req<J, <J as Job>::Mapper, <<J as Job>::Worker as Worker>::Request, <J as Job>::DhtTables>;

type JobResp<J> = Resp<J, <<J as Job>::Worker as Worker>::Response>;

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub enum Resp<J, R> {
    Coordinator(CoordResp<J>),
    User(R),
}

type JobDht<J> = DhtConn<<J as Job>::DhtTables>;

pub type JobConn<J> = sonic::Connection<JobReq<J>, JobResp<J>>;

#[must_use = "this `JobScheduled` may not have scheduled the job on any worker"]
enum JobScheduled {
    Success(WorkerRef),
    NoAvailableWorkers,
}
