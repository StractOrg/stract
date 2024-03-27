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
#![allow(clippy::type_complexity)]
#![allow(dead_code)]
#![allow(unused_variables)]

use std::{net::ToSocketAddrs, sync::Arc};

use crate::{distributed::sonic, webgraph::Webgraph, Result};

pub mod dht;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct DhtTableConn<K, V> {
    _maker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Clone for DhtTableConn<K, V> {
    fn clone(&self) -> Self {
        Self {
            _maker: std::marker::PhantomData,
        }
    }
}

impl<K, V> DhtTableConn<K, V> {
    fn new() -> Self {
        todo!()
    }

    fn get(&self, key: K) -> Option<V> {
        todo!()
    }

    fn put(&self, key: K, value: V) {
        todo!()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct DhtConn<K, V> {
    prev: DhtTableConn<K, V>,
    new: DhtTableConn<K, V>,
}

impl<K, V> Clone for DhtConn<K, V> {
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            new: self.new.clone(),
        }
    }
}

pub trait Job
where
    Self: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + Clone + Send + Sync,
{
    type DhtKey: Send + Sync + serde::Serialize + serde::de::DeserializeOwned;
    type DhtValue: Send + Sync + serde::Serialize + serde::de::DeserializeOwned;
    type Worker: Worker;
    type Mapper: Mapper<Job = Self>;

    fn is_schedulable(&self, _worker: &<<Self as Job>::Worker as Worker>::Remote) -> bool {
        true
    }
}

pub trait Mapper: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + Clone {
    type Job: Job<Mapper = Self>;

    fn map(
        &self,
        job: Self::Job,
        worker: &<<Self as Mapper>::Job as Job>::Worker,
        dht: &DhtConn<
            <<Self as Mapper>::Job as Job>::DhtKey,
            <<Self as Mapper>::Job as Job>::DhtValue,
        >,
    );
}

pub trait Finisher {
    type Job: Job;

    fn is_done(
        dht: &DhtConn<
            <<Self as Finisher>::Job as Job>::DhtKey,
            <<Self as Finisher>::Job as Job>::DhtValue,
        >,
    ) -> bool;
}

pub trait Setup {
    type DhtKey;
    type DhtValue;

    fn init_dht(&self) -> DhtConn<Self::DhtKey, Self::DhtValue>;
    fn setup_round(&self, dht: &DhtConn<Self::DhtKey, Self::DhtValue>);
    fn setup_first_round(&self, dht: &DhtConn<Self::DhtKey, Self::DhtValue>) {
        self.setup_round(dht);
    }
}

pub trait Message<W: Worker>: std::fmt::Debug + Clone {
    type Response;
    fn handle(self, worker: &W) -> Self::Response;
}

#[derive(serde::Serialize, serde::Deserialize)]
enum CoordReq<J, M, K, V> {
    CurrentJob,
    ScheduleJob { job: J, mappers: Vec<M> },
    Setup { dht: DhtConn<K, V> },
}

impl<J, M, K, V> Clone for CoordReq<J, M, K, V>
where
    J: Clone,
    M: Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::CurrentJob => Self::CurrentJob,
            Self::ScheduleJob { job, mappers } => Self::ScheduleJob {
                job: job.clone(),
                mappers: mappers.clone(),
            },
            Self::Setup { dht } => Self::Setup { dht: dht.clone() },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
enum CoordResp<J> {
    CurrentJob(Option<J>),
    ScheduleJob(()),
    Setup(()),
}

#[derive(serde::Serialize, serde::Deserialize)]
enum Req<J, M, R, K, V> {
    Coordinator(CoordReq<J, M, K, V>),
    User(R),
}

impl<J, M, R, K, V> Clone for Req<J, M, R, K, V>
where
    J: Clone,
    M: Clone,
    R: Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::Coordinator(req) => Self::Coordinator(req.clone()),
            Self::User(req) => Self::User(req.clone()),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
enum Resp<J, R> {
    Coordinator(CoordResp<J>),
    User(R),
}

pub struct Server<W>
where
    W: Worker,
{
    dht: Option<Arc<DhtConn<<W::Job as Job>::DhtKey, <W::Job as Job>::DhtValue>>>,
    worker: Arc<W>,
    current_job: Option<W::Job>,
    conn: sonic::Server<
        Req<
            W::Job,
            <W::Job as Job>::Mapper,
            W::Request,
            <W::Job as Job>::DhtKey,
            <W::Job as Job>::DhtValue,
        >,
        Resp<W::Job, W::Response>,
    >,
}

impl<W> Server<W>
where
    W: Worker + 'static,
{
    fn handle(&mut self) -> Result<()> {
        let req = futures::executor::block_on(self.conn.accept())?;

        match req.body().clone() {
            Req::Coordinator(coord_req) => {
                let res = match coord_req {
                    CoordReq::CurrentJob => {
                        Resp::Coordinator(CoordResp::CurrentJob(self.current_job.clone()))
                    }
                    CoordReq::ScheduleJob { job, mappers } => {
                        self.current_job = Some(job.clone());
                        let worker = Arc::clone(&self.worker);
                        let dht = self.dht.clone();

                        std::thread::spawn(move || {
                            for mapper in mappers {
                                mapper.map(
                                    job.clone(),
                                    &worker,
                                    dht.as_ref().expect("DHT not set"),
                                );
                            }
                        });

                        Resp::Coordinator(CoordResp::ScheduleJob(()))
                    }
                    CoordReq::Setup { dht } => {
                        self.dht = Some(Arc::new(dht));
                        Resp::Coordinator(CoordResp::Setup(()))
                    }
                };

                futures::executor::block_on(req.respond(res))?;
            }
            Req::User(user_req) => {
                let worker = Arc::clone(&self.worker);

                std::thread::spawn(move || {
                    let res = Resp::User(worker.handle(user_req.clone()));
                    futures::executor::block_on(req.respond(res)).unwrap();
                });
            }
        };

        Ok(())
    }
}

pub trait Worker: Send + Sync {
    type Remote;

    type Request: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync;
    type Response: serde::Serialize + serde::de::DeserializeOwned + Send + Sync;
    type Job: Job<Worker = Self>;

    fn handle(&self, req: Self::Request) -> Self::Response;

    fn bind(self, addr: impl ToSocketAddrs) -> Server<Self>
    where
        Self: Sized,
    {
        todo!()
    }
}

pub trait RemoteWorker {}

struct Coordinator<J>
where
    J: Job,
{
    workers: Vec<<<J as Job>::Worker as Worker>::Remote>,
    setup: Box<dyn Setup<DhtKey = J::DhtKey, DhtValue = J::DhtValue>>,
    mappers: Vec<J::Mapper>,
}

impl<J> Coordinator<J>
where
    J: Job,
{
    fn new<S>(setup: S, workers: Vec<<<J as Job>::Worker as Worker>::Remote>) -> Self
    where
        S: Setup<DhtKey = J::DhtKey, DhtValue = J::DhtValue> + 'static,
    {
        Self {
            setup: Box::new(setup),
            mappers: Vec::new(),
            workers,
        }
    }

    fn add(&mut self, mapper: J::Mapper) -> &mut Self {
        self.mappers.push(mapper);
        self
    }

    fn run<F>(self, jobs: Vec<J>, finisher: F)
    where
        F: Finisher<Job = J>,
    {
        todo!()
    }
}

/*

repeat vec![
    Box::new(Mapper1::new()) as Box<dyn Mapper<D, W>>,
    Box::new(Mapper2::new()) as Box<dyn Mapper<D, W>>,
]

until Algorithm::is_finished(dht).

Before each round, call Algorithm::setup_round(dht). first rounde call Algorithm::setup_first_round(dht) instead.

*/

type Key = ();
type Value = ();

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CentralityJob {
    shard: u64,
}

impl Job for CentralityJob {
    type DhtKey = Key;
    type DhtValue = Value;
    type Worker = CentralityWorker;
    type Mapper = CentralityMapper;

    fn is_schedulable(&self, worker: &RemoteCentralityWorker) -> bool {
        self.shard == worker.shard()
    }
}

struct CentralitySetup {}

impl Setup for CentralitySetup {
    type DhtKey = Key;
    type DhtValue = Value;

    fn init_dht(&self) -> DhtConn<Key, Value> {
        todo!()
    }

    fn setup_round(&self, _dht: &DhtConn<Key, Value>) {
        todo!()
    }
}

struct CentralityWorker {
    shard: u64,
    graph: Webgraph,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct GetShard;

impl Message<CentralityWorker> for GetShard {
    type Response = u64;

    fn handle(self, worker: &CentralityWorker) -> Self::Response {
        worker.shard
    }
}

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

impl_worker!(CentralityJob, RemoteCentralityWorker => CentralityWorker, [GetShard,]);

struct RemoteCentralityWorker {
    addr: String,
}

impl RemoteCentralityWorker {
    fn shard(&self) -> u64 {
        todo!()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CentralityMapper {}

impl Mapper for CentralityMapper {
    type Job = CentralityJob;

    fn map(&self, _: Self::Job, worker: &CentralityWorker, dht: &DhtConn<Key, Value>) {
        todo!("iterate edges in graph and update dht")
    }
}
