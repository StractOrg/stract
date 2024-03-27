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
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unreachable_code)]

use std::{net::ToSocketAddrs, sync::Arc};

use futures::executor::block_on;

use crate::{
    distributed::{cluster::Cluster, sonic},
    webgraph::Webgraph,
    Result,
};

pub mod dht;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct Table {
    prefix: String,
    round: u64,
}

impl Table {
    fn new(prefix: String) -> Self {
        Self { prefix, round: 0 }
    }

    fn dht(&self) -> dht::Table {
        format!("{}-{}", self.prefix, self.round).into()
    }

    fn next(&self) -> Self {
        Self {
            prefix: self.prefix.clone(),
            round: self.round + 1,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct DhtTableConn<K, V> {
    table: Table,
    client: dht::Client,
    _maker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Clone for DhtTableConn<K, V> {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            client: self.client.clone(),
            _maker: std::marker::PhantomData,
        }
    }
}

impl<K, V> DhtTableConn<K, V>
where
    K: serde::Serialize + serde::de::DeserializeOwned,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    pub async fn new(cluster: &Cluster, prefix: String) -> Self {
        Self {
            table: Table::new(prefix),
            client: dht::Client::new(cluster).await,
            _maker: std::marker::PhantomData,
        }
    }

    pub fn get(&self, key: K) -> Option<V> {
        let key = bincode::serialize(&key).unwrap();

        block_on(self.client.get(self.table.dht(), key.into()))
            .unwrap()
            .map(|v| bincode::deserialize(v.as_bytes()).unwrap())
    }

    pub fn batch_get(&self, keys: Vec<K>) -> Vec<(K, V)> {
        let keys: Vec<dht::Key> = keys
            .into_iter()
            .map(|k| bincode::serialize(&k).unwrap().into())
            .collect::<Vec<_>>();
        let values = block_on(self.client.batch_get(self.table.dht(), keys)).unwrap();

        values
            .into_iter()
            .map(|(k, v)| {
                (
                    bincode::deserialize(k.as_bytes()).unwrap(),
                    bincode::deserialize(v.as_bytes()).unwrap(),
                )
            })
            .collect()
    }

    pub fn set(&self, key: K, value: V) {
        let key = bincode::serialize(&key).unwrap();
        let value = bincode::serialize(&value).unwrap();

        block_on(self.client.set(self.table.dht(), key.into(), value.into())).unwrap();
    }

    pub fn batch_set(&self, pairs: Vec<(K, V)>) {
        let pairs: Vec<(dht::Key, dht::Value)> = pairs
            .into_iter()
            .map(|(k, v)| {
                (
                    bincode::serialize(&k).unwrap().into(),
                    bincode::serialize(&v).unwrap().into(),
                )
            })
            .collect();

        block_on(self.client.batch_set(self.table.dht(), pairs)).unwrap();
    }

    fn next(&self) -> DhtTableConn<K, V> {
        let new = Self {
            table: self.table.next(),
            client: self.client.clone(),
            _maker: std::marker::PhantomData,
        };

        new.init_from(self);

        new
    }

    fn init_from(&self, prev: &DhtTableConn<K, V>) {
        block_on(self.client.clone_table(prev.table.dht(), self.table.dht())).unwrap();
    }

    pub fn drop_table(&self) {
        block_on(self.client.drop_table(self.table.dht())).unwrap();
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct DhtConn<K, V> {
    prev: DhtTableConn<K, V>,
    new: DhtTableConn<K, V>,
}

impl<K, V> DhtConn<K, V>
where
    K: serde::Serialize + serde::de::DeserializeOwned,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    fn new(cluster: &Cluster, prefix: String) -> Self {
        let prev = block_on(DhtTableConn::new(cluster, prefix));
        let new = prev.next();
        Self { prev, new }
    }

    fn drop_prev_tables(&self) {
        let tables = block_on(self.prev.client.all_tables()).unwrap();

        for table in tables {
            if table.as_str().starts_with(&self.prev.table.prefix) {
                block_on(self.prev.client.drop_table(table)).unwrap();
            }
        }
    }

    fn next_round(&mut self) {
        self.prev.drop_table();
        self.prev = self.new.clone();

        self.new = self.prev.next();
    }
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

    fn is_finished(
        &self,
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
        let req = block_on(self.conn.accept())?;

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

                block_on(req.respond(res))?;
            }
            Req::User(user_req) => {
                let worker = Arc::clone(&self.worker);

                std::thread::spawn(move || {
                    let res = Resp::User(worker.handle(user_req.clone()));
                    block_on(req.respond(res)).unwrap();
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
        let mut dht = self.setup.init_dht();
        dht.drop_prev_tables();

        let mut is_first = true;

        while !finisher.is_finished(&dht) {
            if is_first {
                self.setup.setup_first_round(&dht);
            } else {
                self.setup.setup_round(&dht);
            }

            is_first = false;

            todo!();

            dht.next_round();
        }
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

struct CentralitySetup {
    dht: DhtConn<Key, Value>,
}

impl CentralitySetup {
    pub async fn new(cluster: &Cluster, prefix: String) -> Self {
        let dht = DhtConn::new(cluster, prefix);
        Self { dht }
    }
}

impl Setup for CentralitySetup {
    type DhtKey = Key;
    type DhtValue = Value;

    fn init_dht(&self) -> DhtConn<Key, Value> {
        self.dht.clone()
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
