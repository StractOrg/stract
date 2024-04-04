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
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_assignments)]
#![allow(unreachable_code)]

use anyhow::anyhow;
use rayon::prelude::*;
use std::{
    collections::{BTreeMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::ToSocketAddrs;

use futures::executor::block_on;

use crate::{
    ampc::dht::upsert::HyperLogLogUpsert64,
    bloom::BloomFilter,
    distributed::{cluster::Cluster, sonic},
    hyperloglog::HyperLogLog,
    webgraph::{self, Edge, Webgraph},
    Result,
};

use self::dht::{store::UpsertAction, upsert::UpsertEnum};

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

    pub fn upsert<F: Into<UpsertEnum>>(&self, upsert: F, key: K, value: V) -> UpsertAction {
        let key = bincode::serialize(&key).unwrap();
        let value = bincode::serialize(&value).unwrap();

        block_on(
            self.client
                .upsert(self.table.dht(), upsert, key.into(), value.into()),
        )
        .unwrap()
    }

    pub fn batch_upsert<F: Into<UpsertEnum> + Clone>(
        &self,
        upsert: F,
        pairs: Vec<(K, V)>,
    ) -> Vec<(K, UpsertAction)> {
        let pairs: Vec<(dht::Key, dht::Value)> = pairs
            .into_iter()
            .map(|(k, v)| {
                (
                    bincode::serialize(&k).unwrap().into(),
                    bincode::serialize(&v).unwrap().into(),
                )
            })
            .collect();

        block_on(self.client.batch_upsert(self.table.dht(), upsert, pairs))
            .unwrap()
            .into_iter()
            .map(|(k, did_upsert)| (bincode::deserialize(k.as_bytes()).unwrap(), did_upsert))
            .collect()
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
    next: DhtTableConn<K, V>,
}

impl<K, V> DhtConn<K, V>
where
    K: serde::Serialize + serde::de::DeserializeOwned,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    fn new(cluster: &Cluster, prefix: String) -> Self {
        let prev = block_on(DhtTableConn::new(cluster, prefix));
        let next = prev.next();
        Self { prev, next }
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
        self.prev = self.next.clone();

        self.next = self.prev.next();
    }

    pub fn prev(&self) -> &DhtTableConn<K, V> {
        &self.prev
    }

    pub fn next(&self) -> &DhtTableConn<K, V> {
        &self.next
    }
}

impl<K, V> Clone for DhtConn<K, V> {
    fn clone(&self) -> Self {
        Self {
            prev: self.prev.clone(),
            next: self.next.clone(),
        }
    }
}

pub trait Job
where
    Self: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + Clone + Send + Sync,
{
    type DhtKey: Send + Sync + serde::Serialize + serde::de::DeserializeOwned;
    type DhtValue: Send + Sync + serde::Serialize + serde::de::DeserializeOwned;
    type Worker: Worker<Job = Self>;
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
        dht: &DhtTableConn<
            <<Self as Finisher>::Job as Job>::DhtKey,
            <<Self as Finisher>::Job as Job>::DhtValue,
        >,
    ) -> bool;
}

pub trait Setup {
    type DhtKey;
    type DhtValue;

    fn init_dht(&self) -> DhtConn<Self::DhtKey, Self::DhtValue>;
    fn setup_round(&self, dht: &DhtTableConn<Self::DhtKey, Self::DhtValue>) {}
    fn setup_first_round(&self, dht: &DhtTableConn<Self::DhtKey, Self::DhtValue>) {
        self.setup_round(dht);
    }
}

pub trait Message<W: Worker>: std::fmt::Debug + Clone {
    type Response;
    fn handle(self, worker: &W) -> Self::Response;
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum CoordReq<J, M, K, V> {
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
pub enum CoordResp<J> {
    CurrentJob(Option<J>),
    ScheduleJob(()),
    Setup(()),
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum Req<J, M, R, K, V> {
    Coordinator(CoordReq<J, M, K, V>),
    User(R),
}

type JobReq<J> = Req<
    J,
    <J as Job>::Mapper,
    <<J as Job>::Worker as Worker>::Request,
    <J as Job>::DhtKey,
    <J as Job>::DhtValue,
>;

type JobResp<J> = Resp<J, <<J as Job>::Worker as Worker>::Response>;

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
pub enum Resp<J, R> {
    Coordinator(CoordResp<J>),
    User(R),
}

type JobDht<J> = DhtConn<<J as Job>::DhtKey, <J as Job>::DhtValue>;

pub struct Server<W>
where
    W: Worker,
{
    dht: Option<Arc<JobDht<W::Job>>>,
    worker: Arc<W>,
    current_job: Option<W::Job>,
    conn: sonic::Server<JobReq<W::Job>, JobResp<W::Job>>,
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

    fn bind(addr: impl ToSocketAddrs, worker: W) -> Result<Server<W>> {
        let worker = Arc::new(worker);
        let conn = block_on(sonic::Server::bind(addr))?;

        Ok(Server {
            dht: None,
            worker,
            current_job: None,
            conn,
        })
    }

    fn run(&mut self) -> Result<()> {
        loop {
            self.handle()?;
        }
    }
}

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

type JobConn<J> = sonic::Connection<JobReq<J>, JobResp<J>>;

pub trait RemoteWorker
where
    Self: Send + Sync,
{
    type Job: Job;
    fn remote_addr(&self) -> SocketAddr;

    fn schedule_job(
        &self,
        job: &Self::Job,
        mappers: Vec<<Self::Job as Job>::Mapper>,
    ) -> Result<()> {
        self.send(&JobReq::Coordinator(CoordReq::ScheduleJob {
            job: job.clone(),
            mappers,
        }))?;

        Ok(())
    }

    fn send_dht(
        &self,
        dht: &DhtConn<<Self::Job as Job>::DhtKey, <Self::Job as Job>::DhtValue>,
    ) -> Result<()> {
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
struct WorkerRef(usize);

struct Coordinator<J>
where
    J: Job,
{
    workers: BTreeMap<WorkerRef, <<J as Job>::Worker as Worker>::Remote>,
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
            workers: workers
                .into_iter()
                .enumerate()
                .map(|(i, w)| (WorkerRef(i), w))
                .collect(),
        }
    }

    fn add(&mut self, mapper: J::Mapper) -> &mut Self {
        self.mappers.push(mapper);
        self
    }

    fn send_dht_to_workers(&self, dht: &DhtConn<J::DhtKey, J::DhtValue>) -> Result<()> {
        self.workers
            .par_iter()
            .map(|(_, worker)| worker.send_dht(dht))
            .collect::<Result<()>>()?;

        Ok(())
    }

    fn run<F>(self, jobs: Vec<J>, finisher: F) -> Result<()>
    where
        F: Finisher<Job = J>,
    {
        let mut dht = self.setup.init_dht();
        dht.drop_prev_tables();

        self.setup.setup_first_round(&dht.next);
        dht.next_round();

        while !finisher.is_finished(&dht.prev) {
            self.send_dht_to_workers(&dht)?;

            // run round
            let mut sleeper =
                ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(10), 2.0);

            let mut remaining_jobs: VecDeque<_> = jobs.clone().into_iter().collect();
            let mut scheduled_jobs = BTreeMap::new();

            while let Some(job) = remaining_jobs.pop_front() {
                // get current status from workers
                let worker_jobs = self
                    .workers
                    .iter()
                    .map(|(r, w)| (*r, w.current_job()))
                    .collect::<BTreeMap<_, _>>();

                // handle failed workers
                for r in worker_jobs
                    .iter()
                    .filter_map(|(r, j)| j.as_ref().err().map(|_| r))
                {
                    if let Some(job) = scheduled_jobs.remove(r) {
                        remaining_jobs.push_front(job);
                    }
                }

                let potential_workers = worker_jobs
                    .iter()
                    .filter_map(|(r, j)| j.as_ref().ok().map(|_| *r))
                    .filter(|r| job.is_schedulable(&self.workers[r]))
                    .collect::<Vec<_>>();

                if potential_workers.is_empty() {
                    return Err(anyhow!(
                        "Failed to schedule job: no potential workers are responding."
                    ));
                }

                // schedule remaining jobs to idle workers (if any)
                match potential_workers.iter().find(|r| {
                    worker_jobs[r]
                        .as_ref()
                        .expect("references in `possible_workers` should only point to non-failed workers")
                        .is_none()
                }) {
                    Some(free_worker) => {
                        self.workers[free_worker].schedule_job(&job, self.mappers.clone())?;
                        scheduled_jobs.insert(*free_worker, job);
                        sleeper.success();
                    },
                    None => {
                        remaining_jobs.push_front(job);
                        let sleep_duration = sleeper.next();
                        std::thread::sleep(sleep_duration);
                    }
                }
            }

            self.setup.setup_round(&dht.next);
            dht.next_round();
        }

        Ok(())
    }
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

/*

repeat vec![
    Box::new(Mapper1::new()) as Box<dyn Mapper<D, W>>,
    Box::new(Mapper2::new()) as Box<dyn Mapper<D, W>>,
]

until Algorithm::is_finished(dht).

Before each round, call Algorithm::setup_round(dht). first rounde call Algorithm::setup_first_round(dht) instead.

*/

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum Key {
    Node(webgraph::NodeID),
    Meta,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum Value {
    Counter(HyperLogLog<64>),
    Meta { round_had_changes: bool },
}

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

    fn setup_round(&self, dht: &DhtTableConn<Self::DhtKey, Self::DhtValue>) {
        dht.set(
            Key::Meta,
            Value::Meta {
                round_had_changes: false,
            },
        );
    }
}

struct CentralityWorker {
    shard: u64,
    graph: Webgraph,
    changed_nodes: Arc<Mutex<BloomFilter>>,
}

impl CentralityWorker {
    fn new(shard: u64, graph: Webgraph) -> Self {
        let num_nodes = graph.estimate_num_nodes() as u64;
        let mut changed_nodes = BloomFilter::new(num_nodes, 0.05);

        for node in graph.nodes() {
            changed_nodes.insert(node.as_u64());
        }

        Self {
            shard,
            graph,
            changed_nodes: Arc::new(Mutex::new(changed_nodes)),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct GetShard;

impl Message<CentralityWorker> for GetShard {
    type Response = u64;

    fn handle(self, worker: &CentralityWorker) -> Self::Response {
        worker.shard
    }
}

impl_worker!(CentralityJob, RemoteCentralityWorker => CentralityWorker, [GetShard,]);

struct RemoteCentralityWorker {
    shard: u64,
    addr: SocketAddr,
}

impl RemoteCentralityWorker {
    fn shard(&self) -> u64 {
        self.shard
    }
}

impl RemoteWorker for RemoteCentralityWorker {
    type Job = CentralityJob;

    fn remote_addr(&self) -> SocketAddr {
        self.addr
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CentralityMapper {}

impl CentralityMapper {
    fn update_dht(
        &self,
        batch: &[Edge<()>],
        changed_nodes: &Mutex<BloomFilter>,
        new_changed_nodes: &Mutex<BloomFilter>,
        dht: &DhtConn<Key, Value>,
    ) {
        // get old values from prev dht using edge.from where edge.from in changed_nodes

        let mut old_counters: BTreeMap<_, _> = {
            let changed_nodes = changed_nodes.lock().unwrap();

            dht.prev()
                .batch_get(
                    batch
                        .iter()
                        .filter_map(|edge| {
                            if changed_nodes.contains(edge.from.as_u64()) {
                                Some(edge.from)
                            } else {
                                None
                            }
                        })
                        .map(Key::Node)
                        .collect(),
                )
                .into_iter()
                .filter_map(|(key, val)| match (key, val) {
                    (Key::Node(node), Value::Counter(counter)) => Some((node, counter)),
                    _ => None,
                })
                .collect()
        };

        let changes = {
            let changed_nodes = changed_nodes.lock().unwrap();

            // upsert edge.to hyperloglogs in dht.next
            dht.next().batch_upsert(
                HyperLogLogUpsert64,
                batch
                    .iter()
                    .filter(|edge| changed_nodes.contains(edge.from.as_u64()))
                    .map(|edge| {
                        let mut counter = old_counters
                            .remove(&edge.from)
                            .unwrap_or_else(HyperLogLog::default);
                        counter.add(edge.from.as_u64());
                        (Key::Node(edge.to), Value::Counter(counter))
                    })
                    .collect(),
            )
        };

        // update new bloom filter with the nodes that changed
        {
            let mut new_changed_nodes = new_changed_nodes.lock().unwrap();

            for (node, upsert_res) in &changes {
                if let UpsertAction::Merged = upsert_res {
                    if let Key::Node(node) = node {
                        new_changed_nodes.insert(node.as_u64());
                    } else {
                        unreachable!("expected Key::Node in changes");
                    }
                }
            }
        }

        // if any nodes changed, indicate in dht that we aren't finished yet
        {
            if changes.iter().any(|(_, upsert_res)| {
                matches!(upsert_res, UpsertAction::Merged | UpsertAction::Inserted)
            }) {
                dht.next().set(
                    Key::Meta,
                    Value::Meta {
                        round_had_changes: true,
                    },
                )
            }
        }
    }
}

impl Mapper for CentralityMapper {
    type Job = CentralityJob;

    fn map(&self, _: Self::Job, worker: &CentralityWorker, dht: &DhtConn<Key, Value>) {
        let new_changed_nodes = Arc::new(Mutex::new(BloomFilter::empty_from(
            &worker.changed_nodes.lock().unwrap(),
        )));

        let pool = rayon::ThreadPoolBuilder::new().build().unwrap();

        pool.scope(|s| {
            const BATCH_SIZE: usize = 16_384;
            let mut batch = Vec::with_capacity(BATCH_SIZE);

            for edge in worker.graph.edges() {
                batch.push(edge);
                if batch.len() >= BATCH_SIZE {
                    let changed_nodes = Arc::clone(&worker.changed_nodes);
                    let new_changed_nodes = Arc::clone(&new_changed_nodes);
                    let update_batch = batch.clone();

                    s.spawn(move |_| {
                        self.update_dht(&update_batch, &changed_nodes, &new_changed_nodes, dht)
                    });

                    batch.clear();
                }
            }
        });

        *worker.changed_nodes.lock().unwrap() = new_changed_nodes.lock().unwrap().clone();
    }
}

struct CentralityFinish {}

impl Finisher for CentralityFinish {
    type Job = CentralityJob;

    fn is_finished(&self, dht: &DhtTableConn<Key, Value>) -> bool {
        match dht.get(Key::Meta).unwrap() {
            Value::Meta { round_had_changes } => round_had_changes,
            _ => unreachable!("unexpected value in dht for key: {:?}", Key::Meta),
        }
    }
}
