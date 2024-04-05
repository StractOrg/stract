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

use anyhow::anyhow;
use rayon::prelude::*;
use std::{
    collections::{BTreeMap, VecDeque},
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
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

use self::dht::{
    store::UpsertAction,
    upsert::{F64Add, UpsertEnum},
};

pub mod dht;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct Table {
    prefix: String,
    round: u64,
}

impl Table {
    fn new<S: AsRef<str>>(prefix: S) -> Self {
        Self {
            prefix: prefix.as_ref().to_string(),
            round: 0,
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
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

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct DefaultDhtTableConn<K, V> {
    table: Table,
    client: dht::Client,
    _maker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Clone for DefaultDhtTableConn<K, V> {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            client: self.client.clone(),
            _maker: std::marker::PhantomData,
        }
    }
}
impl<K, V> DefaultDhtTableConn<K, V>
where
    K: serde::Serialize + serde::de::DeserializeOwned,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    pub async fn new<S: AsRef<str>>(cluster: &Cluster, prefix: S) -> Self {
        Self {
            table: Table::new(prefix),
            client: dht::Client::new(cluster).await,
            _maker: std::marker::PhantomData,
        }
    }
}

trait DhtTableConn: Clone + serde::Serialize + serde::de::DeserializeOwned {
    type Key: serde::Serialize + serde::de::DeserializeOwned;
    type Value: serde::Serialize + serde::de::DeserializeOwned;

    fn client(&self) -> &dht::Client;
    fn table(&self) -> &Table;
    fn next(&self) -> Self;

    fn get(&self, key: Self::Key) -> Option<Self::Value> {
        let key = bincode::serialize(&key).unwrap();

        block_on(self.client().get(self.table().dht(), key.into()))
            .unwrap()
            .map(|v| bincode::deserialize(v.as_bytes()).unwrap())
    }

    fn batch_get(&self, keys: Vec<Self::Key>) -> Vec<(Self::Key, Self::Value)> {
        let keys: Vec<dht::Key> = keys
            .into_iter()
            .map(|k| bincode::serialize(&k).unwrap().into())
            .collect::<Vec<_>>();
        let values = block_on(self.client().batch_get(self.table().dht(), keys)).unwrap();

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

    fn set(&self, key: Self::Key, value: Self::Value) {
        let key = bincode::serialize(&key).unwrap();
        let value = bincode::serialize(&value).unwrap();

        block_on(
            self.client()
                .set(self.table().dht(), key.into(), value.into()),
        )
        .unwrap();
    }

    fn batch_set(&self, pairs: Vec<(Self::Key, Self::Value)>) {
        let pairs: Vec<(dht::Key, dht::Value)> = pairs
            .into_iter()
            .map(|(k, v)| {
                (
                    bincode::serialize(&k).unwrap().into(),
                    bincode::serialize(&v).unwrap().into(),
                )
            })
            .collect();

        block_on(self.client().batch_set(self.table().dht(), pairs)).unwrap();
    }

    fn upsert<F: Into<UpsertEnum>>(
        &self,
        upsert: F,
        key: Self::Key,
        value: Self::Value,
    ) -> UpsertAction {
        let key = bincode::serialize(&key).unwrap();
        let value = bincode::serialize(&value).unwrap();

        block_on(
            self.client()
                .upsert(self.table().dht(), upsert, key.into(), value.into()),
        )
        .unwrap()
    }

    fn batch_upsert<F: Into<UpsertEnum> + Clone>(
        &self,
        upsert: F,
        pairs: Vec<(Self::Key, Self::Value)>,
    ) -> Vec<(Self::Key, UpsertAction)> {
        let pairs: Vec<(dht::Key, dht::Value)> = pairs
            .into_iter()
            .map(|(k, v)| {
                (
                    bincode::serialize(&k).unwrap().into(),
                    bincode::serialize(&v).unwrap().into(),
                )
            })
            .collect();

        block_on(
            self.client()
                .batch_upsert(self.table().dht(), upsert, pairs),
        )
        .unwrap()
        .into_iter()
        .map(|(k, did_upsert)| (bincode::deserialize(k.as_bytes()).unwrap(), did_upsert))
        .collect()
    }

    fn init_from(&self, prev: &DefaultDhtTableConn<Self::Key, Self::Value>) {
        block_on(
            self.client()
                .clone_table(prev.table().dht(), self.table().dht()),
        )
        .unwrap();
    }

    fn drop_table(&self) {
        block_on(self.client().drop_table(self.table().dht())).unwrap();
    }
}

impl<K, V> DhtTableConn for DefaultDhtTableConn<K, V>
where
    K: serde::Serialize + serde::de::DeserializeOwned,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    type Key = K;
    type Value = V;

    fn client(&self) -> &dht::Client {
        &self.client
    }

    fn table(&self) -> &Table {
        &self.table
    }

    fn next(&self) -> DefaultDhtTableConn<Self::Key, Self::Value> {
        let new = Self {
            table: self.table().next(),
            client: self.client().clone(),
            _maker: std::marker::PhantomData,
        };

        new.init_from(self);

        new
    }
}

pub trait DhtTables
where
    Self: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    fn drop_tables(&self);
    fn next(&self) -> Self;
    fn cleanup_prev_tables(&self);
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct DhtConn<T> {
    prev: T,
    next: T,
}

impl<T> DhtConn<T>
where
    T: DhtTables,
{
    fn new(initial: T) -> Self {
        let next = initial.next();

        Self {
            prev: initial,
            next,
        }
    }

    fn cleanup_prev_tables(&self) {
        self.prev.cleanup_prev_tables();
    }

    fn next_round(&mut self) {
        self.prev.drop_tables();
        self.prev = self.next.clone();

        self.next = self.prev.next();
    }

    pub fn prev(&self) -> &T {
        &self.prev
    }

    pub fn next(&self) -> &T {
        &self.next
    }
}

pub trait Job
where
    Self: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + Clone + Send + Sync,
{
    type DhtTables: DhtTables;
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
        dht: &DhtConn<<<Self as Mapper>::Job as Job>::DhtTables>,
    );
}

pub trait Finisher {
    type Job: Job;

    fn is_finished(&self, dht: &<<Self as Finisher>::Job as Job>::DhtTables) -> bool;
}

pub trait Setup {
    type DhtTables;

    fn init_dht(&self) -> DhtConn<Self::DhtTables>;
    #[allow(unused_variables)] // reason = "dht might be used by implementors"
    fn setup_round(&self, dht: &Self::DhtTables) {}
    fn setup_first_round(&self, dht: &Self::DhtTables) {
        self.setup_round(dht);
    }
}

pub trait Message<W: Worker>: std::fmt::Debug + Clone {
    type Response;
    fn handle(self, worker: &W) -> Self::Response;
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
                    CoordReq::ScheduleJob { job, mapper } => {
                        self.current_job = Some(job.clone());
                        let worker = Arc::clone(&self.worker);
                        let dht = self.dht.clone();

                        std::thread::spawn(move || {
                            mapper.map(job.clone(), &worker, dht.as_ref().expect("DHT not set"));
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
struct WorkerRef(usize);

#[must_use = "this `JobScheduled` may not have scheduled the job on any worker"]
enum JobScheduled {
    Success(WorkerRef),
    NoAvailableWorkers,
}

struct Coordinator<J>
where
    J: Job,
{
    workers: BTreeMap<WorkerRef, <<J as Job>::Worker as Worker>::Remote>,
    setup: Box<dyn Setup<DhtTables = J::DhtTables>>,
    mappers: Vec<J::Mapper>,
}

impl<J> Coordinator<J>
where
    J: Job,
{
    fn new<S>(setup: S, workers: Vec<<<J as Job>::Worker as Worker>::Remote>) -> Self
    where
        S: Setup<DhtTables = J::DhtTables> + 'static,
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

    pub fn with_mapper(&mut self, mapper: J::Mapper) -> &mut Self {
        self.mappers.push(mapper);
        self
    }

    fn send_dht_to_workers(&self, dht: &DhtConn<J::DhtTables>) -> Result<()> {
        self.workers
            .par_iter()
            .map(|(_, worker)| worker.send_dht(dht))
            .collect::<Result<()>>()?;

        Ok(())
    }

    fn schedule_job(
        &self,
        job: J,
        mapper: J::Mapper,
        worker_jobs: &BTreeMap<WorkerRef, Result<Option<J>>>,
    ) -> Result<JobScheduled> {
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
                .expect("references in `potential_workers` should only point to non-failed workers")
                .is_none()
        }) {
            Some(free_worker) => {
                self.workers[free_worker].schedule_job(&job, mapper)?;
                Ok(JobScheduled::Success(*free_worker))
            }
            None => Ok(JobScheduled::NoAvailableWorkers),
        }
    }

    fn run<F>(self, jobs: Vec<J>, finisher: F) -> Result<()>
    where
        F: Finisher<Job = J>,
    {
        let mut dht = self.setup.init_dht();
        dht.cleanup_prev_tables();

        self.setup.setup_first_round(&dht.next);
        dht.next_round();

        while !finisher.is_finished(&dht.prev) {
            self.send_dht_to_workers(&dht)?;

            for mapper in &self.mappers {
                // run round
                let mut remaining_jobs: VecDeque<_> = jobs.clone().into_iter().collect();
                let mut sleeper = ExponentialBackoff::new(
                    Duration::from_millis(100),
                    Duration::from_secs(10),
                    2.0,
                );

                let mut scheduled_jobs: BTreeMap<WorkerRef, J> = BTreeMap::new();

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

                    match self.schedule_job(job.clone(), mapper.clone(), &worker_jobs)? {
                        JobScheduled::Success(worker) => {
                            scheduled_jobs.insert(worker, job);
                            sleeper.success();
                        }
                        JobScheduled::NoAvailableWorkers => {
                            remaining_jobs.push_front(job);
                            let sleep_duration = sleeper.next();
                            std::thread::sleep(sleep_duration);
                        }
                    }
                }

                // await all scheduled jobs
                let mut sleeper = ExponentialBackoff::new(
                    Duration::from_millis(100),
                    Duration::from_secs(10),
                    2.0,
                );
                loop {
                    let worker_jobs = self
                        .workers
                        .iter()
                        .map(|(r, w)| (*r, w.current_job()))
                        .collect::<BTreeMap<_, _>>();

                    let mut finished_workers = 0;
                    for (r, j) in worker_jobs.iter() {
                        match j {
                            Ok(Some(_)) => break,
                            Ok(None) => finished_workers += 1,
                            Err(_) => {
                                if let Some(job) = &scheduled_jobs.remove(r) {
                                    match self.schedule_job(
                                        job.clone(),
                                        mapper.clone(),
                                        &worker_jobs,
                                    )? {
                                        JobScheduled::Success(r) => {
                                            scheduled_jobs.insert(r, job.clone());
                                            break; // need to break to avoid double scheduling to same worker
                                        }
                                        JobScheduled::NoAvailableWorkers => {
                                            // retry scheduling.
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if finished_workers == self.workers.len() {
                        break;
                    }

                    std::thread::sleep(sleeper.next());
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

// TODO: this could be a proc macro instead
macro_rules! impl_dht_tables {
    ($struct:ty, [$($field:ident),*$(,)?]) => {
        impl DhtTables for $struct {
            fn drop_tables(&self) {
                $(self.$field.drop_table();)*
            }

            fn next(&self) -> Self {
                Self {
                    $($field: self.$field.next(),)*
                }
            }

            fn cleanup_prev_tables(&self) {
                $(
                    let tables = block_on(self.$field.client().all_tables()).unwrap();

                    for table in tables {
                        if table.as_str().starts_with(&self.$field.table().prefix()) {
                            block_on(self.$field.client().drop_table(table)).unwrap();
                        }
                    }
                )*
            }
        }
    };
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

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct Meta {
    round_had_changes: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CentralityTables {
    counters: DefaultDhtTableConn<webgraph::NodeID, HyperLogLog<64>>,
    meta: DefaultDhtTableConn<(), Meta>,
    centrality: DefaultDhtTableConn<webgraph::NodeID, f64>,
}

impl_dht_tables!(CentralityTables, [counters, meta, centrality]);

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CentralityJob {
    shard: u64,
}

impl Job for CentralityJob {
    type DhtTables = CentralityTables;
    type Worker = CentralityWorker;
    type Mapper = CentralityMapper;

    fn is_schedulable(&self, worker: &RemoteCentralityWorker) -> bool {
        self.shard == worker.shard()
    }
}

struct CentralitySetup {
    dht: DhtConn<CentralityTables>,
}

impl CentralitySetup {
    pub async fn new(cluster: &Cluster) -> Self {
        let initial = CentralityTables {
            counters: DefaultDhtTableConn::new(cluster, "counters").await,
            meta: DefaultDhtTableConn::new(cluster, "meta").await,
            centrality: DefaultDhtTableConn::new(cluster, "centrality").await,
        };

        let dht = DhtConn::new(initial);

        Self { dht }
    }
}

impl Setup for CentralitySetup {
    type DhtTables = CentralityTables;

    fn init_dht(&self) -> DhtConn<Self::DhtTables> {
        self.dht.clone()
    }

    fn setup_round(&self, dht: &Self::DhtTables) {
        dht.meta.set(
            (),
            Meta {
                round_had_changes: false,
            },
        );
    }
}

struct CentralityWorker {
    shard: u64,
    graph: Webgraph,
    changed_nodes: Arc<Mutex<BloomFilter>>,
    round: AtomicU64,
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
            round: AtomicU64::new(0),
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
enum CentralityMapper {
    Cardinalities,
    Centralities,
}

impl CentralityMapper {
    fn update_dht(
        &self,
        batch: &[Edge<()>],
        changed_nodes: &Mutex<BloomFilter>,
        new_changed_nodes: &Mutex<BloomFilter>,
        dht: &DhtConn<CentralityTables>,
    ) {
        // get old values from prev dht using edge.from where edge.from in changed_nodes

        let mut old_counters: BTreeMap<_, _> = {
            let changed_nodes = changed_nodes.lock().unwrap();

            dht.prev()
                .counters
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
                        .collect(),
                )
                .into_iter()
                .collect()
        };

        let changes = {
            let changed_nodes = changed_nodes.lock().unwrap();

            // upsert edge.to hyperloglogs in dht.next
            dht.next().counters.batch_upsert(
                HyperLogLogUpsert64,
                batch
                    .iter()
                    .filter(|edge| changed_nodes.contains(edge.from.as_u64()))
                    .map(|edge| {
                        let mut counter = old_counters
                            .remove(&edge.from)
                            .unwrap_or_else(HyperLogLog::default);
                        counter.add(edge.from.as_u64());
                        (edge.to, counter)
                    })
                    .collect(),
            )
        };

        // update new bloom filter with the nodes that changed
        {
            let mut new_changed_nodes = new_changed_nodes.lock().unwrap();

            for (node, upsert_res) in &changes {
                if let UpsertAction::Merged = upsert_res {
                    new_changed_nodes.insert(node.as_u64());
                }
            }
        }

        // if any nodes changed, indicate in dht that we aren't finished yet
        {
            if changes.iter().any(|(_, upsert_res)| match upsert_res {
                UpsertAction::Merged => true,
                UpsertAction::NoChange => false,
                UpsertAction::Inserted => true,
            }) {
                dht.next().meta.set(
                    (),
                    Meta {
                        round_had_changes: true,
                    },
                )
            }
        }
    }

    fn update_centralities(
        &self,
        nodes: &[webgraph::NodeID],
        round: u64,
        dht: &DhtConn<CentralityTables>,
    ) {
        let old_counters: BTreeMap<_, _> = dht
            .prev()
            .counters
            .batch_get(nodes.to_vec())
            .into_iter()
            .collect();
        let new_counters: BTreeMap<_, _> = dht
            .next()
            .counters
            .batch_get(nodes.to_vec())
            .into_iter()
            .collect();

        let mut delta = Vec::with_capacity(nodes.len());

        for node in nodes {
            let old_size = old_counters.get(node).map(|s| s.size() as u64).unwrap_or(0);
            let new_size = new_counters.get(node).map(|s| s.size() as u64).unwrap_or(0);

            if let Some(d) = new_size.checked_sub(old_size) {
                delta.push((*node, d as f64 / (round + 1) as f64));
            }
        }

        dht.next().centrality.batch_upsert(F64Add, delta);
    }

    fn map_cardinalities(&self, worker: &CentralityWorker, dht: &DhtConn<CentralityTables>) {
        const BATCH_SIZE: usize = 16_384;
        let new_changed_nodes = Arc::new(Mutex::new(BloomFilter::empty_from(
            &worker.changed_nodes.lock().unwrap(),
        )));

        let pool = rayon::ThreadPoolBuilder::new().build().unwrap();

        pool.scope(|s| {
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

            if !batch.is_empty() {
                let changed_nodes = Arc::clone(&worker.changed_nodes);
                let new_changed_nodes = Arc::clone(&new_changed_nodes);
                let update_batch = batch.clone();

                s.spawn(move |_| {
                    self.update_dht(&update_batch, &changed_nodes, &new_changed_nodes, dht)
                });
            }
        });
        *worker.changed_nodes.lock().unwrap() = new_changed_nodes.lock().unwrap().clone();
    }

    fn map_centralities(&self, worker: &CentralityWorker, dht: &DhtConn<CentralityTables>) {
        const BATCH_SIZE: usize = 16_384;
        let pool = rayon::ThreadPoolBuilder::new().build().unwrap();

        let round = worker.round.fetch_add(1, Ordering::Relaxed);

        // count cardinality of hyperloglogs in dht.next and update count after all mappers are done
        pool.scope(|s| {
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            let changed_nodes = worker.changed_nodes.lock().unwrap();
            for node in worker
                .graph
                .nodes()
                .filter(|node| changed_nodes.contains(node.as_u64()))
            {
                batch.push(node);
                if batch.len() >= BATCH_SIZE {
                    let update_batch = batch.clone();
                    s.spawn(move |_| self.update_centralities(&update_batch, round, dht));

                    batch.clear();
                }
            }

            if !batch.is_empty() {
                s.spawn(move |_| self.update_centralities(&batch, round, dht));
            }
        });
    }
}

impl Mapper for CentralityMapper {
    type Job = CentralityJob;

    fn map(&self, _: Self::Job, worker: &CentralityWorker, dht: &DhtConn<CentralityTables>) {
        match self {
            CentralityMapper::Cardinalities => self.map_cardinalities(worker, dht),
            CentralityMapper::Centralities => self.map_centralities(worker, dht),
        }
    }
}

struct CentralityFinish {}

impl Finisher for CentralityFinish {
    type Job = CentralityJob;

    fn is_finished(&self, dht: &CentralityTables) -> bool {
        dht.meta.get(()).unwrap().round_had_changes
    }
}
