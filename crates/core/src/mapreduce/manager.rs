use super::{Error, MapReduceConnection, Result, Worker};
use super::{Map, Reduce};
use crate::mapreduce::Task;
use distributed::retry_strategy::ExponentialBackoff;
use futures::StreamExt;
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::ops::Deref;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, warn};

#[derive(Debug)]
struct RemoteWorker {
    addr: SocketAddr,
}

impl RemoteWorker {
    fn retry_strategy() -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(10)
            .with_limit(Duration::from_secs(100))
            .take(15)
    }

    async fn connect<W, I, O>(&self) -> Result<MapReduceConnection<I, O>>
    where
        W: Worker,
        I: Map<W, O> + Send,
        O: Serialize + DeserializeOwned + Send,
    {
        for dur in RemoteWorker::retry_strategy() {
            if let Ok(conn) = MapReduceConnection::create(self.addr).await {
                debug!("connected");
                return Ok(conn);
            }

            std::thread::sleep(dur);
        }

        Err(Error::NoResponse)
    }

    async fn perform<W, I, O>(&self, job: I) -> Result<O>
    where
        W: Worker,
        I: Map<W, O> + Send,
        O: Serialize + DeserializeOwned + Send,
    {
        let conn = self.connect::<W, I, O>().await?;
        match conn.send(&Task::Job(job)).await {
            Ok(Some(res)) => Ok(res),
            _ => Err(Error::NoResponse),
        }
    }

    async fn stop<W, I, O>(&self) -> Result<()>
    where
        W: Worker,
        I: Map<W, O> + Send,
        O: Serialize + DeserializeOwned + Send,
    {
        debug!("closing worker {:}", self.addr);
        let conn = self.connect().await?;
        let res = conn.send(&Task::<I>::AllFinished).await?;

        debug_assert!(res.is_none());

        Ok(())
    }
}

struct WorkerGuard<'a> {
    from_pool: &'a WorkerPool,
    worker: Arc<RemoteWorker>,
}

impl<'a> WorkerGuard<'a> {
    fn new(pool: &'a WorkerPool, worker: Arc<RemoteWorker>) -> Self {
        Self {
            worker,
            from_pool: pool,
        }
    }

    async fn success(self) {
        self.from_pool.insert(Arc::clone(&self.worker)).await;
    }
}

impl<'a> Deref for WorkerGuard<'a> {
    type Target = Arc<RemoteWorker>;

    fn deref(&self) -> &Self::Target {
        &self.worker
    }
}

impl<'a> Drop for WorkerGuard<'a> {
    fn drop(&mut self) {
        self.from_pool.put_back();
    }
}

struct WorkerPool {
    all_workers: Vec<Arc<RemoteWorker>>,
    ready_workers: Mutex<Vec<Arc<RemoteWorker>>>,
    running_workers: AtomicU32,
}

impl WorkerPool {
    fn new<A>(workers: &[A]) -> Self
    where
        A: ToSocketAddrs + std::fmt::Debug,
    {
        let all_workers: Vec<Arc<RemoteWorker>> = workers
            .iter()
            .flat_map(|addr| {
                addr.to_socket_addrs().unwrap_or_else(|_| {
                    panic!("failed to transform {addr:?} into a socket address")
                })
            })
            .map(|addr| Arc::new(RemoteWorker { addr }))
            .collect();

        Self {
            ready_workers: Mutex::new(all_workers.clone()),
            all_workers,
            running_workers: AtomicU32::new(0),
        }
    }

    fn put_back(&self) {
        self.running_workers.fetch_sub(1, Ordering::SeqCst);
    }

    async fn insert(&self, worker: Arc<RemoteWorker>) {
        self.ready_workers.lock().await.push(worker);
    }

    async fn get_worker(&self) -> Result<Option<WorkerGuard<'_>>> {
        let mut ready_workers = self.ready_workers.lock().await;
        if ready_workers.len() as u32 + self.running_workers.load(Ordering::SeqCst) == 0 {
            return Err(Error::NoAvailableWorker);
        }

        if let Some(worker) = ready_workers.pop() {
            self.running_workers.fetch_add(1, Ordering::SeqCst);
            Ok(Some(WorkerGuard::new(self, worker)))
        } else {
            Ok(None)
        }
    }

    async fn stop_workers<W, I, O>(&self)
    where
        W: Worker,
        I: Map<W, O> + Send,
        O: Serialize + DeserializeOwned + Send,
    {
        let mut failing_workers = Vec::new();
        for worker in &self.all_workers {
            if worker.stop::<W, I, O>().await.is_err() {
                failing_workers.push(worker);
            }
        }

        if !failing_workers.is_empty() {
            debug!(
                "failed to stop the following workers: {:#?}",
                failing_workers
            );
        }
    }

    fn size(&self) -> usize {
        self.all_workers.len()
    }
}

pub struct Manager {
    pool: WorkerPool,
}

impl Manager {
    pub fn new<A>(workers: &[A]) -> Self
    where
        A: ToSocketAddrs + std::fmt::Debug,
    {
        Self {
            pool: WorkerPool::new(workers),
        }
    }

    async fn try_map<W, I, O>(&self, job: &I) -> Result<O>
    where
        W: Worker,
        I: Map<W, O> + Send + Clone,
        O: Serialize + DeserializeOwned + Send,
    {
        loop {
            match self.pool.get_worker().await? {
                Some(worker) => {
                    let res = worker.perform(job.clone()).await?;
                    worker.success().await;

                    return Ok(res);
                }
                None => std::thread::sleep(std::time::Duration::from_millis(1000)),
            }
        }
    }

    /// Execute job on one of the remote machines. If the remote machine fails for some reason,
    /// the job should be allocated to another machine.
    pub async fn map<W, I, O>(&self, job: I) -> O
    where
        W: Worker,
        I: Map<W, O> + Send + Clone,
        O: Serialize + DeserializeOwned + Send,
    {
        loop {
            match self.try_map(&job).await {
                Ok(res) => return res,
                Err(Error::NoAvailableWorker) => panic!("{}", Error::NoAvailableWorker),
                Err(err) => {
                    warn!("Worker failed - rescheduling job");
                    debug!("{:?}", err);
                }
            }
        }
    }

    fn reduce<O1, O2>(acc: Option<O2>, elem: O1) -> O2
    where
        O1: Serialize + DeserializeOwned + Send,
        O2: From<O1> + Reduce<O1> + Send,
    {
        match acc {
            Some(acc) => acc.reduce(elem),
            None => elem.into(),
        }
    }

    #[allow(clippy::trait_duplication_in_bounds)]
    async fn get_results<W, I, O1, O2>(&self, jobs: impl Iterator<Item = I> + Send) -> Option<O2>
    where
        W: Worker,
        I: Map<W, O1> + Send + Clone,
        O1: Serialize + DeserializeOwned + Send,
        O2: From<O1> + Reduce<O1> + Send + Reduce<O2>,
    {
        let mut acc = None;

        for chunk in jobs.chunks(self.pool.size()).into_iter() {
            let results = futures::stream::iter(chunk.map(|job| self.map::<W, I, O1>(job)))
                .buffer_unordered(self.pool.size())
                .collect::<Vec<_>>()
                .await;

            for elem in results {
                acc = Some(Self::reduce(acc, elem));
            }
        }

        acc
    }

    #[allow(clippy::trait_duplication_in_bounds)]
    pub async fn run<W, I, O1, O2>(self, jobs: impl Iterator<Item = I> + Send) -> Option<O2>
    where
        W: Worker,
        I: Map<W, O1> + Send + Clone,
        O1: Serialize + DeserializeOwned + Send,
        O2: From<O1> + Reduce<O1> + Send + Reduce<O2>,
    {
        let result = self.get_results(jobs).await;
        self.pool.stop_workers::<W, I, O1>().await;

        result
    }
}
