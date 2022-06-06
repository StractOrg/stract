use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::mapreduce::Task;

use super::{Error, Result};
use super::{Map, Reduce};
use async_channel::{unbounded, Receiver, Sender};
use futures::{stream::FuturesUnordered, StreamExt};
use std::net::ToSocketAddrs;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;
use tracing::debug;

#[derive(Debug)]
struct RemoteWorker {
    addr: SocketAddr,
}

impl RemoteWorker {
    fn retry_strategy() -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(10).take(4)
    }

    async fn connect(&self) -> Result<TcpStream> {
        debug!("connecting to {:}", self.addr);
        let stream = Retry::spawn(RemoteWorker::retry_strategy(), || async {
            TcpStream::connect(self.addr).await
        })
        .await?;
        debug!("connected");

        Ok(stream)
    }

    async fn perform<I, O>(&self, job: &I) -> Result<O>
    where
        I: Map<O>,
        O: Reduce<O> + Send,
    {
        let mut stream = self.connect().await?;
        let serialized_job = bincode::serialize(&Task::Job(job))?;
        debug!("sending {:?} bytes", serialized_job.len());
        stream.write(&serialized_job).await?;

        let mut buf = [0; 4096];
        let mut bytes = Vec::new();
        loop {
            if let Ok(size) = stream.read(&mut buf).await {
                debug!("read {:?} bytes", size);
                if size == 0 && bytes.len() == 0 {
                    return Err(Error::NoResponse);
                }
                bytes.extend_from_slice(&buf);
                if size < buf.len() {
                    break;
                }
            }
        }

        Ok(bincode::deserialize(&bytes)?)
    }

    async fn stop<I, O>(&self) -> Result<()>
    where
        I: Map<O>,
        O: Reduce<O> + Send,
    {
        debug!("closing worker {:}", self.addr);
        let mut stream = self.connect().await?;
        let serialized_job = bincode::serialize(&Task::<I>::AllFinished)?;
        debug!("sending {:?} bytes", serialized_job.len());
        stream.write(&serialized_job).await?;
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
        self.from_pool.insert(Arc::clone(&self.worker)).await
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
        self.from_pool.put_back()
    }
}

struct WorkerPool {
    all_workers: Vec<Arc<RemoteWorker>>,
    alive_workers: (Sender<Arc<RemoteWorker>>, Receiver<Arc<RemoteWorker>>),
    running_workers: AtomicU32,
}

impl WorkerPool {
    async fn new<A>(workers: &[A]) -> Self
    where
        A: ToSocketAddrs + std::fmt::Debug,
    {
        let all_workers = workers
            .iter()
            .flat_map(|addr| {
                addr.to_socket_addrs().expect(&format!(
                    "failed to transform {:?} into a socket address",
                    addr
                ))
            })
            .map(|addr| Arc::new(RemoteWorker { addr }))
            .collect();

        let (s, r) = unbounded();

        for worker in &all_workers {
            s.send(Arc::clone(worker)).await.unwrap();
        }

        Self {
            all_workers,
            alive_workers: (s, r),
            running_workers: AtomicU32::new(0),
        }
    }

    fn put_back(&self) {
        self.running_workers.fetch_sub(1, Ordering::SeqCst);
    }

    async fn insert(&self, worker: Arc<RemoteWorker>) {
        let ch = self.alive_workers.0.clone();
        ch.send(worker).await.unwrap();
    }

    async fn get_worker<'a>(&'a self) -> Result<WorkerGuard<'a>> {
        if self.alive_workers.0.len() as u32 + self.running_workers.load(Ordering::SeqCst) == 0 {
            return Err(Error::NoAvailableWorker);
        }

        let worker = self.alive_workers.1.recv().await?;
        self.running_workers.fetch_add(1, Ordering::SeqCst);

        Ok(WorkerGuard::new(self, worker))
    }

    async fn stop_workers<I, O>(&self)
    where
        I: Map<O>,
        O: Reduce<O> + Send,
    {
        let mut failing_workers = Vec::new();
        for worker in &self.all_workers {
            if worker.stop::<I, O>().await.is_err() {
                failing_workers.push(worker)
            }
        }

        if failing_workers.len() > 0 {
            debug!(
                "failed to stop the following workers: {:#?}",
                failing_workers
            );
        }
    }
}

pub struct Manager {
    pool: WorkerPool,
}

impl Manager {
    pub async fn new<A>(workers: &[A]) -> Self
    where
        A: ToSocketAddrs + std::fmt::Debug,
    {
        Self {
            pool: WorkerPool::new(workers).await,
        }
    }

    async fn try_map<I, O>(&self, job: &I) -> Result<O>
    where
        I: Map<O>,
        O: Reduce<O> + Send,
    {
        let worker = self.pool.get_worker().await?;
        let res = worker.perform(job).await?;
        worker.success().await;

        Ok(res)
    }

    /// Execute job on one of the remote machines. If the remote machine fails for some reason,
    /// the job should be allocated to another machine.
    pub async fn map<I, O>(&self, job: I) -> O
    where
        I: Map<O>,
        O: Reduce<O> + Send,
    {
        loop {
            match self.try_map(&job).await {
                Ok(res) => return res,
                Err(Error::NoAvailableWorker) => panic!("{}", Error::NoAvailableWorker),
                Err(_) => {
                    debug!("got err - rescheduling job");
                }
            }
        }
    }

    fn reduce<O: Reduce<O>>(acc: Option<O>, elem: O) -> O {
        match acc {
            Some(acc) => acc.reduce(elem),
            None => elem,
        }
    }

    async fn get_results<I, O>(&self, jobs: Vec<I>) -> Option<O>
    where
        I: Map<O>,
        O: Reduce<O> + Send,
    {
        jobs.into_iter()
            .map(|job| self.map::<I, O>(job))
            .collect::<FuturesUnordered<_>>()
            .fold(
                None,
                |acc, elem| async move { Some(Manager::reduce(acc, elem)) },
            )
            .await
    }

    pub async fn run<I, O>(&self, jobs: Vec<I>) -> Option<O>
    where
        I: Map<O>,
        O: Reduce<O> + Send,
    {
        let result = self.get_results(jobs).await;
        self.pool.stop_workers::<I, O>().await;

        result
    }
}
