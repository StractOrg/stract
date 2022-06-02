use std::net::SocketAddr;

use crate::mapreduce::Task;

use super::{Map, Reduce};
use futures::{stream::FuturesUnordered, StreamExt};
use rand::prelude::SliceRandom;
use std::net::ToSocketAddrs;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::debug;

pub struct Manager {
    workers: Vec<SocketAddr>,
}

impl Manager {
    pub fn new<A>(workers: &[A]) -> Self
    where
        A: ToSocketAddrs,
    {
        Self {
            workers: workers
                .iter()
                .flat_map(|addr| addr.to_socket_addrs().unwrap())
                .collect(),
        }
    }

    /// Execute job on one of the remote machines. If the remote machine fails for some reason,
    /// the job should be allocated to another machine.
    pub async fn map<I, O>(&self, job: I) -> O
    where
        I: Map<O>,
        O: Reduce<O> + Send,
    {
        let worker_addr = self.workers.choose(&mut rand::thread_rng()).unwrap();
        debug!("connecting to {:}", worker_addr);
        let mut stream = TcpStream::connect(worker_addr).await.unwrap();
        debug!("connected");
        let serialized_job = bincode::serialize(&Task::Job(job)).unwrap();
        debug!("sending {:?} bytes", serialized_job.len());
        stream.write(&serialized_job).await.unwrap();

        let mut buf = vec![0; 4096];
        let size = stream.read(&mut buf).await.unwrap();
        bincode::deserialize(&buf[..size]).unwrap()
    }

    fn reduce<O: Reduce<O>>(acc: Option<O>, elem: O) -> O {
        match acc {
            Some(acc) => acc.reduce(elem),
            None => elem,
        }
    }

    pub async fn run<I, O>(&self, jobs: Vec<I>) -> Option<O>
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
}
