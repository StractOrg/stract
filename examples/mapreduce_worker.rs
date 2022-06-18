use std::net::SocketAddr;

use cuely::mapreduce::{Map, Reduce, Worker};
use serde::{Deserialize, Serialize};
use tracing::{debug, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Serialize, Deserialize, Debug)]
struct Job {
    id: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct Count(usize);

impl Map<Count> for Job {
    fn map(self) -> Count {
        debug!("begin map {:?}", self);
        std::thread::sleep(std::time::Duration::from_secs(20)); // simulate some long running task
        debug!("end map");
        Count(1)
    }
}

impl Reduce for Count {
    fn reduce(self, element: Self) -> Self {
        debug!("begin reduce");
        std::thread::sleep(std::time::Duration::from_secs(20));
        debug!("end reduce");
        Count(self.0 + element.0)
    }
}

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let args: Vec<_> = std::env::args().collect();

    Worker::run::<Job, Count>(args[1].parse::<SocketAddr>().unwrap())
        .expect("failed to run worker");
}
