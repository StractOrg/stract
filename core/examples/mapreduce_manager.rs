use std::net::SocketAddr;

use cuely::mapreduce::{Manager, Map, Reduce, StatelessWorker};
use serde::{Deserialize, Serialize};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[derive(Serialize, Deserialize, Debug)]
struct Job {
    id: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct Count(usize);

impl Map<StatelessWorker, Count> for Job {
    fn map(&self, _worker: &StatelessWorker) -> Count {
        std::thread::sleep(std::time::Duration::from_secs(2)); // simulate some long running task
        Count(1)
    }
}

impl Reduce<Count> for Count {
    fn reduce(self, element: Self) -> Self {
        Count(self.0 + element.0)
    }
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let jobs = vec![
        Job { id: 0 },
        Job { id: 1 },
        Job { id: 2 },
        Job { id: 3 },
        Job { id: 4 },
        Job { id: 5 },
    ];

    let manager = Manager::new(&["0.0.0.0:1337".parse::<SocketAddr>().unwrap()]);
    let res: Count = manager.run(jobs.into_iter()).await.unwrap();

    println!("{:?}", res.0);
}
