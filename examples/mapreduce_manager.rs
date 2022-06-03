use std::net::SocketAddr;

use cuely::mapreduce::{Map, MapReduce, Reduce};
use serde::{Deserialize, Serialize};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[derive(Serialize, Deserialize, Debug)]
struct Job {
    id: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct Count(usize);

impl Map<Count> for Job {
    fn map(self) -> Count {
        std::thread::sleep(std::time::Duration::from_secs(2)); // simulate some long running task
        Count(1)
    }
}

impl Reduce for Count {
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

    let res = jobs
        .into_iter()
        .map_reduce(&[
            "0.0.0.0:1337".parse::<SocketAddr>().unwrap(),
            "0.0.0.0:1338".parse::<SocketAddr>().unwrap(),
            "0.0.0.0:1339".parse::<SocketAddr>().unwrap(),
        ])
        .await
        .unwrap();
    println!("{:?}", res.0);
}
