use std::net::SocketAddr;

use cuely::mapreduce::{Map, MapReduce, Reduce};
use serde::{Deserialize, Serialize};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[derive(Serialize, Deserialize, Debug)]
struct Job {
    contents: Vec<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Count(usize);

impl Map<Count> for Job {
    fn map(self) -> Count {
        Count(self.contents.into_iter().filter(|d| *d == 0).count())
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
        Job {
            contents: vec![1, 2, 0, 1, 0, 1, 0],
        },
        Job {
            contents: vec![3, 2, 1],
        },
        Job {
            contents: vec![0, 0],
        },
    ];

    let res = jobs
        .into_iter()
        .map_reduce(&["0.0.0.0:1337".parse::<SocketAddr>().unwrap()])
        .await
        .unwrap();
    println!("{:?}", res.0);
}
