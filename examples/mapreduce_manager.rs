use std::net::SocketAddr;

use cuely::mapreduce::{Map, MapReduce, Reduce};
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
        debug!("begin map");
        std::thread::sleep(std::time::Duration::from_secs(2)); // simulate some long running task
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
        .map_reduce(&["0.0.0.0:1337".parse::<SocketAddr>().unwrap()])
        .unwrap();
    println!("{:?}", res.0);
}
