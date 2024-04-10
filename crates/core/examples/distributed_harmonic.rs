use std::time::Duration;

use clap::Parser;
use stract::distributed::member::ShardId;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Parser)]
struct Args {
    graph_path: String,
    output_path: String,
}

fn start_dht_thread() {
    std::thread::spawn(|| {
        let config = stract::entrypoint::ampc::dht::Config {
            node_id: 1,
            host: "0.0.0.0:3000".parse().unwrap(),
            shard: ShardId::new(1),
            seed_node: None,
            gossip: Some(stract::config::GossipConfig {
                cluster_id: "test".to_string(),
                seed_nodes: None,
                addr: "0.0.0.0:3001".parse().unwrap(),
            }),
        };

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(stract::entrypoint::ampc::dht::run(config))
            .unwrap();
    });
}

fn start_worker_thread(graph_path: String) {
    std::thread::spawn(|| {
        let config = stract::config::HarmonicWorkerConfig {
            gossip: stract::config::GossipConfig {
                cluster_id: "test".to_string(),
                seed_nodes: Some(vec!["0.0.0.0:3001".parse().unwrap()]),
                addr: "0.0.0.0:3003".parse().unwrap(),
            },
            shard: ShardId::new(1),
            graph_path,
            host: "0.0.0.0:3002".parse().unwrap(),
        };

        stract::entrypoint::ampc::harmonic_centrality::worker::run(config).unwrap();
    });
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .without_time()
        .with_target(false)
        .finish()
        .init();

    tracing::info!("Starting distributed harmonic centrality worker");

    let args = Args::parse();
    start_dht_thread();
    std::thread::sleep(Duration::from_secs(1));
    start_worker_thread(args.graph_path);
    std::thread::sleep(Duration::from_secs(1));

    let config = stract::config::HarmonicCoordinatorConfig {
        gossip: stract::config::GossipConfig {
            cluster_id: "test".to_string(),
            seed_nodes: Some(vec!["0.0.0.0:3001".parse().unwrap()]),
            addr: "0.0.0.0:3005".parse().unwrap(),
        },
        host: "0.0.0.0:3004".parse().unwrap(),
        output_path: args.output_path,
    };

    let start = std::time::Instant::now();
    stract::entrypoint::ampc::harmonic_centrality::coordinator::run(config)?;
    tracing::info!("Calculated centrality in: {:?}", start.elapsed());

    Ok(())
}
