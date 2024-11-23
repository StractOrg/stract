use std::{net::SocketAddr, time::Duration};

use clap::Parser;
use stract::{
    distributed::member::ShardId,
    webgraph::{Edge, Node},
    webpage::url_ext::UrlExt,
};
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Parser)]
struct Args {
    warc_path: String,
    graph_path: String,
    output_path: String,
}

fn start_dht_thread(id: u64, host: SocketAddr, gossip: SocketAddr) {
    std::thread::spawn(move || {
        let config = stract::entrypoint::ampc::dht::Config {
            node_id: id,
            host,
            shard: ShardId::new(id),
            seed_node: None,
            gossip: Some(stract::config::GossipConfig {
                seed_nodes: Some(vec!["0.0.0.0:3001".parse().unwrap()]),
                addr: gossip,
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

fn start_worker_thread(graph_path: String, shard: ShardId, host: SocketAddr, gossip: SocketAddr) {
    std::thread::spawn(move || {
        let config = stract::config::HarmonicWorkerConfig {
            gossip: stract::config::GossipConfig {
                seed_nodes: Some(vec!["0.0.0.0:3001".parse().unwrap()]),
                addr: gossip,
            },
            shard,
            graph_path,
            host,
        };

        stract::entrypoint::ampc::harmonic_centrality::worker::run(config).unwrap();
    });
}

fn build_graphs_if_not_exist(warc_path: &str, graph_path: &str) -> anyhow::Result<()> {
    let path = std::path::Path::new(graph_path);

    if path.exists() {
        return Ok(());
    }

    tracing::info!("Building graphs from warc file: {}", warc_path);

    std::fs::create_dir_all(path)?;
    let warc = stract::warc::WarcFile::open(warc_path)?;

    let num_records = warc.records().count();

    let a_path = path.join("graph_a");
    let b_path = path.join("graph_b");

    let mut a = stract::webgraph::WebgraphBuilder::new(a_path, 0u64.into()).open()?;

    let mut b = stract::webgraph::WebgraphBuilder::new(b_path, 0u64.into()).open()?;

    for (i, record) in warc.records().flatten().enumerate() {
        let webpage = match stract::webpage::Html::parse_without_text(
            &record.response.body,
            &record.request.url,
        ) {
            Ok(webpage) => webpage,
            Err(err) => {
                tracing::error!("error parsing webpage: {}", err);
                continue;
            }
        };

        for link in webpage
            .anchor_links()
            .into_iter()
            .filter(|link| matches!(link.destination.scheme(), "http" | "https"))
        {
            let source = Node::from(link.source.clone()).into_host();
            let destination = Node::from(link.destination.clone()).into_host();

            let dest_domain = link.destination.root_domain();
            let source_domain = link.source.root_domain();

            if dest_domain.is_some() && source_domain.is_some() && dest_domain != source_domain {
                if i <= num_records / 2 {
                    a.insert(Edge {
                        from: source,
                        to: destination,
                        rel_flags: link.rel,
                        label: link.text,
                        ..Edge::empty()
                    })?;
                } else {
                    b.insert(Edge {
                        from: source,
                        to: destination,
                        rel_flags: link.rel,
                        label: link.text,
                        ..Edge::empty()
                    })?;
                }
            }
        }
    }

    a.commit()?;
    a.optimize_read()?;

    b.commit()?;
    b.optimize_read()?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::builder().from_env_lossy())
        .without_time()
        .with_target(false)
        .finish()
        .init();

    let args = Args::parse();

    build_graphs_if_not_exist(&args.warc_path, &args.graph_path)?;

    tracing::info!("Starting distributed harmonic centrality worker");

    start_dht_thread(
        1,
        "0.0.0.0:3000".parse().unwrap(),
        "0.0.0.0:3001".parse().unwrap(),
    );
    start_dht_thread(
        2,
        "0.0.0.0:2998".parse().unwrap(),
        "0.0.0.0:2999".parse().unwrap(),
    );
    std::thread::sleep(Duration::from_secs(1));
    let graph_path = std::path::Path::new(&args.graph_path);
    start_worker_thread(
        graph_path.join("graph_a").to_str().unwrap().to_string(),
        ShardId::new(1),
        "0.0.0.0:3002".parse().unwrap(),
        "0.0.0.0:3003".parse().unwrap(),
    );
    start_worker_thread(
        graph_path.join("graph_b").to_str().unwrap().to_string(),
        ShardId::new(2),
        "0.0.0.0:3004".parse().unwrap(),
        "0.0.0.0:3005".parse().unwrap(),
    );
    std::thread::sleep(Duration::from_secs(3));

    let config = stract::config::HarmonicCoordinatorConfig {
        gossip: stract::config::GossipConfig {
            seed_nodes: Some(vec!["0.0.0.0:3001".parse().unwrap()]),
            addr: "0.0.0.0:3007".parse().unwrap(),
        },
        host: "0.0.0.0:3006".parse().unwrap(),
        output_path: args.output_path,
    };

    let start = std::time::Instant::now();
    stract::entrypoint::ampc::harmonic_centrality::coordinator::run(config)?;
    tracing::info!("Calculated centrality in: {:?}", start.elapsed());

    Ok(())
}
