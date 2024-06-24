use std::path::Path;

use clap::Parser;
use stract::config::{IndexerConfig, IndexerGraphConfig};

#[derive(Parser)]
struct Args {
    dual_encoder_path: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let data_path = Path::new("data");
    let path = data_path.join("example_index");

    if path.exists() {
        std::fs::remove_dir_all(&path)?;
    }

    println!("Indexing...");
    let start = std::time::Instant::now();
    stract::entrypoint::indexer::run(&IndexerConfig {
        output_path: path.to_str().unwrap().to_string(),
        limit_warc_files: None,
        skip_warc_files: None,
        warc_source: stract::config::WarcSource::Local(stract::config::LocalConfig {
            folder: ".".to_string(),
            names: vec![data_path
                .join("sample.warc.gz")
                .to_str()
                .unwrap()
                .to_string()],
        }),
        page_webgraph: Some(IndexerGraphConfig::Local {
            path: data_path
                .join("webgraph_page")
                .to_str()
                .unwrap()
                .to_string(),
        }),
        host_centrality_threshold: None,
        topics_path: None,
        host_centrality_store_path: data_path.join("centrality/").to_str().unwrap().to_string(),
        page_centrality_store_path: Some(
            data_path
                .join("centrality_page")
                .to_str()
                .unwrap()
                .to_string(),
        ),
        safety_classifier_path: None,
        minimum_clean_words: None,
        batch_size: 512,
        autocommit_after_num_inserts:
            stract::config::defaults::Indexing::autocommit_after_num_inserts(),
        dual_encoder: args
            .dual_encoder_path
            .map(|p| stract::config::IndexerDualEncoderConfig {
                model_path: p,
                page_centrality_rank_threshold: Some(1_000_000),
            }),
    })?;

    println!("Indexing took {:?}", start.elapsed());

    std::fs::remove_dir_all(path)?;
    Ok(())
}
