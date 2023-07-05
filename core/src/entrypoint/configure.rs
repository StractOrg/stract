// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use tokio::fs::File;
use tokio::io;
use tokio_stream::StreamExt;
use tracing::{debug, info};

use crate::entrypoint::{dmoz_parser, indexer};
use crate::Result;
use std::fs::{self};
use std::path::Path;

use super::{webgraph, Centrality, EntityIndexer};

const DATA_PATH: &str = "data";
const BUCKET_NAME: &str = "public";

fn download_files() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            for name in [
                "enwiki_subset.xml.bz2",
                "queries_us.csv",
                "sample.warc.gz",
                "bangs.json",
                "content.rdf.u8.gz",
            ] {
                info!("Downloading {}", name);
                let body = reqwest::get(format!("http://s3.trystract.com/{BUCKET_NAME}/{name}"))
                    .await
                    .unwrap();

                let mut file = File::create(Path::new(DATA_PATH).join(name)).await.unwrap();
                let mut bytes = body.bytes_stream();

                while let Some(item) = bytes.next().await {
                    io::copy(&mut item.unwrap().as_ref(), &mut file)
                        .await
                        .unwrap();
                }
            }
        });
}

fn create_webgraph() -> Result<()> {
    debug!("Creating webgraph");
    let out_path_tmp = Path::new(DATA_PATH).join("webgraph_tmp");
    let out_path = Path::new(DATA_PATH).join("webgraph");

    if out_path_tmp.exists() {
        std::fs::remove_dir_all(&out_path_tmp)?;
    }
    if out_path.exists() {
        std::fs::remove_dir_all(&out_path)?;
    }

    let warc_path = Path::new(DATA_PATH).join("sample.warc.gz");

    let job = webgraph::Job {
        config: webgraph::JobConfig::Local(crate::LocalConfig {
            folder: ".".to_string(),
            names: vec![warc_path.to_str().unwrap().to_string()],
        }),
        warc_paths: vec![warc_path.to_str().unwrap().to_string()],
        graph_base_path: out_path_tmp.to_str().unwrap().to_string(),
        level: crate::WebgraphLevel::Host,
    };

    let graph = webgraph::process_job(&job);
    std::fs::rename(graph.path, out_path)?;
    std::fs::remove_dir_all(&out_path_tmp)?;

    Ok(())
}

fn calculate_centrality() {
    debug!("Calculating centrality");
    let webgraph_path = Path::new(DATA_PATH).join("webgraph");
    let out_path = Path::new(DATA_PATH).join("centrality");

    Centrality::build_harmonic(&webgraph_path, &out_path);
    Centrality::build_similarity(&webgraph_path, &out_path);
}

fn create_inverted_index() -> Result<()> {
    debug!("Creating inverted index");
    let out_path = Path::new(DATA_PATH).join("index");
    let out_path_tmp = Path::new(DATA_PATH).join("index_tmp");

    if out_path_tmp.exists() {
        std::fs::remove_dir_all(&out_path_tmp)?;
    }
    if out_path.exists() {
        std::fs::remove_dir_all(&out_path)?;
    }

    let warc_path = Path::new(DATA_PATH).join("sample.warc.gz");

    let job = indexer::Job {
        source_config: indexer::JobConfig::Local(crate::LocalConfig {
            folder: ".".to_string(),
            names: vec![warc_path.to_str().unwrap().to_string()],
        }),
        warc_paths: vec![warc_path.to_str().unwrap().to_string()],
        base_path: out_path_tmp.to_str().unwrap().to_string(),
        host_centrality_threshold: None,
        minimum_clean_words: Some(40),
    };

    let webgraph_path = Path::new(DATA_PATH).join("webgraph");
    let centrality_path = Path::new(DATA_PATH).join("centrality");

    let worker = indexer::IndexingWorker::new(
        centrality_path.to_str().unwrap().to_string(),
        Some(webgraph_path.to_str().unwrap().to_string()),
        Some(
            Path::new(DATA_PATH)
                .join("human_annotations")
                .to_str()
                .unwrap()
                .to_string(),
        ),
    );

    let index = indexer::process_job(&job, &worker);
    std::fs::rename(index.path, out_path)?;
    std::fs::remove_dir_all(&out_path_tmp)?;

    Ok(())
}

fn create_entity_index() -> Result<()> {
    let out_path = Path::new(DATA_PATH).join("entity");
    if out_path.exists() {
        std::fs::remove_dir_all(&out_path)?;
    }

    let wiki_path = Path::new(DATA_PATH).join("enwiki_subset.xml.bz2");

    EntityIndexer::run(
        wiki_path.to_str().unwrap().to_string(),
        out_path.to_str().unwrap().to_string(),
    )?;

    Ok(())
}

fn parse_topics() -> Result<()> {
    let dmoz_path = Path::new(DATA_PATH).join("content.rdf.u8.gz");

    let topics = dmoz_parser::parse(dmoz_path)?;
    topics.save(Path::new(DATA_PATH).join("human_annotations"))
}

fn index_files() -> Result<()> {
    create_webgraph()?;
    calculate_centrality();
    parse_topics()?;
    create_inverted_index()?;
    create_entity_index()?;

    Ok(())
}

pub fn run(skip_download: bool) -> Result<()> {
    let p = Path::new(DATA_PATH);

    if !p.exists() {
        fs::create_dir_all(p)?;
    }

    if !skip_download {
        download_files();
    }

    index_files()?;

    Ok(())
}

pub fn alice() -> Result<()> {
    // create /data/alice folder
    let p = Path::new(DATA_PATH).join("alice");

    if !p.exists() {
        fs::create_dir_all(p)?;
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            for name in ["model.safetensors", "tokenizer.json"] {
                info!("Downloading {}", name);
                let body = reqwest::get(format!(
                    "http://s3.trystract.com/{BUCKET_NAME}/alice/{name}"
                ))
                .await
                .unwrap();

                let mut file = File::create(Path::new(DATA_PATH).join("alice").join(name))
                    .await
                    .unwrap();
                let mut bytes = body.bytes_stream();

                while let Some(item) = bytes.next().await {
                    io::copy(&mut item.unwrap().as_ref(), &mut file)
                        .await
                        .unwrap();
                }
            }
        });

    Ok(())
}
