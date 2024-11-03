// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

use crate::config::{
    defaults, IndexerConfig, IndexerDualEncoderConfig, IndexerGraphConfig, LocalConfig,
    WebSpellConfig,
};
use crate::entrypoint::indexer;
use crate::entrypoint::indexer::JobSettings;
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
                "sample.warc.gz",
                "bangs.json",
                "english-wordnet-2022-subset.ttl",
                "lambdamart.txt",
                "test.zim",
                "internet_archive.warc.gz",
            ] {
                let path = Path::new(DATA_PATH).join(name);

                if path.exists() {
                    info!("Skipping {}", name);
                    continue;
                }

                info!("Downloading {}", name);
                let body = reqwest::get(format!("http://s3.stract.com/{BUCKET_NAME}/{name}"))
                    .await
                    .unwrap();

                let progress = body.content_length().map(indicatif::ProgressBar::new);

                let mut file = File::create(path).await.unwrap();
                let mut bytes = body.bytes_stream();

                while let Some(item) = bytes.next().await {
                    let bytes = item.unwrap();
                    if let Some(progress) = &progress {
                        progress.inc(bytes.len() as _);
                    }
                    io::copy(&mut bytes.as_ref(), &mut file).await.unwrap();
                }

                if let Some(progress) = progress {
                    progress.finish_and_clear();
                }
            }
        });
}

fn build_spellchecker() -> Result<()> {
    debug!("Building spellchecker");
    let spellchecker_path = Path::new(DATA_PATH).join("web_spell");

    if !spellchecker_path.exists() {
        crate::entrypoint::web_spell::run(WebSpellConfig {
            languages: vec![whatlang::Lang::Eng],
            output_path: spellchecker_path.to_str().unwrap().to_string(),
            warc_source: crate::config::WarcSource::Local(LocalConfig {
                folder: ".".to_string(),
                names: vec![Path::new(DATA_PATH)
                    .join("sample.warc.gz")
                    .to_str()
                    .unwrap()
                    .to_string()],
            }),
            limit_warc_files: None,
            skip_warc_files: None,
        })?;
    }

    Ok(())
}

fn create_webgraph() -> Result<()> {
    debug!("Creating webgraph");
    let out_path = Path::new(DATA_PATH).join("webgraph");

    if out_path.exists() {
        std::fs::remove_dir_all(&out_path)?;
    }

    let warc_path = Path::new(DATA_PATH).join("sample.warc.gz");

    let job = webgraph::Job {
        config: webgraph::JobConfig::Local(crate::config::LocalConfig {
            folder: ".".to_string(),
            names: vec![warc_path.to_str().unwrap().to_string()],
        }),
        warc_paths: vec![warc_path.to_str().unwrap().to_string()],
    };

    let mut worker = webgraph::WebgraphWorker {
        graph: crate::webgraph::Webgraph::open(&out_path, 0u64.into()).unwrap(),
        host_centrality_store: None,
        canonical_index: None,
    };

    worker.process_job(&job);
    worker.graph.optimize_read().unwrap();

    Ok(())
}

fn calculate_centrality() {
    debug!("Calculating centrality");
    let webgraph_path = Path::new(DATA_PATH).join("webgraph");
    let out_path = Path::new(DATA_PATH).join("centrality");

    if !out_path.exists() {
        Centrality::build_harmonic(&webgraph_path, &out_path);
    }

    let out_path_page = Path::new(DATA_PATH).join("centrality_page");

    if !out_path_page.exists() {
        Centrality::build_approx_harmonic(webgraph_path, out_path_page).unwrap();
    }
}

fn create_inverted_index() -> Result<()> {
    debug!("Creating inverted index");
    let out_path = Path::new(DATA_PATH).join("index");

    if out_path.exists() {
        std::fs::remove_dir_all(&out_path)?;
    }

    let warc_path = Path::new(DATA_PATH).join("sample.warc.gz");

    let job = indexer::Job {
        source_config: crate::config::WarcSource::Local(crate::config::LocalConfig {
            folder: ".".to_string(),
            names: vec![warc_path.to_str().unwrap().to_string()],
        }),
        warc_path: warc_path.to_str().unwrap().to_string(),
        base_path: out_path.to_str().unwrap().to_string(),
        settings: JobSettings {
            host_centrality_threshold: None,
            minimum_clean_words: None,
            batch_size: defaults::Indexing::batch_size(),
            autocommit_after_num_inserts: defaults::Indexing::autocommit_after_num_inserts(),
        },
    };

    let webgraph_path = Path::new(DATA_PATH).join("webgraph");
    let centrality_path = Path::new(DATA_PATH).join("centrality");
    let page_centrality_path = Path::new(DATA_PATH).join("centrality_page");
    let dual_encoder_path = Path::new(DATA_PATH).join("dual_encoder");
    let dual_encoder_path = if !dual_encoder_path.exists() {
        None
    } else {
        Some(dual_encoder_path)
    };

    let worker = crate::block_on(indexer::IndexingWorker::new(
        IndexerConfig {
            host_centrality_store_path: centrality_path.to_str().unwrap().to_string(),
            page_centrality_store_path: Some(page_centrality_path.to_str().unwrap().to_string()),
            page_webgraph: Some(IndexerGraphConfig::Local {
                path: webgraph_path.to_str().unwrap().to_string(),
            }),
            output_path: out_path.to_str().unwrap().to_string(),
            limit_warc_files: None,
            skip_warc_files: None,
            warc_source: job.source_config.clone(),
            host_centrality_threshold: None,
            safety_classifier_path: None,
            minimum_clean_words: None,
            batch_size: defaults::Indexing::batch_size(),
            autocommit_after_num_inserts: defaults::Indexing::autocommit_after_num_inserts(),
            dual_encoder: dual_encoder_path.map(|p| IndexerDualEncoderConfig {
                model_path: p.to_str().unwrap().to_string(),
                page_centrality_rank_threshold: Some(100_000),
            }),
        }
        .into(),
    ));

    let index = job.process(&worker);
    crate::mv(index.path(), &out_path)?;

    Ok(())
}

fn create_entity_index() -> Result<()> {
    let out_path = Path::new(DATA_PATH).join("entity");
    if out_path.exists() {
        std::fs::remove_dir_all(&out_path)?;
    }

    let wiki_path = Path::new(DATA_PATH).join("test.zim");

    EntityIndexer::run(
        wiki_path.to_str().unwrap().to_string(),
        out_path.to_str().unwrap().to_string(),
    )?;

    Ok(())
}

fn index_files() -> Result<()> {
    create_webgraph()?;
    calculate_centrality();
    create_inverted_index()?;
    create_entity_index()?;
    build_spellchecker()?;

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
