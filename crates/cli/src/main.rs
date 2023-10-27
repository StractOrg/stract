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
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde::de::DeserializeOwned;
use std::fs;
use std::path::Path;
use stract_core::entrypoint::autosuggest_scrape::{self, Gl};
use webgraph::WebgraphBuilder;

#[cfg(feature = "dev")]
use stract_core::entrypoint::configure;

use stract_core::entrypoint::indexer::IndexPointer;
use stract_core::entrypoint::{self, api, safety_classifier, search_server, webgraph_server};
use tracing_subscriber::prelude::*;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build an index.
    Indexer {
        #[clap(subcommand)]
        options: IndexingOptions,
    },

    /// Calculate centrality metrics that estimates a websites importance. These metrics are used to rank search results.
    Centrality {
        #[clap(subcommand)]
        mode: CentralityMode,
    },

    /// Parse the DMOZ dataset. DMOZ contains a list of websites and their categories.
    /// It can be used to calculate the topic centrality for websites or augments website descriptions during indexing.
    DmozParser {
        dmoz_file: String,
        output_path: String,
    },

    /// Webgraph specific commands.
    Webgraph {
        #[clap(subcommand)]
        options: WebgraphOptions,
    },

    /// Deploy the search server.
    SearchServer { config_path: String },

    /// Deploy the json http api. The api interacts with
    /// the search servers, webgraph servers etc. to provide the necesarry functionality.
    Api { config_path: String },

    /// Scrape the Google autosuggest API for search queries.
    AutosuggestScrape {
        num_queries: usize,
        gl: Gl,
        ms_sleep_between_req: u64,
        output_dir: String,
    },

    /// Deploy the crawler.
    Crawler {
        #[clap(subcommand)]
        options: Crawler,
    },

    /// Train or run inference on the classifier that predicts if a webpage is NSFW or SFW.
    SafetyClassifier {
        #[clap(subcommand)]
        options: SafetyClassifierOptions,
    },

    /// Setup dev environment.
    #[cfg(feature = "dev")]
    Configure {
        #[clap(long)]
        skip_download: bool,

        #[clap(long)]
        alice: bool,
    },
}

#[derive(Subcommand)]
enum Crawler {
    /// Deploy the crawl worker. The worker is responsible for downloading webpages, saving them to S3,
    /// and sending newly discovered urls back to the crawl coordinator.
    Worker { config_path: String },

    /// Deploy the crawl coordinator. The crawl coordinator is responsible for
    /// distributing crawl jobs to the crawles and deciding which urls to crawl next.
    Coordinator { config_path: String },

    /// Deploy the crawl router. The crawl router is responsible for routing job responses and requests
    /// from the workers to the correct crawl coordinators.
    Router { config_path: String },

    /// Create a crawl plan.
    Plan { config_path: String },
}

/// Commands to train or run inference on the classifier that predicts if a webpage is NSFW or SFW.
#[derive(Subcommand)]
enum SafetyClassifierOptions {
    /// Train the classifier
    Train {
        dataset_path: String,
        output_path: String,
    },

    /// Run a single prediction to test the model
    Predict { model_path: String, text: String },
}

#[derive(Subcommand)]
enum CentralityMode {
    /// Calculate metrics for the host webgraph.
    Host {
        webgraph_path: String,
        output_path: String,
    },
    /// Calculate metrics for the page webgraph.
    Page {
        webgraph_path: String,
        host_centrality_path: String,
        output_path: String,
    },
}

#[derive(Subcommand)]
enum WebgraphOptions {
    /// Create a new webgraph.
    Create { config_path: String },

    /// Merge multiple webgraphs into a single graph.
    Merge {
        #[clap(required = true)]
        paths: Vec<String>,
    },

    /// Deploy the webgraph server. The webgraph server is responsible for serving the webgraph to the search servers.
    /// This is e.g. used to find similar sites etc.
    Server { config_path: String },
}

#[derive(Subcommand)]
enum IndexingOptions {
    /// Create the search index.
    Search { config_path: String },

    /// Create the entity index. Used in the sidebar of the search UI.
    Entity {
        wikipedia_dump_path: String,
        output_path: String,
    },

    /// Merge multiple search indexes into a single index.
    Merge { indexes: Vec<String> },
}

fn load_toml_config<T: DeserializeOwned, P: AsRef<Path>>(path: P) -> T {
    let path = path.as_ref();
    let raw_config = fs::read_to_string(path)
        .with_context(|| format!("Failed to read config: '{}'", path.display()))
        .unwrap();
    toml::from_str(&raw_config)
        .with_context(|| format!("Failed to parse config: '{}'", path.display()))
        .unwrap()
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .without_time()
        .with_target(false)
        .finish()
        .init();

    let args = Args::parse();

    match args.command {
        Commands::Indexer { options } => match options {
            IndexingOptions::Search { config_path } => {
                let config = load_toml_config(config_path);
                entrypoint::Indexer::run(&config)?;
            }
            IndexingOptions::Entity {
                wikipedia_dump_path,
                output_path,
            } => entrypoint::EntityIndexer::run(wikipedia_dump_path, output_path)?,
            IndexingOptions::Merge { indexes } => {
                entrypoint::Indexer::merge(indexes.into_iter().map(IndexPointer::from).collect())?
            }
        },
        Commands::Centrality { mode } => {
            match mode {
                CentralityMode::Host {
                    webgraph_path,
                    output_path,
                } => {
                    entrypoint::Centrality::build_harmonic(&webgraph_path, &output_path);
                    entrypoint::Centrality::build_similarity(&webgraph_path, &output_path);
                }
                CentralityMode::Page {
                    webgraph_path,
                    host_centrality_path,
                    output_path,
                } => entrypoint::Centrality::build_derived_harmonic(
                    webgraph_path,
                    host_centrality_path,
                    output_path,
                )?,
            }
            tracing::info!("Done");
        }
        Commands::Webgraph { options } => match options {
            WebgraphOptions::Create { config_path } => {
                let config = load_toml_config(config_path);
                entrypoint::Webgraph::run(&config)?;
            }
            WebgraphOptions::Merge { mut paths } => {
                let mut webgraph = WebgraphBuilder::new(paths.remove(0)).open();

                for other_path in paths {
                    let other = WebgraphBuilder::new(&other_path).open();
                    webgraph.merge(other);
                    std::fs::remove_dir_all(other_path).unwrap();
                }
            }
            WebgraphOptions::Server { config_path } => {
                let config: stract_config::WebgraphServerConfig = load_toml_config(config_path);

                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(webgraph_server::run(config))?
            }
        },
        Commands::Api { config_path } => {
            let config: stract_config::ApiConfig = load_toml_config(config_path);

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(api::run(config))?
        }
        Commands::SearchServer { config_path } => {
            let config: stract_config::SearchServerConfig = load_toml_config(config_path);

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(search_server::run(config))?
        }
        Commands::AutosuggestScrape {
            num_queries: queries_to_scrape,
            gl,
            ms_sleep_between_req,
            output_dir,
        } => {
            autosuggest_scrape::run(queries_to_scrape, gl, ms_sleep_between_req, output_dir)?;
        }
        #[cfg(feature = "dev")]
        Commands::Configure {
            skip_download,
            alice,
        } => {
            if alice && !skip_download {
                configure::alice()?;
            } else {
                configure::run(skip_download)?;
            }
        }
        Commands::DmozParser {
            dmoz_file,
            output_path,
        } => entrypoint::dmoz_parser::run(dmoz_file, output_path).unwrap(),
        Commands::Crawler { options } => match options {
            Crawler::Worker { config_path } => {
                let config: stract_config::CrawlerConfig = load_toml_config(config_path);

                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(entrypoint::crawler::worker(config))?
            }
            Crawler::Coordinator { config_path } => {
                let config: stract_config::CrawlCoordinatorConfig = load_toml_config(config_path);

                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(entrypoint::crawler::coordinator(config))?
            }
            Crawler::Router { config_path } => {
                let config: stract_config::CrawlRouterConfig = load_toml_config(config_path);

                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(entrypoint::crawler::router(config))?
            }
            Crawler::Plan { config_path } => {
                let config: stract_config::CrawlPlannerConfig = load_toml_config(config_path);

                entrypoint::crawler::planner(config)?;
            }
        },
        Commands::SafetyClassifier { options } => match options {
            SafetyClassifierOptions::Train {
                dataset_path,
                output_path,
            } => safety_classifier::train(dataset_path, output_path)?,
            SafetyClassifierOptions::Predict { model_path, text } => {
                safety_classifier::predict(model_path, &text)?
            }
        },
    }

    Ok(())
}
