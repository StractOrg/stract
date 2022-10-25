// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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
use anyhow::Result;
use clap::{Parser, Subcommand};
use cuely::entrypoint::indexer::IndexPointer;
use cuely::entrypoint::{self, frontend, search_server};
use cuely::{FrontendConfig, SearchServerConfig};
use serde::de::DeserializeOwned;
use std::fs;
use std::path::Path;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Indexer {
        #[clap(subcommand)]
        options: IndexingOptions,
    },
    Centrality {
        webgraph_path: String,
        output_path: String,
    },
    Webgraph {
        #[clap(subcommand)]
        options: WebgraphOptions,
    },
    SearchServer {
        config_path: String,
    },
    Frontend {
        config_path: String,
    },
}

#[derive(Subcommand)]
enum WebgraphOptions {
    Master { config_path: String },
    Worker { address: String },
    Local { config_path: String },
}

#[derive(Subcommand)]
enum IndexingOptions {
    Master {
        config_path: String,
    },
    Worker {
        address: String,
        centrality_store_path: String,
        webgraph_path: Option<String>,
    },
    Local {
        config_path: String,
    },
    Entity {
        wikipedia_dump_path: String,
        output_path: String,
    },
    Merge {
        num_segments: u32,
        indexes: Vec<String>,
    },
}

fn load_toml_config<T: DeserializeOwned, P: AsRef<Path>>(path: P) -> T {
    let raw_config = fs::read_to_string(path).expect("Failed to read config file");
    toml::from_str(&raw_config).expect("Failed to parse config")
}

fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let args = Args::parse();

    match args.command {
        Commands::Indexer { options } => match options {
            IndexingOptions::Master { config_path } => {
                let config = load_toml_config(&config_path);
                entrypoint::Indexer::run_master(&config)?;
            }
            IndexingOptions::Worker {
                address,
                centrality_store_path,
                webgraph_path,
            } => {
                entrypoint::Indexer::run_worker(address, centrality_store_path, webgraph_path)?;
            }
            IndexingOptions::Local { config_path } => {
                let config = load_toml_config(&config_path);
                entrypoint::Indexer::run_locally(&config)?;
            }
            IndexingOptions::Entity {
                wikipedia_dump_path,
                output_path,
            } => entrypoint::EntityIndexer::run(wikipedia_dump_path, output_path)?,
            IndexingOptions::Merge {
                num_segments,
                indexes,
            } => entrypoint::Indexer::merge(
                indexes.into_iter().map(IndexPointer::from).collect(),
                num_segments,
            )?,
        },
        Commands::Centrality {
            webgraph_path,
            output_path,
        } => entrypoint::Centrality::run(webgraph_path, output_path),
        Commands::Webgraph { options } => match options {
            WebgraphOptions::Master { config_path } => {
                let config = load_toml_config(config_path);
                entrypoint::Webgraph::run_master(&config)?;
            }
            WebgraphOptions::Worker { address } => {
                entrypoint::Webgraph::run_worker(address)?;
            }
            WebgraphOptions::Local { config_path } => {
                let config = load_toml_config(config_path);
                entrypoint::Webgraph::run_locally(&config)?;
            }
        },
        Commands::Frontend { config_path } => {
            let config: FrontendConfig = load_toml_config(&config_path);

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(frontend::run(
                    &config.queries_csv_path,
                    &config.host,
                    config.search_servers,
                ))?
        }
        Commands::SearchServer { config_path } => {
            let config: SearchServerConfig = load_toml_config(&config_path);

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(search_server::run(config))?
        }
    }

    Ok(())
}
