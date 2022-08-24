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
use cuely::entrypoint::{self, frontend};
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
    Frontend {
        index_path: String,
        queries_csv_path: String,
        entity_index_path: Option<String>,
        #[clap(default_value = "0.0.0.0:3000")]
        host: String,
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
    },
    Local {
        config_path: String,
    },
    Entity {
        wikipedia_dump_path: String,
        output_path: String,
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
            } => {
                entrypoint::Indexer::run_worker(address, centrality_store_path)?;
            }
            IndexingOptions::Local { config_path } => {
                let config = load_toml_config(&config_path);
                entrypoint::Indexer::run_locally(&config)?;
            }
            IndexingOptions::Entity {
                wikipedia_dump_path,
                output_path,
            } => entrypoint::EntityIndexer::run(wikipedia_dump_path, output_path)?,
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
        Commands::Frontend {
            index_path,
            queries_csv_path,
            host,
            entity_index_path,
        } => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(frontend::run(
                &index_path,
                &queries_csv_path,
                entity_index_path,
                &host,
            ))?,
    }

    Ok(())
}
