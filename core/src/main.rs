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
use anyhow::Result;
use clap::{Parser, Subcommand};
use serde::de::DeserializeOwned;
use std::fs;
use std::path::Path;
use stract::entrypoint::autosuggest_scrape::{self, Gl};
#[cfg(feature = "dev")]
use stract::entrypoint::configure;
use stract::entrypoint::indexer::IndexPointer;
use stract::entrypoint::{self, frontend, search_server, webgraph_server};
use stract::webgraph::WebgraphBuilder;
use stract::{
    AliceLocalConfig, CrawlCoordinatorConfig, CrawlerConfig, FrontendConfig, SearchServerConfig,
    WebgraphServerConfig,
};
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
    /// Commands to deploy the Alice server.
    Alice {
        #[clap(subcommand)]
        options: AliceOptions,
    },

    /// Build the search index.
    Indexer {
        #[clap(subcommand)]
        options: IndexingOptions,
    },

    /// Calculate centrality metrics that estimates a websites importance. These metrics are used to rank search results.
    Centrality {
        #[clap(subcommand)]
        mode: CentralityType,
        webgraph_path: String,
        output_path: String,
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

    /// Deploy the frontend. The frontend is a web server that serves the search UI and interacts with
    /// the search servers, webgraph servers etc. to provide the necesarry functionality.
    Frontend { config_path: String },

    /// Scrape the Google autosuggest API for search queries.
    AutosuggestScrape {
        num_queries: usize,
        gl: Gl,
        ms_sleep_between_req: u64,
        output_dir: String,
    },

    /// Calculate the topic centrality for websites. We use the DMOZ dataset to
    /// determine set of representative websites for each topic. Harmonic centrality approximations
    /// are then calculated for each website in the webgraph.
    ///
    /// This is currently not used in the search engine, so everything related to this might be buggy.
    TopicCentrality {
        index_path: String,
        topics_path: String,
        webgraph_path: String,
        online_harmonic_path: String,
        output_path: String,
    },

    /// Deploy the crawl coordinator. The crawl coordinator is responsible for
    /// distributing crawl jobs to the crawles and deciding which urls to crawl next.
    CrawlCoordinator { config_path: String },

    /// Deploy the crawler. The crawler is responsible for downloading webpages, saving them to S3,
    /// and sending newly discovered urls back to the crawl coordinator.
    Crawler { config_path: String },

    /// Setup dev environment.
    #[cfg(feature = "dev")]
    Configure {
        #[clap(long, takes_value = false)]
        skip_download: bool,

        #[clap(long, takes_value = false)]
        alice: bool,
    },
}

/// Commands to deploy Alice.
#[derive(Subcommand)]
enum AliceOptions {
    /// Deploy Alice server.
    Serve { config_path: String },
    /// Generate a new keypair for Alice to sign states.
    GenerateKey,
}

#[derive(Subcommand)]
enum CentralityType {
    All,
    Harmonic,
    OnlineHarmonic,
    Similarity,
}

#[derive(Subcommand)]
enum WebgraphOptions {
    Master {
        config_path: String,
    },
    Worker {
        address: String,
    },
    Local {
        config_path: String,
    },
    Merge {
        #[clap(required = true)]
        paths: Vec<String>,
        #[clap(long)]
        num_segments: Option<usize>,
    },
    Server {
        config_path: String,
    },
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
        topics_path: Option<String>,
    },
    Local {
        config_path: String,
    },
    Entity {
        wikipedia_dump_path: String,
        output_path: String,
    },
    Merge {
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
                let config = load_toml_config(config_path);
                entrypoint::Indexer::run_master(&config)?;
            }
            IndexingOptions::Worker {
                address,
                centrality_store_path,
                webgraph_path,
                topics_path,
            } => {
                entrypoint::Indexer::run_worker(
                    address,
                    centrality_store_path,
                    webgraph_path,
                    topics_path,
                )?;
            }
            IndexingOptions::Local { config_path } => {
                let config = load_toml_config(config_path);
                entrypoint::Indexer::run_locally(&config)?;
            }
            IndexingOptions::Entity {
                wikipedia_dump_path,
                output_path,
            } => entrypoint::EntityIndexer::run(wikipedia_dump_path, output_path)?,
            IndexingOptions::Merge { indexes } => {
                entrypoint::Indexer::merge(indexes.into_iter().map(IndexPointer::from).collect())?
            }
        },
        Commands::Centrality {
            mode,
            webgraph_path,
            output_path,
        } => match mode {
            CentralityType::Harmonic => {
                entrypoint::Centrality::build_harmonic(webgraph_path, output_path)
            }
            CentralityType::OnlineHarmonic => {
                entrypoint::Centrality::build_online(webgraph_path, output_path)
            }
            CentralityType::Similarity => {
                entrypoint::Centrality::build_similarity(webgraph_path, output_path)
            }
            CentralityType::All => {
                entrypoint::Centrality::build_harmonic(&webgraph_path, &output_path);
                entrypoint::Centrality::build_online(&webgraph_path, &output_path);
                entrypoint::Centrality::build_similarity(&webgraph_path, &output_path);
            }
        },
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
            WebgraphOptions::Merge {
                mut paths,
                num_segments,
            } => {
                let mut webgraph = WebgraphBuilder::new(paths.remove(0)).open();

                for other_path in paths {
                    let other = WebgraphBuilder::new(&other_path).open();
                    webgraph.merge(other);
                    std::fs::remove_dir_all(other_path).unwrap();
                }

                if let Some(num_segments) = num_segments {
                    webgraph.merge_segments(num_segments)
                }
            }
            WebgraphOptions::Server { config_path } => {
                let config: WebgraphServerConfig = load_toml_config(config_path);

                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(webgraph_server::run(config))?
            }
        },
        Commands::Frontend { config_path } => {
            let config: FrontendConfig = load_toml_config(config_path);

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(frontend::run(config))?
        }
        Commands::SearchServer { config_path } => {
            let config: SearchServerConfig = load_toml_config(config_path);

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
        Commands::TopicCentrality {
            index_path,
            topics_path,
            webgraph_path,
            online_harmonic_path,
            output_path,
        } => entrypoint::topic_centrality::run(
            index_path,
            topics_path,
            webgraph_path,
            online_harmonic_path,
            output_path,
        ),
        Commands::Alice { options } => match options {
            AliceOptions::Serve { config_path } => {
                let config: AliceLocalConfig = load_toml_config(config_path);

                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(entrypoint::alice::run(config))?
            }
            AliceOptions::GenerateKey => entrypoint::alice::generate_key(),
        },
        Commands::CrawlCoordinator { config_path } => {
            let config: CrawlCoordinatorConfig = load_toml_config(config_path);

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(entrypoint::crawler::coordinator(config))?
        }
        Commands::Crawler { config_path } => {
            let config: CrawlerConfig = load_toml_config(config_path);

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(entrypoint::crawler::worker(config))?
        }
    }

    Ok(())
}
