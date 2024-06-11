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
use stract::config;
use stract::entrypoint::autosuggest_scrape::{self, Gl};

#[cfg(feature = "dev")]
use stract::entrypoint::configure;

use stract::entrypoint::{
    self, api, entity_search_server, safety_classifier, search_server, webgraph_server,
};
use stract::webgraph::WebgraphBuilder;
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
    SearchServer {
        config_path: String,
    },

    /// Deploy the entity search server.
    EntitySearchServer {
        config_path: String,
    },

    /// Deploy the json http api. The api interacts with
    /// the search servers, webgraph servers etc. to provide the necesarry functionality.
    Api {
        config_path: String,
    },

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
    },

    // Commands for the live index.
    LiveIndex {
        #[clap(subcommand)]
        options: LiveIndex,
    },

    // Build spell correction model.
    WebSpell {
        config_path: String,
    },

    // Commands to compute distributed graph algorithms.
    Ampc {
        #[clap(subcommand)]
        options: AmpcOptions,
    },
}

#[derive(Subcommand)]
enum AmpcOptions {
    /// Start a node for the distributed hash table (DHT).
    Dht { config_path: String },

    /// Start a worker to compute the harmonic centrality of a graph.
    HarmonicWorker { config_path: String },

    /// Start a coordinator to distribute the harmonic centrality computation.
    /// Workers needs to be started before the coordinator.
    HarmonicCoordinator { config_path: String },

    /// Start a worker to compute an approximation of the harmonic centrality of a graph.
    /// The approximation samples O(log n / sample_rate^2) nodes from the graph and computes
    /// shortest paths from the sampled nodes.
    ApproxHarmonicWorker { config_path: String },

    /// Start a coordinator to distribute the approximation of the harmonic centrality computation.
    /// Workers needs to be started before the coordinator.
    ApproxHarmonicCoordinator { config_path: String },
}

#[derive(Subcommand)]
enum LiveIndex {
    /// Create a schedule of which feeds should go to which index.
    Schedule { config_path: String },

    /// Serve the live index.
    Serve { config_path: String },
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

        #[clap(default_value_t = stract::config::defaults::Webgraph::merge_all_segments())]
        merge_all_segments: bool,
    },

    /// Deploy the webgraph server. The webgraph server is responsible for serving the webgraph to the search servers.
    /// This is e.g. used to find similar sites etc.
    Server { config_path: String },
}

#[derive(Subcommand)]
enum IndexingOptions {
    /// Create the search index.
    Search {
        config_path: String,
    },

    /// Merge multiple search indexes into a single index.
    MergeSearch {
        #[clap(required = true)]
        paths: Vec<String>,
    },

    /// Create the entity index. Used in the sidebar of the search UI.
    Entity {
        wikipedia_dump_path: String,
        output_path: String,
    },

    /// Create the feed index. Used to find feeds to put into the live index.
    Feed {
        config_path: String,
    },

    // Create an index of canonical urls.
    Canonical {
        config_path: String,
    },
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
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive("stract=info".parse().unwrap())
                .from_env_lossy(),
        )
        .without_time()
        .with_target(false)
        .finish()
        .init();

    let args = Args::parse();

    match args.command {
        Commands::Indexer { options } => match options {
            IndexingOptions::Search { config_path } => {
                let config = load_toml_config(config_path);
                entrypoint::indexer::run(&config)?;
            }
            IndexingOptions::Entity {
                wikipedia_dump_path,
                output_path,
            } => entrypoint::EntityIndexer::run(wikipedia_dump_path, output_path)?,
            IndexingOptions::Feed { config_path } => {
                let config = load_toml_config(config_path);
                entrypoint::feed_indexer::build(config)?;
            }
            IndexingOptions::MergeSearch { paths } => {
                let pointers = paths
                    .into_iter()
                    .map(entrypoint::indexer::IndexPointer::from)
                    .collect::<Vec<_>>();
                entrypoint::indexer::merge(pointers)?;
            }
            IndexingOptions::Canonical { config_path } => {
                let config: config::CanonicalIndexConfig = load_toml_config(config_path);
                entrypoint::canonical::create(config)?;
            }
        },
        Commands::Centrality { mode } => {
            match mode {
                CentralityMode::Host {
                    webgraph_path,
                    output_path,
                } => {
                    entrypoint::Centrality::build_harmonic(&webgraph_path, &output_path);
                }
                CentralityMode::Page {
                    webgraph_path,
                    output_path,
                } => entrypoint::Centrality::build_approx_harmonic(webgraph_path, output_path)?,
            }
            tracing::info!("Done");
        }
        Commands::Webgraph { options } => match options {
            WebgraphOptions::Create { config_path } => {
                let config = load_toml_config(config_path);
                entrypoint::Webgraph::run(&config)?;
            }
            WebgraphOptions::Merge {
                mut paths,
                merge_all_segments,
            } => {
                let mut webgraph = WebgraphBuilder::new(paths.remove(0))
                    .single_threaded()
                    .open();

                for other_path in paths {
                    let other = WebgraphBuilder::new(&other_path).single_threaded().open();
                    webgraph.merge(other)?;
                }

                if merge_all_segments {
                    webgraph.optimize_read(); // save space in id2node db
                    webgraph.merge_all_segments(Default::default())?;
                }

                webgraph.optimize_read();
            }
            WebgraphOptions::Server { config_path } => {
                let config: config::WebgraphServerConfig = load_toml_config(config_path);

                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(webgraph_server::run(config))?;
            }
        },
        Commands::Api { config_path } => {
            let config: config::ApiConfig = load_toml_config(config_path);

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(api::run(config))?;
        }
        Commands::SearchServer { config_path } => {
            let config: config::SearchServerConfig = load_toml_config(config_path);

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(search_server::run(config))?;
        }
        Commands::EntitySearchServer { config_path } => {
            let config: config::EntitySearchServerConfig = load_toml_config(config_path);

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(entity_search_server::run(config))?;
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
        Commands::Configure { skip_download } => {
            configure::run(skip_download)?;
        }
        Commands::DmozParser {
            dmoz_file,
            output_path,
        } => entrypoint::dmoz_parser::run(dmoz_file, output_path).unwrap(),
        Commands::Crawler { options } => match options {
            Crawler::Worker { config_path } => {
                let config: config::CrawlerConfig = load_toml_config(config_path);

                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(entrypoint::crawler::worker(config))?;
            }
            Crawler::Coordinator { config_path } => {
                let config: config::CrawlCoordinatorConfig = load_toml_config(config_path);

                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(entrypoint::crawler::coordinator(config))?;
            }
            Crawler::Router { config_path } => {
                let config: config::CrawlRouterConfig = load_toml_config(config_path);

                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(entrypoint::crawler::router(config))?;
            }
            Crawler::Plan { config_path } => {
                let config: config::CrawlPlannerConfig = load_toml_config(config_path);

                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(entrypoint::crawler::planner(config))?;
            }
        },
        Commands::SafetyClassifier { options } => match options {
            SafetyClassifierOptions::Train {
                dataset_path,
                output_path,
            } => safety_classifier::train(dataset_path, output_path)?,
            SafetyClassifierOptions::Predict { model_path, text } => {
                safety_classifier::predict(model_path, &text)?;
            }
        },
        Commands::LiveIndex { options } => match options {
            LiveIndex::Schedule { config_path } => {
                let config = load_toml_config(config_path);
                entrypoint::live_index::schedule(config)?;
            }
            LiveIndex::Serve { config_path } => {
                let config = load_toml_config(config_path);

                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(entrypoint::live_index::serve(config))?;
            }
        },
        Commands::WebSpell { config_path } => {
            let config: config::WebSpellConfig = load_toml_config(config_path);
            entrypoint::web_spell::run(config)?;
        }
        Commands::Ampc { options } => match options {
            AmpcOptions::Dht { config_path } => {
                let config: config::DhtConfig = load_toml_config(config_path);

                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(entrypoint::ampc::dht::run(config))?;
            }
            AmpcOptions::HarmonicWorker { config_path } => {
                let config: config::HarmonicWorkerConfig = load_toml_config(config_path);
                entrypoint::ampc::harmonic_centrality::worker::run(config)?;
            }
            AmpcOptions::HarmonicCoordinator { config_path } => {
                let config: config::HarmonicCoordinatorConfig = load_toml_config(config_path);
                entrypoint::ampc::harmonic_centrality::coordinator::run(config)?;
            }

            AmpcOptions::ApproxHarmonicWorker { config_path } => {
                let config: config::ApproxHarmonicWorkerConfig = load_toml_config(config_path);
                entrypoint::ampc::approximated_harmonic_centrality::worker::run(config)?;
            }
            AmpcOptions::ApproxHarmonicCoordinator { config_path } => {
                let config: config::ApproxHarmonicCoordinatorConfig = load_toml_config(config_path);
                entrypoint::ampc::approximated_harmonic_centrality::coordinator::run(config)?;
            }
        },
    }

    Ok(())
}
