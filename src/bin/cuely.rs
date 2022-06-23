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
use clap::Parser;
use cuely::entrypoint::{CentralityEntrypoint, Indexer, WebgraphEntrypoint};
use cuely::Config;
use std::fs;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
struct Args {
    config_file: String,
    mapreduce_worker_addr: Option<String>,
}

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let args = Args::parse();

    let raw_config = fs::read_to_string(args.config_file).expect("Failed to read config file");

    let config: Config = toml::from_str(&raw_config).expect("Failed to parse config");

    match config {
        Config::Indexer(config) => {
            Indexer::new(config, args.mapreduce_worker_addr)
                .run()
                .expect("Failed to index documents");
        }
        Config::Webgraph(config) => WebgraphEntrypoint::new(config, args.mapreduce_worker_addr)
            .run()
            .expect("Failed to build webgraph"),

        Config::Centrality(config) => CentralityEntrypoint::from(config).run(),
    }
}
