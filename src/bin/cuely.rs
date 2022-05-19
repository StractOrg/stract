use cuely::{Config, Indexer, Mode};
use std::fs;

#[tokio::main]
async fn main() {
    let raw_config =
        fs::read_to_string("configs/indexer.toml").expect("Failed to read config file");

    let config: Config = toml::from_str(&raw_config).expect("Failed to parse config");

    match config.mode {
        Mode::Indexer => {
            Indexer::from_config(config)
                .run()
                .await
                .expect("Failed to index documents");
        }
    }
}
