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

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use crate::{distributed::sonic, entrypoint::api, generic_query, Result};

const CONFIG_FOLDER: &str = "~/.config/stract";
const CONFIG_NAME: &str = "admin.toml";

trait ExpandUser {
    fn expand_user(&self) -> PathBuf;
}

impl ExpandUser for Path {
    fn expand_user(&self) -> PathBuf {
        let mut path = self.to_path_buf();
        if path.starts_with("~") {
            if let Some(home) = dirs::home_dir() {
                path = home.join(path.strip_prefix("~").unwrap());
            }
        }

        path
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Config {
    pub host: SocketAddr,
}

impl Config {
    pub fn save(&self) -> Result<()> {
        let path = Path::new(CONFIG_FOLDER).expand_user();

        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }

        let path = path.join(CONFIG_NAME);

        let config = toml::to_string(&self).unwrap();
        std::fs::write(path, config)?;

        Ok(())
    }

    pub fn load() -> Result<Self> {
        let path = Path::new(CONFIG_FOLDER).expand_user().join(CONFIG_NAME);

        let config = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&config)?;

        Ok(config)
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        self.save().ok();
    }
}

pub fn init(host: SocketAddr) -> Result<()> {
    let config = Config { host };
    config.save()?;

    Ok(())
}

pub async fn status() -> Result<()> {
    let config = Config::load()?;
    let mut conn = sonic::service::Connection::create(config.host).await?;

    let status = conn.send_without_timeout(api::ClusterStatus).await?;

    println!("Members:");
    for member in status.members {
        println!("  - {}: {}", member.id, member.service);
    }

    Ok(())
}

pub async fn top_keyphrases(top: usize) -> Result<()> {
    let config = Config::load()?;
    let mut conn = sonic::service::Connection::create(config.host).await?;

    let keyphrases = conn
        .send_without_timeout(api::TopKeyphrases { top })
        .await?;

    println!("id,text,score");
    for (i, keyphrase) in keyphrases.iter().enumerate() {
        println!("{},{},{}", i + 1, keyphrase.text(), keyphrase.score());
    }

    Ok(())
}

pub async fn index_size() -> Result<()> {
    let config = Config::load()?;
    let mut conn: sonic::service::Connection<api::ManagementService> =
        sonic::service::Connection::create(config.host).await?;

    let size: generic_query::size::SizeResponse = conn.send_without_timeout(api::Size).await?;

    println!("Number of pages in index: {}", size.pages);

    Ok(())
}
