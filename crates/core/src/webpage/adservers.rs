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

use std::collections::HashSet;

const LIST: &str = include_str!("adservers.txt");
pub static AD_SERVERS: std::sync::LazyLock<AdServers> = std::sync::LazyLock::new(AdServers::new);

pub struct AdServers {
    servers: HashSet<String>,
}

impl AdServers {
    pub fn new() -> Self {
        let servers = LIST
            .lines()
            .filter(|line| !line.starts_with('#'))
            .map(|line| line.trim().to_lowercase())
            .map(|line| line.trim_end_matches(',').to_string())
            .collect();

        Self { servers }
    }

    pub fn is_adserver(&self, host: &str) -> bool {
        self.servers.contains(host)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn adserver() {
        assert!(AD_SERVERS.is_adserver("doubleclick.net"));
        assert!(!AD_SERVERS.is_adserver("google.com"));
    }
}
