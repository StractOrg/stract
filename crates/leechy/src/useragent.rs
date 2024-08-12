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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

use rand::prelude::*;
use std::sync::LazyLock;

static USER_AGENTS: LazyLock<Vec<(usize, String)>> = LazyLock::new(|| {
    include_str!("useragents.txt")
        .lines()
        .map(|line| line.trim().to_string())
        .enumerate()
        .collect()
});

pub struct UserAgent(String);

impl UserAgent {
    pub fn random_weighted() -> Self {
        let mut rng = thread_rng();

        UserAgent(
            USER_AGENTS
                .choose_weighted(&mut rng, |(rank, _)| 1 / (rank + 1))
                .unwrap()
                .1
                .clone(),
        )
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for UserAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for UserAgent {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
