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
use std::time::Duration;

pub use self::index::LiveIndex;
pub use self::index_manager::IndexManager;

pub mod crawler;
pub mod index;
mod index_manager;

pub use self::crawler::Crawler;

const TTL: Duration = Duration::from_secs(60 * 60 * 24 * 60); // 60 days
const PRUNE_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60); // 6 hours
const COMPACT_INTERVAL: Duration = Duration::from_secs(60 * 60); // 1 hours
const AUTO_COMMIT_INTERVAL: Duration = Duration::from_secs(10 * 60); // 10 minutes
const EVENT_LOOP_INTERVAL: Duration = Duration::from_secs(5);
const BATCH_SIZE: usize = 512;
