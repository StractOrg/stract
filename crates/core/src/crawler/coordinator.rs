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

use super::{file_queue::FileQueue, Job, Result};
use std::{path::Path, sync::Mutex};

pub struct CrawlCoordinator {
    jobs: Mutex<FileQueue<Job>>,
}

impl CrawlCoordinator {
    pub fn new<P: AsRef<Path>>(jobs_queue: P) -> Result<Self> {
        Ok(Self {
            jobs: Mutex::new(FileQueue::open(jobs_queue)?),
        })
    }

    pub fn sample_job(&self) -> Result<Option<Job>> {
        self.jobs.lock().unwrap_or_else(|e| e.into_inner()).pop()
    }
}
