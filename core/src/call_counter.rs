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
use std::collections::VecDeque;
use std::time::{Duration, Instant};

pub struct CallCounter {
    call_times: VecDeque<Instant>,
    window: Duration,
}

impl CallCounter {
    // Constructs a new `CallCounter` with a specified window duration for the rolling average.
    pub fn new(window: Duration) -> CallCounter {
        CallCounter {
            call_times: VecDeque::new(),
            window,
        }
    }

    // Count the method call.
    pub fn count(&mut self) {
        let now = Instant::now();
        self.call_times.push_back(now);
    }

    fn prune(&mut self) {
        let now = Instant::now();

        while let Some(&old) = self.call_times.front() {
            if now.duration_since(old) < self.window {
                break;
            }

            self.call_times.pop_front();
        }
    }

    // Calculate the average calls per second.
    pub fn avg_per_second(&mut self) -> f64 {
        self.prune();
        self.call_times.len() as f64 / self.window.as_secs_f64()
    }
}
