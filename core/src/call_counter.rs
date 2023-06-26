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

#[derive(Debug, Clone, Copy)]
struct WeightedInstant {
    weight: usize,
    instant: Instant,
}

pub struct CallCounter {
    call_times: VecDeque<WeightedInstant>,
    window: Duration,
    cur_weights: usize,
}

impl CallCounter {
    // Constructs a new `CallCounter` with a specified window duration for the rolling average.
    pub fn new(window: Duration) -> CallCounter {
        CallCounter {
            call_times: VecDeque::new(),
            window,
            cur_weights: 0,
        }
    }

    pub fn count_with_weight(&mut self, weight: usize) {
        let now = Instant::now();
        self.call_times.push_back(WeightedInstant {
            weight,
            instant: now,
        });
        self.cur_weights += weight;
    }

    fn prune(&mut self) {
        let now = Instant::now();

        while let Some(&old) = self.call_times.front() {
            if now.duration_since(old.instant) < self.window {
                break;
            }

            self.cur_weights -= old.weight;
            self.call_times.pop_front();
        }
    }

    // Calculate the average calls per second.
    pub fn avg_per_second(&mut self) -> f64 {
        self.prune();
        self.cur_weights as f64 / self.window.as_secs_f64()
    }
}
