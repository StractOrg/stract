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
use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    time::{Duration, SystemTime},
};

pub struct TTLCache<K, V> {
    ttl: Duration,
    data: HashMap<K, V>,
    insertion_order: VecDeque<K>,
    insertion_times: HashMap<K, SystemTime>,
    max_size: usize,
}

impl<K: Hash + Eq + Clone, V> TTLCache<K, V> {
    pub fn with_ttl_and_max_size(ttl: Duration, max_size: usize) -> Self {
        Self {
            ttl,
            data: HashMap::new(),
            insertion_order: VecDeque::new(),
            insertion_times: HashMap::new(),
            max_size,
        }
    }

    pub fn insert(&mut self, key: K, val: V) {
        self.prune_old_entries();
        let current_time = SystemTime::now();

        if self.data.insert(key.clone(), val).is_some() {
            let (idx, _) = self
                .insertion_order
                .iter()
                .enumerate()
                .find(|(_, k)| *k == &key)
                .unwrap();

            self.insertion_order.remove(idx);
        }

        self.insertion_times.insert(key.clone(), current_time);
        self.insertion_order.push_back(key);
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let current_time = SystemTime::now();

        self.insertion_times.get(key).and_then(|insertion_time| {
            if current_time.duration_since(*insertion_time).unwrap() < self.ttl {
                self.data.get(key)
            } else {
                None
            }
        })
    }

    fn prune_old_entries(&mut self) {
        let current_time = SystemTime::now();

        while self.data.len() >= self.max_size {
            let front = self.insertion_order.pop_front().unwrap();
            self.insertion_times.remove(&front);
            self.data.remove(&front);
        }

        while let Some(front) = self.insertion_order.front() {
            if current_time
                .duration_since(*self.insertion_times.get(front).unwrap())
                .unwrap()
                <= self.ttl
            {
                break;
            }

            let front = self.insertion_order.pop_front().unwrap();
            self.insertion_times.remove(&front);
            self.data.remove(&front);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let mut cache = TTLCache::with_ttl_and_max_size(Duration::from_millis(50), 5);

        cache.insert(0, 0);
        std::thread::sleep(Duration::from_millis(30));
        cache.insert(1, 1);

        assert_eq!(cache.get(&0), Some(&0));
        assert_eq!(cache.get(&1), Some(&1));

        std::thread::sleep(Duration::from_millis(30));

        assert_eq!(cache.get(&0), None);
        assert_eq!(cache.get(&1), Some(&1));

        std::thread::sleep(Duration::from_millis(30));

        assert_eq!(cache.get(&0), None);
        assert_eq!(cache.get(&1), None);

        cache.insert(2, 2);

        assert_eq!(cache.data.len(), 1);
        assert_eq!(cache.insertion_order.len(), 1);
        assert_eq!(cache.insertion_times.len(), 1);
    }

    #[test]
    fn max_size() {
        let mut cache = TTLCache::with_ttl_and_max_size(Duration::from_millis(50), 1);

        cache.insert(0, 0);
        cache.insert(1, 1);

        assert_eq!(cache.get(&0), None);
        assert_eq!(cache.get(&1), Some(&1));
    }
}
