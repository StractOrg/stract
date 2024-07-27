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

pub struct LogGroup {
    base: f64,
    groups: u64,
}

impl LogGroup {
    pub fn new(num_items: u64, groups: u64) -> Self {
        let base = ((num_items as f64).ln() / groups as f64).exp();

        Self { base, groups }
    }

    pub fn group(&self, item: u64) -> u64 {
        (((item + 1) as f64).log(self.base) as u64).min(self.groups - 1)
    }

    pub fn num_groups(&self) -> u64 {
        self.groups
    }
}

pub struct HarmonicRankGroup {
    log_group: LogGroup,
}

impl HarmonicRankGroup {
    pub fn new(num_hosts: u64, groups: u64) -> Self {
        Self {
            log_group: LogGroup::new(num_hosts, groups),
        }
    }

    pub fn group(&self, rank: u64) -> u64 {
        self.log_group.num_groups() - self.log_group.group(rank) - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_group() {
        let lg = LogGroup::new(100, 10);

        assert_eq!(lg.group(0), 0);
        assert_eq!(lg.group(1), 1);
        assert_eq!(lg.group(2), 2);
        assert_eq!(lg.group(10), 5);
        assert_eq!(lg.group(11), 5);
        assert_eq!(lg.group(90), 9);
        assert_eq!(lg.group(99), 9);

        // items over num_items are grouped together with the last group
        assert_eq!(lg.group(100), 9);
        assert_eq!(lg.group(1000), 9);

        let lg = LogGroup::new(40_000_000, 10);

        for i in 0..5 {
            assert_eq!(lg.group(i), 0, "i = {}", i);
        }
    }

    #[test]
    fn test_harmonic_rank_group() {
        let hrg = HarmonicRankGroup::new(100, 10);

        assert_eq!(hrg.group(0), 9);
        assert_eq!(hrg.group(1), 8);
        assert_eq!(hrg.group(10), 4);
        assert_eq!(hrg.group(11), 4);
        assert_eq!(hrg.group(90), 0);
        assert_eq!(hrg.group(99), 0);
        assert_eq!(hrg.group(100), 0);
    }
}
