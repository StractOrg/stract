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

use crate::{log_group::HarmonicRankGroup, webgraph::Edge};

/// Number of groups to divide the backlinks into.
/// If this is changed, also change the grouped backlink fields in the schema.
const NUM_GROUPS: u64 = 10;

#[derive(Debug)]
pub struct Group {
    group: u64,
    backlinks: Vec<Edge<String>>,
}

impl Group {
    fn new(group: u64) -> Self {
        Self {
            group,
            backlinks: Vec::new(),
        }
    }

    fn insert(&mut self, backlink: Edge<String>) {
        self.backlinks.push(backlink);
    }

    pub fn group(&self) -> u64 {
        self.group
    }

    pub fn backlinks(&self) -> &[Edge<String>] {
        &self.backlinks
    }
}

#[derive(Debug)]
pub struct GroupedBacklinks {
    groups: Vec<Group>,
}

impl GroupedBacklinks {
    pub fn empty() -> Self {
        let groups = (0..NUM_GROUPS).map(Group::new).collect();
        Self { groups }
    }

    pub fn all(&self) -> &[Group] {
        &self.groups
    }

    pub fn get(&self, group: u64) -> Option<&Group> {
        self.groups.get(group as usize)
    }

    fn add(&mut self, group: u64, backlink: Edge<String>) {
        if let Some(group) = self.groups.get_mut(group as usize) {
            group.insert(backlink)
        }
    }
}

impl Default for GroupedBacklinks {
    fn default() -> Self {
        Self::empty()
    }
}

/// Groups backlinks by their harmonic centrality rank.
pub struct BacklinkGrouper {
    grouper: HarmonicRankGroup,
    groups: GroupedBacklinks,
}

impl BacklinkGrouper {
    pub fn new(num_hosts: u64) -> Self {
        Self {
            grouper: HarmonicRankGroup::new(num_hosts, NUM_GROUPS),
            groups: GroupedBacklinks::empty(),
        }
    }

    pub fn add(&mut self, backlink: Edge<String>) {
        let rank = backlink.from.host_rank();

        let group = self.grouper.group(rank);
        self.groups.add(group, backlink);
    }

    pub fn groups(self) -> GroupedBacklinks {
        self.groups
    }
}

#[cfg(test)]
mod tests {
    use crate::webgraph::NodeDatum;

    use super::*;

    #[test]
    fn test_grouped_backlinks() {
        let mut grouper = BacklinkGrouper::new(10);
        grouper.add(Edge {
            from: NodeDatum::new(0u64, 0u64),
            to: NodeDatum::new(1u64, 0u64),
            label: String::new(),
            rel: Default::default(),
        });
        grouper.add(Edge {
            from: NodeDatum::new(0u64, 1u64),
            to: NodeDatum::new(1u64, 0u64),
            label: String::new(),
            rel: Default::default(),
        });
        grouper.add(Edge {
            from: NodeDatum::new(0u64, 2u64),
            to: NodeDatum::new(1u64, 0u64),
            label: String::new(),
            rel: Default::default(),
        });
        grouper.add(Edge {
            from: NodeDatum::new(0u64, 3u64),
            to: NodeDatum::new(1u64, 0u64),
            label: String::new(),
            rel: Default::default(),
        });
        grouper.add(Edge {
            from: NodeDatum::new(0u64, 4u64),
            to: NodeDatum::new(1u64, 0u64),
            label: String::new(),
            rel: Default::default(),
        });
        grouper.add(Edge {
            from: NodeDatum::new(0u64, 5u64),
            to: NodeDatum::new(1u64, 0u64),
            label: String::new(),
            rel: Default::default(),
        });
        grouper.add(Edge {
            from: NodeDatum::new(0u64, 6u64),
            to: NodeDatum::new(1u64, 0u64),
            label: String::new(),
            rel: Default::default(),
        });
        grouper.add(Edge {
            from: NodeDatum::new(0u64, 7u64),
            to: NodeDatum::new(1u64, 0u64),
            label: String::new(),
            rel: Default::default(),
        });
        grouper.add(Edge {
            from: NodeDatum::new(0u64, 8u64),
            to: NodeDatum::new(1u64, 0u64),
            label: String::new(),
            rel: Default::default(),
        });
        grouper.add(Edge {
            from: NodeDatum::new(0u64, 9u64),
            to: NodeDatum::new(1u64, 0u64),
            label: String::new(),
            rel: Default::default(),
        });

        {
            let groups = grouper.groups();
            assert_eq!(groups.all().len(), 10);
            assert_eq!(
                groups
                    .all()
                    .iter()
                    .map(|g| g.backlinks().len())
                    .sum::<usize>(),
                10
            );

            for (i, group) in groups.all().iter().enumerate() {
                assert_eq!(group.group(), i as u64);
            }
        }
    }
}
