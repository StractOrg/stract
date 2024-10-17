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

use crate::{
    log_group::HarmonicRankGroup,
    webgraph::{Edge, SmallEdgeWithLabel},
};

/// Number of groups to divide the backlinks into.
/// If this is changed, also change the grouped backlink fields in the schema.
const NUM_GROUPS: u64 = 10;

#[derive(Debug)]
pub struct Group {
    group: u64,
    backlinks: Vec<SmallEdgeWithLabel>,
}

impl Group {
    fn new(group: u64) -> Self {
        Self {
            group,
            backlinks: Vec::new(),
        }
    }

    fn insert(&mut self, backlink: SmallEdgeWithLabel) {
        self.backlinks.push(backlink);
    }

    pub fn group(&self) -> u64 {
        self.group
    }

    pub fn backlinks(&self) -> &[SmallEdgeWithLabel] {
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

    fn add(&mut self, group: u64, backlink: SmallEdgeWithLabel) {
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

    pub fn add(&mut self, backlink: SmallEdgeWithLabel, host_rank: u64) {
        let group = self.grouper.group(host_rank);
        self.groups.add(group, backlink);
    }

    pub fn groups(self) -> GroupedBacklinks {
        self.groups
    }
}

#[cfg(test)]
mod tests {
    use crate::webgraph::NodeID;

    use super::*;

    #[test]
    fn test_grouped_backlinks() {
        let mut grouper = BacklinkGrouper::new(10);
        grouper.add(
            SmallEdgeWithLabel {
                from: NodeID::from(10u64),
                to: NodeID::from(1u64),
                label: String::new(),
                rel_flags: Default::default(),
            },
            0,
        );
        grouper.add(
            SmallEdgeWithLabel {
                from: NodeID::from(9u64),
                to: NodeID::from(1u64),
                label: String::new(),
                rel_flags: Default::default(),
            },
            1,
        );
        grouper.add(
            SmallEdgeWithLabel {
                from: NodeID::from(8u64),
                to: NodeID::from(1u64),
                label: String::new(),
                rel_flags: Default::default(),
            },
            2,
        );
        grouper.add(
            SmallEdgeWithLabel {
                from: NodeID::from(7u64),
                to: NodeID::from(1u64),
                label: String::new(),
                rel_flags: Default::default(),
            },
            3,
        );
        grouper.add(
            SmallEdgeWithLabel {
                from: NodeID::from(6u64),
                to: NodeID::from(1u64),
                label: String::new(),
                rel_flags: Default::default(),
            },
            4,
        );
        grouper.add(
            SmallEdgeWithLabel {
                from: NodeID::from(5u64),
                to: NodeID::from(1u64),
                label: String::new(),
                rel_flags: Default::default(),
            },
            5,
        );
        grouper.add(
            SmallEdgeWithLabel {
                from: NodeID::from(4u64),
                to: NodeID::from(1u64),
                label: String::new(),
                rel_flags: Default::default(),
            },
            6,
        );
        grouper.add(
            SmallEdgeWithLabel {
                from: NodeID::from(3u64),
                to: NodeID::from(1u64),
                label: String::new(),
                rel_flags: Default::default(),
            },
            7,
        );
        grouper.add(
            SmallEdgeWithLabel {
                from: NodeID::from(2u64),
                to: NodeID::from(1u64),
                label: String::new(),
                rel_flags: Default::default(),
            },
            8,
        );
        grouper.add(
            SmallEdgeWithLabel {
                from: NodeID::from(1u64),
                to: NodeID::from(1u64),
                label: String::new(),
                rel_flags: Default::default(),
            },
            9,
        );

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
