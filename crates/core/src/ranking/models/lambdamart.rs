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

use std::{path::Path, str::FromStr};

use core::SignalEnumDiscriminants;

use crate::{
    enum_map::EnumMap,
    ranking::{core, SignalCalculation, SignalEnum},
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("leaf not found")]
    LeafNotFound,
    #[error("no features found")]
    NoFeatures,

    #[error("couldn't find end of trees")]
    NoEndOfTrees,

    #[error("Signal error: {0}")]
    Signal(#[from] core::Error),

    #[error("ParseInt error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("ParseFloat error: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone, Debug)]
enum NodeOrLeaf {
    Node(usize),
    Leaf(usize),
}

pub trait AsValue {
    fn as_value(&self) -> f64;
}

impl AsValue for f64 {
    fn as_value(&self) -> f64 {
        *self
    }
}

impl AsValue for SignalCalculation {
    fn as_value(&self) -> f64 {
        self.score
    }
}

#[derive(Debug)]
struct Node {
    threshold: f64,
    feature: Option<SignalEnum>,
    leaf_value: f64,
    left: Option<NodeOrLeaf>,
    right: Option<NodeOrLeaf>,
}

impl Node {
    fn next<V: AsValue>(&self, features: &EnumMap<SignalEnum, V>) -> Option<&NodeOrLeaf> {
        self.feature.and_then(|feature| {
            let value = features.get(feature).map(|v| v.as_value()).unwrap_or(0.0);
            if value <= self.threshold {
                self.left.as_ref()
            } else {
                self.right.as_ref()
            }
        })
    }
}

struct Tree {
    nodes: Vec<Node>,
}

impl Tree {
    fn parse(s: &str, header: &Header) -> Result<Self> {
        let mut split_features = Vec::new();
        let mut thresholds = Vec::new();
        let mut leaf_values = Vec::new();
        let mut lefts = Vec::new();
        let mut rights = Vec::new();

        for line in s.lines() {
            if let Some((key, value)) = line.split_once('=') {
                match key {
                    "split_feature" => {
                        for name in value.split(' ') {
                            let idx: usize = name.parse()?;
                            split_features.push(header.features[idx]);
                        }
                    }
                    "threshold" => {
                        for thresh in value.split(' ') {
                            let thresh: f64 = thresh.parse()?;
                            thresholds.push(thresh);
                        }
                    }
                    "leaf_value" => {
                        for value in value.split(' ') {
                            let value: f64 = value.parse()?;
                            leaf_values.push(value);
                        }
                    }
                    "left_child" => {
                        for left in value.split(' ') {
                            let left: i32 = left.parse()?;
                            if left.is_negative() {
                                // equivalent to ~ operator in python/C
                                lefts.push(NodeOrLeaf::Leaf(left.unsigned_abs() as usize - 1));
                            } else {
                                lefts.push(NodeOrLeaf::Node(left as usize));
                            }
                        }
                    }
                    "right_child" => {
                        for right in value.split(' ') {
                            let right: i32 = right.parse()?;
                            if right.is_negative() {
                                // equivalent to ~ operator in python/C
                                rights.push(NodeOrLeaf::Leaf(right.unsigned_abs() as usize - 1));
                            } else {
                                rights.push(NodeOrLeaf::Node(right as usize));
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        let mut nodes = Vec::new();

        let mut offset = None;
        for leaf_value in &leaf_values {
            offset = match offset {
                Some(cur) => {
                    if cur < *leaf_value {
                        Some(cur)
                    } else {
                        Some(*leaf_value)
                    }
                }
                None => Some(*leaf_value),
            }
        }
        offset = offset.map(|offset| offset.abs() + 1.0);

        for leaf_value in leaf_values {
            let offest = offset.unwrap();

            nodes.push(Node {
                threshold: 0.0,
                feature: None,
                leaf_value: leaf_value + offest,
                left: None,
                right: None,
            });
        }

        for (idx, feature) in split_features.iter().enumerate() {
            nodes[idx].feature = Some(*feature);
        }

        for (idx, threshold) in thresholds.iter().enumerate() {
            nodes[idx].threshold = *threshold;
        }

        for (idx, left) in lefts.iter().enumerate() {
            nodes[idx].left = Some(left.clone());
        }

        for (idx, right) in rights.iter().enumerate() {
            nodes[idx].right = Some(right.clone());
        }

        Ok(Self { nodes })
    }

    fn predict<V: AsValue>(&self, features: &EnumMap<SignalEnum, V>) -> Result<f64> {
        let mut node = &self.nodes[0];
        while let Some(next) = node.next(features) {
            node = match next {
                NodeOrLeaf::Node(index) => &self.nodes[*index],
                NodeOrLeaf::Leaf(index) => return Ok(self.nodes[*index].leaf_value),
            };
        }

        Err(Error::LeafNotFound)
    }
}

struct Header {
    features: Vec<SignalEnum>,
}

impl Header {
    fn parse(s: &str) -> Result<Self> {
        let mut features = Vec::new();

        for lin in s.lines() {
            if let Some((key, value)) = lin.split_once('=') {
                if key == "feature_names" {
                    for name in value.split(' ') {
                        features.push(SignalEnum::from(SignalEnumDiscriminants::from_str(name)?));
                    }
                }
            }
        }

        if features.is_empty() {
            return Err(Error::NoFeatures);
        }

        Ok(Self { features })
    }
}

pub struct LambdaMART {
    trees: Vec<Tree>,
}

impl LambdaMART {
    pub fn parse(s: &str) -> Result<Self> {
        let lines: Vec<_> = s.lines().map(|s| s.to_string()).collect();
        let end_header = lines
            .iter()
            .enumerate()
            .find(|(_, line)| line.is_empty())
            .map(|(idx, _)| idx)
            .unwrap();

        let header: String = itertools::intersperse(
            lines[..end_header].iter().map(|s| s.to_string()),
            "\n".to_string(),
        )
        .collect();
        let header = Header::parse(&header)?;

        let start_trees = end_header + 1;
        let end_trees = lines
            .iter()
            .enumerate()
            .find(|(_, line)| line.trim() == "end of trees")
            .map(|(idx, _)| idx)
            .ok_or(Error::NoEndOfTrees)?;

        // chunk lines by empty lines
        let mut trees = Vec::new();

        let mut start_tree = start_trees;

        while start_tree < end_trees {
            let end_tree = lines
                .iter()
                .enumerate()
                .skip(start_tree)
                .find(|(_, line)| line.is_empty())
                .map(|(idx, _)| idx)
                .unwrap();

            let tree = &lines[start_tree..end_tree];
            let tree = Tree::parse(&tree.join("\n"), &header)?;
            trees.push(tree);

            start_tree = end_tree + 2;
        }

        Ok(Self { trees })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let s = std::fs::read_to_string(path)?;

        Self::parse(&s)
    }

    pub fn predict<V: AsValue>(&self, features: &EnumMap<SignalEnum, V>) -> f64 {
        self.trees
            .iter()
            .map(|t| t.predict(features).unwrap())
            .sum::<f64>()
            / (self.trees.len() as f64)
    }
}

#[cfg(test)]
mod tests {
    use crate::ranking;

    use super::*;

    #[test]
    fn simple() {
        let model = include_str!("../../../testcases/lambdamart.txt");
        let model = LambdaMART::parse(model).unwrap();
        assert!(!model.trees.is_empty());

        let mut features = EnumMap::new();
        features.insert(ranking::core::Bm25BacklinkText.into(), 85.7750244140625);
        features.insert(ranking::core::Bm25CleanBody.into(), 67.41311645507812);
        features.insert(ranking::core::Bm25CleanBodyBigrams.into(), 0.0);
        features.insert(ranking::core::Bm25CleanBodyTrigrams.into(), 0.0);
        features.insert(ranking::core::IdfSumDomain.into(), 43.332096099853516);
        features.insert(ranking::core::IdfSumDomainIfHomepage.into(), 0.0);
        features.insert(ranking::core::IdfSumDomainIfHomepageNoTokenizer.into(), 0.0);
        features.insert(
            ranking::core::IdfSumDomainNameIfHomepageNoTokenizer.into(),
            0.0,
        );
        features.insert(ranking::core::IdfSumDomainNameNoTokenizer.into(), 0.0);
        features.insert(ranking::core::IdfSumDomainNoTokenizer.into(), 0.0);
        features.insert(ranking::core::IdfSumSite.into(), 61.47410202026367);
        features.insert(ranking::core::IdfSumSiteNoTokenizer.into(), 0.0);
        features.insert(
            ranking::core::Bm25StemmedCleanBody.into(),
            65.94627380371094,
        );
        features.insert(ranking::core::Bm25StemmedTitle.into(), 0.0);
        features.insert(ranking::core::Bm25Title.into(), 59.817813873291016);
        features.insert(ranking::core::Bm25TitleBigrams.into(), 0.0);
        features.insert(ranking::core::IdfSumTitleIfHomepage.into(), 0.0);
        features.insert(ranking::core::Bm25TitleTrigrams.into(), 0.0);
        features.insert(ranking::core::IdfSumUrl.into(), 57.07925033569336);
        features.insert(ranking::core::FetchTimeMs.into(), 0.023255813953488372);
        features.insert(ranking::core::HostCentrality.into(), 0.017958538);
        features.insert(ranking::core::InboundSimilarity.into(), 0.0);
        features.insert(ranking::core::IsHomepage.into(), 0.0);
        features.insert(ranking::core::PageCentrality.into(), 0.008253236);
        features.insert(ranking::core::Region.into(), 0.16622349570454012);
        features.insert(ranking::core::TrackerScore.into(), 0.07692307692307693);
        features.insert(ranking::core::UpdateTimestamp.into(), 0.0);
        features.insert(ranking::core::UrlDigits.into(), 0.25);
        features.insert(ranking::core::UrlSlashes.into(), 0.3333333333333333);

        assert_eq!((model.predict(&features) * 1000.0) as u64, 1050);
    }
}
