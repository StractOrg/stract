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
use stdx::enum_map::EnumMap;

use crate::ranking::{signal, Signal};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("leaf not found")]
    LeafNotFound,
    #[error("no features found")]
    NoFeatures,

    #[error("Signal error: {0}")]
    Signal(#[from] signal::Error),

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

#[derive(Debug)]
struct Node {
    threshold: f64,
    feature: Option<Signal>,
    leaf_value: f64,
    left: Option<NodeOrLeaf>,
    right: Option<NodeOrLeaf>,
}

impl Node {
    fn next<V: AsValue>(&self, features: &EnumMap<Signal, V>) -> Option<&NodeOrLeaf> {
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
    fn parse(s: &str) -> Result<Self> {
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

        let start_tree = end_header + 1;
        let end_tree = lines
            .iter()
            .enumerate()
            .skip(start_tree)
            .find(|(_, line)| line.is_empty())
            .map(|(idx, _)| idx)
            .unwrap();

        let tree = &lines[start_tree..end_tree];

        let mut split_features = Vec::new();
        let mut thresholds = Vec::new();
        let mut leaf_values = Vec::new();
        let mut lefts = Vec::new();
        let mut rights = Vec::new();

        for line in tree {
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

    fn predict<V: AsValue>(&self, features: &EnumMap<Signal, V>) -> Result<f64> {
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
    features: Vec<Signal>,
}

impl Header {
    fn parse(s: &str) -> Result<Self> {
        let mut features = Vec::new();

        for lin in s.lines() {
            if let Some((key, value)) = lin.split_once('=') {
                if key == "feature_names" {
                    for name in value.split(' ') {
                        features.push(Signal::from_str(name)?);
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
    tree: Tree,
}

impl LambdaMART {
    pub fn open(path: &Path) -> Result<Self> {
        let s = std::fs::read_to_string(path)?;
        let tree = Tree::parse(&s)?;
        Ok(Self { tree })
    }

    pub fn predict<V: AsValue>(&self, features: &EnumMap<Signal, V>) -> f64 {
        self.tree.predict(features).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "train new model with updated signals"]
    fn simple() {
        let tree = include_str!("../../../testcases/lambdamart.txt");
        let tree = Tree::parse(tree).unwrap();
        assert!(!tree.nodes.is_empty());

        let mut features = EnumMap::new();
        features.insert(Signal::Region, 0.720491639);
        features.insert(Signal::Bm25Url, 63.6011428833);
        features.insert(Signal::Bm25StemmedTitle, 67.2495346069);
        features.insert(Signal::TrackerScore, 0.125);
        features.insert(Signal::Bm25TitleBigrams, 102.8286361694);
        features.insert(Signal::Bm25CleanBodyBigrams, 116.3994293213);
        features.insert(Signal::Bm25CleanBodyTrigrams, 114.053276062);
        features.insert(Signal::CrossEncoder, 0.0017519296);
        features.insert(Signal::Bm25Domain, 89.1253814697);
        features.insert(Signal::Bm25CleanBody, 54.1200180054);
        features.insert(Signal::FetchTimeMs, 0.0018281536);
        features.insert(Signal::Bm25AllBody, 43.6125068665);
        features.insert(Signal::Bm25Site, 87.6877670288);
        features.insert(Signal::Bm25TitleTrigrams, 86.8362731934);
        features.insert(Signal::Bm25Title, 75.2071838379);
        features.insert(Signal::HostCentrality, 0.031143193);
        features.insert(Signal::Bm25StemmedCleanBody, 52.1262779236);

        assert_eq!((tree.predict(&features).unwrap() * 1000.0) as u64, 1016);
    }
}
