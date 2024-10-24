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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

use bloom::fast_stable_hash_128;
use url::Url;
use utoipa::ToSchema;

use crate::{intmap, webpage::url_ext::UrlExt};

#[derive(
    Debug,
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
pub struct NodeID(u64);

impl NodeID {
    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        NodeID(u64::from_be_bytes(bytes))
    }

    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl From<u128> for NodeID {
    fn from(val: u128) -> Self {
        NodeID(val as u64)
    }
}

impl From<u64> for NodeID {
    fn from(val: u64) -> Self {
        NodeID(val)
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct FullNodeID {
    pub host: NodeID,
    pub id: NodeID,
}

impl From<Node> for FullNodeID {
    fn from(value: Node) -> Self {
        let id = value.id();
        let host = value.into_host().id();

        FullNodeID { host, id }
    }
}

impl intmap::Key for NodeID {
    const BIG_PRIME: Self = NodeID(11400714819323198549);

    fn wrapping_mul(self, rhs: Self) -> Self {
        NodeID(self.0.wrapping_mul(rhs.0))
    }

    fn modulus_usize(self, rhs: usize) -> usize {
        (self.0 % (rhs as u64)) as usize
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    name: String,
}

impl Node {
    #[cfg(test)]
    pub fn new_for_test(name: &str) -> Self {
        Node {
            name: name.to_string(),
        }
    }

    pub fn empty() -> Self {
        Node {
            name: String::new(),
        }
    }

    /// Dangerous! No validation is done on the input string.
    pub fn from_str_not_validated(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }

    pub fn into_host(self) -> Node {
        let url = if self.name.contains("://") {
            Url::parse(&self.name)
        } else {
            Url::parse(&("http://".to_string() + self.name.as_str()))
        };

        match url {
            Ok(url) => {
                let host = url.normalized_host().unwrap_or_default().to_string();
                Node { name: host }
            }
            Err(_) => Node {
                name: String::new(),
            },
        }
    }

    pub fn as_str(&self) -> &str {
        self.name.as_str()
    }

    pub fn id(&self) -> NodeID {
        fast_stable_hash_128(self.name.as_bytes()).into()
    }
}

#[cfg(test)]
impl From<String> for Node {
    fn from(name: String) -> Self {
        let url = if name.contains("://") {
            Url::parse(&name).unwrap()
        } else {
            Url::parse(&("http://".to_string() + name.as_str())).unwrap()
        };

        Node::from(&url)
    }
}

impl From<&Url> for Node {
    fn from(url: &Url) -> Self {
        let normalized = normalize_url(url);
        Node { name: normalized }
    }
}

#[cfg(test)]
impl From<&str> for Node {
    fn from(name: &str) -> Self {
        name.to_string().into()
    }
}

impl From<Url> for Node {
    fn from(url: Url) -> Self {
        Self::from(&url)
    }
}

pub fn normalize_url(url: &Url) -> String {
    let mut url = url.clone();
    url.normalize_in_place();

    let scheme = url.scheme();
    let mut normalized = url
        .as_str()
        .strip_prefix(scheme)
        .unwrap_or_default()
        .strip_prefix("://")
        .unwrap_or_default()
        .to_string();

    if let Some(stripped) = normalized.strip_prefix("www.") {
        normalized = stripped.to_string();
    }

    if let Some(prefix) = normalized.strip_suffix('/') {
        normalized = prefix.to_string();
    }

    normalized.to_lowercase()
}
