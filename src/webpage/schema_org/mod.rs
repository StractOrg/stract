// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use std::collections::HashMap;

use kuchiki::NodeRef;
use serde::{Deserialize, Serialize};

mod json_ld;
mod microdata;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Property {
    String(String),
    Item(Item),
}

impl Property {
    pub fn try_into_string(self) -> Option<String> {
        if let Property::String(s) = self {
            Some(s)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Item {
    #[serde(rename = "@type")]
    pub itemtype: Option<OneOrMany<String>>,
    #[serde(flatten)]
    pub properties: HashMap<String, OneOrMany<Property>>,
}
impl Item {
    pub fn types_contains(&self, itemtype: &str) -> bool {
        match &self.itemtype {
            Some(tt) => match tt {
                OneOrMany::One(this_type) => itemtype == this_type,
                OneOrMany::Many(itemtypes) => itemtypes.iter().any(|t| t == itemtype),
            },
            None => false,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
#[serde(untagged)]
pub enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    pub fn one(self) -> Option<T> {
        match self {
            OneOrMany::One(one) => Some(one),
            OneOrMany::Many(many) => many.into_iter().next(),
        }
    }

    pub fn many(self) -> Vec<T> {
        match self {
            OneOrMany::One(one) => vec![one],
            OneOrMany::Many(many) => many,
        }
    }
}

pub fn parse(root: NodeRef) -> Vec<Item> {
    let mut res = self::json_ld::parse(root.clone());
    res.append(&mut self::microdata::parse_schema(root));

    res
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;

    use super::*;

    #[test]
    fn image_object_example() {
        // example taken from https://schema.org/ImageObject
        let json = r#"
        {
            "@context": "https://schema.org",
            "@type": "ImageObject",
            "author": "Jane Doe",
            "contentLocation": "Puerto Vallarta, Mexico",
            "contentUrl": "mexico-beach.jpg",
            "datePublished": "2008-01-25",
            "description": "I took this picture while on vacation last year.",
            "name": "Beach in Mexico"
          }
        "#;

        let item: Item = serde_json::from_str(json).unwrap();
        assert_eq!(
            item,
            Item {
                itemtype: Some(OneOrMany::One("ImageObject".to_string())),
                properties: hashmap! {
                    "@context".to_string() => OneOrMany::One(Property::String("https://schema.org".to_string())),
                    "author".to_string() => OneOrMany::One(Property::String("Jane Doe".to_string())),
                    "contentLocation".to_string() => OneOrMany::One(Property::String("Puerto Vallarta, Mexico".to_string())),
                    "contentUrl".to_string() => OneOrMany::One(Property::String("mexico-beach.jpg".to_string())),
                    "datePublished".to_string() => OneOrMany::One(Property::String("2008-01-25".to_string())),
                    "description".to_string() => OneOrMany::One(Property::String("I took this picture while on vacation last year.".to_string())),
                    "name".to_string() => OneOrMany::One(Property::String("Beach in Mexico".to_string())),
                }
            }
        );
    }
}
