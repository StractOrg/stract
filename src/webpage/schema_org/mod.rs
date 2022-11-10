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

use kuchiki::NodeRef;
use serde::{Deserialize, Serialize};

mod json_ld;
mod microdata;

type Text = String;
type Wrapper<T> = Option<OneOrMany<Box<T>>>;

pub fn parse(root: NodeRef) -> Vec<SchemaOrg> {
    let mut res = self::json_ld::parse(root.clone());
    res.append(&mut self::microdata::parse_schema(root));

    res
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

impl From<String> for OneOrMany<String> {
    fn from(value: String) -> Self {
        Self::One(value)
    }
}

/// https://schema.org/Thing
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Thing {
    pub name: Wrapper<Text>,
    pub description: Wrapper<Text>,
    pub disambiguating_description: Wrapper<Text>,
    pub alternate_name: Wrapper<Text>,
    pub additional_type: Wrapper<Text>,
    pub image: Wrapper<Text>,
}

/// https://schema.org/Person
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Person {
    #[serde(flatten)]
    pub thing: Thing,
    pub image: Wrapper<Text>,
    pub name: Wrapper<Text>,
    pub same_as: Wrapper<Text>,
}

/// https://schema.org/Country
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Country {
    #[serde(flatten)]
    pub thing: Thing,
    #[serde(flatten)]
    pub place: Place,
}

/// https://schema.org/Place
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Place {
    #[serde(flatten)]
    pub thing: Thing,
    pub address: Wrapper<PostalAddressOrText>,
    pub telephone: Wrapper<Text>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
#[serde(untagged)]
pub enum CountryOrText {
    Country(Country),
    Text(Text),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
#[serde(untagged)]
pub enum PostalAddressOrText {
    PostalAddress(PostalAddress),
    Text(Text),
}

/// https://schema.org/PostalAddress
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PostalAddress {
    pub address_country: Wrapper<CountryOrText>,
    pub address_locality: Wrapper<Text>,
}
/// https://schema.org/Intangible
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Intangible {
    #[serde(flatten)]
    pub thing: Thing,
}

/// https://schema.org/Organization
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Organization {
    #[serde(flatten)]
    pub thing: Thing,
    pub name: Wrapper<Text>,
    pub legal_name: Wrapper<Text>,
    pub email: Wrapper<Text>,
    pub keywords: Wrapper<Text>,
    pub address: Wrapper<PostalAddressOrText>,
    pub url: Wrapper<Text>,
}

#[non_exhaustive]
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
#[serde(tag = "@type")]
pub enum SchemaOrg {
    ImageObject(ImageObject),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
#[serde(untagged)]
pub enum PersonOrOrganization {
    Person(Box<Person>),
    Name(Text),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ImageObject {
    #[serde(flatten)]
    pub thing: Thing,
    pub author: Wrapper<PersonOrOrganization>,
    pub content_url: Wrapper<Text>,
}

#[cfg(test)]
mod tests {
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

        let parsed: SchemaOrg = serde_json::from_str(json).unwrap();
        assert_eq!(
            parsed,
            SchemaOrg::ImageObject(ImageObject {
                author: Some(OneOrMany::One(Box::new(PersonOrOrganization::Name(
                    "Jane Doe".to_string()
                )))),
                content_url: Some(OneOrMany::One(Box::new("mexico-beach.jpg".to_string()))),
                thing: Thing {
                    name: Some(OneOrMany::One(Box::new("Beach in Mexico".to_string()))),
                    description: Some(OneOrMany::One(Box::new(
                        "I took this picture while on vacation last year.".to_string()
                    ))),
                    ..Default::default()
                }
            }),
        );
    }
}
