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

use std::collections::HashMap;

use kuchiki::NodeRef;

use crate::tokenizer::FlattenedJson;
use crate::{OneOrMany, Result};

mod json_ld;
mod microdata;

/// All itemtypes will be prefixed with TYPE_PREFIX
/// in the flattened json. This allows us to make sure that
/// the matching during search starts at an itemtype.
pub const TYPE_PREFIX: char = '$';

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub enum Property {
    String(String),
    Item(Item),
}
impl Property {
    pub(crate) fn try_into_string(&self) -> Option<String> {
        match self {
            Property::String(s) => Some(s.clone()),
            Property::Item(_) => None,
        }
    }

    pub(crate) fn try_into_item(&self) -> Option<Item> {
        match self {
            Property::String(_) => None,
            Property::Item(it) => Some(it.clone()),
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
#[serde(untagged)]
enum FlattenedJsonMap {
    Leaf(String),
    Node(HashMap<RawOneOrMany<String>, HashMap<String, RawOneOrMany<FlattenedJsonMap>>>),
}

impl From<Property> for FlattenedJsonMap {
    fn from(value: Property) -> Self {
        match value {
            Property::String(s) => FlattenedJsonMap::Leaf(s),
            Property::Item(item) => {
                let mut res = HashMap::new();

                if let Some(tt) = item.itemtype {
                    let recursive = item
                        .properties
                        .into_iter()
                        .map(|(key, val)| {
                            (key.chars().skip_while(|c| *c == TYPE_PREFIX).collect(), val)
                        })
                        .map(|(key, val)| match val {
                            OneOrMany::One(one) => {
                                (key, RawOneOrMany::One(FlattenedJsonMap::from(one)))
                            }
                            OneOrMany::Many(many) => (
                                key,
                                RawOneOrMany::Many(
                                    many.into_iter().map(FlattenedJsonMap::from).collect(),
                                ),
                            ),
                        })
                        .collect();

                    match tt {
                        OneOrMany::One(one) => {
                            let one: String =
                                one.chars().skip_while(|c| *c == TYPE_PREFIX).collect();
                            let one = TYPE_PREFIX.to_string() + one.as_str();
                            res.insert(RawOneOrMany::One(one), recursive)
                        }

                        OneOrMany::Many(many) => {
                            let many = many
                                .into_iter()
                                .map(|s| {
                                    s.chars()
                                        .skip_while(|c| *c == TYPE_PREFIX)
                                        .collect::<String>()
                                })
                                .map(|s| TYPE_PREFIX.to_string() + s.as_str())
                                .collect();

                            res.insert(RawOneOrMany::Many(many), recursive)
                        }
                    };
                }

                FlattenedJsonMap::Node(res)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct RawItem {
    #[serde(rename = "@type")]
    itemtype: Option<RawOneOrMany<String>>,
    #[serde(flatten)]
    properties: HashMap<String, RawOneOrMany<RawProperty>>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(untagged)]
enum RawProperty {
    String(String),
    Item(RawItem),
}
impl RawProperty {
    #[cfg(test)]
    fn try_into_item(&self) -> Option<RawItem> {
        match self {
            RawProperty::String(_) => None,
            RawProperty::Item(it) => Some(it.clone()),
        }
    }

    #[cfg(test)]
    fn try_into_string(&self) -> Option<String> {
        match self {
            RawProperty::String(s) => Some(s.clone()),
            RawProperty::Item(_) => None,
        }
    }
}

impl From<RawProperty> for Property {
    fn from(value: RawProperty) -> Self {
        match value {
            RawProperty::String(s) => Self::String(s),
            RawProperty::Item(it) => Self::Item(Item::from(it)),
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Item {
    pub itemtype: Option<OneOrMany<String>>,
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

    fn into_flattened_json_map(self) -> FlattenedJsonMap {
        FlattenedJsonMap::from(Property::Item(self))
    }
}

impl From<RawItem> for Item {
    fn from(value: RawItem) -> Self {
        Self {
            itemtype: value.itemtype.map(|tt| match tt {
                RawOneOrMany::One(one) => OneOrMany::One(one),
                RawOneOrMany::Many(many) => OneOrMany::Many(many),
            }),
            properties: value
                .properties
                .into_iter()
                .map(|(key, val)| match val {
                    RawOneOrMany::One(prop) => (key, OneOrMany::One(Property::from(prop))),
                    RawOneOrMany::Many(props) => (
                        key,
                        OneOrMany::Many(props.into_iter().map(Property::from).collect()),
                    ),
                })
                .collect(),
        }
    }
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    PartialEq,
    Eq,
    Debug,
    Clone,
    Hash,
)]
#[serde(untagged)]
enum RawOneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> RawOneOrMany<T> {
    #[cfg(test)]
    pub fn one(self) -> Option<T> {
        match self {
            RawOneOrMany::One(one) => Some(one),
            RawOneOrMany::Many(many) => many.into_iter().next(),
        }
    }

    #[cfg(test)]
    pub fn many(self) -> Vec<T> {
        match self {
            RawOneOrMany::One(one) => vec![one],
            RawOneOrMany::Many(many) => many,
        }
    }
}

pub fn parse(root: NodeRef) -> Vec<Item> {
    let mut res = self::json_ld::parse(root.clone());
    res.append(&mut self::microdata::parse_schema(root));

    res.into_iter().map(Item::from).collect()
}

pub(crate) fn flattened_json(schemas: Vec<Item>) -> Result<FlattenedJson> {
    let single_maps: Vec<_> = schemas
        .into_iter()
        .map(|item| item.into_flattened_json_map())
        .collect();
    FlattenedJson::new(&single_maps)
}

#[cfg(test)]
mod tests {
    use kuchiki::traits::TendrilSink;
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

        let item: RawItem = serde_json::from_str(json).unwrap();
        assert_eq!(
            item,
            RawItem {
                itemtype: Some(RawOneOrMany::One("ImageObject".to_string())),
                properties: hashmap! {
                    "@context".to_string() => RawOneOrMany::One(RawProperty::String("https://schema.org".to_string())),
                    "author".to_string() => RawOneOrMany::One(RawProperty::String("Jane Doe".to_string())),
                    "contentLocation".to_string() => RawOneOrMany::One(RawProperty::String("Puerto Vallarta, Mexico".to_string())),
                    "contentUrl".to_string() => RawOneOrMany::One(RawProperty::String("mexico-beach.jpg".to_string())),
                    "datePublished".to_string() => RawOneOrMany::One(RawProperty::String("2008-01-25".to_string())),
                    "description".to_string() => RawOneOrMany::One(RawProperty::String("I took this picture while on vacation last year.".to_string())),
                    "name".to_string() => RawOneOrMany::One(RawProperty::String("Beach in Mexico".to_string())),
                }
            }
        );
    }

    #[test]
    fn stackoverflow_question() {
        let html = include_str!("../../../testcases/schema_org/stackoverflow.html");
        let root = kuchiki::parse_html().one(html);
        let res = microdata::parse_schema(root);

        assert_eq!(res.len(), 1);

        assert!(res[0].properties.contains_key("image"));
        assert!(res[0].properties.contains_key("primaryImageOfPage"));
        assert_eq!(
            res[0].properties.get("name"),
            Some(&RawOneOrMany::One(RawProperty::String(
                "RegEx match open tags except XHTML self-contained tags".to_string()
            )))
        );

        let main = res[0]
            .properties
            .get("mainEntity")
            .unwrap()
            .clone()
            .one()
            .unwrap()
            .try_into_item()
            .unwrap();

        assert_eq!(
            main.itemtype,
            Some(RawOneOrMany::One("Question".to_string()))
        );
        assert_eq!(
            main.properties.get("name"),
            Some(&RawOneOrMany::One(RawProperty::String(
                "RegEx match open tags except XHTML self-contained tags".to_string()
            )))
        );
        assert_eq!(
            main.properties.get("dateCreated"),
            Some(&RawOneOrMany::One(RawProperty::String(
                "2009-11-13T22:38:26".to_string()
            )))
        );

        assert!(
            main.properties
                .get("suggestedAnswer")
                .unwrap()
                .clone()
                .many()
                .len()
                > 10
        );

        assert!(main.properties.contains_key("acceptedAnswer"));

        let text = main.properties.get("text").unwrap().clone().many();

        assert_eq!(text[0], RawProperty::String("Locked . Comments on this question have been disabled, but it is still accepting new answers and other interactions. Learn more .\nI need to match all of these opening tags:\n".to_string()));

        assert_eq!(
            text[1],
            RawProperty::Item(RawItem {
                itemtype: Some(RawOneOrMany::One("SoftwareSourceCode".to_string())),
                properties: hashmap! {
                    "text".to_string() => RawOneOrMany::One(RawProperty::String("<p>\n<a href=\"foo\">\n".to_string()))
                }
            })
        );
    }

    #[test]
    fn stackoverflow_question_with_code() {
        let html = include_str!("../../../testcases/schema_org/stackoverflow_with_code.html");
        let root = kuchiki::parse_html().one(html);
        let res = microdata::parse_schema(root);

        assert_eq!(res.len(), 1);

        assert!(res[0].properties.contains_key("image"));
        assert!(res[0].properties.contains_key("primaryImageOfPage"));
        assert_eq!(
            res[0].properties.get("name"),
            Some(&RawOneOrMany::One(RawProperty::String(
                "Almacenar y comparar valor de atributo de un objeto en javascript".to_string()
            )))
        );

        let main = res[0]
            .properties
            .get("mainEntity")
            .unwrap()
            .clone()
            .one()
            .unwrap()
            .try_into_item()
            .unwrap();

        assert_eq!(
            main.itemtype,
            Some(RawOneOrMany::One("Question".to_string()))
        );
        assert_eq!(
            main.properties.get("name"),
            Some(&RawOneOrMany::One(RawProperty::String(
                "Almacenar y comparar valor de atributo de un objeto en javascript".to_string()
            )))
        );
        assert_eq!(
            main.properties.get("dateCreated"),
            Some(&RawOneOrMany::One(RawProperty::String(
                "2018-05-10T10:17:26".to_string()
            )))
        );

        assert!(main.properties.contains_key("acceptedAnswer"));

        let text = main.properties.get("text").unwrap().clone().many();

        assert!(text[0].try_into_string().is_some());

        let answer = main
            .properties
            .get("acceptedAnswer")
            .unwrap()
            .clone()
            .one()
            .unwrap()
            .try_into_item()
            .unwrap();
        let parts = answer.properties.get("text").cloned().unwrap().many();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].try_into_string().unwrap(), "En pulsador_cerrado tienes una definición de clase que luego tienes que instanciar según lo que he visto en la documentación. Entiendo que lo que quieres hacer quedaría de la siguiente forma:\n".to_string());
        assert_eq!(
            parts[1]
                .try_into_item()
                .unwrap()
                .itemtype
                .unwrap()
                .one()
                .unwrap(),
            "SoftwareSourceCode".to_string()
        );
        assert!(
            parts[1]
                .try_into_item()
                .unwrap()
                .properties
                .get("text")
                .cloned()
                .unwrap()
                .one()
                .unwrap()
                .try_into_string()
                .unwrap()
                .len()
                > 100
        );
        assert_eq!(
            parts[2].try_into_string().unwrap(),
            "Espero que te funcione".to_string()
        );
    }

    #[test]
    fn recipe() {
        let html = include_str!("../../../testcases/schema_org/recipe.html");
        let root = kuchiki::parse_html().one(html);
        let res = microdata::parse_schema(root);

        assert!(res.len() > 20);

        let recipe = res
            .into_iter()
            .find(|item| {
                if let Some(itemtype) = &item.itemtype {
                    itemtype.clone().one().unwrap() == "Recipe"
                } else {
                    false
                }
            })
            .unwrap();

        assert_eq!(
            recipe.properties.get("recipeCategory"),
            Some(&RawOneOrMany::One(RawProperty::String(
                "Aftensmad".to_string()
            )))
        );

        assert_eq!(
            recipe.properties.get("name"),
            Some(&RawOneOrMany::One(RawProperty::String(
                "One Pot Pasta med chorizo".to_string()
            )))
        );

        assert_eq!(
            recipe.properties.get("recipeYield"),
            Some(&RawOneOrMany::One(RawProperty::String("4".to_string())))
        );

        assert_eq!(
            recipe.properties.get("cookTime"),
            Some(&RawOneOrMany::One(RawProperty::String("PT25M".to_string())))
        );

        assert_eq!(
            recipe.properties.get("recipeIngredient"),
            Some(&RawOneOrMany::Many(vec![
                RawProperty::String("400 g spaghetti".to_string()),
                RawProperty::String("1 dåse hakkede tomater".to_string()),
                RawProperty::String("1 håndfuld frisk basilikum, bladene herfra".to_string()),
                RawProperty::String("1 løg, finthakket".to_string()),
                RawProperty::String("2 fed hvidløg, finthakket".to_string()),
                RawProperty::String("20 cherrytomater, skåret i både".to_string()),
                RawProperty::String("0,50 squash, groftrevet".to_string()),
                RawProperty::String("1 tsk oregano, tørret".to_string()),
                RawProperty::String("50 g chorizo, finthakket".to_string()),
                RawProperty::String("5 dl grøntsagsbouillon".to_string()),
                RawProperty::String("2 spsk olivenolie".to_string()),
                RawProperty::String("1 tsk chiliflager, kan undlades".to_string()),
                RawProperty::String("1 tsk salt".to_string()),
                RawProperty::String("sort peber, friskkværnet".to_string()),
                RawProperty::String("50 g parmesan, friskrevet til servering".to_string()),
                RawProperty::String("1 håndfuld frisk basilikum, blade".to_string()),
            ]))
        );

        let desc = recipe
            .properties
            .get("description")
            .unwrap()
            .clone()
            .one()
            .unwrap()
            .try_into_string()
            .unwrap();

        assert!(!desc.contains("PT25MPT10M4"));

        let instructions = recipe
            .properties
            .get("recipeInstructions")
            .unwrap()
            .clone()
            .one()
            .unwrap()
            .try_into_string()
            .unwrap();

        assert_eq!(&instructions, "Helt enkelt som navnet antyder, så kom alle ingredienserne i en stor gryde på én gang. Kog retten op, rør godt rundt i gryden og skru ned for varmen. Lad det simrekoge under låg i 10-12 minutter, til spaghettien er perfekt kogt – al dente med lidt bid i. Server med revet parmesan og basilikum.");
    }
}
