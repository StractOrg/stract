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

    pub fn try_into_item(self) -> Option<Item> {
        if let Property::Item(item) = self {
            Some(item)
        } else {
            None
        }
    }
}

pub type SingleMap = HashMap<OneOrMany<String>, HashMap<String, OneOrMany<Property>>>;

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

    pub fn into_single_map(self) -> Option<SingleMap> {
        self.itemtype.map(|tt| {
            let mut res = HashMap::new();

            let properties = self
                .properties
                .into_iter()
                .filter_map(|(key, prop)| {
                    let new_prop = match prop {
                        OneOrMany::One(one) => match one {
                            Property::String(s) => Some(vec![Property::String(s)]),
                            Property::Item(item) => item.into_single_map().map(|item| {
                                let mut items = Vec::new();

                                for (tt, item) in item.into_iter() {
                                    res.entry(tt).or_insert_with(HashMap::new);
                                    if !item.is_empty() {
                                        items.push(Property::Item(Item {
                                            properties: item,
                                            itemtype: None,
                                        }));
                                    }
                                }

                                items
                            }),
                        },
                        OneOrMany::Many(many) => {
                            if many.is_empty() {
                                None
                            } else {
                                Some(
                                    many.into_iter()
                                        .filter_map(|prop| match prop {
                                            Property::String(s) => Some(vec![Property::String(s)]),
                                            Property::Item(item) => {
                                                item.into_single_map().map(|item| {
                                                    let mut items = Vec::new();

                                                    for (tt, item) in item.into_iter() {
                                                        res.entry(tt).or_insert_with(HashMap::new);
                                                        if !item.is_empty() {
                                                            items.push(Property::Item(Item {
                                                                properties: item,
                                                                itemtype: None,
                                                            }));
                                                        }
                                                    }

                                                    items
                                                })
                                            }
                                        })
                                        .flatten()
                                        .collect(),
                                )
                            }
                        }
                    };

                    new_prop.map(|mut new_prop| {
                        if new_prop.len() == 1 {
                            (key, OneOrMany::One(new_prop.pop().unwrap()))
                        } else {
                            (key, OneOrMany::Many(new_prop))
                        }
                    })
                })
                .collect();

            res.insert(tt, properties);

            res
        })
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Hash)]
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
            Some(&OneOrMany::One(Property::String(
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

        assert_eq!(main.itemtype, Some(OneOrMany::One("Question".to_string())));
        assert_eq!(
            main.properties.get("name"),
            Some(&OneOrMany::One(Property::String(
                "RegEx match open tags except XHTML self-contained tags".to_string()
            )))
        );
        assert_eq!(
            main.properties.get("dateCreated"),
            Some(&OneOrMany::One(Property::String(
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

        assert_eq!(text[0], Property::String("Locked . Comments on this question have been disabled, but it is still accepting new answers and other interactions. Learn more .\nI need to match all of these opening tags:\n".to_string()));

        assert_eq!(
            text[1],
            Property::Item(Item {
                itemtype: Some(OneOrMany::One("SourceCode".to_string())),
                properties: hashmap! {
                    "text".to_string() => OneOrMany::One(Property::String("<p>\n<a href=\"foo\">\n".to_string()))
                }
            })
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
            Some(&OneOrMany::One(Property::String("Aftensmad".to_string())))
        );

        assert_eq!(
            recipe.properties.get("name"),
            Some(&OneOrMany::One(Property::String(
                "One Pot Pasta med chorizo".to_string()
            )))
        );

        assert_eq!(
            recipe.properties.get("recipeYield"),
            Some(&OneOrMany::One(Property::String("4".to_string())))
        );

        assert_eq!(
            recipe.properties.get("cookTime"),
            Some(&OneOrMany::One(Property::String("PT25M".to_string())))
        );

        assert_eq!(
            recipe.properties.get("recipeIngredient"),
            Some(&OneOrMany::Many(vec![
                Property::String("400 g spaghetti".to_string()),
                Property::String("1 dåse hakkede tomater".to_string()),
                Property::String("1 håndfuld frisk basilikum, bladene herfra".to_string()),
                Property::String("1 løg, finthakket".to_string()),
                Property::String("2 fed hvidløg, finthakket".to_string()),
                Property::String("20 cherrytomater, skåret i både".to_string()),
                Property::String("0,50 squash, groftrevet".to_string()),
                Property::String("1 tsk oregano, tørret".to_string()),
                Property::String("50 g chorizo, finthakket".to_string()),
                Property::String("5 dl grøntsagsbouillon".to_string()),
                Property::String("2 spsk olivenolie".to_string()),
                Property::String("1 tsk chiliflager, kan undlades".to_string()),
                Property::String("1 tsk salt".to_string()),
                Property::String("sort peber, friskkværnet".to_string()),
                Property::String("50 g parmesan, friskrevet til servering".to_string()),
                Property::String("1 håndfuld frisk basilikum, blade".to_string()),
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
