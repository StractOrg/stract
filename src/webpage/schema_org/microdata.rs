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

//! Spec compliant microdata parser: https://html.spec.whatwg.org/multipage/microdata.htm

use kuchiki::NodeRef;
use std::collections::HashMap;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("Html node was expected to have an itemprop attribute, but did not have one.")]
    ExpectedItemProp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Property {
    String(String),
    Item(Item),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Item {
    itemtype: Option<Vec<String>>,
    properties: HashMap<String, Vec<Property>>,
}

/// implementation of https://html.spec.whatwg.org/multipage/microdata.html#associating-names-with-items
/// TODO: handle itemrefs
fn parse_item(root: NodeRef) -> Result<Item> {
    debug_assert!(root
        .as_element()
        .unwrap()
        .attributes
        .borrow()
        .contains("itemscope"));

    let itemtype = root
        .as_element()
        .unwrap()
        .attributes
        .borrow()
        .get("itemtype")
        .map(|s| s.split_ascii_whitespace().map(String::from).collect());

    let mut properties: HashMap<String, Vec<Property>> = HashMap::new();
    let mut pending: Vec<_> = root.children().collect();

    while let Some(current) = pending.pop() {
        if let Some(elem) = current.clone().as_element() {
            if !elem.attributes.borrow().contains("itemscope") {
                pending.extend(current.children());
            }

            if elem.attributes.borrow().contains("itemprop") {
                let property = if elem.attributes.borrow().contains("itemscope") {
                    Property::Item(parse_item(current)?)
                } else {
                    match elem.name.local.to_string().as_str() {
                        "meta" => Property::String(
                            elem.attributes
                                .borrow()
                                .get("content")
                                .map(String::from)
                                .unwrap_or_default(),
                        ),
                        "audio" | "embed" | "iframe" | "img" | "source" | "track" | "video" => {
                            Property::String(
                                elem.attributes
                                    .borrow()
                                    .get("src")
                                    .map(String::from)
                                    .unwrap_or_default(),
                            )
                        }
                        "a" | "area" | "link" => Property::String(
                            elem.attributes
                                .borrow()
                                .get("href")
                                .map(String::from)
                                .unwrap_or_default(),
                        ),
                        "object" => Property::String(
                            elem.attributes
                                .borrow()
                                .get("data")
                                .map(String::from)
                                .unwrap_or_default(),
                        ),
                        "data" | "meter" => Property::String(
                            elem.attributes
                                .borrow()
                                .get("value")
                                .map(String::from)
                                .unwrap_or_default(),
                        ),
                        "time" => Property::String(
                            elem.attributes
                                .borrow()
                                .get("datetime")
                                .map(String::from)
                                .unwrap_or_else(|| current.text_contents()),
                        ),
                        _ => Property::String(current.text_contents()),
                    }
                };

                properties
                    .entry(
                        elem.attributes
                            .borrow()
                            .get("itemprop")
                            .ok_or(Error::ExpectedItemProp)?
                            .to_string(),
                    )
                    .or_default()
                    .push(property);
            }
        }
    }

    Ok(Item {
        itemtype,
        properties,
    })
}

struct Parser {}

#[cfg(test)]
mod tests {
    use kuchiki::traits::TendrilSink;
    use maplit::hashmap;

    use super::*;

    #[test]
    fn single_simple_item() {
        let root = kuchiki::parse_html()
            .one(
                r#"
  <figure itemscope itemtype="http://n.whatwg.org/work">
   <img itemprop="work" src="images/house.jpeg" alt="A white house, boarded up, sits in a forest.">
   <figcaption itemprop="title">The <span>house</span> I found.</figcaption>
  </figure>
        "#,
            )
            .select_first("figure")
            .unwrap()
            .as_node()
            .clone();

        assert_eq!(
            parse_item(root),
            Ok(Item {
                itemtype: Some(vec![String::from("http://n.whatwg.org/work")]),
                properties: hashmap! {
                    "work".to_string() => vec![Property::String("images/house.jpeg".to_string())],
                    "title".to_string() => vec![Property::String("The house I found.".to_string())],
                }
            })
        );
    }

    #[test]
    fn single_complex_item() {
        let root = kuchiki::parse_html()
            .one(
                r##"
<article itemscope itemtype="http://schema.org/BlogPosting">
 <section>
  <h1>Comments</h1>
  <article itemprop="comment" itemscope itemtype="http://schema.org/UserComments" id="c1">
   <link itemprop="url" href="#c1">
   <footer>
    <p>Posted by: <span itemprop="creator" itemscope itemtype="http://schema.org/Person">
     <span itemprop="name">Greg</span>
    </span></p>
    <p><time itemprop="commentTime" datetime="2013-08-29">15 minutes ago</time></p>
   </footer>
   <p>Ha!</p>
  </article>
 </section>
</article>
        "##,
            )
            .select_first("article")
            .unwrap()
            .as_node()
            .clone();

        let expected = Ok(Item {
            itemtype: Some(vec![String::from("http://schema.org/BlogPosting")]),
            properties: hashmap! {
                "comment".to_string() => vec![
                    Property::Item(
                        Item {
                            itemtype: Some(vec!["http://schema.org/UserComments".to_string()]),
                            properties: hashmap! {
                                "url".to_string() => vec![Property::String("#c1".to_string())],
                                "creator".to_string() =>  vec![
                                    Property::Item(Item {
                                        itemtype: Some(vec!["http://schema.org/Person".to_string()]),
                                        properties: hashmap! {
                                            "name".to_string() => vec![Property::String("Greg".to_string())]
                                        }
                                    })],
                                "commentTime".to_string() => vec![Property::String("2013-08-29".to_string())]
                            }
                })],
            },
        });

        assert_eq!(parse_item(root), expected);
    }
}
