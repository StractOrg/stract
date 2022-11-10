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

//! Almost spec compliant microdata parser: https://html.spec.whatwg.org/multipage/microdata.htm

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use kuchiki::NodeRef;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

use crate::webpage::Url;

use super::{OneOrMany, SchemaOrg};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Html node was expected to have an itemscope attribute, but did not have one.")]
    ExpectedItemScope,

    #[error("Error while serializing/deserializing to/from bytes")]
    Serialization(#[from] bincode::Error),

    #[error("Could not convert to/from JSON")]
    Json(#[from] serde_json::Error),

    #[error("Expected itemtype to be a different format")]
    MalformedItemType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum Property {
    String(String),
    DateTime(NaiveDateTime),
    Url(Url),
    Item(Item),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Item {
    #[serde(rename = "@type")]
    itemtype: Option<OneOrMany<String>>,
    #[serde(flatten)]
    properties: HashMap<String, OneOrMany<Property>>,
}

/// implementation of https://html.spec.whatwg.org/multipage/microdata.html#associating-names-with-items
/// TODO: handle itemrefs
fn parse_item(root: NodeRef) -> Result<Item> {
    if !root
        .as_element()
        .unwrap()
        .attributes
        .borrow()
        .contains("itemscope")
    {
        return Err(Error::ExpectedItemScope);
    }

    let itemtype = root
        .as_element()
        .unwrap()
        .attributes
        .borrow()
        .get("itemtype")
        .map(|s| {
            let itemtype: Vec<_> = s.split_ascii_whitespace().map(String::from).collect();

            if itemtype.len() == 1 {
                OneOrMany::One(itemtype.into_iter().next().unwrap())
            } else {
                OneOrMany::Many(itemtype)
            }
        });

    let mut properties: HashMap<String, Vec<Property>> = HashMap::new();
    let mut pending: Vec<_> = root.children().collect();

    while let Some(current) = pending.pop() {
        if let Some(elem) = current.clone().as_element() {
            if !elem.attributes.borrow().contains("itemscope") {
                pending.extend(current.children());
            }

            if let Some(itemprop) = elem.attributes.borrow().get("itemprop") {
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
                            if let Some(url) = elem.attributes.borrow().get("src") {
                                Property::Url(url.into())
                            } else {
                                Property::String(String::new())
                            }
                        }
                        "a" | "area" | "link" => {
                            if let Some(url) = elem.attributes.borrow().get("href") {
                                Property::Url(url.into())
                            } else {
                                Property::String(String::new())
                            }
                        }
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
                        "time" => {
                            let time = elem
                                .attributes
                                .borrow()
                                .get("datetime")
                                .map(String::from)
                                .unwrap_or_else(|| current.text_contents());

                            if let Ok(time) = DateTime::parse_from_rfc2822(&time) {
                                Property::DateTime(time.naive_utc())
                            } else if let Ok(time) = DateTime::parse_from_rfc3339(&time) {
                                Property::DateTime(time.naive_utc())
                            } else if let Ok(time) =
                                NaiveDateTime::parse_from_str(&time, "%Y-%m-%dT%H:%M")
                            {
                                Property::DateTime(time)
                            } else if let Ok(time) =
                                NaiveDateTime::parse_from_str(&time, "%Y-%m-%dT%H:%M:%S")
                            {
                                Property::DateTime(time)
                            } else if let Ok(time) =
                                NaiveDateTime::parse_from_str(&time, "%Y-%m-%dT%H:%M:%S%.3f")
                            {
                                Property::DateTime(time)
                            } else if let Ok(time) =
                                NaiveDateTime::parse_from_str(&time, "%Y-%m-%d %H:%M")
                            {
                                Property::DateTime(time)
                            } else if let Ok(time) =
                                NaiveDateTime::parse_from_str(&time, "%Y-%m-%d %H:%M:%S")
                            {
                                Property::DateTime(time)
                            } else if let Ok(time) =
                                NaiveDateTime::parse_from_str(&time, "%Y-%m-%d %H:%M:%S%.3f")
                            {
                                Property::DateTime(time)
                            } else if let Ok(date) = NaiveDate::parse_from_str(&time, "%Y-%m") {
                                Property::DateTime(date.and_hms(0, 0, 0))
                            } else if let Ok(date) = NaiveDate::parse_from_str(&time, "%Y-%m-%d") {
                                Property::DateTime(date.and_hms(0, 0, 0))
                            } else if let Ok(date) = NaiveDate::parse_from_str(&time, "%m-%d") {
                                Property::DateTime(date.and_hms(0, 0, 0))
                            } else {
                                Property::String(time)
                            }
                        }
                        _ => Property::String(current.text_contents()),
                    }
                };

                properties
                    .entry(itemprop.to_string())
                    .or_default()
                    .push(property);
            }
        }
    }

    Ok(Item {
        itemtype,
        properties: properties
            .into_iter()
            .map(|(name, properties)| {
                if properties.len() == 1 {
                    (name, OneOrMany::One(properties.into_iter().next().unwrap()))
                } else {
                    (name, OneOrMany::Many(properties))
                }
            })
            .collect(),
    })
}

fn parse(root: NodeRef) -> Vec<Item> {
    let mut res = Vec::new();
    let mut pending = Vec::new();
    pending.push(root);

    while let Some(current) = pending.pop() {
        if let Some(elem) = current.as_element() {
            if elem.attributes.borrow().contains("itemscope") {
                res.push(parse_item(current).unwrap());
            } else {
                pending.extend(current.children())
            }
        } else {
            pending.extend(current.children())
        }
    }

    res
}

impl TryFrom<Item> for SchemaOrg {
    type Error = Error;

    fn try_from(mut item: Item) -> std::result::Result<Self, Self::Error> {
        match &item.itemtype {
            Some(OneOrMany::One(itemtype)) => {
                item.itemtype = Some(OneOrMany::One(
                    itemtype
                        .split('/')
                        .last()
                        .map(String::from)
                        .ok_or(Error::MalformedItemType)?,
                ));

                let json = serde_json::to_string(&item)?;
                Ok(serde_json::from_str(&json)?)
            }
            _ => Err(Error::MalformedItemType),
        }
    }
}

pub fn parse_schema(root: NodeRef) -> Vec<SchemaOrg> {
    parse(root)
        .into_iter()
        .filter_map(|item| SchemaOrg::try_from(item).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use kuchiki::traits::TendrilSink;
    use maplit::hashmap;

    use crate::webpage::schema_org::{ImageObject, OneOrMany, PersonOrOrganization, Thing};

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
            parse_item(root).unwrap(),
            Item {
                itemtype: Some(OneOrMany::One(String::from("http://n.whatwg.org/work"))),
                properties: hashmap! {
                    "work".to_string() => OneOrMany::One(Property::Url("images/house.jpeg".into())),
                    "title".to_string() => OneOrMany::One(Property::String("The house I found.".to_string())),
                }
            }
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

        let expected = Item {
            itemtype: Some(OneOrMany::One(String::from(
                "http://schema.org/BlogPosting",
            ))),
            properties: hashmap! {
                "comment".to_string() => OneOrMany::One(
                    Property::Item(
                        Item {
                            itemtype: Some(OneOrMany::One("http://schema.org/UserComments".to_string())),
                            properties: hashmap! {
                                "url".to_string() => OneOrMany::One(Property::Url("#c1".into())),
                                "creator".to_string() =>  OneOrMany::One(
                                    Property::Item(Item {
                                        itemtype: Some(OneOrMany::One("http://schema.org/Person".to_string())),
                                        properties: hashmap! {
                                            "name".to_string() => OneOrMany::One(Property::String("Greg".to_string()))
                                        }
                                    })),
                                "commentTime".to_string() => OneOrMany::One(Property::DateTime(NaiveDate::parse_from_str("2013-08-29", "%Y-%m-%d").unwrap().and_hms(0, 0, 0)))
                            }
                })),
            },
        };

        assert_eq!(parse_item(root.clone()).unwrap(), expected);
        assert_eq!(parse(root), vec![expected]);
    }

    #[test]
    fn entire_website() {
        let root = kuchiki::parse_html()
            .one(
                r##"
        <html lang="en">
        <title>My Blog</title>
        <article itemscope itemtype="http://schema.org/BlogPosting">
            <header>
            <h1 itemprop="headline">Progress report</h1>
            <p><time itemprop="datePublished" datetime="2013-08-29">today</time></p>
            <link itemprop="url" href="?comments=0">
            </header>
            <p>All in all, he's doing well with his swim lessons. The biggest thing was he had trouble
            putting his head in, but we got it down.</p>
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
            <article itemprop="comment" itemscope itemtype="http://schema.org/UserComments" id="c2">
            <link itemprop="url" href="#c2">
            <footer>
            <p>Posted by: <span itemprop="creator" itemscope itemtype="http://schema.org/Person">
                <span itemprop="name">Charlotte</span>
            </span></p>
            <p><time itemprop="commentTime" datetime="2013-08-29">5 minutes ago</time></p>
            </footer>
            <p>When you say "we got it down"...</p>
            </article>
            </section>
        </article>
        <h2>Second article</h2>
        <article itemscope itemtype="http://schema.org/BlogPosting">
            <header>
            <h1 itemprop="headline">Progress report</h1>
            <p><time itemprop="datePublished" datetime="2013-08-29">today</time></p>
            <link itemprop="url" href="?comments=0">
            </header>
            <p>All in all, he's doing well with his swim lessons. The biggest thing was he had trouble
            putting his head in, but we got it down.</p>
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
            <article itemprop="comment" itemscope itemtype="http://schema.org/UserComments" id="c2">
            <link itemprop="url" href="#c2">
            <footer>
            <p>Posted by: <span itemprop="creator" itemscope itemtype="http://schema.org/Person">
                <span itemprop="name">Charlotte</span>
            </span></p>
            <p><time itemprop="commentTime" datetime="2013-08-29">5 minutes ago</time></p>
            </footer>
            <p>When you say "we got it down"...</p>
            </article>
            </section>
        </article>
        </html>
        "##,
            );

        let res = parse(root);
        assert_eq!(res.len(), 2);

        let expected_article = Item {
            itemtype: Some(OneOrMany::One(String::from(
                "http://schema.org/BlogPosting",
            ))),
            properties: hashmap! {
                "headline".to_string() => OneOrMany::One(Property::String(String::from("Progress report"))),
                "datePublished".to_string() => OneOrMany::One(Property::DateTime(NaiveDate::parse_from_str("2013-08-29", "%Y-%m-%d").unwrap().and_hms(0, 0, 0))),
                "url".to_string() => OneOrMany::One(Property::Url(Url::from("?comments=0"))),
                "comment".to_string() => OneOrMany::Many(vec![
                    Property::Item(
                            Item {
                                itemtype: Some(OneOrMany::One("http://schema.org/UserComments".to_string())),
                                properties: hashmap! {
                                    "url".to_string() => OneOrMany::One(Property::Url("#c2".into())),
                                    "creator".to_string() =>  OneOrMany::One(
                                        Property::Item(Item {
                                            itemtype: Some(OneOrMany::One("http://schema.org/Person".to_string())),
                                            properties: hashmap! {
                                                "name".to_string() => OneOrMany::One(Property::String("Charlotte".to_string()))
                                            }
                                        })),
                                    "commentTime".to_string() => OneOrMany::One(Property::DateTime(NaiveDate::parse_from_str("2013-08-29", "%Y-%m-%d").unwrap().and_hms(0, 0, 0)))
                                }
                    }),
                    Property::Item(
                        Item {
                            itemtype: Some(OneOrMany::One("http://schema.org/UserComments".to_string())),
                            properties: hashmap! {
                                "url".to_string() => OneOrMany::One(Property::Url("#c1".into())),
                                "creator".to_string() =>  OneOrMany::One(
                                    Property::Item(Item {
                                        itemtype: Some(OneOrMany::One("http://schema.org/Person".to_string())),
                                        properties: hashmap! {
                                            "name".to_string() => OneOrMany::One(Property::String("Greg".to_string()))
                                        }
                                    })),
                                "commentTime".to_string() => OneOrMany::One(Property::DateTime(NaiveDate::parse_from_str("2013-08-29", "%Y-%m-%d").unwrap().and_hms(0, 0, 0)))
                            }
                })
                ]),
            },
        };

        dbg!(&res[0]);
        dbg!(&expected_article);

        assert_eq!(res, vec![expected_article.clone(), expected_article])
    }

    #[test]
    fn website_without_microdata() {
        let root = kuchiki::parse_html()
            .one(
                r##"
        <html lang="en">
        <title>My Blog</title>
        <article>
            <header>
            <h1>Progress report</h1>
            <p><time datetime="2013-08-29">today</time></p>
            <link href="?comments=0">
            </header>
            <p>All in all, he's doing well with his swim lessons. The biggest thing was he had trouble
            putting his head in, but we got it down.</p>
            <section>
            <h1>Comments</h1>
            <article id="c1">
            <lin href="#c1">
            <footer>
            <p>Posted by: <span>
                <span itemprop="name">Greg</span>
            </span></p>
            <p><time datetime="2013-08-29">15 minutes ago</time></p>
            </footer>
            <p>Ha!</p>
            </article>
            <article id="c2">
            <lin href="#c2">
            <footer>
            <p>Posted by: <span>
                <span itemprop="name">Charlotte</span>
            </span></p>
            <p><time datetime="2013-08-29">5 minutes ago</time></p>
            </footer>
            <p>When you say "we got it down"...</p>
            </article>
            </section>
        </article>
        </html>
        "##,
            );

        assert_eq!(parse(root).len(), 0);
    }

    #[test]
    fn schema_image_object_example() {
        let root = kuchiki::parse_html().one(
            r##"
            <html>
                <div itemscope itemtype="https://schema.org/ImageObject">
                <h2 itemprop="name">Beach in Mexico</h2>
                <img src="mexico-beach.jpg"
                alt="Sunny, sandy beach."
                itemprop="contentUrl" />
        
                By <span itemprop="author">Jane Doe</span>
                Photographed in
                <span itemprop="contentLocation">Puerto Vallarta, Mexico</span>
                Date uploaded:
                <meta itemprop="datePublished" content="2008-01-25">Jan 25, 2008
        
                <span itemprop="description">I took this picture while on vacation last year.</span>
                </div>
            </html>
            "##,
        );

        assert_eq!(
            parse_schema(root),
            vec![SchemaOrg::ImageObject(ImageObject {
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
            })]
        );
    }
}
