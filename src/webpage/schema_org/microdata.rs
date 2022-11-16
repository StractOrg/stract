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

use kuchiki::NodeRef;
use std::collections::HashMap;
use thiserror::Error;

use super::{Item, OneOrMany, Property};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Html node was expected to have an itemscope attribute, but did not have one.")]
    ExpectedItemScope,

    #[error("Error while serializing/deserializing to/from bytes")]
    Serialization(#[from] bincode::Error),

    #[error("Could not convert to/from JSON")]
    Json(#[from] serde_json::Error),
}

fn rec_text_contents(node: NodeRef) -> Vec<Property> {
    let mut res = Vec::new();

    for child in node.children() {
        match child.data() {
            kuchiki::NodeData::Element(elem) => {
                if elem.name.local.to_string().as_str() == "code" {
                    let mut properties = HashMap::new();
                    properties.insert(
                        "text".to_string(),
                        OneOrMany::One(Property::String(child.text_contents())),
                    );

                    res.push(Property::Item(Item {
                        itemtype: Some(OneOrMany::One("SourceCode".to_string())),
                        properties,
                    }))
                } else if !elem.attributes.borrow().contains("itemprop")
                    && !elem.attributes.borrow().contains("itemscope")
                {
                    res.append(&mut rec_text_contents(child));
                }
            }
            kuchiki::NodeData::Text(text) => {
                let had_line_ending = text.borrow().ends_with('\n');
                let mut s: String =
                    itertools::intersperse(text.borrow().split_whitespace(), " ").collect();

                if had_line_ending {
                    s.push('\n');
                }
                res.push(Property::String(s));
            }
            _ => {}
        }
    }

    res
}

fn text_contents(node: NodeRef) -> Vec<Property> {
    let properties = rec_text_contents(node);

    // merge consecutive strings
    let mut res = Vec::new();
    let mut current = None;

    for prop in properties {
        match &prop {
            Property::String(s) => {
                current = match current {
                    Some(current) => match current {
                        Property::String(mut current_str) => {
                            current_str.push(' ');
                            current_str.push_str(s);
                            Some(Property::String(current_str))
                        }
                        Property::Item(_) => {
                            res.push(current);
                            Some(prop)
                        }
                    },
                    None => Some(prop),
                }
            }
            Property::Item(_) => {
                if let Some(current) = current {
                    res.push(current);
                }

                current = Some(prop)
            }
        }
    }

    if let Some(current) = current {
        res.push(current)
    }

    // trim strings
    for prop in &mut res {
        if let Property::String(s) = prop {
            let had_line_ending = s.ends_with('\n');

            *s = itertools::intersperse(
                s.lines()
                    .map(|l| itertools::intersperse(l.split_whitespace(), " ").collect::<String>())
                    .filter(|s| !s.is_empty()),
                "\n".to_string(),
            )
            .collect::<String>()
            .to_string();

            if had_line_ending {
                s.push('\n');
            }
        }
    }

    res.reverse();

    res
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
                let properties_for_prop = if elem.attributes.borrow().contains("itemscope") {
                    vec![Property::Item(parse_item(current)?)]
                } else {
                    match elem.name.local.to_string().as_str() {
                        "meta" => vec![Property::String(
                            elem.attributes
                                .borrow()
                                .get("content")
                                .map(String::from)
                                .unwrap_or_default(),
                        )],
                        "audio" | "embed" | "iframe" | "img" | "source" | "track" | "video" => {
                            if let Some(url) = elem.attributes.borrow().get("src") {
                                vec![Property::String(url.to_string())]
                            } else {
                                vec![Property::String(String::new())]
                            }
                        }
                        "a" | "area" | "link" => {
                            if let Some(url) = elem.attributes.borrow().get("href") {
                                vec![Property::String(url.to_string())]
                            } else {
                                vec![Property::String(String::new())]
                            }
                        }
                        "object" => vec![Property::String(
                            elem.attributes
                                .borrow()
                                .get("data")
                                .map(String::from)
                                .unwrap_or_default(),
                        )],
                        "data" | "meter" => vec![Property::String(
                            elem.attributes
                                .borrow()
                                .get("value")
                                .map(String::from)
                                .unwrap_or_default(),
                        )],
                        "time" => {
                            let time = elem
                                .attributes
                                .borrow()
                                .get("datetime")
                                .map(String::from)
                                .unwrap_or_else(|| current.text_contents());

                            vec![Property::String(time)]
                        }
                        _ => text_contents(current.clone()),
                    }
                };

                for itemprop in itemprop.split_ascii_whitespace() {
                    properties
                        .entry(itemprop.to_string())
                        .or_default()
                        .append(&mut properties_for_prop.clone());
                }
            }
        }
    }

    Ok(Item {
        itemtype,
        properties: properties
            .into_iter()
            .filter_map(|(name, mut properties)| {
                properties.reverse();

                if properties.is_empty() {
                    None
                } else if properties.len() == 1 {
                    Some((name, OneOrMany::One(properties.into_iter().next().unwrap())))
                } else {
                    Some((name, OneOrMany::Many(properties)))
                }
            })
            .collect(),
    })
}

fn parse(root: NodeRef) -> Vec<Item> {
    let mut res = Vec::new();
    let mut pending: Vec<_> = root.inclusive_descendants().collect();

    while let Some(current) = pending.pop() {
        if let Some(elem) = current.as_element() {
            if elem.attributes.borrow().contains("itemscope")
                && !elem.attributes.borrow().contains("itemprop")
            {
                res.push(parse_item(current).unwrap());
            }
        }
    }

    res.reverse();

    res
}

fn fix_type_for_schema(mut item: Item) -> Item {
    if let Some(OneOrMany::One(itemtype)) = &item.itemtype {
        if let Some(last) = itemtype.split('/').last() {
            item.itemtype = Some(OneOrMany::One(last.to_string()));
        }
    }

    item.properties = item
        .properties
        .into_iter()
        .map(|(key, properties)| match properties {
            OneOrMany::One(property) => {
                if let Property::Item(subitem) = property {
                    (
                        key,
                        OneOrMany::One(Property::Item(fix_type_for_schema(subitem))),
                    )
                } else {
                    (key, OneOrMany::One(property))
                }
            }
            OneOrMany::Many(properties) => (
                key,
                OneOrMany::Many(
                    properties
                        .into_iter()
                        .map(|property| {
                            if let Property::Item(subitem) = property {
                                Property::Item(fix_type_for_schema(subitem))
                            } else {
                                property
                            }
                        })
                        .collect(),
                ),
            ),
        })
        .collect();

    item
}

pub fn parse_schema(root: NodeRef) -> Vec<Item> {
    parse(root).into_iter().map(fix_type_for_schema).collect()
}

#[cfg(test)]
mod tests {
    use kuchiki::traits::TendrilSink;
    use maplit::hashmap;

    use crate::webpage::schema_org::OneOrMany;

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
                    "work".to_string() => OneOrMany::One(Property::String("images/house.jpeg".to_string())),
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
                                "url".to_string() => OneOrMany::One(Property::String("#c1".to_string())),
                                "creator".to_string() =>  OneOrMany::One(
                                    Property::Item(Item {
                                        itemtype: Some(OneOrMany::One("http://schema.org/Person".to_string())),
                                        properties: hashmap! {
                                            "name".to_string() => OneOrMany::One(Property::String("Greg".to_string()))
                                        }
                                    })),
                                "commentTime".to_string() => OneOrMany::One(Property::String("2013-08-29".to_string()))
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
                "datePublished".to_string() => OneOrMany::One(Property::String("2013-08-29".to_string())),
                "url".to_string() => OneOrMany::One(Property::String("?comments=0".to_string())),
                "comment".to_string() => OneOrMany::Many(vec![
                        Property::Item(
                            Item {
                                itemtype: Some(OneOrMany::One("http://schema.org/UserComments".to_string())),
                                properties: hashmap! {
                                    "url".to_string() => OneOrMany::One(Property::String("#c1".to_string())),
                                    "creator".to_string() =>  OneOrMany::One(
                                        Property::Item(Item {
                                            itemtype: Some(OneOrMany::One("http://schema.org/Person".to_string())),
                                            properties: hashmap! {
                                                "name".to_string() => OneOrMany::One(Property::String("Greg".to_string()))
                                            }
                                        })),
                                    "commentTime".to_string() => OneOrMany::One(Property::String("2013-08-29".to_string()))
                                }
                    }),
                    Property::Item(
                            Item {
                                itemtype: Some(OneOrMany::One("http://schema.org/UserComments".to_string())),
                                properties: hashmap! {
                                    "url".to_string() => OneOrMany::One(Property::String("#c2".to_string())),
                                    "creator".to_string() =>  OneOrMany::One(
                                        Property::Item(Item {
                                            itemtype: Some(OneOrMany::One("http://schema.org/Person".to_string())),
                                            properties: hashmap! {
                                                "name".to_string() => OneOrMany::One(Property::String("Charlotte".to_string()))
                                            }
                                        })),
                                    "commentTime".to_string() => OneOrMany::One(Property::String("2013-08-29".to_string()))
                                }
                    })
                ]),
            },
        };

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

        let res = parse_schema(root);

        assert_eq!(
            res,
            vec![Item {
                itemtype: Some(OneOrMany::One("ImageObject".to_string())),
                properties: hashmap! {
                    "author".to_string() => OneOrMany::One(Property::String("Jane Doe".to_string())),
                    "contentLocation".to_string() => OneOrMany::One(Property::String("Puerto Vallarta, Mexico".to_string())),
                    "contentUrl".to_string() => OneOrMany::One(Property::String("mexico-beach.jpg".to_string())),
                    "datePublished".to_string() => OneOrMany::One(Property::String("2008-01-25".to_string())),
                    "description".to_string() => OneOrMany::One(Property::String("I took this picture while on vacation last year.".to_string())),
                    "name".to_string() => OneOrMany::One(Property::String("Beach in Mexico".to_string())),
                }
            }]
        );
    }

    #[test]
    fn schema_person_example() {
        let root = kuchiki::parse_html().one(
            r##"
            <div itemscope itemtype="https://schema.org/Person">
            <span itemprop="name">Jane Doe</span>
            <img src="janedoe.jpg" itemprop="image" alt="Photo of Jane Doe"/>
      
            <span itemprop="jobTitle">Professor</span>
            <div itemprop="address" itemscope itemtype="https://schema.org/PostalAddress">
              <span itemprop="streetAddress">
                20341 Whitworth Institute
                405 N. Whitworth
              </span>
              <span itemprop="addressLocality">Seattle</span>,
              <span itemprop="addressRegion">WA</span>
              <span itemprop="postalCode">98052</span>
            </div>
            <span itemprop="telephone">(425) 123-4567</span>
            <a href="mailto:jane-doe@xyz.edu" itemprop="email">
              jane-doe@xyz.edu</a>
      
            Jane's home page:
            <a href="http://www.janedoe.com" itemprop="url">janedoe.com</a>
      
            Graduate students:
            <a href="http://www.xyz.edu/students/alicejones.html" itemprop="colleague">
              Alice Jones</a>
            <a href="http://www.xyz.edu/students/bobsmith.html" itemprop="colleague">
              Bob Smith</a>
          </div>
            "##,
        );
        let expected_json = r#"
        {
            "@type": "Person",
            "address": {
              "@type": "PostalAddress",
              "addressLocality": "Seattle",
              "addressRegion": "WA",
              "postalCode": "98052",
              "streetAddress": "20341 Whitworth Institute 405 N. Whitworth"
            },
            "colleague": [
              "http://www.xyz.edu/students/alicejones.html",
              "http://www.xyz.edu/students/bobsmith.html"
            ],
            "email": "mailto:jane-doe@xyz.edu",
            "image": "janedoe.jpg",
            "jobTitle": "Professor",
            "name": "Jane Doe",
            "telephone": "(425) 123-4567",
            "url": "http://www.janedoe.com"
          }
        "#;

        let res = parse_schema(root);

        assert_eq!(res.len(), 1);
        assert_eq!(res, vec![serde_json::from_str(expected_json).unwrap()]);
    }
}
