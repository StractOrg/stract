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

use std::{
    collections::{BTreeMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
};

use crate::{
    entity_index::{
        entity::{Entity, Link, Paragraph, Span},
        EntityIndex,
    },
    Result,
};

use bzip2::bufread::MultiBzDecoder;
use parse_wiki_text::Node;
use quick_xml::{events::Event, Reader};

struct EntityIterator<R: BufRead> {
    inside_title: bool,
    inside_text: bool,
    buf: Vec<u8>,
    current_entity: Option<EntityBuilder>,
    reader: Reader<R>,
}

impl<R: BufRead> From<R> for EntityIterator<R> {
    fn from(reader: R) -> Self {
        Self {
            inside_title: false,
            inside_text: false,
            buf: Vec::new(),
            current_entity: None,
            reader: Reader::from_reader(reader),
        }
    }
}

impl<R: BufRead> Iterator for EntityIterator<R> {
    type Item = Entity;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.reader.read_event(&mut self.buf) {
                Ok(Event::Start(ref e)) if e.name() == b"page" => {
                    self.current_entity = Some(EntityBuilder::new());
                }
                Ok(Event::End(ref e)) if e.name() == b"page" => {
                    if let Some(entity) = self.current_entity.take() {
                        if let Some(entity) = entity.build() {
                            return Some(entity);
                        }
                    }
                }
                Ok(Event::Empty(ref e)) if e.name() == b"redirect" => {
                    self.current_entity = None;
                }
                Ok(Event::Start(ref e)) if e.name() == b"title" => {
                    self.inside_title = true;
                }
                Ok(Event::End(ref e)) if e.name() == b"title" => {
                    self.inside_title = false;
                }
                Ok(Event::Start(ref e)) if e.name() == b"text" => {
                    self.inside_text = true;
                }
                Ok(Event::End(ref e)) if e.name() == b"text" => {
                    self.inside_text = false;
                }
                Ok(Event::Text(e)) => {
                    if self.inside_title {
                        if let Some(entity) = &mut self.current_entity {
                            let bytes = e.unescaped().unwrap();
                            entity.append_title(self.reader.decode(&bytes).unwrap());
                        }
                    } else if self.inside_text {
                        if let Some(entity) = &mut self.current_entity {
                            let bytes = e.unescaped().unwrap();
                            entity.append_text(self.reader.decode(&bytes).unwrap());
                        }
                    }
                }
                Ok(Event::Eof) | Err(_) => break,
                _ => (),
            }
        }

        None
    }
}

pub struct EntityIndexer;

impl EntityIndexer {
    pub fn run(wikipedia_dump_path: String, output_path: String) -> Result<()> {
        let reader = BufReader::new(File::open(wikipedia_dump_path)?);
        let reader = BufReader::new(MultiBzDecoder::new(reader));
        let mut index = EntityIndex::open(output_path)?;

        for entity in EntityIterator::from(reader)
            .take(1000)
            .filter(|entity| !entity.categories.is_empty())
        {
            index.insert(entity);
        }

        index.commit();

        Ok(())
    }
}

#[derive(Debug)]
pub struct EntityBuilder {
    title: String,
    text: String,
}

impl EntityBuilder {
    pub fn new() -> Self {
        EntityBuilder {
            title: String::new(),
            text: String::new(),
        }
    }

    pub fn append_title(&mut self, text: &str) {
        self.title.push_str(text);
    }

    pub fn append_text(&mut self, text: &str) {
        self.text.push_str(text);
    }

    fn extract_info(&self) -> BTreeMap<String, Span> {
        let parsed_wiki = parse_wiki_text::Configuration::default().parse(&self.text);
        let mut result = BTreeMap::new();
        for node in parsed_wiki.nodes.into_iter() {
            match node {
                Node::Template { end: _, name, parameters, start: _ } if name.iter().any(|n| matches!(n, Node::Text { end: _, start: _, value } if value.starts_with("Infobox"))) => {
                    for parameter in parameters {
                        if let Some(names) = parameter.name {
                            if let Some(&Node::Text { end: _, start: _, value: info_name}) = names.first() {
                                let value = Span::from(parameter.value);
                                result.insert(info_name.to_string(), value);
                            }
                        }
                    }
                    break;
                }
                _ => {}
            }
        }

        result
            .into_iter()
            .filter(|(_key, val)| !val.text.is_empty())
            .collect()
    }

    fn extract_text(&self) -> (Option<Span>, Vec<Paragraph>) {
        let parsed_wiki = parse_wiki_text::Configuration::default().parse(&self.text);
        let mut current_paragraph: Option<Paragraph> = None;
        let mut paragraphs = Vec::new();

        for node in parsed_wiki
            .nodes
            .into_iter()
            .skip_while(|node| matches!(node, Node::ParagraphBreak { end: _, start: _ }))
        {
            match node {
                Node::Text {
                    end: _,
                    start: _,
                    value,
                } => match current_paragraph.as_mut() {
                    Some(current_paragraph) => {
                        current_paragraph.content.text.push_str(value);
                    }
                    None => {
                        current_paragraph = Some(Paragraph {
                            title: None,
                            content: Span {
                                text: value.to_string(),
                                links: Vec::new(),
                            },
                        })
                    }
                },
                Node::Heading {
                    end: _,
                    level: _,
                    nodes,
                    start: _,
                } => {
                    if let Some(current_paragraph) = current_paragraph.take() {
                        paragraphs.push(current_paragraph)
                    }

                    let title: String = itertools::intersperse(
                        nodes.into_iter().filter_map(|node| match node {
                            Node::Text {
                                end: _,
                                start: _,
                                value,
                            } => Some(value),
                            _ => None,
                        }),
                        "",
                    )
                    .collect();

                    let title = if title.is_empty() { None } else { Some(title) };

                    current_paragraph = Some(Paragraph {
                        title,
                        content: Span {
                            text: String::new(),
                            links: Vec::new(),
                        },
                    })
                }
                Node::Link {
                    end: _,
                    start: _,
                    target,
                    text,
                } => {
                    let text: String = itertools::intersperse(
                        text.into_iter().filter_map(|node| match node {
                            Node::Text {
                                end: _,
                                start: _,
                                value,
                            } => Some(value),
                            _ => None,
                        }),
                        "",
                    )
                    .collect();

                    match current_paragraph.as_mut() {
                        Some(current_paragraph) => {
                            let link = Link {
                                start: current_paragraph.content.text.chars().count(),
                                end: current_paragraph.content.text.chars().count()
                                    + text.chars().count(),
                                target: target.to_string(),
                            };
                            current_paragraph.content.add_link(text, link)
                        }
                        None => {
                            let link = Link {
                                start: 0,
                                end: text.chars().count(),
                                target: target.to_string(),
                            };
                            current_paragraph = Some(Paragraph {
                                title: None,
                                content: Span {
                                    text,
                                    links: vec![link],
                                },
                            })
                        }
                    }
                }
                _ => {}
            }
        }

        let page_abstract = if !paragraphs.is_empty() {
            let page_abstract = paragraphs.remove(0).content;
            Some(page_abstract)
        } else {
            None
        };

        (page_abstract, paragraphs)
    }

    fn extract_categories(&self) -> HashSet<String> {
        let mut result = HashSet::new();
        let parsed_wiki = parse_wiki_text::Configuration::default().parse(&self.text);

        for node in parsed_wiki.nodes {
            if let Node::Category {
                end: _,
                ordinal: _,
                start: _,
                target,
            } = node
            {
                if let Some((_, cat)) = target.split_once(':') {
                    result.insert(cat.to_string());
                }
            }
        }

        result
    }

    pub fn build(self) -> Option<Entity> {
        let categories = self.extract_categories();

        if categories.contains("Disambiguation") {
            return None;
        }

        let mut info = self.extract_info();
        let image = info.remove("image").map(|span| span.text.into());

        let (page_abstract, paragraphs) = self.extract_text();

        page_abstract.map(|page_abstract| Entity {
            title: self.title,
            info,
            image,
            page_abstract,
            paragraphs,
            categories,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aristotle() {
        let entity = EntityBuilder {
            title: "Aristotle".to_string(),
            text: include_str!("../../testcases/entity/aristotle.txt").to_string(),
        }
        .build()
        .unwrap();

        assert_eq!(&entity.title, "Aristotle");
        assert_eq!(
            entity.info.get("birth_date"),
            Some(&Span {
                text: "384 BC".to_string(),
                links: vec![]
            })
        );
        assert_eq!(
            entity.info.get("birth_place"),
            Some(&Span {
                text: "Stagira, Chalcidice".to_string(),
                links: vec![
                    Link {
                        start: 0,
                        end: 7,
                        target: "Stagira (ancient city)".to_string()
                    },
                    Link {
                        start: 9,
                        end: 19,
                        target: "Chalcidice".to_string()
                    }
                ]
            })
        );
        assert_eq!(
            entity.info.get("death_date"),
            Some(&Span {
                text: "322 BC (aged 61–62)".to_string(),
                links: vec![]
            })
        );

        assert_eq!(
            entity.image,
            Some("Aristotle Altemps Inv8575.jpg".to_string().into())
        );

        assert!(
            entity.page_abstract.text.trim().starts_with("Aristotle (;  Aristotélēs, ; 384–322BC) was a Greek philosopher and polymath during the Classical period in Ancient Greece"));

        assert!(entity.categories.contains("Aristotle"));
        assert!(entity.categories.contains("Acting theorists"));

        assert_eq!(
            entity.page_abstract.links[0],
            Link {
                start: 58,
                end: 69,
                target: "philosopher".to_string()
            }
        )
    }

    #[test]
    fn barack_obama() {
        let entity = EntityBuilder {
            title: "Barack Obama".to_string(),
            text: include_str!("../../testcases/entity/obama.txt").to_string(),
        }
        .build()
        .unwrap();

        assert_eq!(&entity.title, "Barack Obama");
        assert_eq!(
            entity.info.get("office"),
            Some(&Span {
                text: "President of the United States".to_string(),
                links: vec![]
            })
        );
        assert_eq!(
            entity.info.get("birth_date"),
            Some(&Span {
                text: "1961 8 4".to_string(), // this is not optimal, but good enough for now
                links: vec![]
            })
        );
        assert_eq!(
            entity.info.get("spouse"),
            Some(&Span {
                text: "Michelle Robinson October 3, 1992".to_string(),
                links: vec![Link {
                    start: 0,
                    end: 17,
                    target: "Michelle Obama".to_string()
                }]
            })
        );
        assert_eq!(
            entity.info.get("birth_place"),
            Some(&Span {
                text: "Honolulu, Hawaii, U.S.".to_string(),
                links: vec![
                    Link {
                        start: 0,
                        end: 8,
                        target: "Honolulu".to_string()
                    },
                    Link {
                        start: 10,
                        end: 16,
                        target: "Hawaii".to_string()
                    }
                ]
            })
        );
        assert_eq!(entity.info.get("death_date"), None);

        assert_eq!(
            entity.image,
            Some("President Barack Obama.jpg".to_string().into())
        );

        assert!(
            entity.page_abstract.text.trim().starts_with("Barack Hussein Obama II (   ; born August 4, 1961) is an American politician who served as the 44th president of the United States from 2009 to 2017. A member of the Democratic Party, he was the first African-American  president of the United States."));

        assert!(entity.categories.contains("Nobel Peace Prize laureates"));
        assert!(entity.categories.contains("Obama family"));
    }

    #[test]
    fn algorithm() {
        let entity = EntityBuilder {
            title: "Algorithm".to_string(),
            text: include_str!("../../testcases/entity/algorithm.txt").to_string(),
        }
        .build()
        .unwrap();

        assert_eq!(&entity.title, "Algorithm");
        assert!(entity.info.is_empty());
        assert!(entity.image.is_none());

        assert!(entity.page_abstract.text.trim().starts_with("In mathematics and computer science, an algorithm () is a finite sequence of rigorous instructions, typically used to solve a class of specific problemss or to perform a computation."));

        assert!(entity.categories.contains("Algorithms"));
    }

    #[test]
    fn skip_disambiguation_pages() {
        assert!(EntityBuilder {
            title: "Test".to_string(),
            text: include_str!("../../testcases/entity/disambiguation.txt").to_string(),
        }
        .build()
        .is_none());
    }
}
