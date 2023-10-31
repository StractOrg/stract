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

use flate2::read::MultiGzDecoder;
use quick_xml::events::Event;
use url::Url;

use crate::{
    human_website_annotations::{Info, Topic},
    Result,
};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use crate::human_website_annotations;

struct PageIterator<R: BufRead> {
    reader: quick_xml::Reader<R>,
    buf: Vec<u8>,
}

impl<R: BufRead> From<R> for PageIterator<R> {
    fn from(reader: R) -> Self {
        Self {
            reader: quick_xml::Reader::from_reader(reader),
            buf: Vec::new(),
        }
    }
}

impl<R: BufRead> Iterator for PageIterator<R> {
    type Item = Page;

    fn next(&mut self) -> Option<Self::Item> {
        let mut current_page = None;
        let mut inside_topic = false;
        let mut inside_desc = false;

        while let Ok(event) = self.reader.read_event_into(&mut self.buf) {
            match event {
                Event::Start(ref e) if e.name().as_ref() == b"ExternalPage" => {
                    let url_attr = e
                        .attributes()
                        .filter(std::result::Result::is_ok)
                        .find(|attr| attr.as_ref().unwrap().key.as_ref() == b"about")
                        .unwrap()
                        .unwrap();
                    let url = url_attr.decode_and_unescape_value(&self.reader).unwrap();
                    current_page = Some(Page::new(url.into_owned()));
                    inside_desc = false;
                    inside_topic = false;
                }
                Event::End(ref e) if e.name().as_ref() == b"ExternalPage" => break,
                Event::Start(ref e) if e.name().as_ref() == b"topic" && current_page.is_some() => {
                    inside_topic = true;
                }
                Event::End(ref e) if e.name().as_ref() == b"topic" && current_page.is_some() => {
                    inside_topic = false;
                }
                Event::Start(ref e)
                    if e.name().as_ref() == b"d:Description" && current_page.is_some() =>
                {
                    inside_desc = true;
                }
                Event::End(ref e)
                    if e.name().as_ref() == b"d:Description" && current_page.is_some() =>
                {
                    inside_desc = false;
                }
                Event::Text(ref e) => {
                    if inside_topic {
                        if let Some(page) = &mut current_page {
                            let topic = e.unescape().unwrap();
                            page.topic.push_str(topic.as_ref());
                        }
                    } else if inside_desc {
                        if let Some(page) = &mut current_page {
                            let desc = e.unescape().unwrap();
                            page.description.push_str(desc.as_ref());
                        }
                    }
                }
                Event::Eof => break,
                _ => (),
            }
        }

        current_page
    }
}

#[derive(Debug, Default)]
struct Page {
    url: String,
    description: String,
    topic: String,
}

impl Page {
    fn new(url: String) -> Self {
        Self {
            url,
            ..Default::default()
        }
    }
}

pub fn parse(dmoz_file: &Path) -> Result<human_website_annotations::Mapper> {
    let file = File::open(dmoz_file)?;
    let reader = BufReader::new(file);
    let reader = BufReader::new(MultiGzDecoder::new(reader));
    let mut map = HashMap::new();

    for page in PageIterator::from(reader) {
        let url = match Url::parse(&page.url) {
            Ok(url) => url,
            Err(_) => continue,
        };

        if url.path() != "/" {
            continue;
        }

        if page.topic.contains("World") {
            continue;
        }

        let topic = Topic::from_string("/".to_string() + page.topic.as_str());

        let info = Info {
            description: page.description,
            topic,
        };

        map.insert(url.host_str().unwrap().to_string(), info);
    }

    Ok(map.into())
}

pub fn run(dmoz_file: &Path, output_path: &Path) -> Result<()> {
    let mapper = parse(dmoz_file)?;
    mapper.save(output_path)?;

    Ok(())
}
