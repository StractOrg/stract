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

use std::collections::BTreeMap;

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::{
    entity_index::{entity::Span, StoredEntity},
    searcher::LocalSearcher,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DisplayedEntity {
    pub title: String,
    pub small_abstract: String,
    pub image_base64: Option<String>,
    pub related_entities: Vec<DisplayedEntity>,
    pub info: Vec<(String, String)>,
}

fn prepare_info(info: BTreeMap<String, Span>, searcher: &LocalSearcher) -> Vec<(String, String)> {
    let mut info: Vec<_> = info.into_iter().collect();

    info.sort_by(|(a, _), (b, _)| {
        searcher
            .attribute_occurrence(b)
            .unwrap_or(0)
            .cmp(&searcher.attribute_occurrence(a).unwrap_or(0))
    });

    info.into_iter()
        .map(|(key, value)| {
            let mut value = entity_link_to_html(value, 150).replace('*', "•");

            if value.starts_with('•') || value.starts_with("\n•") {
                if let Some(first_bullet) = value.find('•') {
                    value = value.chars().skip(first_bullet + 1).collect();
                }
            }

            let value = maybe_prettify_entity_date(value);

            (key.replace('_', " "), value)
        })
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "caption"
                    | "image size"
                    | "label"
                    | "landscape"
                    | "signature"
                    | "name"
                    | "website"
                    | "logo"
                    | "image caption"
                    | "alt"
            )
        })
        .take(5)
        .collect()
}

impl DisplayedEntity {
    pub fn from(entity: StoredEntity, searcher: &LocalSearcher) -> Self {
        let entity_abstract = Span {
            text: entity.entity_abstract,
            links: entity.links,
        };

        let small_abstract = entity_link_to_html(entity_abstract, 300);

        let image_base64 = entity
            .image
            .and_then(|image| searcher.entity_image(image))
            .map(|image| base64::encode(image.as_raw_bytes()));

        Self {
            title: entity.title,
            small_abstract,
            image_base64,
            related_entities: entity
                .related_entities
                .into_iter()
                .map(|entity| DisplayedEntity::from(entity, searcher))
                .collect(),
            info: prepare_info(entity.info, searcher),
        }
    }
}

fn maybe_prettify_entity_date(value: String) -> String {
    if let Ok(date) = NaiveDate::parse_from_str(value.trim(), "%Y %-m %-d") {
        return format!("{}", date.format("%d/%m/%Y"));
    }

    if value.split_whitespace().count() == 6 {
        let first_date = NaiveDate::parse_from_str(
            itertools::intersperse(value.split_whitespace().take(3), " ")
                .collect::<String>()
                .as_str(),
            "%Y %-m %-d",
        );
        let second_date = NaiveDate::parse_from_str(
            itertools::intersperse(value.split_whitespace().skip(3), " ")
                .collect::<String>()
                .as_str(),
            "%Y %-m %-d",
        );

        if let (Ok(first_date), Ok(second_date)) = (first_date, second_date) {
            // the dates are reversed from the parser, so we need to return second_date before first_date
            return format!("{}", second_date.format("%d/%m/%Y"))
                + " - "
                + format!("{}", first_date.format("%d/%m/%Y")).as_str();
        }
    }

    value
}

fn entity_link_to_html(span: Span, trunace_to: usize) -> String {
    let mut s = span.text;

    let truncated = s.len() > trunace_to;
    if truncated {
        s = s.chars().take(trunace_to).collect();
    }

    let chars = s.chars();
    let num_chars = chars.clone().count();

    let mut res = String::new();

    let mut last_link_end = 0;
    for link in span.links {
        if link.start > num_chars {
            break;
        }

        res += chars
            .clone()
            .skip(last_link_end)
            .take(link.start - last_link_end)
            .collect::<String>()
            .as_str();

        let link_text: String = chars
            .clone()
            .skip(link.start)
            .take(link.end - link.start)
            .collect();

        res += format!(
            "<a href=\"https://en.wikipedia.org/wiki/{}\">",
            link.target.replace(' ', "_")
        )
        .as_str();

        res += link_text.as_str();

        res += "</a>";

        last_link_end = link.end;
    }

    res += chars
        .clone()
        .skip(last_link_end)
        .collect::<String>()
        .as_str();

    if truncated {
        res += "...";
    }

    res
}

#[cfg(test)]
mod tests {
    use crate::entity_index::entity::Link;

    use super::*;

    #[test]
    fn simple_link_to_html() {
        assert_eq!(
            entity_link_to_html(
                Span {
                    text: "some text with a link".to_string(),
                    links: vec![Link {
                        start: 5,
                        end: 9,
                        target: "text article".to_string()
                    }]
                },
                10000
            ),
            "some <a href=\"https://en.wikipedia.org/wiki/text_article\">text</a> with a link"
                .to_string()
        );
    }

    #[test]
    fn truncated_link_to_html() {
        assert_eq!(
            entity_link_to_html(
                Span {
                    text: "some text".to_string(),
                    links: vec![Link {
                        start: 5,
                        end: 9,
                        target: "text article".to_string()
                    }]
                },
                7
            ),
            "some <a href=\"https://en.wikipedia.org/wiki/text_article\">te</a>...".to_string()
        );
    }

    #[test]
    fn einstein_date() {
        assert_eq!(
            maybe_prettify_entity_date("1879 3 14 ".to_string()),
            "14/03/1879".to_string()
        );
    }

    #[test]
    fn entity_date_span_prettify() {
        assert_eq!(
            maybe_prettify_entity_date(" 1999 5 27 1879 3 14  ".to_string()),
            "14/03/1879 - 27/05/1999".to_string()
        );
    }
}
