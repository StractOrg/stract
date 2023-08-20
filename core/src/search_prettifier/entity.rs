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

use std::collections::BTreeMap;

use chrono::NaiveDate;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    entity_index::{entity::Span, EntityMatch},
    searcher::LocalSearcher,
};

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DisplayedEntity {
    pub title: String,
    pub small_abstract: String,
    pub image_base64: Option<String>,
    pub related_entities: Vec<DisplayedEntity>,
    pub info: Vec<(String, String)>,
    pub match_score: f32,
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

            if let Some((_, rest)) = value.split_once('•') {
                value = rest.to_string();
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
    pub fn from(m: EntityMatch, searcher: &LocalSearcher) -> Self {
        let entity_abstract = Span {
            text: m.entity.entity_abstract,
            links: m.entity.links,
        };

        let small_abstract = entity_link_to_html(entity_abstract, 300);

        let image_base64 = m
            .entity
            .image
            .and_then(|image| searcher.entity_image(image))
            .map(|image| base64::encode(image.as_raw_bytes()));

        Self {
            title: m.entity.title,
            small_abstract,
            image_base64,
            related_entities: m
                .entity
                .related_entities
                .into_iter()
                .map(|m| DisplayedEntity::from(m, searcher))
                .collect(),
            info: prepare_info(m.entity.info, searcher),
            match_score: m.score,
        }
    }
}

fn maybe_prettify_entity_date(value: String) -> String {
    let parse_ymd = |date| NaiveDate::parse_from_str(date, "%Y %-m %-d");

    if let Ok(date) = parse_ymd(value.trim()) {
        return date.format("%d/%m/%Y").to_string();
    }

    // the dates are reversed from the parser, so we parse the second date first
    if let Some((y2, m2, d2, y1, m1, d1)) = value.split_whitespace().collect_tuple() {
        if let (Ok(fst_date), Ok(snd_date)) = (
            parse_ymd(&[y1, m1, d1].join(" ")),
            parse_ymd(&[y2, m2, d2].join(" ")),
        ) {
            return format!(
                "{} - {}",
                fst_date.format("%d/%m/%Y"),
                snd_date.format("%d/%m/%Y"),
            );
        }
    }

    value
}

fn entity_link_to_html(span: Span, trunace_to: usize) -> String {
    let (s, maybe_ellipsis) = if span.text.len() > trunace_to {
        (&span.text[0..trunace_to], "...")
    } else {
        (&*span.text, "")
    };

    span.links.iter().rfold(s.to_string(), |mut acc, link| {
        let (start, end) = (link.start, link.end.min(acc.len()));
        if start > acc.len() {
            return acc;
        }

        let anchor_start = format!(
            r#"<a class="hover:underline" href="https://en.wikipedia.org/wiki/{}">"#,
            link.target.replace(' ', "_"),
        );
        acc.insert_str(start, &anchor_start);
        acc.insert_str(end + anchor_start.len(), "</a>");

        acc
    }) + maybe_ellipsis
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
            "some <a class=\"hover:underline\" href=\"https://en.wikipedia.org/wiki/text_article\">text</a> with a link"
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
            "some <a class=\"hover:underline\" href=\"https://en.wikipedia.org/wiki/text_article\">te</a>...".to_string()
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
