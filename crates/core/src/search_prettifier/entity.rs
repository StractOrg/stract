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

use chrono::NaiveDate;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use entity_index::{
    entity::{EntitySnippet, Span},
    EntityMatch,
};

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DisplayedEntity {
    pub title: String,
    pub small_abstract: EntitySnippet,
    pub image_id: Option<String>,
    pub related_entities: Vec<DisplayedEntity>,
    pub info: Vec<(String, EntitySnippet)>,
    pub match_score: f32,
}

impl From<EntityMatch> for DisplayedEntity {
    fn from(m: EntityMatch) -> Self {
        let entity_abstract = Span {
            text: m.entity.entity_abstract,
            links: m.entity.links,
        };

        let small_abstract = EntitySnippet::from_span(&entity_abstract, 300);

        Self {
            title: m.entity.title,
            small_abstract,
            image_id: m.entity.image_id,
            related_entities: m
                .entity
                .related_entities
                .into_iter()
                .map(DisplayedEntity::from)
                .collect(),
            info: m
                .entity
                .best_info
                .into_iter()
                .map(|(name, span)| (name, EntitySnippet::from_span(&span, 150)))
                .map(|(name, mut snippet)| {
                    for f in snippet.fragments.iter_mut() {
                        if let Some(formatted) = maybe_prettify_entity_date(f.text()) {
                            *f.text_mut() = formatted;
                        }
                    }

                    (name, snippet)
                })
                .collect(),
            match_score: m.score,
        }
    }
}

fn maybe_prettify_entity_date(value: &str) -> Option<String> {
    let parse_ymd = |date| NaiveDate::parse_from_str(date, "%Y %-m %-d");

    if let Ok(date) = parse_ymd(value.trim()) {
        return Some(date.format("%d/%m/%Y").to_string());
    }

    // the dates are reversed from the parser, so we parse the second date first
    if let Some((y2, m2, d2, y1, m1, d1)) = value.split_whitespace().collect_tuple() {
        if let (Ok(fst_date), Ok(snd_date)) = (
            parse_ymd(&[y1, m1, d1].join(" ")),
            parse_ymd(&[y2, m2, d2].join(" ")),
        ) {
            return Some(format!(
                "{} - {}",
                fst_date.format("%d/%m/%Y"),
                snd_date.format("%d/%m/%Y"),
            ));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use entity_index::entity::Link;

    use super::*;

    #[test]
    fn simple_link_to_html() {
        assert_eq!(
            EntitySnippet::from_span(
                &Span {
                    text: "some text with a link".to_string(),
                    links: vec![Link {
                        start: 5,
                        end: 9,
                        target: "text article".to_string()
                    }]
                },
                10000
            )
            .to_md(None),
            "some [text](https://en.wikipedia.org/wiki/text_article) with a link".to_string()
        );
    }

    #[test]
    fn truncated_link_to_html() {
        assert_eq!(
            EntitySnippet::from_span(
                &Span {
                    text: "some text".to_string(),
                    links: vec![Link {
                        start: 5,
                        end: 9,
                        target: "text article".to_string()
                    }]
                },
                7
            )
            .to_md(None),
            "some [te](https://en.wikipedia.org/wiki/text_article)...".to_string()
        );
    }

    #[test]
    fn einstein_date() {
        assert_eq!(
            maybe_prettify_entity_date("1879 3 14 ").as_deref(),
            Some("14/03/1879")
        );
    }

    #[test]
    fn entity_date_span_prettify() {
        assert_eq!(
            maybe_prettify_entity_date(" 1999 5 27 1879 3 14  ").as_deref(),
            Some("14/03/1879 - 27/05/1999")
        );
    }
}
