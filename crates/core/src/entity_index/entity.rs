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

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug)]
pub struct Entity {
    pub is_disambiguation: bool,
    pub title: String,
    pub page_abstract: Span,
    pub info: Vec<(String, Span)>,
    pub image: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    pub text: String,
    pub links: Vec<Link>,
}

impl Span {
    #[cfg(test)]
    pub fn new(text: &str) -> Self {
        Self {
            text: text.to_string(),
            links: vec![],
        }
    }

    pub fn merge(&mut self, other_span: Span) {
        let orig_end = self.text.len();
        self.text.push_str(&other_span.text);

        for link in other_span.links {
            self.links.push(Link {
                start: orig_end + link.start,
                end: orig_end + link.end,
                target: link.target,
            });
        }
    }

    pub fn add_text(&mut self, text: &str) {
        self.text.push_str(text);
    }

    pub fn add_link(&mut self, text: &str, target: String) {
        let link = Link {
            target,
            start: self.text.len(),
            end: self.text.len() + text.len(),
        };
        self.links.push(link);
        self.text.push_str(text);
    }

    pub fn trim_end(&mut self) {
        self.text = self.text.trim_end().to_string();

        while let Some(last_link) = self.links.last() {
            if last_link.end > self.text.len() {
                self.links.pop();
            } else {
                break;
            }
        }
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct Link {
    pub start: usize,
    pub end: usize,
    pub target: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum EntitySnippetFragment {
    Normal { text: String },
    Link { text: String, href: String },
}

#[derive(Default, Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EntitySnippet {
    pub fragments: Vec<EntitySnippetFragment>,
}

impl EntitySnippetFragment {
    pub fn text(&self) -> &str {
        match self {
            EntitySnippetFragment::Normal { text } | EntitySnippetFragment::Link { text, .. } => {
                text
            }
        }
    }
    pub fn text_mut(&mut self) -> &mut String {
        match self {
            EntitySnippetFragment::Normal { text } | EntitySnippetFragment::Link { text, .. } => {
                text
            }
        }
    }
}

impl EntitySnippet {
    pub fn from_span(span: &Span, truncate_to: usize) -> Self {
        let (s, maybe_ellipsis) = if span.text.len() > truncate_to {
            let mut truncate_to = truncate_to;
            while !span.text.is_char_boundary(truncate_to) {
                truncate_to -= 1;
            }
            (&span.text[0..truncate_to], "...")
        } else {
            (&*span.text, "")
        };

        let mut last_end = 0;
        let mut fragments = span
            .links
            .iter()
            .filter(|link| s.len() > link.start)
            .flat_map(|link| {
                let end = link.end.min(s.len());
                let split = std::mem::replace(&mut last_end, end);
                [
                    EntitySnippetFragment::Normal {
                        text: s[split..link.start].to_string(),
                    },
                    EntitySnippetFragment::Link {
                        text: s[link.start..end].to_string(),
                        href: format!(
                            "https://en.wikipedia.org/wiki/{}",
                            link.target.replace(' ', "_"),
                        ),
                    },
                ]
            })
            .filter(|s| !s.text().is_empty())
            .collect_vec();

        let remainder = s[last_end..].to_string() + maybe_ellipsis;

        if !remainder.is_empty() {
            match fragments.last_mut() {
                Some(EntitySnippetFragment::Normal { text }) => *text += &remainder,
                _ => fragments.push(EntitySnippetFragment::Normal { text: remainder }),
            }
        }

        EntitySnippet { fragments }
    }

    #[cfg(test)]
    pub fn to_md(&self, strip_href_prefix: Option<&str>) -> String {
        self.fragments
            .iter()
            .map(|s| match (s, strip_href_prefix) {
                (EntitySnippetFragment::Normal { text }, _) => text.clone(),
                (EntitySnippetFragment::Link { text, href }, Some(prefix))
                    if &href.trim_start_matches(prefix).replace('_', " ") == text =>
                {
                    format!("[[{}]]", text)
                }
                (EntitySnippetFragment::Link { text, href }, Some(prefix)) => {
                    format!("[{}]({})", text, href.trim_start_matches(prefix))
                }
                (EntitySnippetFragment::Link { text, href }, None) => {
                    format!("[{}]({href})", text)
                }
            })
            .join("")
    }
}
