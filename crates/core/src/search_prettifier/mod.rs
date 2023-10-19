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

mod entity;
mod stack_overflow;

use std::collections::HashMap;

use chrono::{NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use url::Url;
use utoipa::ToSchema;
use webpage::url_ext::UrlExt;

use crate::{
    inverted_index::RetrievedWebpage,
    ranking::{Signal, SignalScore},
    snippet::TextSnippet,
    spell::{self, CorrectionTerm},
};

pub use self::stack_overflow::{create_stackoverflow_sidebar, CodeOrText};
pub use entity::DisplayedEntity;

pub use self::stack_overflow::{stackoverflow_snippet, StackOverflowAnswer, StackOverflowQuestion};

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Snippet {
    Normal {
        date: Option<String>,
        text: TextSnippet,
    },
    StackOverflowQA {
        question: StackOverflowQuestion,
        answers: Vec<StackOverflowAnswer>,
    },
}

impl Snippet {
    pub fn text(&self) -> Option<&TextSnippet> {
        match self {
            Snippet::Normal { text, .. } => Some(text),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HighlightedSpellCorrection {
    pub raw: String,
    pub highlighted: String,
}

impl From<spell::Correction> for HighlightedSpellCorrection {
    fn from(correction: spell::Correction) -> Self {
        let mut highlighted = String::new();
        let mut raw = String::new();

        for term in correction.terms {
            match term {
                CorrectionTerm::Corrected(correction) => {
                    highlighted
                        .push_str(&("<b><i>".to_string() + correction.as_str() + "</i></b>"));
                    raw.push_str(&correction);
                }
                CorrectionTerm::NotCorrected(orig) => {
                    highlighted.push_str(&orig);
                    raw.push_str(&orig);
                }
            }

            raw.push(' ');
            highlighted.push(' ');
        }

        raw = raw.trim_end().to_string();
        highlighted = highlighted.trim_end().to_string();

        Self { raw, highlighted }
    }
}

fn prettify_url(url: &Url) -> String {
    let mut pretty_url = url.clone();
    pretty_url.set_query(None);

    let scheme = pretty_url.scheme().to_string();

    let mut pretty_url = pretty_url.to_string();

    if let Some(stripped) = pretty_url.strip_prefix((scheme.clone() + "://").as_str()) {
        pretty_url = stripped.to_string();
    }

    pretty_url = pretty_url.trim_end_matches('/').to_string();

    pretty_url = pretty_url.replace('/', " › ");
    pretty_url = scheme + "://" + pretty_url.as_str();

    pretty_url
}

fn prettify_date(date: NaiveDateTime) -> String {
    let current_time = Utc::now().naive_utc();
    let diff = current_time.signed_duration_since(date);

    let num_hours = diff.num_hours() + 1;
    if num_hours < 24 {
        if num_hours <= 1 {
            return "1 hour ago".to_string();
        }

        return format!("{num_hours} hours ago");
    }

    let num_days = diff.num_days();
    if num_days < 30 {
        if num_days <= 1 {
            return "1 day ago".to_string();
        }

        return format!("{num_days} days ago");
    }

    format!("{}", date.format("%d. %b. %Y"))
}

fn generate_snippet(webpage: &RetrievedWebpage) -> Snippet {
    let last_updated = webpage.updated_time.map(prettify_date);

    let url = Url::parse(&webpage.url).unwrap();

    if url.root_domain().unwrap_or_default() == "stackoverflow.com"
        && webpage
            .schema_org
            .iter()
            .any(|item| item.types_contains("QAPage"))
    {
        if let Ok(snippet) = stackoverflow_snippet(webpage) {
            return snippet;
        }
    }

    Snippet::Normal {
        date: last_updated,
        text: webpage.snippet.clone(),
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DisplayedWebpage {
    pub title: String,
    pub url: String,
    pub site: String,
    pub domain: String,
    pub host: String,
    pub pretty_url: String,
    pub snippet: Snippet,
    pub ranking_signals: Option<HashMap<Signal, SignalScore>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DisplayedAnswer {
    pub title: String,
    pub url: String,
    pub pretty_url: String,
    pub snippet: String,
    pub answer: String,
}

impl From<RetrievedWebpage> for DisplayedWebpage {
    fn from(webpage: RetrievedWebpage) -> Self {
        let snippet = generate_snippet(&webpage);

        let url = Url::parse(&webpage.url).unwrap();
        let domain = url.root_domain().unwrap_or_default().to_string();
        let host = url.host_str().unwrap_or_default().to_string();
        let pretty_url = prettify_url(&url);

        Self {
            title: webpage.title,
            site: url.host_str().unwrap_or_default().to_string(),
            url: webpage.url,
            pretty_url,
            domain,
            host,
            snippet,
            ranking_signals: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(tag = "type", content = "value", rename_all = "camelCase")]
pub enum DisplayedSidebar {
    Entity(DisplayedEntity),
    StackOverflow {
        title: String,
        answer: StackOverflowAnswer,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};

    #[test]
    fn prettify_date_in_hours() {
        let date = Utc::now().naive_utc();
        assert_eq!(prettify_date(date), "1 hour ago".to_string());

        let date = (Utc::now() - chrono::Duration::seconds(4000)).naive_utc();
        assert_eq!(prettify_date(date), "2 hours ago".to_string());
    }

    #[test]
    fn prettify_date_days() {
        let date = (Utc::now() - chrono::Duration::days(1)).naive_utc();
        assert_eq!(prettify_date(date), "1 day ago".to_string());

        let date = (Utc::now() - chrono::Duration::days(2)).naive_utc();
        assert_eq!(prettify_date(date), "2 days ago".to_string());
    }

    #[test]
    fn prettify_date_rest() {
        let date = DateTime::parse_from_rfc3339("1996-12-19T16:39:57-08:00")
            .unwrap()
            .naive_local();
        assert_eq!(prettify_date(date), "19. Dec. 1996".to_string());
    }
}
