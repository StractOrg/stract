// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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
mod schema_org;
mod stack_overflow;

use std::collections::HashMap;

use chrono::{NaiveDateTime, Utc};

use url::Url;
use utoipa::ToSchema;

#[cfg(feature = "return_body")]
use crate::api::search::ReturnBody;

use crate::{
    highlighted::HighlightedFragment,
    inverted_index::RetrievedWebpage,
    ranking::{SignalEnumDiscriminants, SignalScore},
    searcher::SearchQuery,
    snippet::TextSnippet,
    web_spell::{self, CorrectionTerm},
    webpage::url_ext::UrlExt,
};

pub use self::stack_overflow::{create_stackoverflow_sidebar, CodeOrText};
pub use entity::DisplayedEntity;
pub use schema_org::{OneOrManyProperty, OneOrManyString, Property, StructuredData};

pub use self::stack_overflow::{stackoverflow_snippet, StackOverflowAnswer, StackOverflowQuestion};

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone, ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct Snippet {
    pub date: Option<String>,
    pub text: TextSnippet,
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone, ToSchema,
)]
#[serde(tag = "_type", rename_all = "camelCase")]
pub enum RichSnippet {
    StackOverflowQA {
        question: StackOverflowQuestion,
        answers: Vec<StackOverflowAnswer>,
    },
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone, ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct HighlightedSpellCorrection {
    pub raw: String,
    pub highlighted: Vec<HighlightedFragment>,
}

impl From<web_spell::Correction> for HighlightedSpellCorrection {
    fn from(correction: web_spell::Correction) -> Self {
        let mut highlighted = Vec::new();
        let mut raw = String::new();

        for term in correction.terms {
            match term {
                CorrectionTerm::Corrected {
                    orig: _,
                    correction,
                } => {
                    let mut correction = correction.trim().to_string();
                    correction.push(' ');

                    highlighted.push(HighlightedFragment::new_highlighted(correction.clone()));
                    raw.push_str(&correction);
                }
                CorrectionTerm::NotCorrected(orig) => {
                    let mut orig = orig.trim().to_string();
                    orig.push(' ');

                    highlighted.push(HighlightedFragment::new_normal(orig.clone()));
                    raw.push_str(&orig);
                }
            }
        }

        raw = raw.trim_end().to_string();

        if let Some(last) = highlighted.last_mut() {
            last.text = last.text.trim_end().to_string();
        }

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

    Snippet {
        date: last_updated,
        text: webpage.snippet.clone(),
    }
}

fn generate_rich_snippet(webpage: &RetrievedWebpage) -> Option<RichSnippet> {
    let url = Url::parse(&webpage.url).unwrap();

    if url.root_domain().unwrap_or_default() == "stackoverflow.com"
        && webpage
            .schema_org
            .iter()
            .any(|item| item.types_contains("QAPage"))
    {
        if let Ok((question, answers)) = stackoverflow_snippet(webpage) {
            return Some(RichSnippet::StackOverflowQA { question, answers });
        }
    }

    None
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct DisplayedWebpage {
    pub title: String,
    pub url: String,
    pub site: String,
    pub domain: String,
    pub pretty_url: String,
    pub snippet: Snippet,
    #[cfg(feature = "return_body")]
    pub body: Option<String>,
    pub rich_snippet: Option<RichSnippet>,
    pub ranking_signals: Option<HashMap<SignalEnumDiscriminants, SignalScore>>,
    pub structured_data: Option<Vec<StructuredData>>,
    pub score: Option<f64>,
    pub likely_has_ads: bool,
    pub likely_has_paywall: bool,
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct DisplayedAnswer {
    pub title: String,
    pub url: String,
    pub pretty_url: String,
    pub snippet: String,
    pub answer: String,
}

impl DisplayedWebpage {
    pub fn new(webpage: RetrievedWebpage, query: &SearchQuery) -> Self {
        let snippet = generate_snippet(&webpage);
        let rich_snippet = generate_rich_snippet(&webpage);

        let url = Url::parse(&webpage.url).unwrap();
        let domain = url.root_domain().unwrap_or_default().to_string();
        let pretty_url = prettify_url(&url);

        let structured_data = if query.return_structured_data {
            Some(
                webpage
                    .schema_org
                    .into_iter()
                    .map(StructuredData::from)
                    .collect(),
            )
        } else {
            None
        };

        #[cfg(feature = "return_body")]
        let body = query.return_body.map(|r| match r {
            ReturnBody::All => webpage.body,
            ReturnBody::Truncated(n) => webpage.body.chars().take(n).collect::<String>(),
        });

        Self {
            title: webpage.title,
            site: url.normalized_host().unwrap_or_default().to_string(),
            url: webpage.url,
            pretty_url,
            domain,
            snippet,
            #[cfg(feature = "return_body")]
            body,
            ranking_signals: None,
            score: None,
            likely_has_ads: webpage.likely_has_ads,
            likely_has_paywall: webpage.likely_has_paywall,
            rich_snippet,
            structured_data,
        }
    }
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone, ToSchema,
)]
#[serde(tag = "_type", content = "value", rename_all = "camelCase")]
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
    use chrono::DateTime;

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
