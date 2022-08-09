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

use axum::Extension;
use chrono::{NaiveDateTime, Utc};

use crate::{index::RetrievedWebpage, webpage::Url};
use std::collections::HashMap;
use std::sync::Arc;

use super::{HtmlTemplate, State};
use askama::Template;
use axum::{extract, response::IntoResponse};

pub fn html_escape(s: &str) -> String {
    html_escape::decode_html_entities(s)
        .chars()
        .filter(|c| !matches!(c, '<' | '>' | '&'))
        .collect()
}

pub struct DisplayedWebpage {
    pub title: String,
    pub url: String,
    pub domain: String,
    pub pretty_url: String,
    pub snippet: String,
    pub body: String,
    pub primary_image_uuid: Option<String>,
    pub last_updated: Option<String>,
}

const MAX_PRETTY_URL_LEN: usize = 50;
const MAX_TITLE_LEN: usize = 50;

fn prettify_url(url: Url) -> String {
    let mut pretty_url = url.strip_query().to_string();

    if pretty_url.ends_with('/') {
        pretty_url = pretty_url.chars().take(pretty_url.len() - 1).collect();
    }

    let protocol = Url::from(pretty_url.clone()).protocol().to_string() + "://";
    pretty_url = Url::from(pretty_url.clone())
        .strip_protocol()
        .replace('/', " › ");
    pretty_url = protocol + &pretty_url;

    if pretty_url.len() > MAX_PRETTY_URL_LEN {
        pretty_url = pretty_url.chars().take(MAX_PRETTY_URL_LEN).collect();
        pretty_url += "...";
    }

    pretty_url
}

fn prettify_date(date: NaiveDateTime) -> String {
    let current_time = Utc::now().naive_utc();
    let diff = current_time.signed_duration_since(date);

    let num_hours = diff.num_hours() + 1;
    if num_hours < 24 {
        if num_hours <= 1 {
            return "1 hour ago".to_string();
        } else {
            return format!("{num_hours} hours ago");
        }
    }

    let num_days = diff.num_days();
    if num_days < 30 {
        if num_days <= 1 {
            return "1 day ago".to_string();
        } else {
            return format!("{num_days} days ago");
        }
    }

    format!("{}", date.format("%d. %b. %Y"))
}

impl From<RetrievedWebpage> for DisplayedWebpage {
    fn from(webpage: RetrievedWebpage) -> Self {
        let last_updated = webpage.updated_time.map(|date| prettify_date(date));

        let url: Url = webpage.url.clone().into();
        let domain = url.domain().to_string();
        let pretty_url = prettify_url(url);

        let mut title = html_escape(&webpage.title);

        if title.len() > MAX_TITLE_LEN {
            title = title.chars().take(MAX_TITLE_LEN).collect();
            title += "...";
        }

        Self {
            title,
            url: webpage.url,
            pretty_url,
            domain,
            snippet: webpage.snippet, // snippet has already been html-escaped.
            body: webpage.body,
            primary_image_uuid: webpage.primary_image_uuid,
            last_updated,
        }
    }
}

#[derive(Template)]
#[template(path = "search.html", escape = "none")]
struct SearchTemplate {
    search_result: Vec<DisplayedWebpage>,
    query: String,
}

pub async fn route(
    extract::Query(params): extract::Query<HashMap<String, String>>,
    Extension(state): Extension<Arc<State>>,
) -> impl IntoResponse {
    let mut search_result = Vec::new();
    let mut displayed_query = String::new();

    if let Some(query) = params.get("q") {
        displayed_query = query.clone();
        let result = state.searcher.search(query).expect("Search failed");

        search_result = result
            .documents
            .into_iter()
            .map(DisplayedWebpage::from)
            .collect();
    }

    let template = SearchTemplate {
        search_result,
        query: displayed_query,
    };
    HtmlTemplate(template)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::*;

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
