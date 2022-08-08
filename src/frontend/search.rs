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
}

const MAX_PRETTY_URL_LEN: usize = 50;
const MAX_TITLE_LEN: usize = 50;

impl From<RetrievedWebpage> for DisplayedWebpage {
    fn from(webpage: RetrievedWebpage) -> Self {
        let url: Url = webpage.url.clone().into();
        let domain = url.domain().to_string();
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
