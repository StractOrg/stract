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

use std::sync::Arc;

use super::{HtmlTemplate, State};
use askama::Template;
use axum::{extract, response::IntoResponse};

pub const DEFAULT_OPTICS: [OpticLink; 7] = [
    OpticLink {
        name: "Copycats removal",
        url:
            "https://raw.githubusercontent.com/StractOrg/sample-optics/main/copycats_removal.optic",
        description: "Remove common copycat websites from search results.",
    },
    OpticLink {
        name: "Hacker News",
        url: "https://raw.githubusercontent.com/StractOrg/sample-optics/main/hacker_news.optic",
        description: "Only return results from websites that are popular on Hacker News.",
    },
    OpticLink {
        name: "Discussions",
        url: "https://raw.githubusercontent.com/StractOrg/sample-optics/main/discussions.optic",
        description: "Only return results from forums or similar types of QA pages.",
    },
    OpticLink {
        name: "10K Short",
        url: "https://raw.githubusercontent.com/StractOrg/sample-optics/main/10k_short.optic",
        description: "Remove the top 10,000 most popular websites from search results.",
    },
    OpticLink {
        name: "Indieweb & blogroll",
        url: "https://raw.githubusercontent.com/StractOrg/sample-optics/main/indiweb_blogroll.optic",
        description: "Search only in the indieweb + a list of blogs from blogroll.org and some hand-picked blogs from hackernews.",
    },
    OpticLink {
        name: "Devdocs",
        url: "https://raw.githubusercontent.com/StractOrg/sample-optics/main/devdocs.optic",
        description: "Only return results from some of the developer documentation sites listed on devdocs.io. This is a non-exhaustive list.",
    },
    OpticLink {
        name: "Academic",
        url: "https://raw.githubusercontent.com/StractOrg/sample-optics/main/academic.optic",
        description: "Search exclusively in academic sites (.edu, .ac.uk, arxiv.org etc.). This is a non-exhaustive list.",
    },
];

#[derive(Debug, Clone)]
pub struct OpticLink {
    pub name: &'static str,
    pub url: &'static str,
    pub description: &'static str,
}

#[allow(clippy::unused_async)]
pub async fn route(extract::State(state): extract::State<Arc<State>>) -> impl IntoResponse {
    let template = OpticsTemplate {
        default_optics: DEFAULT_OPTICS.to_vec(),
        with_alice: state.config.with_alice,
    };
    HtmlTemplate(template)
}

#[derive(Template)]
#[template(path = "settings/index.html")]
struct OpticsTemplate {
    default_optics: Vec<OpticLink>,
    with_alice: Option<bool>,
}
