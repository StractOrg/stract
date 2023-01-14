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

use super::HtmlTemplate;
use askama::Template;
use axum::response::IntoResponse;

pub const DEFAULT_OPTICS: [OpticLink; 3] = [
    OpticLink {
        name: "Copycats removal",
        url: "https://raw.githubusercontent.com/Cuely/sample-optics/main/copycats_removal.optic",
    },
    OpticLink {
        name: "Hacker News",
        url: "https://raw.githubusercontent.com/Cuely/sample-optics/main/hacker_news.optic",
    },
    OpticLink {
        name: "Discussions",
        url: "https://raw.githubusercontent.com/Cuely/sample-optics/main/discussions.optic",
    },
];

#[derive(Debug, Clone)]
pub struct OpticLink {
    pub name: &'static str,
    pub url: &'static str,
}

#[allow(clippy::unused_async)]
pub async fn route() -> impl IntoResponse {
    let template = OpticsTemplate {
        default_optics: DEFAULT_OPTICS.to_vec(),
    };
    HtmlTemplate(template)
}

#[derive(Template)]
#[template(path = "settings/index.html")]
struct OpticsTemplate {
    default_optics: Vec<OpticLink>,
}
