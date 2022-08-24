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

use std::{collections::HashMap, sync::Arc};

use axum::{extract, response::IntoResponse, Extension, Json};
use serde::Serialize;

use super::State;

fn highlight(query: &str, suggestion: &str) -> String {
    let idx = suggestion
        .chars()
        .zip(query.chars())
        .position(|(suggestion_char, query_char)| suggestion_char != query_char)
        .unwrap_or(query.len());

    let mut new_suggestion: String = suggestion.chars().take(idx).collect();
    new_suggestion += "<b>";
    new_suggestion += suggestion.chars().skip(idx).collect::<String>().as_str();
    new_suggestion += "</b>";
    new_suggestion
}

#[derive(Serialize)]
struct Suggestion {
    highlighted: String,
    raw: String,
}

pub async fn route(
    extract::Query(params): extract::Query<HashMap<String, String>>,
    Extension(state): Extension<Arc<State>>,
) -> impl IntoResponse {
    if let Some(query) = params.get("q") {
        let mut suggestions = Vec::new();

        for suggestion in state.autosuggest.suggestions(query).unwrap() {
            let highlighted = highlight(query, &suggestion);
            suggestions.push(Suggestion {
                highlighted,
                raw: suggestion,
            });
        }

        Json(suggestions)
    } else {
        Json(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suffix_highlight() {
        assert_eq!(&highlight("", "test"), "<b>test</b>");
        assert_eq!(&highlight("t", "test"), "t<b>est</b>");
        assert_eq!(&highlight("te", "test"), "te<b>st</b>");
        assert_eq!(&highlight("tes", "test"), "tes<b>t</b>");
        assert_eq!(&highlight("test", "test"), "test<b></b>");
    }
}
