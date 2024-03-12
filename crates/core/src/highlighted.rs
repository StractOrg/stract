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

use utoipa::ToSchema;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum HighlightedKind {
    Normal,
    Highlighted,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HighlightedFragment {
    pub kind: HighlightedKind,
    pub text: String,
}

impl HighlightedFragment {
    pub fn new_unhighlighted(text: String) -> Self {
        Self::new_normal(text)
    }

    pub fn new_normal(text: String) -> Self {
        Self {
            kind: HighlightedKind::Normal,
            text,
        }
    }

    pub fn new_highlighted(text: String) -> Self {
        Self {
            kind: HighlightedKind::Highlighted,
            text,
        }
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}
