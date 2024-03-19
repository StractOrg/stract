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

use crate::query;
use crate::widgets::{Widget, Widgets};

pub struct WidgetManager {
    widgets: Widgets,
}

impl WidgetManager {
    pub fn new(widgets: Widgets) -> WidgetManager {
        Self { widgets }
    }

    pub async fn widget(&self, query: &str) -> Option<Widget> {
        let parsed_terms = query::parser::parse(query).ok()?;

        self.widgets.widget(
            parsed_terms
                .into_iter()
                .filter_map(|term| {
                    if let query::parser::Term::SimpleOrPhrase(
                        query::parser::SimpleOrPhrase::Simple(simple),
                    ) = term
                    {
                        Some(String::from(simple))
                    } else {
                        None
                    }
                })
                .join(" ")
                .as_str(),
        )
    }
}
