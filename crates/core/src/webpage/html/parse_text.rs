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

use whatlang::Lang;

use crate::webpage::just_text::{JustText, Paragraph};

use super::Html;

impl Html {
    pub fn parse_text(&mut self) {
        let paragraphs = JustText::paragraphs(self.root.clone());

        self.lang = paragraphs
            .iter()
            .max_by_key(|paragraph| paragraph.text.len())
            .and_then(|paragraph| {
                whatlang::detect(&paragraph.text).and_then(|info| {
                    if info.is_reliable() && info.confidence() > 0.95 {
                        Some(info.lang())
                    } else {
                        None
                    }
                })
            });

        self.all_text = Html::calculate_all_text(&paragraphs, &self.lang.unwrap_or(Lang::Eng));
        self.clean_text = Html::calculate_clean_text(&paragraphs, &self.lang.unwrap_or(Lang::Eng));
    }

    fn calculate_clean_text(paragraphs: &[Paragraph], lang: &Lang) -> Option<String> {
        let text = JustText::default().extract_from_paragraphs(paragraphs, lang);

        if text.is_empty() {
            None
        } else {
            Some(text)
        }
    }
    fn calculate_all_text(paragraphs: &[Paragraph], lang: &Lang) -> Option<String> {
        let text = JustText {
            max_link_density: 20.0,
            length_low: 0,
            length_high: 0,
            stopwords_low: -1.0,
            stopwords_high: -1.0,
            max_heading_distance: 10000,
        }
        .extract_from_paragraphs(paragraphs, lang);

        if text.is_empty() {
            None
        } else {
            Some(text)
        }
    }
}
