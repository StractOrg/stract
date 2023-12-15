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
use crate::{enum_map::EnumSet, Result};

use super::Html;

#[allow(clippy::enum_variant_names)]
pub enum Microformat {
    HCard,
    HEvent,
    HEntry,
    HRecipe,
    HReview,
    HProduct,
}

pub const ALL_MICROFORMATS: [Microformat; 6] = [
    Microformat::HCard,
    Microformat::HEvent,
    Microformat::HEntry,
    Microformat::HRecipe,
    Microformat::HReview,
    Microformat::HProduct,
];

impl Microformat {
    pub fn as_str(&self) -> &str {
        match self {
            Microformat::HCard => "h-card",
            Microformat::HEvent => "h-event",
            Microformat::HEntry => "h-entry",
            Microformat::HRecipe => "h-recipe",
            Microformat::HReview => "h-review",
            Microformat::HProduct => "h-product",
        }
    }
}

impl From<Microformat> for usize {
    fn from(value: Microformat) -> Self {
        match value {
            Microformat::HCard => 0,
            Microformat::HEvent => 1,
            Microformat::HEntry => 2,
            Microformat::HRecipe => 3,
            Microformat::HReview => 4,
            Microformat::HProduct => 5,
        }
    }
}

impl TryFrom<usize> for Microformat {
    type Error = anyhow::Error;

    fn try_from(value: usize) -> Result<Self> {
        match value {
            0 => Ok(Microformat::HCard),
            1 => Ok(Microformat::HEvent),
            2 => Ok(Microformat::HEntry),
            3 => Ok(Microformat::HRecipe),
            4 => Ok(Microformat::HReview),
            5 => Ok(Microformat::HProduct),
            _ => Err(anyhow::anyhow!("Unknown microformat")),
        }
    }
}

impl Html {
    pub fn microformats(&self) -> EnumSet<Microformat> {
        let mut microformats = EnumSet::new();

        for node in self.root.inclusive_descendants() {
            if let Some(element) = node.as_element() {
                if let Some(class) = element.attributes.borrow().get("class") {
                    for microformat in ALL_MICROFORMATS {
                        if class.to_lowercase().as_str() == microformat.as_str() {
                            microformats.insert(microformat);
                        }
                    }
                }
            }
        }

        microformats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn microformats() {
        let html = Html::parse(
            r#"
            <html>
                <head>
                </head>
                <body>
                    <article class="h-entry">
                        <h1 class="p-name">Microformats are amazing</h1>
                        <p class="e-content">This is the content of the article</p>
                        <a class="u-url" href="https://example.com/microformats">Permalink</a>
                        <a class="u-author" href="https://example.com">Author</a>
                        <p class="search-product">substrings should not match</p>
                        <time class="dt-published" datetime="2021-01-01T00:00:00+00:00">2021-01-01</time>
                    </article>

                    <div class="h-RECIPE">
                        For some reason this site also has a recipe
                    </div>
                </body>
            </html>
            "#,
            "https://www.example.com/",
        ).unwrap();

        let microformats = html.microformats();

        assert!(microformats.contains(Microformat::HEntry));
        assert!(microformats.contains(Microformat::HRecipe));
        assert!(!microformats.contains(Microformat::HCard));
        assert!(!microformats.contains(Microformat::HProduct));
    }
}
