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

use super::Html;

impl Html {
    pub fn h1(&self) -> impl Iterator<Item = String> {
        self.root
            .select("h1")
            .expect("css selector should be valid")
            .map(|node| node.as_node().text_contents().trim().to_string())
    }

    pub fn h2(&self) -> impl Iterator<Item = String> {
        self.root
            .select("h2")
            .expect("css selector should be valid")
            .map(|node| node.as_node().text_contents().trim().to_string())
    }

    pub fn h3(&self) -> impl Iterator<Item = String> {
        self.root
            .select("h3")
            .expect("css selector should be valid")
            .map(|node| node.as_node().text_contents().trim().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    #[test]
    fn test_h1() {
        let html = Html::parse("<h1>Hello</h1><h2>World</h2>", "https://example.com").unwrap();
        assert_eq!(html.h1().collect_vec(), ["Hello"]);
    }

    #[test]
    fn test_h2() {
        let html = Html::parse("<h1>Hello</h1><h2>World</h2>", "https://example.com").unwrap();
        assert_eq!(html.h2().collect_vec(), ["World"]);
    }

    #[test]
    fn test_h3() {
        let html = Html::parse(
            "<h1>Hello</h1><h2>World</h2><h3>!</h3>",
            "https://example.com",
        )
        .unwrap();
        assert_eq!(html.h3().collect_vec(), ["!"]);
    }
}
