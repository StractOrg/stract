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

use super::{script::Script, Token};

/// A segment is a part of a text where the entire segment has the same script and langage.
#[derive(Clone)]
pub struct Segment<'a> {
    full_text: &'a str,
    span: std::ops::Range<usize>,
    script: Script,
}

impl<'a> Segment<'a> {
    pub fn text(&self) -> &'a str {
        &self.full_text[self.span.clone()]
    }

    pub fn tokenize(&self) -> impl Iterator<Item = Token<'a>> + 'a {
        let offset = self.span.start;
        let script = self.script;

        script
            .tokenizer()
            .tokenize(self.text())
            .map(move |mut token| {
                token.offset(offset);
                token
            })
    }
}

pub trait Segmenter {
    fn segments(&self) -> SegmentIterator;
}

impl Segmenter for str {
    fn segments(&self) -> SegmentIterator<'_> {
        SegmentIterator::new(self)
    }
}

impl Segmenter for String {
    fn segments(&self) -> SegmentIterator<'_> {
        SegmentIterator::new(self)
    }
}

pub struct SegmentIterator<'a> {
    prev_end: usize,
    input: &'a str,
}

impl<'a> SegmentIterator<'a> {
    pub fn new(input: &'a str) -> Self {
        Self { input, prev_end: 0 }
    }
}

impl<'a> Iterator for SegmentIterator<'a> {
    type Item = Segment<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.prev_end >= self.input.len() {
            return None;
        }

        let start = self.prev_end;
        let mut end = start;
        let mut script = None;

        while end < self.input.len() {
            let c = self.input[end..].chars().next().unwrap();
            let next_script = Script::from(c);

            if let Some(script) = &script {
                if &next_script != script {
                    break;
                }
            } else {
                script = Some(next_script);
            }

            end += c.len_utf8();
        }

        self.prev_end = end;

        Some(Segment {
            script: script.unwrap_or_default(),
            full_text: self.input,
            span: start..end,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_segments() {
        let txt = "Hello, world! This is a test.";
        let segments: Vec<_> = txt.segments().collect();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].text(), txt);
        assert_eq!(segments[0].script, Script::Latin);

        let txt = "こんにちは、世界！";
        let segments: Vec<_> = txt.segments().collect();

        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].text(), txt);
        assert_eq!(segments[0].script, Script::Other);

        let txt = "Hello, こんにちは、世界！";
        let segments: Vec<_> = txt.segments().collect();

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].text(), "Hello, ");
        assert_eq!(segments[0].script, Script::Latin);
        assert_eq!(segments[1].text(), "こんにちは、世界！");
        assert_eq!(segments[1].script, Script::Other);
    }

    proptest! {
        #[test]
        fn proptest_byte_offsets(txt in ".*") {
            for segment in txt.segments() {
                assert!(!segment.text().is_empty());
            }
        }
    }
}
