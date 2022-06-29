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

use std::collections::{HashMap, HashSet};

use logos::{Lexer, Logos};
use whatlang::Lang;

use super::lexer::Token;

// implementation of the JustText algorithm described in this thesis: https://is.muni.cz/th/45523/fi_d/phdthesis.pdf
// reference implementation: https://github.com/miso-belica/jusText/blob/main/justext/core.py

const MAX_LINK_DENSITY_DEFAULT: f64 = 0.2;
const LENGTH_LOW_DEFAULT: usize = 70;
const LENGTH_HIGH_DEFAULT: usize = 200;
const STOPWORDS_LOW_DEFAULT: f64 = 0.30;
const STOPWORDS_HIGH_DEFAULT: f64 = 0.32;
const MAX_HEADING_DISTANCE_DEFAULT: usize = 200;

#[derive(Debug, Clone)]
enum IntermediateClassification {
    Good,
    NearGood,
    Short,
    Bad,
}

#[derive(Debug)]
enum Classification {
    Good,
    Bad,
}

pub struct JustText {
    max_link_density: f64,
    length_low: usize,
    length_high: usize,
    stopwords_low: f64,
    stopwords_high: f64,
    max_heading_distance: usize,

    stopwords: HashMap<Lang, HashSet<String>>,
}

impl Default for JustText {
    fn default() -> Self {
        // TODO: make this static and oncecell
        let mut stopwords = HashMap::new();

        stopwords.insert(
            Lang::Eng,
            include_str!("../../stopwords/English.txt")
                .lines()
                .map(|s| s.to_lowercase())
                .collect(),
        );

        Self {
            max_link_density: MAX_LINK_DENSITY_DEFAULT,
            length_low: LENGTH_LOW_DEFAULT,
            length_high: LENGTH_HIGH_DEFAULT,
            stopwords_low: STOPWORDS_LOW_DEFAULT,
            stopwords_high: STOPWORDS_HIGH_DEFAULT,
            max_heading_distance: MAX_HEADING_DISTANCE_DEFAULT,
            stopwords,
        }
    }
}

#[derive(Clone, Debug)]
struct Paragraph {
    is_heading: bool,
    tags_count: usize,
    chars_count_in_links: usize,
    text: String,
    last_was_whitespace: bool,
}

impl Paragraph {
    fn new() -> Self {
        Self {
            tags_count: 0,
            chars_count_in_links: 0,
            is_heading: false,
            text: String::new(),
            last_was_whitespace: false,
        }
    }

    fn contains_text(&self) -> bool {
        !self.text.is_empty()
    }

    fn append_text(&mut self, text: &str) -> usize {
        let all_whitespace = text
            .chars()
            .all(|c| c.is_whitespace() || c == '\n' || c == '\r');
        if self.last_was_whitespace && all_whitespace {
            return 0;
        }

        self.last_was_whitespace = all_whitespace;

        self.text.push_str(text);

        text.len()
    }

    fn link_density(&self) -> f64 {
        self.chars_count_in_links as f64 / self.text.len() as f64
    }
}

#[derive(Debug)]
struct ClassifiedParagraph {
    paragraph: Paragraph,
    classification: Classification,
}

#[derive(Debug)]
struct IntermediateClassifiedParagraph {
    paragraph: Paragraph,
    classification: IntermediateClassification,
}

// preprocessor removes scripts, comments, style, embed, forms and head.
struct Preprocessor<const N: usize> {
    removed_tags: [&'static str; N],
    num_open_tags: [usize; N],
}

impl<const N: usize> Preprocessor<N> {
    fn new(removed_tags: [&'static str; N]) -> Self {
        Self {
            removed_tags,
            num_open_tags: [0; N],
        }
    }

    fn update(&mut self, tok: &Token) {
        match tok {
            Token::StartTag(tag) => {
                if let Some((_, n)) = self
                    .removed_tags
                    .iter()
                    .zip(self.num_open_tags.iter_mut())
                    .find(|(name, _)| **name == tag.name())
                {
                    *n += 1;
                }
            }
            Token::EndTag(tag) => {
                if let Some((_, n)) = self
                    .removed_tags
                    .iter()
                    .zip(self.num_open_tags.iter_mut())
                    .find(|(name, _)| **name == tag.name())
                {
                    *n -= 1;
                }
            }
            Token::SelfTerminatingTag(_) | Token::Error => {}
        }
    }

    fn is_inside_removed(&self) -> bool {
        self.num_open_tags.iter().any(|n| *n > 0)
    }
}

impl JustText {
    fn paragraphs<'a>(&self, mut tokens: Lexer<'a, Token<'a>>, raw: &str) -> Vec<Paragraph> {
        let mut res = Vec::new();

        let mut preprocessor = Preprocessor::new(["script", "style", "embed", "form", "head"]);

        let mut br = false;
        let mut link = false;
        let mut paragraph = Paragraph::new();

        let mut heading_count = 0;

        while let Some(tok) = tokens.next() {
            preprocessor.update(&tok);

            if preprocessor.is_inside_removed() {
                continue;
            }

            match tok {
                Token::StartTag(tag) => {
                    if matches!(tag.name(), "h1" | "h2" | "h3" | "h4" | "h5" | "h6") {
                        heading_count += 1;
                    }

                    if matches!(
                        tag.name(),
                        "body"
                            | "blockquote"
                            | "caption"
                            | "center"
                            | "col"
                            | "colgroup"
                            | "dd"
                            | "div"
                            | "dl"
                            | "dt"
                            | "fieldset"
                            | "form"
                            | "legend"
                            | "optgroup"
                            | "option"
                            | "p"
                            | "pre"
                            | "table"
                            | "td"
                            | "textarea"
                            | "tfoot"
                            | "th"
                            | "thead"
                            | "tr"
                            | "ul"
                            | "li"
                            | "h1"
                            | "h2"
                            | "h3"
                            | "h4"
                            | "h5"
                            | "h6"
                    ) || (tag.name() == "br" && br)
                    {
                        if tag.name() == "br" {
                            // the <br><br> is a paragraph separator and should
                            // not be included in the number of tags within the
                            // paragraph
                            paragraph.tags_count -= 1;
                        }

                        paragraph.is_heading = heading_count > 0;

                        if paragraph.contains_text() {
                            res.push(paragraph);
                        }

                        paragraph = Paragraph::new();
                    } else {
                        br = tag.name() == "br";
                        if br {
                            paragraph.append_text(" ");
                        } else if tag.name() == "a" {
                            link = true;
                        }
                        paragraph.tags_count += 1;
                    }
                }
                Token::EndTag(tag) => {
                    if matches!(tag.name(), "h1" | "h2" | "h3" | "h4" | "h5" | "h6") {
                        heading_count -= 1;
                    }

                    paragraph.append_text(" ");

                    match tag.name() {
                        "body" | "blockquote" | "caption" | "center" | "col" | "colgroup"
                        | "dd" | "div" | "dl" | "dt" | "fieldset" | "form" | "legend"
                        | "optgroup" | "option" | "p" | "pre" | "table" | "td" | "textarea"
                        | "tfoot" | "th" | "thead" | "tr" | "ul" | "li" | "h1" | "h2" | "h3"
                        | "h4" | "h5" | "h6" => {
                            if paragraph.contains_text() {
                                res.push(paragraph);
                            }

                            paragraph = Paragraph::new();
                        }
                        _ => {}
                    }

                    if tag.name() == "a" {
                        link = false;
                    }
                }
                Token::Error => {
                    let span = tokens.span();
                    let chars_added = paragraph.append_text(&raw[span]);

                    if link {
                        paragraph.chars_count_in_links += chars_added;
                    }

                    br = false;
                }
                Token::SelfTerminatingTag(_) => {}
            }
        }

        if paragraph.contains_text() {
            res.push(paragraph);
        }

        res
    }

    fn calculate_stopword_density(&self, paragraph: &Paragraph) -> f64 {
        let lang = whatlang::detect_lang(&paragraph.text).unwrap_or(Lang::Eng);
        let stopwords = self
            .stopwords
            .get(&lang)
            .unwrap_or_else(|| self.stopwords.get(&Lang::Eng).unwrap());

        paragraph
            .text
            .split_whitespace()
            .filter(|word| stopwords.contains(&word.to_lowercase()))
            .count() as f64
            / paragraph.text.split_whitespace().count() as f64
    }

    fn initial_classification(
        &self,
        paragraphs: Vec<Paragraph>,
    ) -> Vec<IntermediateClassifiedParagraph> {
        // TODO: re-use paragraphs to save allocations
        let mut res = Vec::new();

        for paragraph in paragraphs {
            let classification = {
                let stopword_density: f64 = self.calculate_stopword_density(&paragraph);

                if paragraph.link_density() > self.max_link_density
                    || paragraph.text.contains("\\xa9")
                    || paragraph.text.contains("&copy")
                {
                    IntermediateClassification::Bad
                } else if paragraph.text.len() < self.length_low {
                    if paragraph.chars_count_in_links > 0 {
                        IntermediateClassification::Bad
                    } else {
                        IntermediateClassification::Short
                    }
                } else if stopword_density >= self.stopwords_high {
                    if paragraph.text.len() > self.length_high {
                        IntermediateClassification::Good
                    } else {
                        IntermediateClassification::NearGood
                    }
                } else if stopword_density >= self.stopwords_low {
                    IntermediateClassification::NearGood
                } else {
                    IntermediateClassification::Bad
                }
            };

            res.push(IntermediateClassifiedParagraph {
                paragraph,
                classification,
            });
        }

        res
    }

    fn contextual_classification(
        &self,
        mut paragraphs: Vec<IntermediateClassifiedParagraph>,
    ) -> Vec<ClassifiedParagraph> {
        // good headings
        let num_paragraphs = paragraphs.len();
        for i in 0..num_paragraphs {
            let paragraph = &paragraphs[i];
            if !(paragraph.paragraph.is_heading && paragraph.classification.is_short()) {
                continue;
            }

            let mut j = i + 1;
            let mut distance = 0;
            while j < num_paragraphs && distance < self.max_heading_distance {
                if matches!(
                    paragraphs[j].classification,
                    IntermediateClassification::Good
                ) {
                    let paragraph = &mut paragraphs[i];
                    paragraph.classification = IntermediateClassification::NearGood;
                }
                distance += paragraphs[j].paragraph.text.len();
                j += 1;
            }
        }

        // classify short
        let mut new_classes: Vec<_> = paragraphs
            .iter()
            .map(|par| par.classification.clone())
            .collect();

        for i in 0..num_paragraphs {
            if !matches!(
                paragraphs[i].classification,
                IntermediateClassification::Short
            ) {
                continue;
            }

            let prev_neighbour = Self::get_prev_neighbour(&paragraphs, i, true);
            let next_neighbour = Self::get_next_neighbour(&paragraphs, i, true);

            if matches!(prev_neighbour, IntermediateClassification::Good)
                && matches!(next_neighbour, IntermediateClassification::Good)
            {
                new_classes[i] = IntermediateClassification::Good;
            } else if matches!(prev_neighbour, IntermediateClassification::Bad)
                && matches!(next_neighbour, IntermediateClassification::Bad)
            {
                new_classes[i] = IntermediateClassification::Bad;
            } else if (matches!(prev_neighbour, IntermediateClassification::Bad)
                && matches!(
                    Self::get_prev_neighbour(&paragraphs, i, false),
                    IntermediateClassification::NearGood
                ))
                || (matches!(next_neighbour, IntermediateClassification::Bad)
                    && matches!(
                        Self::get_next_neighbour(&paragraphs, i, false),
                        IntermediateClassification::NearGood
                    ))
            {
                new_classes[i] = IntermediateClassification::Good;
            } else {
                new_classes[i] = IntermediateClassification::Bad;
            }
        }

        for (paragraph, classification) in paragraphs.iter_mut().zip(new_classes.into_iter()) {
            paragraph.classification = classification;
        }

        // revise neargood
        for i in 0..num_paragraphs {
            if !matches!(
                paragraphs[i].classification,
                IntermediateClassification::NearGood
            ) {
                continue;
            }

            let prev_neighbour = Self::get_prev_neighbour(&paragraphs, i, true);
            let next_neighbour = Self::get_next_neighbour(&paragraphs, i, true);

            let mut paragraph = &mut paragraphs[i];

            match (prev_neighbour, next_neighbour) {
                (IntermediateClassification::Bad, IntermediateClassification::Bad) => {
                    paragraph.classification = IntermediateClassification::Bad;
                }
                _ => {
                    paragraph.classification = IntermediateClassification::Good;
                }
            }
        }

        let mut res = Vec::new();
        for paragraph in &paragraphs {
            res.push(ClassifiedParagraph {
                paragraph: paragraph.paragraph.clone(),
                classification: match paragraph.classification {
                    IntermediateClassification::Good => Classification::Good,
                    IntermediateClassification::NearGood => Classification::Good,
                    IntermediateClassification::Short => Classification::Bad,
                    IntermediateClassification::Bad => Classification::Bad,
                },
            })
        }

        res
    }

    fn get_prev_neighbour(
        paragraphs: &[IntermediateClassifiedParagraph],
        idx: usize,
        ignore_neargood: bool,
    ) -> IntermediateClassification {
        Self::get_neighbour(paragraphs, idx, ignore_neargood, -1, -1)
    }

    fn get_next_neighbour(
        paragraphs: &[IntermediateClassifiedParagraph],
        idx: usize,
        ignore_neargood: bool,
    ) -> IntermediateClassification {
        Self::get_neighbour(paragraphs, idx, ignore_neargood, 1, paragraphs.len() as i32)
    }

    fn get_neighbour(
        paragraphs: &[IntermediateClassifiedParagraph],
        idx: usize,
        ignore_neargood: bool,
        inc: i32,
        boundary: i32,
    ) -> IntermediateClassification {
        let mut idx = idx as i32;

        while idx + inc != boundary {
            idx += inc;
            if matches!(
                paragraphs[idx as usize].classification,
                IntermediateClassification::Good
            ) || matches!(
                paragraphs[idx as usize].classification,
                IntermediateClassification::Bad
            ) || (!ignore_neargood
                && matches!(
                    paragraphs[idx as usize].classification,
                    IntermediateClassification::NearGood,
                ))
            {
                return paragraphs[idx as usize].classification.clone();
            }
        }

        IntermediateClassification::Bad
    }

    fn extract_text(&self, paragraphs: Vec<ClassifiedParagraph>) -> String {
        paragraphs
            .into_iter()
            .filter(|paragraph| match paragraph.classification {
                Classification::Good => true,
                Classification::Bad => false,
            })
            .map(|paragraph| paragraph.paragraph.text)
            .collect::<Vec<_>>()
            .join(" ")
    }

    pub fn extract(&self, html: &str) -> String {
        let lex = Token::lexer(html);

        let paragraphs = self.paragraphs(lex, html);
        let init_classified = self
            .initial_classification(paragraphs)
            .into_iter()
            .filter(|par| par.paragraph.text.chars().any(|c| !c.is_whitespace()))
            .collect();

        let classified = self.contextual_classification(init_classified);

        self.extract_text(classified)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }
}
impl IntermediateClassification {
    fn is_short(&self) -> bool {
        matches!(self, IntermediateClassification::Short)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_link_density() {
        let paragraphs = vec![
            Paragraph {
                is_heading: false,
                tags_count: 0,
                chars_count_in_links: 0,
                text: "0123456789".repeat(2),
                last_was_whitespace: false,
            },
            Paragraph {
                is_heading: false,
                tags_count: 0,
                chars_count_in_links: 20,
                text: "0123456789".repeat(2),
                last_was_whitespace: false,
            },
            Paragraph {
                is_heading: false,
                tags_count: 0,
                chars_count_in_links: 40,
                text: "0123456789".repeat(8),
                last_was_whitespace: false,
            },
            Paragraph {
                is_heading: false,
                tags_count: 0,
                chars_count_in_links: 39,
                text: "0123456789".repeat(8),
                last_was_whitespace: false,
            },
            Paragraph {
                is_heading: false,
                tags_count: 0,
                chars_count_in_links: 41,
                text: "0123456789".repeat(8),
                last_was_whitespace: false,
            },
        ];

        let just_text = JustText::default();
        let initial_classification = just_text.initial_classification(paragraphs);

        assert!(matches!(
            initial_classification[0].classification,
            IntermediateClassification::Short
        ));
        assert!(matches!(
            initial_classification[1].classification,
            IntermediateClassification::Bad
        ));
        assert!(matches!(
            initial_classification[2].classification,
            IntermediateClassification::Bad
        ));
        assert!(matches!(
            initial_classification[3].classification,
            IntermediateClassification::Bad
        ));

        let res = just_text.contextual_classification(initial_classification);

        assert!(matches!(res[0].classification, Classification::Bad));
        assert!(matches!(res[1].classification, Classification::Bad));
        assert!(matches!(res[2].classification, Classification::Bad));
        assert!(matches!(res[3].classification, Classification::Bad));
    }
}
