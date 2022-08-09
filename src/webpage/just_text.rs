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

use lazy_static::lazy_static;
use logos::{Lexer, Logos};
use whatlang::Lang;

use super::{lexer::Token, Preprocessor};

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

#[derive(Debug, Clone)]
enum Classification {
    Final(FinalClassification),
    Intermediate(IntermediateClassification),
}

#[derive(Debug, Clone)]
enum FinalClassification {
    Good,
    Bad,
}

macro_rules! include_stopwords {
    ($($file:expr => $lang:expr),*) => {{
        let mut stopwords = HashMap::new();

        $(
            stopwords.insert(
                $lang,
                include_str!($file)
                    .lines()
                    .map(|s| s.to_lowercase())
                    .collect(),
            );
        )*

        stopwords
    }};
}

lazy_static! {
    static ref STOPWORDS: HashMap<Lang, HashSet<String>> = {
        include_stopwords!(
                "../../stopwords/Afrikaans.txt" => Lang::Afr,
                "../../stopwords/Arabic.txt" => Lang::Ara,
                "../../stopwords/Armenian.txt" => Lang::Hye,
                "../../stopwords/Azerbaijani.txt" => Lang::Aze,
                "../../stopwords/Belarusian.txt" => Lang::Bel,
                "../../stopwords/Bengali.txt" => Lang::Ben,
                "../../stopwords/Bulgarian.txt" => Lang::Bul,
                "../../stopwords/Catalan.txt" => Lang::Cat,
                "../../stopwords/Croatian.txt" => Lang::Hrv,
                "../../stopwords/Czech.txt" => Lang::Ces,
                "../../stopwords/Danish.txt" => Lang::Dan,
                "../../stopwords/Dutch.txt" => Lang::Nld,
                "../../stopwords/English.txt" => Lang::Eng,
                "../../stopwords/Esperanto.txt" => Lang::Epo,
                "../../stopwords/Estonian.txt" => Lang::Est,
                "../../stopwords/Finnish.txt" => Lang::Fin,
                "../../stopwords/French.txt" => Lang::Fra,
                "../../stopwords/Georgian.txt" => Lang::Kat,
                "../../stopwords/German.txt" => Lang::Deu,
                "../../stopwords/Greek.txt" => Lang::Ell,
                "../../stopwords/Gujarati.txt" => Lang::Guj,
                "../../stopwords/Hebrew.txt" => Lang::Heb,
                "../../stopwords/Hindi.txt" => Lang::Hin,
                "../../stopwords/Hungarian.txt" => Lang::Hun,
                "../../stopwords/Indonesian.txt" => Lang::Ind,
                "../../stopwords/Italian.txt" => Lang::Ita,
                "../../stopwords/Javanese.txt" => Lang::Jav,
                "../../stopwords/Kannada.txt" => Lang::Kan,
                "../../stopwords/Korean.txt" => Lang::Kor,
                "../../stopwords/Latin.txt" => Lang::Lat,
                "../../stopwords/Latvian.txt" => Lang::Lav,
                "../../stopwords/Lithuanian.txt" => Lang::Lit,
                "../../stopwords/Macedonian.txt" => Lang::Mkd,
                "../../stopwords/Malayalam.txt" => Lang::Mal,
                "../../stopwords/Marathi.txt" => Lang::Mar,
                "../../stopwords/Nepali.txt" => Lang::Nep,
                "../../stopwords/Persian.txt" => Lang::Pes,
                "../../stopwords/Polish.txt" => Lang::Pol,
                "../../stopwords/Portuguese.txt" => Lang::Por,
                "../../stopwords/Romanian.txt" => Lang::Ron,
                "../../stopwords/Russian.txt" => Lang::Rus,
                "../../stopwords/Serbian.txt" => Lang::Srp,
                "../../stopwords/Slovak.txt" => Lang::Slk,
                "../../stopwords/Slovenian.txt" => Lang::Slv,
                "../../stopwords/Spanish.txt" => Lang::Spa,
                "../../stopwords/Japanese.txt" => Lang::Jpn
        )
    };
}

pub struct JustText {
    pub max_link_density: f64,
    pub length_low: usize,
    pub length_high: usize,
    pub stopwords_low: f64,
    pub stopwords_high: f64,
    pub max_heading_distance: usize,
}

impl Default for JustText {
    fn default() -> Self {
        Self {
            max_link_density: MAX_LINK_DENSITY_DEFAULT,
            length_low: LENGTH_LOW_DEFAULT,
            length_high: LENGTH_HIGH_DEFAULT,
            stopwords_low: STOPWORDS_LOW_DEFAULT,
            stopwords_high: STOPWORDS_HIGH_DEFAULT,
            max_heading_distance: MAX_HEADING_DISTANCE_DEFAULT,
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
                Token::SelfTerminatingTag(_) | Token::BeginComment | Token::EndComment => {}
            }
        }

        if paragraph.contains_text() {
            res.push(paragraph);
        }

        res
    }

    fn calculate_stopword_density(
        &self,
        paragraph: &Paragraph,
        stopwords: &HashSet<String>,
    ) -> f64 {
        paragraph
            .text
            .split_whitespace()
            .filter(|word| stopwords.contains(&word.to_lowercase()))
            .count() as f64
            / paragraph.text.split_whitespace().count() as f64
    }

    fn initial_classification(&self, paragraphs: Vec<Paragraph>) -> Vec<ClassifiedParagraph> {
        let mut res = Vec::new();

        let stopwords = paragraphs
            .iter()
            .max_by_key(|p| p.text.len())
            .and_then(|paragraph| {
                let lang = whatlang::detect_lang(&paragraph.text).unwrap_or(Lang::Eng);
                STOPWORDS.get(&lang)
            })
            .unwrap_or_else(|| STOPWORDS.get(&Lang::Eng).unwrap());

        for paragraph in paragraphs {
            let classification = {
                let stopword_density: f64 = self.calculate_stopword_density(&paragraph, stopwords);

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

            res.push(ClassifiedParagraph {
                paragraph,
                classification: Classification::Intermediate(classification),
            });
        }

        res
    }

    fn contextual_classification(&self, paragraphs: &mut Vec<ClassifiedParagraph>) {
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
                    Classification::Intermediate(IntermediateClassification::Good)
                ) {
                    let paragraph = &mut paragraphs[i];
                    paragraph.classification =
                        Classification::Intermediate(IntermediateClassification::NearGood);
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
                Classification::Intermediate(IntermediateClassification::Short)
            ) {
                continue;
            }

            let prev_neighbour = Self::get_prev_neighbour(paragraphs, i, true);
            let next_neighbour = Self::get_next_neighbour(paragraphs, i, true);

            if matches!(
                prev_neighbour,
                Classification::Intermediate(IntermediateClassification::Good)
            ) && matches!(
                next_neighbour,
                Classification::Intermediate(IntermediateClassification::Good)
            ) {
                new_classes[i] = Classification::Intermediate(IntermediateClassification::Good);
            } else if matches!(
                prev_neighbour,
                Classification::Intermediate(IntermediateClassification::Bad)
            ) && matches!(
                next_neighbour,
                Classification::Intermediate(IntermediateClassification::Bad)
            ) {
                new_classes[i] = Classification::Intermediate(IntermediateClassification::Bad);
            } else if (matches!(
                prev_neighbour,
                Classification::Intermediate(IntermediateClassification::Bad)
            ) && matches!(
                Self::get_prev_neighbour(paragraphs, i, false),
                Classification::Intermediate(IntermediateClassification::NearGood)
            )) || (matches!(
                next_neighbour,
                Classification::Intermediate(IntermediateClassification::Bad)
            ) && matches!(
                Self::get_next_neighbour(paragraphs, i, false),
                Classification::Intermediate(IntermediateClassification::NearGood)
            )) {
                new_classes[i] = Classification::Intermediate(IntermediateClassification::Good);
            } else {
                new_classes[i] = Classification::Intermediate(IntermediateClassification::Bad);
            }
        }

        for (paragraph, classification) in paragraphs.iter_mut().zip(new_classes.into_iter()) {
            paragraph.classification = classification;
        }

        // revise neargood
        for i in 0..num_paragraphs {
            if !matches!(
                paragraphs[i].classification,
                Classification::Intermediate(IntermediateClassification::NearGood)
            ) {
                continue;
            }

            let prev_neighbour = Self::get_prev_neighbour(paragraphs, i, true);
            let next_neighbour = Self::get_next_neighbour(paragraphs, i, true);

            let mut paragraph = &mut paragraphs[i];

            match (prev_neighbour, next_neighbour) {
                (
                    Classification::Intermediate(IntermediateClassification::Bad),
                    Classification::Intermediate(IntermediateClassification::Bad),
                ) => {
                    paragraph.classification =
                        Classification::Intermediate(IntermediateClassification::Bad);
                }
                _ => {
                    paragraph.classification =
                        Classification::Intermediate(IntermediateClassification::Good);
                }
            }
        }

        for paragraph in paragraphs.iter_mut() {
            paragraph.classification = match &paragraph.classification {
                Classification::Final(_) => paragraph.classification.clone(),
                Classification::Intermediate(intermediate) => match intermediate {
                    IntermediateClassification::Good => {
                        Classification::Final(FinalClassification::Good)
                    }
                    IntermediateClassification::NearGood => {
                        Classification::Final(FinalClassification::Good)
                    }
                    IntermediateClassification::Short => {
                        Classification::Final(FinalClassification::Bad)
                    }
                    IntermediateClassification::Bad => {
                        Classification::Final(FinalClassification::Bad)
                    }
                },
            };
        }
    }

    fn get_prev_neighbour(
        paragraphs: &[ClassifiedParagraph],
        idx: usize,
        ignore_neargood: bool,
    ) -> Classification {
        Self::get_neighbour(paragraphs, idx, ignore_neargood, -1, -1)
    }

    fn get_next_neighbour(
        paragraphs: &[ClassifiedParagraph],
        idx: usize,
        ignore_neargood: bool,
    ) -> Classification {
        Self::get_neighbour(paragraphs, idx, ignore_neargood, 1, paragraphs.len() as i32)
    }

    fn get_neighbour(
        paragraphs: &[ClassifiedParagraph],
        idx: usize,
        ignore_neargood: bool,
        inc: i32,
        boundary: i32,
    ) -> Classification {
        let mut idx = idx as i32;

        while idx + inc != boundary {
            idx += inc;
            if matches!(
                paragraphs[idx as usize].classification,
                Classification::Intermediate(IntermediateClassification::Good)
            ) || matches!(
                paragraphs[idx as usize].classification,
                Classification::Intermediate(IntermediateClassification::Bad)
            ) || (!ignore_neargood
                && matches!(
                    paragraphs[idx as usize].classification,
                    Classification::Intermediate(IntermediateClassification::NearGood),
                ))
            {
                return paragraphs[idx as usize].classification.clone();
            }
        }

        Classification::Intermediate(IntermediateClassification::Bad)
    }

    fn extract_text(&self, paragraphs: Vec<ClassifiedParagraph>) -> String {
        paragraphs
            .into_iter()
            .filter(|paragraph| {
                matches!(
                    paragraph.classification,
                    Classification::Final(FinalClassification::Good)
                )
            })
            .map(|paragraph| paragraph.paragraph.text)
            .collect::<Vec<_>>()
            .join(" ")
    }

    pub fn extract(&self, html: &str) -> String {
        let lex = Token::lexer(html);

        let paragraphs = self.paragraphs(lex, html);
        let mut classified = self
            .initial_classification(paragraphs)
            .into_iter()
            .filter(|par| par.paragraph.text.chars().any(|c| !c.is_whitespace()))
            .collect();

        self.contextual_classification(&mut classified);

        self.extract_text(classified)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }
}
impl Classification {
    fn is_short(&self) -> bool {
        matches!(
            self,
            Classification::Intermediate(IntermediateClassification::Short)
        )
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
        let mut classifications = just_text.initial_classification(paragraphs);

        assert!(classifications[0].classification.is_short());
        assert!(matches!(
            classifications[1].classification,
            Classification::Intermediate(IntermediateClassification::Bad)
        ));
        assert!(matches!(
            classifications[2].classification,
            Classification::Intermediate(IntermediateClassification::Bad)
        ));
        assert!(matches!(
            classifications[3].classification,
            Classification::Intermediate(IntermediateClassification::Bad)
        ));

        just_text.contextual_classification(&mut classifications);

        assert!(matches!(
            classifications[0].classification,
            Classification::Final(FinalClassification::Bad)
        ));
        assert!(matches!(
            classifications[1].classification,
            Classification::Final(FinalClassification::Bad)
        ));
        assert!(matches!(
            classifications[2].classification,
            Classification::Final(FinalClassification::Bad)
        ));
        assert!(matches!(
            classifications[3].classification,
            Classification::Final(FinalClassification::Bad)
        ));
    }
}
