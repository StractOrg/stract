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

use hashbrown::HashSet;
use kuchiki::{iter::NodeEdge, ElementData, NodeRef};
use whatlang::Lang;

use crate::stopwords;

pub struct Preprocessor<const N: usize> {
    removed_tags: [&'static str; N],
    num_open_tags: [i64; N],
}

impl<const N: usize> Preprocessor<N> {
    pub fn new(removed_tags: [&'static str; N]) -> Self {
        Self {
            removed_tags,
            num_open_tags: [0; N],
        }
    }

    pub fn update(&mut self, edge: &NodeEdge<NodeRef>) {
        match edge {
            NodeEdge::Start(node) => {
                if let Some(element) = node.as_element() {
                    let element_name: &str = &element.name.local;
                    if let Some((_, open_tags)) = self
                        .removed_tags
                        .iter()
                        .zip(self.num_open_tags.iter_mut())
                        .find(|(name, _)| **name == element_name)
                    {
                        *open_tags += 1;
                    }
                }
            }
            NodeEdge::End(node) => {
                if let Some(element) = node.as_element() {
                    let element_name: &str = &element.name.local;
                    if let Some((_, open_tags)) = self
                        .removed_tags
                        .iter()
                        .zip(self.num_open_tags.iter_mut())
                        .find(|(name, _)| **name == element_name)
                    {
                        *open_tags -= 1;
                    }
                }
            }
        }
    }

    pub fn is_inside_removed(&self) -> bool {
        self.num_open_tags.iter().any(|n| *n > 0)
    }
}

// implementation of the JustText algorithm described in this thesis: https://is.muni.cz/th/45523/fi_d/phdthesis.pdf
// reference implementation: https://github.com/miso-belica/jusText/blob/main/justext/core.py

/// Paragraphs that have a length density above this threshold
/// will be classified as bad.
const MAX_LINK_DENSITY_DEFAULT: f64 = 0.2; // originally 0.2

/// paragraphs that has a length less than this is
/// either bad or short.
const LENGTH_LOW_DEFAULT: usize = 50; // originally 70

/// Paragraphs that has a length greater than this are classified
/// as good given they have a higher stopword density than `STOPWORDS_HIGH_DEFAULT`.
const LENGTH_HIGH_DEFAULT: usize = 100; // originally 200

/// If stopword density is greater than this threshold, the paragraph
/// is classified as neargood.
const STOPWORDS_LOW_DEFAULT: f64 = 0.15; // originaly 0.30

/// If stopword density is greater than this threshold, the paragraph
/// is either classified as good or neargood depending on the
/// `LENGTH_HIGH_DEFAULT`.
const STOPWORDS_HIGH_DEFAULT: f64 = 0.2; // originaly 0.32

/// Max number of heading characters that will be classified
/// as good or neargood.
const MAX_HEADING_DISTANCE_DEFAULT: usize = 200; // originally 200

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
pub struct Paragraph {
    is_heading: bool,
    tags_count: usize,
    chars_count_in_links: usize,
    pub text: String,
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

    #[allow(clippy::cast_precision_loss)]
    fn link_density(&self) -> f64 {
        self.chars_count_in_links as f64 / self.text.len() as f64
    }
}

#[derive(Debug)]
struct ClassifiedParagraph {
    paragraph: Paragraph,
    classification: Classification,
}

fn is_heading(elem: &ElementData) -> bool {
    matches!(
        elem.name.local.as_ref(),
        "h1" | "h2" | "h3" | "h4" | "h5" | "h6"
    )
}

fn is_paragraph_breaking(elem: &ElementData) -> bool {
    matches!(
        elem.name.local.as_ref(),
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
    )
}

impl JustText {
    pub fn paragraphs(root: NodeRef) -> Vec<Paragraph> {
        let mut res = Vec::new();

        let mut preprocessor =
            Preprocessor::new(["script", "style", "embed", "head", "noscript", "iframe"]);

        let mut br = false;
        let mut link = false;
        let mut paragraph = Paragraph::new();

        let mut heading_count = 0;

        for edge in root.traverse() {
            preprocessor.update(&edge);
            if preprocessor.is_inside_removed() {
                continue;
            }

            match edge {
                NodeEdge::Start(node) => {
                    if let Some(element) = node.as_element() {
                        if is_heading(element) {
                            heading_count += 1;
                        }

                        if is_paragraph_breaking(element)
                            || (element.name.local.as_ref() == "br" && br)
                        {
                            if element.name.local.as_ref() == "br" {
                                // the <br><br> is a paragraph separator and should
                                // not be included in the number of tags within the
                                // paragraph
                                if paragraph.tags_count > 0 {
                                    paragraph.tags_count -= 1;
                                }
                            }

                            paragraph.is_heading = heading_count > 0;

                            if paragraph.contains_text() {
                                res.push(paragraph);
                            }

                            paragraph = Paragraph::new();
                        } else {
                            br = element.name.local.as_ref() == "br";
                            if br {
                                paragraph.append_text(" ");
                            } else if element.name.local.as_ref() == "a" {
                                link = true;
                            }
                            paragraph.tags_count += 1;
                        }
                    }

                    if let Some(text) = node.as_text() {
                        let raw_text = text.borrow();
                        let text = raw_text.as_str();
                        let chars_added = paragraph.append_text(text);

                        if link {
                            paragraph.chars_count_in_links += chars_added;
                        }

                        br = false;
                    }
                }
                NodeEdge::End(node) => {
                    if let Some(element) = node.as_element() {
                        if is_heading(element) {
                            heading_count -= 1;
                        }

                        paragraph.append_text(" ");

                        if is_paragraph_breaking(element) {
                            if paragraph.contains_text() {
                                res.push(paragraph);
                            }

                            paragraph = Paragraph::new();
                        }

                        if element.name.local.as_ref() == "a" {
                            link = false;
                        }
                    }
                }
            }
        }

        if paragraph.contains_text() {
            res.push(paragraph);
        }

        res
    }

    #[allow(clippy::cast_precision_loss)]
    fn calculate_stopword_density(paragraph: &Paragraph, stopwords: &HashSet<String>) -> f64 {
        paragraph
            .text
            .split_whitespace()
            .filter(|word| stopwords.contains(*word))
            .count() as f64
            / paragraph.text.split_whitespace().count() as f64
    }

    fn initial_classification(
        &self,
        paragraphs: &[Paragraph],
        lang: &Lang,
    ) -> Vec<ClassifiedParagraph> {
        let mut res = Vec::new();

        let stopwords = stopwords::get(lang).unwrap_or_else(|| stopwords::get(&Lang::Eng).unwrap());

        for paragraph in paragraphs {
            let classification = {
                let stopword_density: f64 = Self::calculate_stopword_density(paragraph, stopwords);

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
                paragraph: paragraph.clone(),
                classification: Classification::Intermediate(classification),
            });
        }

        res
    }

    fn new_class(
        prev_neighbour: &Classification,
        next_neighbour: &Classification,
        paragraphs: &[ClassifiedParagraph],
        i: usize,
    ) -> Classification {
        if matches!(
            prev_neighbour,
            Classification::Intermediate(IntermediateClassification::Good)
        ) && matches!(
            next_neighbour,
            Classification::Intermediate(IntermediateClassification::Good)
        ) {
            Classification::Intermediate(IntermediateClassification::Good)
        } else if matches!(
            prev_neighbour,
            Classification::Intermediate(IntermediateClassification::Bad)
        ) && matches!(
            next_neighbour,
            Classification::Intermediate(IntermediateClassification::Bad)
        ) {
            Classification::Intermediate(IntermediateClassification::Bad)
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
            Classification::Intermediate(IntermediateClassification::Good)
        } else {
            Classification::Intermediate(IntermediateClassification::Bad)
        }
    }

    fn update_good_headings(&self, paragraphs: &mut [ClassifiedParagraph]) {
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
    }

    fn classify_short(paragraphs: &mut [ClassifiedParagraph]) {
        let mut new_classes: Vec<_> = paragraphs
            .iter()
            .map(|par| par.classification.clone())
            .collect();

        for i in 0..paragraphs.len() {
            if !matches!(
                paragraphs[i].classification,
                Classification::Intermediate(IntermediateClassification::Short)
            ) {
                continue;
            }

            let prev_neighbour = Self::get_prev_neighbour(paragraphs, i, true);
            let next_neighbour = Self::get_next_neighbour(paragraphs, i, true);
            new_classes[i] = Self::new_class(&prev_neighbour, &next_neighbour, paragraphs, i);
        }

        for (paragraph, classification) in paragraphs.iter_mut().zip(new_classes) {
            paragraph.classification = classification;
        }
    }

    fn revise_neargood(paragraphs: &mut [ClassifiedParagraph]) {
        for i in 0..paragraphs.len() {
            if !matches!(
                paragraphs[i].classification,
                Classification::Intermediate(IntermediateClassification::NearGood)
            ) {
                continue;
            }

            let prev_neighbour = Self::get_prev_neighbour(paragraphs, i, true);
            let next_neighbour = Self::get_next_neighbour(paragraphs, i, true);

            let paragraph = &mut paragraphs[i];

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
                    IntermediateClassification::Good | IntermediateClassification::NearGood => {
                        Classification::Final(FinalClassification::Good)
                    }
                    IntermediateClassification::Short | IntermediateClassification::Bad => {
                        Classification::Final(FinalClassification::Bad)
                    }
                },
            };
        }
    }

    fn contextual_classification(&self, paragraphs: &mut [ClassifiedParagraph]) {
        self.update_good_headings(paragraphs);
        Self::classify_short(paragraphs);
        Self::revise_neargood(paragraphs);
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

    fn extract_text(paragraphs: Vec<ClassifiedParagraph>) -> String {
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

    pub fn extract_from_paragraphs(&self, paragraphs: &[Paragraph], lang: &Lang) -> String {
        let mut classified = self
            .initial_classification(paragraphs, lang)
            .into_iter()
            .filter(|par| par.paragraph.text.chars().any(|c| !c.is_whitespace()))
            .collect::<Vec<_>>();

        self.contextual_classification(&mut classified);

        Self::extract_text(classified)
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
        let mut classifications = just_text.initial_classification(&paragraphs, &Lang::Eng);

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
