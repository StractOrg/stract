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

use std::ops::Range;

use crate::config::SnippetConfig;
use crate::highlighted::{HighlightedFragment, HighlightedKind};
use crate::query::Query;
use crate::tokenizer::{BigramTokenizer, Normal, Stemmed, Tokenizer, TrigramTokenizer};
use crate::web_spell::sentence_ranges;
use crate::webpage::region::Region;
use hashbrown::{HashMap, HashSet};
use utoipa::ToSchema;

use itertools::Itertools;
use whatlang::Lang;

/// For now we use an algorithm similar to the `UnifiedHighlighter` in lucene <https://lucene.apache.org/core/7_3_1/highlighter/org/apache/lucene/search/uhighlight/UnifiedHighlighter.html>.
/// The document text is treated as the entire corpus, and each passage is scored as a document in this corpus using BM25.
/// The top scoring passage is used as the start of a snippet, maybe combined with the subsequent passage(s) in order to
/// reach the desired snippet length.
///
/// In the future we want to implement something closer to the method described in <https://cs.pomona.edu/~dkauchak/ir_project/whitepapers/Snippet-IL.pdf>.
/// This might require us to store each paragraph of the webpage separately to get adequate performance (maybe we can split passages online with adequate performance
/// but we need to test this).

const K1: f64 = 1.2;
const B: f64 = 0.75;

#[derive(Debug)]
struct PassageCandidate {
    score: f64,
    text: String,
    doc_terms: HashMap<String, u64>,
}

#[derive(Default, Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TextSnippet {
    pub fragments: Vec<HighlightedFragment>,
}

impl TextSnippet {
    pub fn unhighlighted_string(&self) -> String {
        self.fragments
            .iter()
            .map(|f| f.text.clone())
            .collect::<Vec<_>>()
            .join("")
    }
}

struct SnippetBuilder {
    fragment: String,
    highlights: Vec<Range<usize>>,
}

impl SnippetBuilder {
    fn highlight(&mut self, terms: &HashSet<String>, lang: whatlang::Lang) {
        for mut tokenizer in [
            Tokenizer::Stemmed(Stemmed::with_forced_language(lang)),
            Tokenizer::Normal(Normal::default()),
            Tokenizer::Bigram(BigramTokenizer::default()),
            Tokenizer::Trigram(TrigramTokenizer::default()),
        ] {
            let mut stream =
                tantivy::tokenizer::Tokenizer::token_stream(&mut tokenizer, &self.fragment);
            while let Some(tok) = stream.next() {
                if terms.contains(&tok.text) {
                    self.highlights.push(tok.offset_from..tok.offset_to);
                }
            }
        }

        // remove overlapping ranges
        self.highlights
            .sort_by(|a, b| a.start.cmp(&b.start).then(a.end.cmp(&b.end)));
        self.highlights
            .dedup_by(|a, b| a.start == b.start && a.end >= b.end);
    }

    fn build(self) -> TextSnippet {
        let mut fragments = Vec::new();

        let mut last_end = 0;

        for range in self.highlights {
            if range.start > last_end {
                fragments.push(HighlightedFragment {
                    kind: HighlightedKind::Normal,
                    text: self.fragment[last_end..range.start].to_string(),
                });
            }

            fragments.push(HighlightedFragment {
                kind: HighlightedKind::Highlighted,
                text: self.fragment[range.start..range.end].to_string(),
            });

            last_end = range.end;
        }

        if last_end < self.fragment.len() {
            fragments.push(HighlightedFragment {
                kind: HighlightedKind::Normal,
                text: self.fragment[last_end..].to_string(),
            });
        }

        TextSnippet { fragments }
    }
}

fn snippet_string_builder(
    text: &str,
    terms: &[String],
    lang: whatlang::Lang,
    config: SnippetConfig,
    mut tokenizer: Tokenizer,
) -> SnippetBuilder {
    let terms: HashSet<String> = terms
        .iter()
        .flat_map(|term| {
            let mut stream = tantivy::tokenizer::Tokenizer::token_stream(&mut tokenizer, term);

            let mut res = Vec::new();
            while let Some(tok) = stream.next() {
                res.push(tok.text.clone());
            }

            res.into_iter()
        })
        .collect();

    let mut passages: Vec<_> = sentence_ranges(text)
        .into_iter()
        .filter(|offset| offset.end - offset.start > config.min_passage_width)
        .map(|offset| {
            let sentence = text[offset].to_string();

            let mut doc_terms = HashMap::new();

            {
                let mut stream =
                    tantivy::tokenizer::Tokenizer::token_stream(&mut tokenizer, &sentence);
                while let Some(tok) = stream.next() {
                    *doc_terms.entry(tok.text.clone()).or_insert(0) += 1;
                }
            }

            PassageCandidate {
                score: 0.0,
                text: sentence,
                doc_terms,
            }
        })
        .collect();

    if passages.is_empty() {
        let mut snippet = SnippetBuilder {
            fragment: text.chars().take(config.desired_num_chars).collect(),
            highlights: Vec::new(),
        };

        snippet.highlight(&terms, lang);

        return snippet;
    }

    let mut total_d_size = 0;

    for passage in &passages {
        total_d_size += passage.doc_terms.len();
    }

    let avg_d_size = total_d_size / passages.len();

    let mut n: HashMap<_, _> = terms.iter().map(|term| (term.to_string(), 0)).collect();

    for term in &terms {
        for passage in &passages {
            if passage.doc_terms.contains_key(term) {
                *n.entry(term.to_string()).or_insert(0) += 1;
            }
        }
    }

    let idf: HashMap<_, _> = n
        .into_iter()
        .map(|(term, freq)| {
            (
                term,
                (((passages.len() as f64) - (freq as f64) + 0.5) / ((freq as f64) + 0.5) + 1.0)
                    .ln(),
            )
        })
        .collect();

    for passage in &mut passages {
        for term in &terms {
            let f = *passage.doc_terms.get(term).unwrap_or(&0) as f64;
            passage.score += idf[term]
                * ((f * (K1 + 1.0))
                    / (f + K1
                        * (1.0 - B + B * (passage.doc_terms.len() as f64 / avg_d_size as f64))));
        }
    }

    let best_idx = passages
        .iter()
        .position_max_by(|a, b| a.score.total_cmp(&b.score))
        .expect("passages cannot be empty at this point");

    let best_passage = &passages[best_idx];
    let mut snippet = SnippetBuilder {
        fragment: best_passage.text.clone(),
        highlights: Vec::new(),
    };

    if snippet.fragment.len() > config.desired_num_chars + config.delta_num_chars {
        // TODO: find 'desired_num_chars' sized window that contains most highlights
        // instead of taking the prefix of the passage as a snippet
        snippet.fragment = snippet
            .fragment
            .chars()
            .take(config.desired_num_chars + config.delta_num_chars)
            .collect();
    } else {
        let mut next_passage_idx = best_idx + 1;

        while snippet.fragment.len() < config.desired_num_chars - config.delta_num_chars
            && next_passage_idx < passages.len()
        {
            snippet.fragment += " ";
            snippet.fragment += &passages[next_passage_idx].text;
            next_passage_idx += 1;
        }

        if snippet.fragment.len() > config.desired_num_chars + config.delta_num_chars {
            snippet.fragment = snippet
                .fragment
                .chars()
                .take(config.desired_num_chars + config.delta_num_chars)
                .collect();
        }
    }
    snippet.highlight(&terms, lang);

    snippet
}

fn snippet_string(
    text: &str,
    terms: &[String],
    lang: whatlang::Lang,
    config: SnippetConfig,
) -> TextSnippet {
    let tokenizer = Tokenizer::Normal(Normal::default());
    let snip = snippet_string_builder(text, terms, lang, config.clone(), tokenizer).build();

    if !snip.fragments.is_empty()
        && snip
            .fragments
            .iter()
            .any(|f| f.kind == HighlightedKind::Highlighted)
    {
        return snip;
    }

    let tokenizer = Tokenizer::Stemmed(Stemmed::with_forced_language(lang));
    snippet_string_builder(text, terms, lang, config, tokenizer).build()
}

pub fn generate(query: &Query, text: &str, region: &Region, config: SnippetConfig) -> TextSnippet {
    let lang = match region.lang() {
        Some(lang) => lang,
        None => match config.num_words_for_lang_detection {
            Some(num_words) => whatlang::detect_lang(
                text.split_whitespace()
                    .take(num_words)
                    .collect::<String>()
                    .as_str(),
            ),
            None => whatlang::detect_lang(text),
        }
        .unwrap_or(Lang::Eng),
    };

    if text.is_empty() {
        return TextSnippet {
            fragments: vec![HighlightedFragment {
                kind: HighlightedKind::Normal,
                text: "".to_string(),
            }],
        };
    }

    match config.max_considered_words {
        Some(num_words) => {
            let text = text.split_whitespace().take(num_words).join(" ");
            snippet_string(&text, query.simple_terms(), lang, config)
        }
        None => snippet_string(text, query.simple_terms(), lang, config),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        index::Index,
        search_prettifier::Snippet,
        searcher::{LocalSearcher, SearchQuery},
        webpage::Webpage,
    };

    const TEST_TEXT: &str = r#"Rust is a systems programming language sponsored by
Mozilla which describes it as a "safe, concurrent, practical language", supporting functional and
imperative-procedural paradigms. Rust is syntactically similar to C++[according to whom?],
but its designers intend it to provide better memory safety while still maintaining
performance.
Rust is free and open-source software, released under an MIT License, or Apache License
2.0. Its designers have refined the language through the experiences of writing the Servo
web browser layout engine[14] and the Rust compiler. A large proportion of current commits
to the project are from community members.[15]
Rust won first place for "most loved programming language" in the Stack Overflow Developer
Survey in 2016, 2017, and 2018."#;

    const HIGHLIGHTEN_PREFIX: &str = "<b>";
    const HIGHLIGHTEN_POSTFIX: &str = "</b>";

    fn highlight(snippet: Snippet) -> String {
        let text = snippet.text;

        text.fragments
            .into_iter()
            .map(|HighlightedFragment { kind, text }| match kind {
                HighlightedKind::Normal => text,
                HighlightedKind::Highlighted => {
                    format!("{HIGHLIGHTEN_PREFIX}{}{HIGHLIGHTEN_POSTFIX}", text)
                }
            })
            .collect()
    }

    #[test]
    fn snippet_during_search() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Website for runners</title>
                            </head>
                            <body>
                                {TEST_TEXT}
                            </body>
                        </html>
                    "#
                    ),
                    "https://www.example.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let result = searcher
            .search(&SearchQuery {
                query: "rust language".to_string(),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.webpages.len(), 1);
        assert_eq!(highlight(result.webpages[0].snippet.clone()), format!("{HIGHLIGHTEN_PREFIX}Rust{HIGHLIGHTEN_POSTFIX} is a systems programming {HIGHLIGHTEN_PREFIX}language{HIGHLIGHTEN_POSTFIX} sponsored by Mozilla which describes it as a \"safe, concurrent, practical {HIGHLIGHTEN_PREFIX}language{HIGHLIGHTEN_POSTFIX}\", supporting functional and imperative-procedural paradigms. {HIGHLIGHTEN_PREFIX}Rust{HIGHLIGHTEN_POSTFIX} is syntactically similar to C++[according to whom?"));
    }

    #[test]
    fn stemmed_words_snippet_highlight() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Website for runners</title>
                            </head>
                            <body>
                                {TEST_TEXT}
                            </body>
                        </html>
                    "#
                    ),
                    "https://www.example.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let result = searcher
            .search(&SearchQuery {
                query: "describe".to_string(),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.webpages.len(), 1);
        assert_eq!(highlight(result.webpages[0].snippet.clone()), format!("Rust is a systems programming language sponsored by Mozilla which {HIGHLIGHTEN_PREFIX}describes{HIGHLIGHTEN_POSTFIX} it as a \"safe, concurrent, practical language\", supporting functional and imperative-procedural paradigms. Rust is syntactically similar to C++[according to whom?"));
    }

    #[test]
    fn test_stemmed_term() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Website for runners</title>
                            </head>
                            <body>
                                {TEST_TEXT}
                            </body>
                        </html>
                    "#
                    ),
                    "https://www.example.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let result = searcher
            .search(&SearchQuery {
                query: "paradigms".to_string(),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.webpages.len(), 1);
        assert_eq!(
            highlight(result.webpages[0].snippet.clone()),
            format!("Rust is a systems programming language sponsored by Mozilla which describes it as a \"safe, concurrent, practical language\", supporting functional and imperative-procedural {HIGHLIGHTEN_PREFIX}paradigms{HIGHLIGHTEN_POSTFIX}. Rust is syntactically similar to C++[according to whom?")
        );
    }

    #[test]
    fn empty_query() {
        assert_eq!(
            highlight(Snippet {
                date: None,
                text: snippet_string(
                    "this is a test",
                    &[],
                    whatlang::Lang::Eng,
                    SnippetConfig::default()
                )
            })
            .as_str(),
            "this is a test"
        );
    }

    #[test]
    fn empty_text() {
        assert_eq!(
            highlight(Snippet {
                date: None,
                text: snippet_string(
                    "",
                    &["test".to_string()],
                    whatlang::Lang::Eng,
                    SnippetConfig::default()
                )
            })
            .as_str(),
            ""
        );

        assert_eq!(
            highlight(Snippet {
                date: None,
                text: snippet_string("", &[], whatlang::Lang::Eng, SnippetConfig::default())
            })
            .as_str(),
            ""
        );
    }

    #[test]
    fn compounded_terms() {
        let snip = snippet_string_builder(
            "this is a test",
            &["thisis".to_string()],
            whatlang::Lang::Eng,
            SnippetConfig::default(),
            Tokenizer::Normal(Normal::default()),
        );

        let mut terms = HashSet::new();
        terms.insert("thisis".to_string());

        assert_eq!(
            highlight(Snippet {
                date: None,
                text: snip.build()
            })
            .as_str(),
            "<b>this is</b> a test"
        );
    }
}
