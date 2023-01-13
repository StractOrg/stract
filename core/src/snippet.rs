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

use crate::schema::TextField;
use crate::search_prettifier::html_escape;
use crate::tokenizer::{Stemmed, Tokenizer};
use crate::webpage::region::Region;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use tantivy::tokenizer::{TextAnalyzer, Token};
use tantivy::{Score, Searcher};
use whatlang::Lang;

/// For now we use the snippet generator from tantivy with modifications.
/// In the future we want to implement something closer to the method described in <https://cs.pomona.edu/~dkauchak/ir_project/whitepapers/Snippet-IL.pdf>.
/// This will require us to store each paragraph of the webpage separately to get adequate performance.
/// Implementing SnippetIL will also allow us to correctly add "..." to the snippet.

const DEFAULT_MAX_NUM_CHARS: usize = 250;

#[derive(Debug)]
struct FragmentCandidate {
    score: Score,
    start_offset: usize,
    stop_offset: usize,
    highlighted: Vec<Range<usize>>,
}

impl FragmentCandidate {
    /// Create a basic `FragmentCandidate`
    ///
    /// `score`, `num_chars` are set to 0
    /// and `highlighted` is set to empty vec
    /// `stop_offset` is set to `start_offset`, which is taken as a param.
    fn new(start_offset: usize) -> FragmentCandidate {
        FragmentCandidate {
            score: 0.0,
            start_offset,
            stop_offset: start_offset,
            highlighted: vec![],
        }
    }

    /// Updates `score` and `highlighted` fields of the objects.
    ///
    /// taking the token and terms, the token is added to the fragment.
    /// if the token is one of the terms, the score
    /// and highlighted fields are updated in the fragment.
    fn try_add_token(&mut self, token: &Token, terms: &BTreeMap<String, Score>) {
        self.stop_offset = token.offset_to;

        if let Some(&score) = terms.get(&token.text.to_lowercase()) {
            self.score += score;
            self.highlighted.push(token.offset_from..token.offset_to);
        }
    }
}

/// `Snippet`
/// Contains a fragment of a document, and some highlighed parts inside it.
#[derive(Debug)]
struct SnippetString {
    fragment: String,
    highlighted: Vec<Range<usize>>,
}

const HIGHLIGHTEN_PREFIX: &str = "<b style=\"font-weight: 700;\">";
const HIGHLIGHTEN_POSTFIX: &str = "</b>";

impl SnippetString {
    /// Returns a hignlightned html from the `Snippet`.
    fn to_html(&self) -> String {
        let mut html = String::new();
        let mut start_from: usize = 0;

        for item in self.highlighted.iter() {
            if item.start < start_from {
                start_from = item.end;
                continue;
            }
            html.push_str(&html_escape(&self.fragment[start_from..item.start]));
            html.push_str(HIGHLIGHTEN_PREFIX);
            html.push_str(&html_escape(&self.fragment[item.clone()]));
            html.push_str(HIGHLIGHTEN_POSTFIX);
            start_from = item.end;
        }
        html.push_str(&html_escape(
            &self.fragment[start_from..self.fragment.len()],
        ));
        html
    }
}

/// Returns a non-empty list of "good" fragments.
///
/// If no target term is within the text, then the function
/// should return an empty Vec.
///
/// If a target term is within the text, then the returned
/// list is required to be non-empty.
///
/// The returned list is non-empty and contain less
/// than 12 possibly overlapping fragments.
///
/// All fragments should contain at least one target term
/// and have at most `max_num_chars` characters (not bytes).
///
/// It is ok to emit non-overlapping fragments, for instance,
/// one short and one long containing the same keyword, in order
/// to leave optimization opportunity to the fragment selector
/// upstream.
///
/// Fragments must be valid in the sense that `&text[fragment.start..fragment.stop]`\
/// has to be a valid string.
fn search_fragments(
    tokenizer: &TextAnalyzer,
    text: &str,
    terms: &BTreeMap<String, Score>,
    max_num_chars: usize,
) -> Vec<FragmentCandidate> {
    let mut token_stream = tokenizer.token_stream(text);
    let mut fragment = FragmentCandidate::new(0);
    let mut fragments: Vec<FragmentCandidate> = vec![];
    while let Some(next) = token_stream.next() {
        if (next.offset_to - fragment.start_offset) > max_num_chars {
            if fragment.score > 0.0 {
                fragments.push(fragment)
            };
            fragment = FragmentCandidate::new(next.offset_from);
        }
        fragment.try_add_token(next, terms);
    }
    if fragment.score > 0.0 {
        fragments.push(fragment)
    }

    fragments
}

/// Returns a Snippet
///
/// Takes a vector of `FragmentCandidate`s and the text.
/// Figures out the best fragment from it and creates a snippet.
fn select_best_fragment_combination(fragments: &[FragmentCandidate], text: &str) -> SnippetString {
    let best_fragment_opt = fragments.iter().max_by(|left, right| {
        let cmp_score = left
            .score
            .partial_cmp(&right.score)
            .unwrap_or(Ordering::Equal);
        if cmp_score == Ordering::Equal {
            (right.start_offset, right.stop_offset).cmp(&(left.start_offset, left.stop_offset))
        } else {
            cmp_score
        }
    });
    if let Some(fragment) = best_fragment_opt {
        let fragment_text = &text[fragment.start_offset..fragment.stop_offset];
        let highlighted = fragment
            .highlighted
            .iter()
            .map(|item| item.start - fragment.start_offset..item.end - fragment.start_offset)
            .collect();
        SnippetString {
            fragment: fragment_text.to_string(),
            highlighted,
        }
    } else {
        // when there no fragments to chose from,
        // for now create a empty snippet
        SnippetString {
            fragment: String::new(),
            highlighted: vec![],
        }
    }
}

struct SnippetGenerator {
    terms_text: BTreeMap<String, Score>,
    tokenizer: TextAnalyzer,
    max_num_chars: usize,
}

impl SnippetGenerator {
    /// Creates a new snippet generator
    fn create(
        searcher: &Searcher,
        query: &dyn tantivy::query::Query,
        tokenizer: tantivy::tokenizer::TextAnalyzer,
        field: Field,
    ) -> crate::Result<SnippetGenerator> {
        let clean_field = searcher
            .schema()
            .get_field(Field::Text(TextField::CleanBody).name())
            .unwrap();

        let field = searcher.schema().get_field(field.name()).unwrap();

        let mut terms: BTreeSet<&tantivy::Term> = BTreeSet::new();
        query.query_terms(&mut |term, _| {
            if term.field() == clean_field {
                terms.insert(term);
            }
        });

        let mut terms_text: BTreeMap<String, Score> = Default::default();
        for term in terms {
            let term_string = if let Some(term_str) = term.as_str() {
                tokenizer
                    .token_stream(term_str)
                    .next()
                    .unwrap()
                    .text
                    .to_string()
            } else {
                continue;
            };

            let term = tantivy::Term::from_field_text(field, &term_string);
            let doc_freq = searcher.doc_freq(&term)?;
            if doc_freq > 0 {
                let score = 1.0 / (1.0 + doc_freq as Score);
                terms_text.insert(term_string, score);
            }
        }

        Ok(SnippetGenerator {
            terms_text,
            tokenizer,
            max_num_chars: DEFAULT_MAX_NUM_CHARS,
        })
    }

    /// Generates a snippet for the given text.
    fn snippet(&self, text: &str) -> SnippetString {
        let fragment_candidates =
            search_fragments(&self.tokenizer, text, &self.terms_text, self.max_num_chars);
        select_best_fragment_combination(&fragment_candidates[..], text)
    }
}

use crate::{query::Query, schema::Field, Result};

pub fn generate(
    query: &Query,
    text: &str,
    dirty_text: &str,
    description: &Option<String>,
    dmoz_description: &Option<String>,
    region: &Region,
    searcher: &tantivy::Searcher,
) -> Result<String> {
    let lang = match region.lang() {
        Some(lang) => lang,
        None => whatlang::detect_lang(text).unwrap_or(Lang::Eng),
    };

    let tokenizer = Stemmed::with_forced_language(lang).into();
    let generator = SnippetGenerator::create(
        searcher,
        query,
        tokenizer,
        Field::Text(TextField::StemmedCleanBody),
    )?;

    let mut snippet = generator.snippet(text);

    if snippet.fragment.is_empty() {
        if text.is_empty() {
            match dmoz_description {
                Some(desc) => {
                    snippet = generate_snippet_without_stem(
                        desc,
                        searcher,
                        query,
                        Field::Text(TextField::DmozDescription),
                    )?;
                }
                None => match description {
                    Some(desc) => {
                        snippet = generate_snippet_without_stem(
                            desc,
                            searcher,
                            query,
                            Field::Text(TextField::Description),
                        )?;
                    }
                    None => {
                        snippet = generate_snippet_without_stem(
                            dirty_text,
                            searcher,
                            query,
                            Field::Text(TextField::AllBody),
                        )?;
                    }
                },
            }
        } else {
            snippet.fragment = text.chars().take(DEFAULT_MAX_NUM_CHARS).collect();
        }
    }

    let highlighted = snippet.to_html() + "...";

    Ok(highlighted)
}

fn generate_snippet_without_stem(
    text: &str,
    searcher: &Searcher,
    query: &Query,
    field: Field,
) -> Result<SnippetString> {
    let tokenizer = Tokenizer::default().into();
    let generator = SnippetGenerator::create(searcher, query, tokenizer, field)?;

    let mut snippet = generator.snippet(text);

    if snippet.fragment.is_empty() {
        snippet.fragment = text.chars().take(DEFAULT_MAX_NUM_CHARS).collect();
    }

    Ok(snippet)
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
    use maplit::btreemap;
    use tantivy::tokenizer::SimpleTokenizer;

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

    fn snippet_text(snippet: Snippet) -> String {
        match snippet {
            Snippet::Normal { date: _, text } => text,
            _ => panic!("The snippet was not text"),
        }
    }

    #[test]
    fn snippet_during_search() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
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
            ))
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let result = searcher
            .search(&SearchQuery {
                query: "rust language".to_string(),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.num_hits, 1);
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(snippet_text(result.webpages[0].snippet.clone()), format!("{HIGHLIGHTEN_PREFIX}Rust{HIGHLIGHTEN_POSTFIX} is a systems programming {HIGHLIGHTEN_PREFIX}language{HIGHLIGHTEN_POSTFIX} sponsored by Mozilla which describes it as a \"safe, concurrent, practical {HIGHLIGHTEN_PREFIX}language{HIGHLIGHTEN_POSTFIX}\", supporting functional and imperative-procedural paradigms. {HIGHLIGHTEN_PREFIX}Rust{HIGHLIGHTEN_POSTFIX} is syntactically similar to C++[according to whom?], but its..."));
    }

    #[test]
    fn stemmed_words_snippet_highlight() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
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
            ))
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let result = searcher
            .search(&SearchQuery {
                query: "describe".to_string(),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.num_hits, 1);
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(snippet_text(result.webpages[0].snippet.clone()), format!("Rust is a systems programming language sponsored by Mozilla which {HIGHLIGHTEN_PREFIX}describes{HIGHLIGHTEN_POSTFIX} it as a \"safe, concurrent, practical language\", supporting functional and imperative-procedural paradigms. Rust is syntactically similar to C++[according to whom?], but its..."));
    }

    #[test]
    fn test_snippet() {
        let terms = btreemap! {
            String::from("rust") => 1.0,
            String::from("language") => 0.9
        };
        let fragments = search_fragments(&From::from(SimpleTokenizer), TEST_TEXT, &terms, 100);
        assert_eq!(fragments.len(), 7);
        {
            let first = &fragments[0];
            assert_eq!(first.score, 1.9);
            assert_eq!(first.stop_offset, 89);
        }
        let snippet = select_best_fragment_combination(&fragments[..], TEST_TEXT);
        assert_eq!(
            snippet.fragment,
            "Rust is a systems programming language sponsored by\nMozilla which describes it as a \
             \"safe"
        );
        assert_eq!(
            snippet.to_html(),
            format!("{HIGHLIGHTEN_PREFIX}Rust{HIGHLIGHTEN_POSTFIX} is a systems programming {HIGHLIGHTEN_PREFIX}language{HIGHLIGHTEN_POSTFIX} sponsored by\nMozilla which \
             describes it as a \"safe")
        )
    }

    #[test]
    fn test_snippet_scored_fragment() {
        {
            let terms = btreemap! {
                String::from("rust") =>1.0,
                String::from("language") => 0.9
            };
            let fragments = search_fragments(&From::from(SimpleTokenizer), TEST_TEXT, &terms, 20);
            {
                let first = &fragments[0];
                assert_eq!(first.score, 1.0);
                assert_eq!(first.stop_offset, 17);
            }
            let snippet = select_best_fragment_combination(&fragments[..], TEST_TEXT);
            assert_eq!(
                snippet.to_html(),
                format!("{HIGHLIGHTEN_PREFIX}Rust{HIGHLIGHTEN_POSTFIX} is a systems")
            )
        }
        {
            let terms = btreemap! {
                String::from("rust") =>0.9,
                String::from("language") => 1.0
            };
            let fragments = search_fragments(&From::from(SimpleTokenizer), TEST_TEXT, &terms, 20);
            // assert_eq!(fragments.len(), 7);
            {
                let first = &fragments[0];
                assert_eq!(first.score, 0.9);
                assert_eq!(first.stop_offset, 17);
            }
            let snippet = select_best_fragment_combination(&fragments[..], TEST_TEXT);
            assert_eq!(
                snippet.to_html(),
                format!("programming {HIGHLIGHTEN_PREFIX}language{HIGHLIGHTEN_POSTFIX}")
            )
        }
    }

    #[test]
    fn test_snippet_in_second_fragment() {
        let text = "a b c d e f g";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("c"), 1.0);

        let fragments = search_fragments(&From::from(SimpleTokenizer), text, &terms, 3);

        assert_eq!(fragments.len(), 1);
        {
            let first = &fragments[0];
            assert_eq!(first.score, 1.0);
            assert_eq!(first.start_offset, 4);
            assert_eq!(first.stop_offset, 7);
        }

        let snippet = select_best_fragment_combination(&fragments[..], text);
        assert_eq!(snippet.fragment, "c d");
        assert_eq!(
            snippet.to_html(),
            format!("{HIGHLIGHTEN_PREFIX}c{HIGHLIGHTEN_POSTFIX} d")
        );
    }

    #[test]
    fn test_snippet_with_term_at_the_end_of_fragment() {
        let text = "a b c d e f f g";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("f"), 1.0);

        let fragments = search_fragments(&From::from(SimpleTokenizer), text, &terms, 3);

        assert_eq!(fragments.len(), 2);
        {
            let first = &fragments[0];
            assert_eq!(first.score, 1.0);
            assert_eq!(first.stop_offset, 11);
            assert_eq!(first.start_offset, 8);
        }

        let snippet = select_best_fragment_combination(&fragments[..], text);
        assert_eq!(snippet.fragment, "e f");
        assert_eq!(
            snippet.to_html(),
            format!("e {HIGHLIGHTEN_PREFIX}f{HIGHLIGHTEN_POSTFIX}")
        );
    }

    #[test]
    fn test_snippet_with_second_fragment_has_the_highest_score() {
        let text = "a b c d e f g";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("f"), 1.0);
        terms.insert(String::from("a"), 0.9);

        let fragments = search_fragments(&From::from(SimpleTokenizer), text, &terms, 7);

        assert_eq!(fragments.len(), 2);
        {
            let first = &fragments[0];
            assert_eq!(first.score, 0.9);
            assert_eq!(first.stop_offset, 7);
            assert_eq!(first.start_offset, 0);
        }

        let snippet = select_best_fragment_combination(&fragments[..], text);
        assert_eq!(snippet.fragment, "e f g");
        assert_eq!(
            snippet.to_html(),
            format!("e {HIGHLIGHTEN_PREFIX}f{HIGHLIGHTEN_POSTFIX} g")
        );
    }

    #[test]
    fn test_snippet_with_term_not_in_text() {
        let text = "a b c d";

        let mut terms = BTreeMap::new();
        terms.insert(String::from("z"), 1.0);

        let fragments = search_fragments(&From::from(SimpleTokenizer), text, &terms, 3);

        assert_eq!(fragments.len(), 0);

        let snippet = select_best_fragment_combination(&fragments[..], text);
        assert_eq!(snippet.fragment, "");
        assert_eq!(snippet.to_html(), "");
    }

    #[test]
    fn test_snippet_with_no_terms() {
        let text = "a b c d";

        let terms = BTreeMap::new();
        let fragments = search_fragments(&From::from(SimpleTokenizer), text, &terms, 3);
        assert_eq!(fragments.len(), 0);

        let snippet = select_best_fragment_combination(&fragments[..], text);
        assert_eq!(snippet.fragment, "");
        assert_eq!(snippet.to_html(), "");
    }

    #[test]
    fn test_stemmed_term() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
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
            ))
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let result = searcher
            .search(&SearchQuery {
                query: "paradigms".to_string(),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.num_hits, 1);
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(
            snippet_text(result.webpages[0].snippet.clone()),
            format!("Rust is a systems programming language sponsored by Mozilla which describes it as a \"safe, concurrent, practical language\", supporting functional and imperative-procedural {HIGHLIGHTEN_PREFIX}paradigms{HIGHLIGHTEN_POSTFIX}. Rust is syntactically similar to C++[according to whom?], but its...")
        );
    }
}
