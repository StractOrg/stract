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

use crate::{
    inverted_index::InvertedIndex,
    query::parser::TermCompound,
    ranking::SignalCoefficient,
    schema::text_field,
    search_ctx::Ctx,
    searcher::SearchQuery,
    webpage::{region::Region, safety_classifier},
    Error, Result,
};

use optics::{HostRankings, Optic};

use tantivy::query::{BooleanQuery, Occur, QueryClone};

mod const_query;
pub mod intersection;
pub mod optic;
pub mod parser;
mod pattern_query;
mod plan;
pub mod shortcircuit;
pub mod union;

use self::{optic::AsMultipleTantivyQuery, parser::SimpleOrPhrase};
use parser::Term;

pub const MAX_TERMS_FOR_NGRAM_LOOKUPS: usize = 16;

#[derive(Debug)]
pub struct Query {
    simple_terms_text: Vec<String>,
    tantivy_query: Box<dyn tantivy::query::Query>,
    host_rankings: HostRankings,
    offset: usize,
    region: Option<Region>,
    optics: Vec<Optic>,
    top_n: usize,
    count_results_exact: bool,
    signal_coefficients: SignalCoefficient,
    lang: Option<whatlang::Lang>,
}

impl Clone for Query {
    fn clone(&self) -> Self {
        Self {
            simple_terms_text: self.simple_terms_text.clone(),
            tantivy_query: self.tantivy_query.box_clone(),
            host_rankings: self.host_rankings.clone(),
            offset: self.offset,
            region: self.region,
            optics: self.optics.clone(),
            top_n: self.top_n,
            count_results_exact: self.count_results_exact,
            signal_coefficients: self.signal_coefficients.clone(),
            lang: self.lang,
        }
    }
}

impl Query {
    pub fn parse(ctx: &Ctx, query: &SearchQuery, index: &InvertedIndex) -> Result<Query> {
        let lang = whatlang::detect_lang(&query.query);

        let parsed_terms = parser::truncate(parser::parse(&query.query)?);

        if parsed_terms.is_empty() {
            tracing::error!("No terms found in query");
            return Err(Error::EmptyQuery.into());
        }

        if parsed_terms
            .iter()
            .all(|t| matches!(t, Term::PossibleBang { .. }))
        {
            tracing::error!("No non-bang terms found in query");
            return Err(Error::EmptyQuery.into());
        }

        let simple_terms_text: Vec<String> = parsed_terms
            .iter()
            .filter_map(|term| term.as_simple_text().map(|s| s.to_string()))
            .flat_map(|term| {
                // term might be a phrase, so we split it into words
                term.split_ascii_whitespace()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            })
            .collect();

        let mut plan = plan::initial(parsed_terms).expect("terms are not empty and not all bangs");

        let schema = index.schema();

        if query.safe_search {
            plan = plan.and(plan::Node::Not(Box::new(plan::Node::Term(
                plan::Term::new(
                    parser::SimpleTerm::from(safety_classifier::Label::NSFW.to_string()).into(),
                    text_field::SafetyClassification.into(),
                ),
            ))));
        }

        let mut tantivy_query = plan
            .into_query()
            .as_tantivy(lang.as_ref(), &schema)
            .expect("there should at least be one field in the index");

        let mut optics = Vec::new();
        if let Some(site_rankigns_optic) = query.host_rankings.clone().map(|sr| sr.into_optic()) {
            optics.push(site_rankigns_optic);
        }

        if let Some(optic) = &query.optic {
            optics.push(optic.clone());
        }

        for optic in &optics {
            let mut subqueries = vec![(Occur::Must, tantivy_query.box_clone())];
            subqueries.append(&mut optic.as_multiple_tantivy(&schema, &ctx.columnfield_reader));
            tantivy_query = Box::new(BooleanQuery::new(subqueries));
        }

        Ok(Query {
            host_rankings: optics.iter().fold(HostRankings::default(), |mut acc, el| {
                acc.merge_into(el.host_rankings.clone());
                acc
            }),
            simple_terms_text,
            tantivy_query,
            optics,
            offset: query.num_results * query.page,
            region: query.selected_region,
            top_n: query.num_results,
            count_results_exact: query.count_results_exact,
            signal_coefficients: query.signal_coefficients(),
            lang,
        })
    }

    pub fn count_results_exact(&self) -> bool {
        self.count_results_exact
    }

    pub fn simple_terms(&self) -> &[String] {
        &self.simple_terms_text
    }

    pub fn optics(&self) -> &[Optic] {
        &self.optics
    }

    pub fn num_results(&self) -> usize {
        self.top_n
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn region(&self) -> Option<&Region> {
        self.region.as_ref()
    }

    pub fn host_rankings(&self) -> &HostRankings {
        &self.host_rankings
    }

    pub fn signal_coefficients(&self) -> SignalCoefficient {
        self.signal_coefficients.clone()
    }

    pub fn lang(&self) -> Option<whatlang::Lang> {
        self.lang
    }
}

impl tantivy::query::Query for Query {
    fn weight(
        &self,
        enable_scoring: tantivy::query::EnableScoring,
    ) -> tantivy::Result<Box<dyn tantivy::query::Weight>> {
        self.tantivy_query.weight(enable_scoring)
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a tantivy::Term, bool)) {
        self.tantivy_query.query_terms(visitor)
    }
}

#[cfg(test)]
mod tests {
    use crate::{index::Index, rand_words, searcher::LocalSearcher, webpage::Webpage};
    use proptest::prelude::*;

    use super::*;

    fn empty_index() -> InvertedIndex {
        InvertedIndex::temporary().unwrap()
    }

    #[test]
    fn simple_parse() {
        let index = empty_index();
        let ctx = index.local_search_ctx();

        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "this is a simple query".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");

        assert_eq!(
            query.simple_terms(),
            vec![
                "this".to_string(),
                "is".to_string(),
                "a".to_string(),
                "simple".to_string(),
                "query".to_string(),
            ]
        );
    }

    #[test]
    fn parse_trailing_leading_whitespace() {
        let index = empty_index();
        let ctx = index.local_search_ctx();

        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "   this is a simple query   ".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");

        assert_eq!(
            query.simple_terms(),
            vec![
                "this".to_string(),
                "is".to_string(),
                "a".to_string(),
                "simple".to_string(),
                "query".to_string(),
            ]
        );
    }

    #[test]
    fn parse_weird_characters() {
        let index = empty_index();
        let ctx = index.local_search_ctx();

        let terms = Query::parse(
            &ctx,
            &SearchQuery {
                query: "123".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query")
        .simple_terms()
        .to_vec();
        assert_eq!(terms, vec!["123".to_string()]);

        let terms = Query::parse(
            &ctx,
            &SearchQuery {
                query: "123 33".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query")
        .simple_terms()
        .to_vec();
        assert_eq!(terms, vec!["123".to_string(), "33".to_string()]);

        let terms = Query::parse(
            &ctx,
            &SearchQuery {
                query: "term! term# $".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query")
        .simple_terms()
        .to_vec();
        assert_eq!(
            terms,
            vec!["term!".to_string(), "term#".to_string(), "$".to_string()]
        );
    }

    #[test]
    fn simple_terms_phrase() {
        let index = empty_index();
        let ctx = index.local_search_ctx();

        let terms = Query::parse(
            &ctx,
            &SearchQuery {
                query: "\"test term\"".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query")
        .simple_terms()
        .to_vec();

        assert_eq!(terms, vec!["test".to_string(), "term".to_string()]);
    }

    #[test]
    fn not_query() {
        let mut index = Index::temporary().expect("Unable to open index");
        let query = SearchQuery {
            query: "test -website".to_string(),
            ..Default::default()
        };

        index
            .insert(
                &Webpage::test_parse(
                    r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website
                            </body>
                        </html>
                    "#,
                    "https://www.first.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This test page does not contain the forbidden word
                            </body>
                        </html>
                    "#,
                    "https://www.second.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.second.com/");
    }

    #[test]
    fn site_query() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website
                            </body>
                        </html>
                    "#,
                    "https://www.first.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This test page does not contain the forbidden word
                            </body>
                        </html>
                    "#,
                    "https://www.second.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This test page does not contain the forbidden word
                            </body>
                        </html>
                    "#,
                    "https://www.second.com/first",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let query = SearchQuery {
            query: "test site:first.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");

        let query = SearchQuery {
            query: "test site:www.first.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");

        let query = SearchQuery {
            query: "test -site:first.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 2);

        assert!(result
            .webpages
            .iter()
            .all(|w| w.url != "https://www.first.com/"));
    }

    #[test]
    fn links_to_query() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website
                                <a href="https://www.second.com/example/abc">Second</a>
                            </body>
                        </html>
                    "#,
                    "https://www.first.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This test page does not contain the forbidden word
                                <a href="https://www.first.com">First</a>
                            </body>
                        </html>
                    "#,
                    "https://www.second.com/example/abc",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let query = SearchQuery {
            query: "test linksto:first.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.second.com/example/abc");

        let query = SearchQuery {
            query: "test linkto:www.first.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.second.com/example/abc");

        let query = SearchQuery {
            query: "test -linkto:first.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");

        let query = SearchQuery {
            query: "test linkto:second.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");

        let query = SearchQuery {
            query: "test linkto:www.second.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");

        let query = SearchQuery {
            query: "test linkto:second.com/example".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");

        let query = SearchQuery {
            query: "test linksto:second.com/example/abc".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");
    }

    #[test]
    fn links_to_uppercase() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website
                                <a href="https://www.SeCoNd.CoM/eXaMpLe/AbC">Second</a>
                            </body>
                        </html>
                    "#,
                    "https://www.first.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This test page does not contain the forbidden word
                                <a href="https://www.first.com">First</a>
                            </body>
                        </html>
                    "#,
                    "https://www.second.com/example/AbC",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let query = SearchQuery {
            query: "test linkto:second.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");
    }

    #[test]
    fn title_query() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website
                            </body>
                        </html>
                    "#,
                    "https://www.first.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This is a test website
                            </body>
                        </html>
                    "#,
                    "https://www.second.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let query = SearchQuery {
            query: "intitle:website".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");
    }

    #[test]
    fn url_query() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website
                            </body>
                        </html>
                    "#,
                    "https://www.first.com/forum",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This is a test website
                            </body>
                        </html>
                    "#,
                    "https://www.second.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let query = SearchQuery {
            query: "test inurl:forum".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.first.com/forum");
    }

    #[test]
    fn empty_query() {
        let index = empty_index();
        let ctx = index.local_search_ctx();

        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "".to_string(),
                ..Default::default()
            },
            &index,
        );

        assert!(query.is_err());
        assert_eq!(
            query.err().unwrap().to_string(),
            anyhow::Error::from(Error::EmptyQuery).to_string()
        );
    }

    #[test]
    fn query_term_only_special_char() {
        let index = empty_index();
        let ctx = index.local_search_ctx();

        let _query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "&".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");
    }

    #[test]
    fn site_query_split_domain() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.the-first.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This test page does not contain the forbidden word {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.second.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let query = SearchQuery {
            query: "test site:first.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 0);

        let query = SearchQuery {
            query: "test site:the-first.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.the-first.com/");

        let query = SearchQuery {
            query: "test site:www.the-first.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.the-first.com/");
    }

    #[test]
    fn phrase_query() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.first.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This is a bad test website {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.second.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let query = SearchQuery {
            query: "\"Test website\"".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");

        let query = SearchQuery {
            query: "\"Test website\" site:www.second.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 0);
    }

    #[test]
    fn match_compound_words() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.first.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");

        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Testwebsite</title>
                            </head>
                            <body>
                                This is a testwebsite {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.second.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let query = SearchQuery {
            query: "testwebsite".to_string(),
            ..Default::default()
        };

        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 2);

        let query = SearchQuery {
            query: "test website".to_string(),
            ..Default::default()
        };

        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 2);
    }

    #[test]
    fn deduplicate_terms() {
        let a = parser::parse("the the the the the").unwrap();
        let a = plan::initial(a).unwrap();
        let a = a.into_query();

        let b = parser::parse("the the the the the the the the the the the the").unwrap();
        let b = plan::initial(b).unwrap();
        let b = b.into_query();

        assert_eq!(a.len(), b.len());
    }

    #[test]
    fn safe_search() {
        let mut index = Index::temporary().expect("Unable to open index");
        let mut webpage = Webpage::test_parse(
            &format!(
                r#"
                <html>
                    <head>
                        <title>Test website</title>
                    </head>
                    <body>
                        This is a test website {}
                    </body>
                </html>
            "#,
                rand_words(1000)
            ),
            "https://www.sfw.com",
        )
        .unwrap();

        webpage.safety_classification = Some(safety_classifier::Label::SFW);
        webpage.html.set_clean_text("sfw".to_string());

        index.insert(&webpage).expect("failed to insert webpage");

        let mut webpage = Webpage::test_parse(
            &format!(
                r#"
                <html>
                    <head>
                        <title>Test website</title>
                    </head>
                    <body>
                        This is a test website {}
                    </body>
                </html>
            "#,
                rand_words(1000)
            ),
            "https://www.nsfw.com",
        )
        .unwrap();

        webpage.safety_classification = Some(safety_classifier::Label::NSFW);
        webpage.html.set_clean_text("nsfw".to_string());

        index.insert(&webpage).expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let query = SearchQuery {
            query: "test".to_string(),
            safe_search: false,
            ..Default::default()
        };

        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 2);

        let query = SearchQuery {
            query: "test".to_string(),
            safe_search: true,
            ..Default::default()
        };

        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);

        assert_eq!(result.webpages[0].url, "https://www.sfw.com/");
    }

    #[test]
    fn suffix_domain_prefix_path_site_operator() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.first.com/example",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This is a test website {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.second.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This is a test website {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.third.io",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let query = SearchQuery {
            query: "test site:.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 2);

        let query = SearchQuery {
            query: "test site:.com/example".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);

        let query = SearchQuery {
            query: "test site:first.com/example".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);

        let query = SearchQuery {
            query: "test site:first.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);

        let query = SearchQuery {
            query: "test site:www.first.com".to_string(),
            ..Default::default()
        };

        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);
    }

    #[test]
    fn exact_url_operator() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.first.com/example",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This is a test website {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.second.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This is a test website {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.third.io",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let query = SearchQuery {
            query: "test exacturl:https://www.first.com/example".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 1);

        let query = SearchQuery {
            query: "test exacturl:https://www.first.com".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 0);
    }

    #[test]
    fn mix_phrase_term_query() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.first.com/example",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This is a test website {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.second.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This is a test website {}
                            </body>
                        </html>
                    "#,
                        rand_words(1000)
                    ),
                    "https://www.third.io",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let query = SearchQuery {
            query: "\"test test\" website".to_string(),
            ..Default::default()
        };
        let result = searcher.search(&query).expect("Search failed");
        assert_eq!(result.webpages.len(), 2);
    }

    fn fixture(query: &str) -> Result<(), TestCaseError> {
        if query.trim().is_empty() {
            return Ok(());
        }

        let parsed_terms = parser::truncate(
            parser::parse(query).map_err(|_| TestCaseError::fail("parse failed"))?,
        );
        let plan =
            plan::initial(parsed_terms).ok_or(TestCaseError::fail("plan should not be empty"))?;
        let _ = plan.into_query();

        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4096))]
        #[test]
        fn test_query_parse_non_panic(query in ".*") {
            fixture(&query)?;
        }
    }
}
