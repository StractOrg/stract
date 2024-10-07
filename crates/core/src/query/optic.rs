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

use itertools::Itertools;
use optics::{Action, MatchLocation, Matching, Optic, PatternPart, Rule};
use std::iter;
use tantivy::{
    query::{BooleanQuery, Occur, QueryClone},
    schema::Schema,
};
use url::Url;

use crate::{
    numericalfield_reader::NumericalFieldReader,
    schema::text_field,
    webpage::{schema_org, url_ext::UrlExt},
};

use super::{const_query::ConstQuery, pattern_query::PatternQuery, union::UnionQuery};

pub trait AsTantivyQuery {
    fn as_tantivy(
        &self,
        schema: &Schema,
        columnfield_reader: &NumericalFieldReader,
    ) -> Box<dyn tantivy::query::Query>;
}

pub trait AsMultipleTantivyQuery {
    fn as_multiple_tantivy(
        &self,
        schema: &Schema,
        columnfield_reader: &NumericalFieldReader,
    ) -> Vec<(Occur, Box<dyn tantivy::query::Query>)>;
}

impl AsMultipleTantivyQuery for Optic {
    fn as_multiple_tantivy(
        &self,
        schema: &Schema,
        columnfields: &NumericalFieldReader,
    ) -> Vec<(Occur, Box<dyn tantivy::query::Query>)> {
        if self.discard_non_matching {
            let block = (
                Occur::Must,
                UnionQuery::from(
                    self.rules
                        .iter()
                        .filter(|rule| !matches!(rule.action, Action::Discard))
                        .filter_map(|rule| rule.as_searchable_rule(schema, columnfields))
                        .map(|(occur, rule)| {
                            BooleanQuery::from(vec![(occur, rule.query)]).box_clone()
                        })
                        .collect_vec(),
                )
                .box_clone(),
            );

            self.rules
                .iter()
                .filter(|rule| matches!(rule.action, Action::Discard))
                .chain(iter::once(&self.host_rankings.rules()))
                .filter_map(|rule| rule.as_searchable_rule(schema, columnfields))
                .map(|(occur, rule)| (occur, rule.query))
                .chain(iter::once(block))
                .collect()
        } else {
            self.rules
                .iter()
                .chain(iter::once(&self.host_rankings.rules()))
                .filter_map(|rule| rule.as_searchable_rule(schema, columnfields))
                .map(|(occur, rule)| (occur, rule.query))
                .collect()
        }
    }
}

pub struct SearchableRule {
    pub query: Box<dyn tantivy::query::Query>,
    pub boost: f64,
}

pub trait AsSearchableRule {
    fn as_searchable_rule(
        &self,
        schema: &Schema,
        columnfield_reader: &NumericalFieldReader,
    ) -> Option<(Occur, SearchableRule)>;
}

impl AsSearchableRule for Rule {
    fn as_searchable_rule(
        &self,
        schema: &Schema,
        columnfield_reader: &NumericalFieldReader,
    ) -> Option<(Occur, SearchableRule)> {
        let mut subqueries: Vec<_> = self
            .matches
            .iter()
            .filter_map(|and_rule| {
                let mut and_queries: Vec<_> = and_rule
                    .iter()
                    .map(|matching| (Occur::Must, matching.as_tantivy(schema, columnfield_reader)))
                    .collect();

                // Empty queries never match anything. A priori these shouldn't exist, but it doesn't
                // really cost us anything to check.
                // (though, technically it's an extra check or two for every rule? But rules aren't parsed very often)
                if and_queries.is_empty() {
                    None
                } else {
                    let query = if and_queries.len() == 1 {
                        and_queries.pop().unwrap().1
                    } else {
                        Box::new(BooleanQuery::from(and_queries))
                    };
                    Some((Occur::Should, query))
                }
            })
            .collect();

        if subqueries.is_empty() {
            return None;
        }

        let subquery = if subqueries.len() == 1 {
            subqueries.pop().unwrap().1
        } else {
            Box::new(BooleanQuery::from(subqueries))
        };

        match &self.action {
            Action::Boost(boost) => Some((
                Occur::Should,
                SearchableRule {
                    query: Box::new(ConstQuery::new(subquery, 1.0)),
                    boost: *boost as f64,
                },
            )),
            Action::Downrank(boost) => Some((
                Occur::Should,
                SearchableRule {
                    query: Box::new(ConstQuery::new(subquery, 1.0)),
                    boost: *boost as f64 * -1.0,
                },
            )),
            Action::Discard => Some((
                Occur::MustNot,
                SearchableRule {
                    query: subquery,
                    boost: 0.0,
                },
            )),
        }
    }
}

impl AsTantivyQuery for Matching {
    fn as_tantivy(
        &self,
        schema: &Schema,
        columnfield_reader: &NumericalFieldReader,
    ) -> Box<dyn tantivy::query::Query> {
        let field = match &self.location {
            MatchLocation::Site => text_field::UrlForSiteOperator.into(),
            MatchLocation::Url => text_field::Url.into(),
            MatchLocation::Domain => {
                // if pattern is "|raw|" and `raw` is actually a site
                // instead of a domain, change match location to site
                if self.pattern.len() == 3
                    && matches!(self.pattern[0], PatternPart::Anchor)
                    && matches!(self.pattern[2], PatternPart::Anchor)
                {
                    if let PatternPart::Raw(domain) = &self.pattern[1] {
                        if let Ok(url) = Url::parse(&format!("https://{domain}")) {
                            if let Some(real_domain) = url.root_domain() {
                                if domain.as_str() != real_domain {
                                    return Box::new(ConstQuery::new(
                                        Box::new(PatternQuery::new(
                                            self.pattern.clone(),
                                            text_field::UrlForSiteOperator.into(),
                                            schema,
                                            columnfield_reader.clone(),
                                        )),
                                        1.0,
                                    ));
                                }
                            }
                        }
                    }
                }

                text_field::Domain.into()
            }
            MatchLocation::Title => text_field::Title.into(),
            MatchLocation::Description => text_field::Description.into(),
            MatchLocation::Content => text_field::CleanBody.into(),
            MatchLocation::MicroformatTag => text_field::MicroformatTags.into(),
            MatchLocation::Schema => {
                let mut pattern = self.pattern.clone();
                // add TYPE_PREFIX to first term in pattern to ensure
                // we match from the beginning of the path
                if let Some(PatternPart::Raw(first_term)) = pattern
                    .iter_mut()
                    .find(|p| matches!(p, PatternPart::Raw(_)))
                {
                    *first_term = format!("{}{}", schema_org::TYPE_PREFIX, first_term);
                }

                return Box::new(ConstQuery::new(
                    Box::new(PatternQuery::new(
                        pattern,
                        text_field::FlattenedSchemaOrgJson.into(),
                        schema,
                        columnfield_reader.clone(),
                    )),
                    1.0,
                ));
            }
        };

        ConstQuery::new(
            PatternQuery::new(
                self.pattern.clone(),
                field,
                schema,
                columnfield_reader.clone(),
            )
            .box_clone(),
            1.0,
        )
        .box_clone()
    }
}

#[cfg(test)]
mod tests {
    use optics::{HostRankings, Optic};

    use crate::{
        bangs::Bangs,
        enum_map,
        index::Index,
        searcher::{
            api::{ApiSearcher, Config},
            live::LiveSearcher,
            LocalSearchClient, LocalSearcher, SearchQuery,
        },
        webgraph::{Node, WebgraphWriter},
        webpage::{html::links::RelFlags, Html, Webpage},
    };

    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[test]
    #[allow(clippy::too_many_lines)]
    fn discard_and_boost_hosts() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website A</title>
                        </head>
                        <body>
                            {CONTENT} {}
                            example example example
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://www.a.com",
                )
                .unwrap(),
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website B</title>
                        </head>
                        <body>
                            {CONTENT} {}
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://www.b.com",
                )
                .unwrap(),
                host_centrality: 0.01,
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "website".to_string(),
                signal_coefficients: enum_map! {
                    crate::ranking::SignalEnum::from(crate::ranking::signals::core::Bm25Title) => 1_000_000.0

                }
                .into(),
                ..Default::default()
            })
            .unwrap()
            .webpages;

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].url, "https://www.b.com/");
        assert_eq!(res[1].url, "https://www.a.com/");

        let res = searcher
            .search_sync(&SearchQuery {
                query: "website".to_string(),
                optic: Some(
                    Optic::parse(
                        r#"
                        Rule {
                            Matches {
                                Domain("b.com")
                            },
                            Action(Discard)
                        }
                    "#,
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].url, "https://www.a.com/");

        let res = searcher
            .search_sync(&SearchQuery {
                query: "website".to_string(),
                optic: Some(
                    Optic::parse(
                        r#"
                        Rule {
                            Matches {
                                Domain("a.com")
                            },
                            Action(Boost(100))
                        }
                    "#,
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].url, "https://www.a.com/");
        assert_eq!(res[1].url, "https://www.b.com/");
    }

    #[test]
    fn example_optics_dont_crash() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website A</title>
                        </head>
                        <body>
                            {CONTENT}
                            example example example
                        </body>
                    </html>
                "#
                    ),
                    "https://www.a.com/this/is/a/pattern",
                )
                .unwrap(),
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website B</title>
                        </head>
                        <body>
                            {CONTENT}
                        </body>
                    </html>
                "#
                    ),
                    "https://www.b.com/this/is/b/pattern",
                )
                .unwrap(),
                host_centrality: 0.0001,
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let _ = searcher
            .search_sync(&SearchQuery {
                query: "website".to_string(),
                optic: Some(
                    Optic::parse(include_str!(
                        "../../../optics/testcases/samples/quickstart.optic"
                    ))
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;

        let _ = searcher
            .search_sync(&SearchQuery {
                query: "website".to_string(),
                optic: Some(
                    Optic::parse(include_str!(
                        "../../../optics/testcases/samples/hacker_news.optic"
                    ))
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;

        let _ = searcher
            .search_sync(&SearchQuery {
                query: "website".to_string(),
                optic: Some(
                    Optic::parse(include_str!(
                        "../../../optics/testcases/samples/copycats_removal.optic"
                    ))
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn empty_discard() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website A</title>
                        </head>
                        <body>
                            {CONTENT} {}
                            example example example
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://www.a.com/this/is/a/pattern",
                )
                .unwrap(),
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website B</title>
                        </head>
                        <body>
                            {CONTENT} {}
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://www.b.com/this/is/b/pattern",
                )
                .unwrap(),
                host_centrality: 0.0001,
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website B</title>
                        </head>
                        <body>
                            {CONTENT} {}
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://www.c.com/this/is/c/pattern",
                )
                .unwrap(),
                host_centrality: 0.0001,
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "website".to_string(),
                optic: Some(
                    Optic::parse(
                        r#"
                    DiscardNonMatching;
                    Rule {
                        Matches {
                            Domain("a.com")
                        },
                        Action(Boost(6))
                    };
                    Rule {
                        Matches {
                            Domain("b.com")
                        },
                        Action(Boost(1))
                    };
                "#,
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].url, "https://www.a.com/this/is/a/pattern");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn liked_hosts() {
        let dir = crate::gen_temp_dir().unwrap();
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        let mut writer = WebgraphWriter::new(
            &dir,
            crate::executor::Executor::single_thread(),
            crate::webgraph::Compression::default(),
            None,
        );

        writer.insert(
            Node::from("https://www.e.com").into_host(),
            Node::from("https://www.a.com").into_host(),
            String::new(),
            RelFlags::default(),
        );
        writer.insert(
            Node::from("https://www.a.com").into_host(),
            Node::from("https://www.e.com").into_host(),
            String::new(),
            RelFlags::default(),
        );

        writer.insert(
            Node::from("https://www.c.com").into_host(),
            Node::from("https://www.c.com").into_host(),
            String::new(),
            RelFlags::default(),
        );

        writer.insert(
            Node::from("https://www.b.com").into_host(),
            Node::from("https://www.e.com").into_host(),
            String::new(),
            RelFlags::default(),
        );
        writer.insert(
            Node::from("https://www.e.com").into_host(),
            Node::from("https://www.b.com").into_host(),
            String::new(),
            RelFlags::default(),
        );

        let graph = writer.finalize();

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website A</title>
                        </head>
                        <body>
                            {CONTENT} {}
                            example example example
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://www.a.com/this/is/a/pattern",
                )
                .unwrap(),
                fetch_time_ms: 500,
                node_id: Some(Node::from("www.a.com").into_host().id()),
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website B</title>
                        </head>
                        <body>
                            {CONTENT} {}
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://www.b.com/this/is/b/pattern",
                )
                .unwrap(),
                host_centrality: 0.0001,
                fetch_time_ms: 500,
                node_id: Some(Node::from("www.b.com").into_host().id()),
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website C</title>
                        </head>
                        <body>
                            {CONTENT} {}
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://www.c.com/this/is/c/pattern",
                )
                .unwrap(),
                host_centrality: 0.0002,
                fetch_time_ms: 500,
                node_id: Some(Node::from("www.c.com").into_host().id()),
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher: ApiSearcher<_, LiveSearcher, _> = ApiSearcher::new(
            LocalSearchClient::from(LocalSearcher::from(index)),
            Bangs::empty(),
            Config::default(),
        )
        .with_webgraph(graph);

        let res = searcher
            .search(&SearchQuery {
                query: "website".to_string(),
                signal_coefficients: crate::enum_map! {
                    crate::ranking::SignalEnum::from(crate::ranking::signals::InboundSimilarity) => 100_000.0
                }.into(),

                optic: Some(Optic {
                    host_rankings: HostRankings {
                        liked: vec!["www.a.com".to_string(), "www.b.com".to_string()],
                        disliked: vec!["www.c.com".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_websites_result()
            .webpages;

        assert_eq!(res.len(), 3);
        assert_eq!(res[0].url, "https://www.b.com/this/is/b/pattern");
        assert_eq!(res[1].url, "https://www.a.com/this/is/a/pattern");
        assert_eq!(res[2].url, "https://www.c.com/this/is/c/pattern");
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn schema_org_search() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website A</title>
                            <script type="application/ld+json">
                                {{
                                "@context": "https://schema.org",
                                "@type": "ImageObject",
                                "author": "Jane Doe",
                                "contentLocation": "Puerto Vallarta, Mexico",
                                "contentUrl": "mexico-beach.jpg",
                                "datePublished": "2008-01-25",
                                "description": "I took this picture while on vacation last year.",
                                "name": "Beach in Mexico",
                                "BlogPosting": "whatever"
                                }}
                            </script>
                        </head>
                        <body>
                            {CONTENT} {}
                            example example example
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://www.a.com/",
                )
                .unwrap(),
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r##"
                    <html>
                        <head>
                            <title>Website B</title>
                        </head>
                        <body>
                            <article itemscope itemtype="http://schema.org/BlogPosting">
                                <section>
                                <h1>Comments</h1>
                                <article itemprop="comment" itemscope itemtype="http://schema.org/UserComments" id="c1">
                                <link itemprop="url" href="#c1">
                                <footer>
                                    <p>Posted by: <span itemprop="creator" itemscope itemtype="http://schema.org/Person">
                                    <span itemprop="name">Greg</span>
                                    </span></p>
                                    <p><time itemprop="commentTime" datetime="2013-08-29">15 minutes ago</time></p>
                                </footer>
                                <p>Ha!</p>
                                </article>
                                </section>
                            </article>
                            {CONTENT} {}
                        </body>
                    </html>
                "##,
                        crate::rand_words(100)
                    ),
                    "https://www.b.com/",
                ).unwrap(),
                host_centrality: 0.0001,
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index.commit().unwrap();
        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "website".to_string(),
                optic: Some(
                    Optic::parse(
                        r#"
                        DiscardNonMatching;
                        Rule {
                            Matches {
                                Schema("BlogPosting")
                            }
                        }
                    "#,
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].url, "https://www.b.com/");

        let res = searcher
            .search_sync(&SearchQuery {
                query: "website".to_string(),
                optic: Some(
                    Optic::parse(
                        r#"
                        DiscardNonMatching;
                        Rule {
                            Matches {
                                Schema("BlogPosting.comment")
                            }
                        }
                    "#,
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].url, "https://www.b.com/");

        let res = searcher
            .search_sync(&SearchQuery {
                query: "website".to_string(),
                optic: Some(
                    Optic::parse(
                        r#"
                        DiscardNonMatching;
                        Rule {
                            Matches {
                                Schema("ImageObject")
                            }
                        }
                    "#,
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].url, "https://www.a.com/");

        let res = searcher
            .search_sync(&SearchQuery {
                query: "website".to_string(),
                optic: Some(
                    Optic::parse(
                        r#"
                        DiscardNonMatching;
                        Rule {
                            Matches {
                                Schema("Person")
                            }
                        }
                    "#,
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].url, "https://www.b.com/");
    }

    #[test]
    fn pattern_same_phrase() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website A</title>
                        </head>
                        <body>
                            {CONTENT} {}
                            example example example
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://chat.stackoverflow.com",
                )
                .unwrap(),
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "site:stackoverflow.com".to_string(),
                optic: Some(
                    Optic::parse(
                        r#"
                    DiscardNonMatching;
                    Rule {
                        Matches {
                            Site("a.com")
                        },
                        Action(Boost(6))
                    };
                    Rule {
                        Matches {
                            Site("stackoverflow.blog")
                        },
                        Matches {
                            Site("chat.b.eu")
                        },
                        Action(Boost(1))
                    };
                "#,
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;

        assert_eq!(res.len(), 0);
    }

    #[test]
    fn discard_all_discard_like() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website A</title>
                        </head>
                        <body>
                            {CONTENT} {}
                            example example example
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://a.com",
                )
                .unwrap(),
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website B</title>
                        </head>
                        <body>
                            {CONTENT} {}
                            example example example
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://b.com/",
                )
                .unwrap(),
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse(
                        r#"
                    DiscardNonMatching;
                    Rule {
                        Matches {
                            Site("b.com")
                        }
                    };
                "#,
                    )
                    .unwrap(),
                ),
                host_rankings: Some(HostRankings {
                    liked: vec!["a.com".to_string()],
                    disliked: vec![],
                    blocked: vec![],
                }),
                ..Default::default()
            })
            .unwrap()
            .webpages;

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].url, "https://b.com/");
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn special_pattern_syntax() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>This is an example website</title>
                            </head>
                            <body>
                                {CONTENT} {}
                                This is an example
                            </body>
                        </html>
                    "#,
                        crate::rand_words(1000)
                    ),
                    "https://example.com",
                )
                .unwrap(),
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);
        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].url, "https://example.com/");

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Title(\"is\") }, Action(Discard) }").unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 0);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Title(\"|is\") }, Action(Discard) }").unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Title(\"|This\") }, Action(Discard) }").unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 0);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Title(\"|This an\") }, Action(Discard) }")
                        .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Title(\"|This * an\") }, Action(Discard) }")
                        .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 0);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Site(\"example.com\") }, Action(Discard) }")
                        .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 0);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Site(\"|example.com\") }, Action(Discard) }")
                        .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 0);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Site(\"|example.com|\") }, Action(Discard) }")
                        .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 0);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Title(\"website.com|\") }, Action(Discard) }")
                        .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);
    }

    #[test]
    fn active_optic_with_blocked_hosts() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>This is an example website</title>
                            </head>
                            <body>
                                {CONTENT} {}
                                This is an example
                            </body>
                        </html>
                    "#,
                        crate::rand_words(1000)
                    ),
                    "https://example.com",
                )
                .unwrap(),
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse(
                        "DiscardNonMatching; Rule { Matches { Title(\"is\") }, Action(Boost(0)) }",
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse(
                        "DiscardNonMatching; Rule { Matches { Title(\"is\") }, Action(Boost(0)) }",
                    )
                    .unwrap(),
                ),
                host_rankings: Some(HostRankings {
                    liked: vec![],
                    disliked: vec![],
                    blocked: vec![String::from("example.com")],
                }),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 0);
    }

    #[test]
    fn empty_optic_noop() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>This is an example website</title>
                            </head>
                            <body>
                                {CONTENT} {}
                                This is an example
                            </body>
                        </html>
                    "#,
                        crate::rand_words(1000)
                    ),
                    "https://example.com",
                )
                .unwrap(),
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(Optic::parse("").unwrap()),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Title(\"\") }, Action(Discard) }").unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn wildcard_edge_cases() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>This is an example website</title>
                            </head>
                            <body>
                                {CONTENT} {}
                                This is an example
                            </body>
                        </html>
                    "#,
                        crate::rand_words(1000)
                    ),
                    "https://example.com",
                )
                .unwrap(),
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Another thing with no words in common</title>
                            </head>
                            <body>
                                {CONTENT} {}
                                This is an example
                            </body>
                        </html>
                    "#,
                        crate::rand_words(1000)
                    ),
                    "https://example.com",
                )
                .unwrap(),
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Title(\"*\") }, Action(Discard) }").unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 0);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Title(\"* is\") }, Action(Discard) }").unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Title(\"* This is\") }, Action(Discard) }")
                        .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Title(\"example *\") }, Action(Discard) }")
                        .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse(
                        "Rule { Matches { Title(\"example website *\") }, Action(Discard) }",
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);
    }

    #[test]
    fn empty_double_anchor() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        let mut page = Webpage {
            html: Html::parse(
                r#"
                        <html>
                            <head>
                                <title>This is an example website</title>
                            </head>
                            <body>
                                Test
                            </body>
                        </html>
                    "#,
                "https://example.com/",
            )
            .unwrap(),
            fetch_time_ms: 500,
            ..Default::default()
        };

        page.html.set_clean_text("".to_string());

        index.insert(&page).expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("DiscardNonMatching; Rule { Matches { Content(\"||\") }, Action(Boost(0)) }")
                        .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse(
                        "DiscardNonMatching; Rule { Matches { Content(\"|\") }, Action(Boost(0)) }",
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);
    }

    #[test]
    fn indieweb_search() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        let mut page = Webpage {
            html: Html::parse(
                r#"
                        <html>
                            <head>
                                <title>This is an example indie website</title>
                            </head>
                            <body>
                                <article class="h-entry">
                                    <h1 class="p-name">Microformats are amazing</h1>
                                    <p class="e-content">This is the content of the article</p>
                                    <a class="u-url" href="https://example.com/microformats">Permalink</a>
                                    <a class="u-author" href="https://example.com">Author</a>
                                    <time class="dt-published" datetime="2021-01-01T00:00:00+00:00">2021-01-01</time>
                                </article>
                            </body>
                        </html>
                    "#,
                "https://example.com/",
            ).unwrap(),
            fetch_time_ms: 500,
            ..Default::default()
        };

        page.html.set_clean_text("".to_string());

        index.insert(&page).expect("failed to insert webpage");

        let mut page = Webpage {
            html: Html::parse(
                r#"
                        <html>
                            <head>
                                <title>This is an example non-indie website</title>
                            </head>
                            <body>
                                example example example
                            </body>
                        </html>
                    "#,
                "https://non-indie-example.com/",
            )
            .unwrap(),
            fetch_time_ms: 500,
            ..Default::default()
        };

        page.html.set_clean_text("".to_string());

        index.insert(&page).expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 2);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse(
                        "DiscardNonMatching; Rule { Matches { MicroformatTag(\"|h-*\") } }",
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].domain, "example.com");
    }

    #[test]
    fn site_double_anchor() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        let mut page = Webpage {
            html: Html::parse(
                r#"
                        <html>
                            <head>
                                <title>This is an example site</title>
                            </head>
                            <body>
                                test example
                            </body>
                        </html>
                    "#,
                "https://example.com/test",
            )
            .unwrap(),
            fetch_time_ms: 500,
            ..Default::default()
        };

        page.html.set_clean_text("".to_string());

        index.insert(&page).expect("failed to insert webpage");

        let mut page = Webpage {
            html: Html::parse(
                r#"
                        <html>
                            <head>
                                <title>This is another sample website</title>
                            </head>
                            <body>
                                example example example
                            </body>
                        </html>
                    "#,
                "https://another-example.com/",
            )
            .unwrap(),
            fetch_time_ms: 500,
            ..Default::default()
        };

        page.html.set_clean_text("".to_string());

        index.insert(&page).expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 2);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse(
                        "DiscardNonMatching; Rule { Matches { Site(\"|example.com|\") } }",
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].url, "https://example.com/test");

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Site(\"|example.com|\") }, Action(Discard) }")
                        .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].url, "https://another-example.com/");
    }

    #[test]
    fn apostrophe_token() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        let mut page = Webpage {
            html: Html::parse(
                r#"
                        <html>
                            <head>
                                <title>Mikkel's collection</title>
                            </head>
                            <body>
                                test example
                            </body>
                        </html>
                    "#,
                "https://example.com/",
            )
            .unwrap(),
            ..Default::default()
        };

        page.html.set_clean_text("".to_string());

        index.insert(&page).expect("failed to insert webpage");

        let mut page = Webpage {
            html: Html::parse(
                r#"
                        <html>
                            <head>
                                <title>Another's collection</title>
                            </head>
                            <body>
                                test example
                            </body>
                        </html>
                    "#,
                "https://another-example.com/",
            )
            .unwrap(),
            ..Default::default()
        };

        page.html.set_clean_text("".to_string());

        index.insert(&page).expect("failed to insert webpage");

        let mut page = Webpage {
            html: Html::parse(
                r#"
                        <html>
                            <head>
                                <title>A thirds's site</title>
                            </head>
                            <body>
                                test example
                            </body>
                        </html>
                    "#,
                "https://a-third-example.com/",
            )
            .unwrap(),
            ..Default::default()
        };

        page.html.set_clean_text("".to_string());

        index.insert(&page).expect("failed to insert webpage");

        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("Rule { Matches { Title(\"*'s collection\") }, Action(Discard) }")
                        .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].url, "https://a-third-example.com/");
    }

    #[test]
    fn discard_double_matching() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        let mut page = Webpage {
            html: Html::parse(
                r#"
                        <html>
                            <head>
                                <title>Mikkel's collection</title>
                            </head>
                            <body>
                                test example
                            </body>
                        </html>
                    "#,
                "https://example.com/",
            )
            .unwrap(),
            ..Default::default()
        };

        page.html.set_clean_text("".to_string());

        index.insert(&page).expect("failed to insert webpage");

        let mut page = Webpage {
            html: Html::parse(
                r#"
                        <html>
                            <head>
                                <title>Another's collection</title>
                            </head>
                            <body>
                                test example
                            </body>
                        </html>
                    "#,
                "https://another-example.com/",
            )
            .unwrap(),
            ..Default::default()
        };

        page.html.set_clean_text("".to_string());

        index.insert(&page).expect("failed to insert webpage");

        let mut page = Webpage {
            html: Html::parse(
                r#"
                        <html>
                            <head>
                                <title>A thirds's site</title>
                            </head>
                            <body>
                                test example
                            </body>
                        </html>
                    "#,
                "https://a-third-example.com/",
            )
            .unwrap(),
            ..Default::default()
        };

        page.html.set_clean_text("".to_string());

        index.insert(&page).expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("DiscardNonMatching; Rule { Matches { Title(\"*'s collection\") }, Action(Discard) }; Rule { Matches { Site(\"*.com\") } }")
                        .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].url, "https://a-third-example.com/");
    }

    #[test]
    fn test_site_in_domain_rule() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        let page = Webpage {
            html: Html::parse(
                r#"
                        <html>
                            <head>
                                <title>Example site</title>
                            </head>
                            <body>
                                test example
                            </body>
                        </html>
                    "#,
                "https://site.example.com/",
            )
            .unwrap(),
            ..Default::default()
        };

        index.insert(&page).expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse(
                        "DiscardNonMatching; Rule { Matches { Domain(\"|site.example.com|\") } } ",
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse(
                        "DiscardNonMatching; Rule { Matches { Domain(\"|example.com|\") } } ",
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 1);

        let res = searcher
            .search_sync(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse("DiscardNonMatching; Rule { Matches { Domain(\"|another.example.com|\") } } ")
                        .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap()
            .webpages;
        assert_eq!(res.len(), 0);
    }
}
