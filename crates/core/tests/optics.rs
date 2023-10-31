mod common;

use optics::{Optic, SiteRankings};
use webgraph::{Node, WebgraphWriter};
use webpage::{Html, Webpage};

use stract_core::{
    ranking::inbound_similarity::InboundSimilarity,
    searcher::{LocalSearcher, SearchQuery},
};

use crate::common::{rand_words, temporary_index};

const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

#[test]
fn discard_and_boost_sites() {
    let mut index = temporary_index().expect("Unable to open index");

    index
        .insert(Webpage {
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
                    rand_words(100)
                ),
                "https://www.a.com",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");
    index
        .insert(Webpage {
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
                    rand_words(100)
                ),
                "https://www.b.com",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.01,
            page_centrality: 0.0,

            pre_computed_score: 0.0,
            fetch_time_ms: 500,
            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");

    index.commit().expect("failed to commit index");
    let searcher = LocalSearcher::from(index);

    let res = searcher
        .search(&SearchQuery {
            query: "website".to_string(),
            ..Default::default()
        })
        .unwrap()
        .webpages;

    assert_eq!(res.len(), 2);
    assert_eq!(res[0].url, "https://www.b.com/");
    assert_eq!(res[1].url, "https://www.a.com/");

    let res = searcher
        .search(&SearchQuery {
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
        .search(&SearchQuery {
            query: "website".to_string(),
            optic: Some(
                Optic::parse(
                    r#"
                        Rule {
                            Matches {
                                Domain("a.com")
                            },
                            Action(Boost(10))
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
    let mut index = temporary_index().expect("Unable to open index");

    index
        .insert(Webpage {
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
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");
    index
        .insert(Webpage {
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
            backlink_labels: vec![],
            host_centrality: 0.0001,
            page_centrality: 0.0,

            pre_computed_score: 0.0,
            fetch_time_ms: 500,
            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");

    index.commit().expect("failed to commit index");
    let searcher = LocalSearcher::from(index);

    let _ = searcher
        .search(&SearchQuery {
            query: "website".to_string(),
            optic: Some(
                Optic::parse(include_str!(
                    "../../optics/testcases/samples/quickstart.optic"
                ))
                .unwrap(),
            ),
            ..Default::default()
        })
        .unwrap()
        .webpages;

    let _ = searcher
        .search(&SearchQuery {
            query: "website".to_string(),
            optic: Some(
                Optic::parse(include_str!(
                    "../../optics/testcases/samples/hacker_news.optic"
                ))
                .unwrap(),
            ),
            ..Default::default()
        })
        .unwrap()
        .webpages;

    let _ = searcher
        .search(&SearchQuery {
            query: "website".to_string(),
            optic: Some(
                Optic::parse(include_str!(
                    "../../optics/testcases/samples/copycats_removal.optic"
                ))
                .unwrap(),
            ),
            ..Default::default()
        })
        .unwrap()
        .webpages;
}

#[test]
fn empty_discard() {
    let mut index = temporary_index().expect("Unable to open index");

    index
        .insert(Webpage {
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
                    rand_words(100)
                ),
                "https://www.a.com/this/is/a/pattern",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");
    index
        .insert(Webpage {
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
                    rand_words(100)
                ),
                "https://www.b.com/this/is/b/pattern",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0001,
            page_centrality: 0.0,

            pre_computed_score: 0.0,
            fetch_time_ms: 500,
            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");
    index
        .insert(Webpage {
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
                    rand_words(100)
                ),
                "https://www.c.com/this/is/c/pattern",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0001,
            page_centrality: 0.0,

            pre_computed_score: 0.0,
            fetch_time_ms: 500,
            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");

    index.commit().expect("failed to commit index");
    let searcher = LocalSearcher::from(index);

    let res = searcher
        .search(&SearchQuery {
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

#[test]
fn liked_sites() {
    let mut index = temporary_index().expect("Unable to open index");

    let mut writer = WebgraphWriter::new(
        &stdx::gen_temp_path(),
        executor::Executor::single_thread(),
        webgraph::Compression::default(),
    );

    writer.insert(
        Node::from("https://www.e.com").into_host(),
        Node::from("https://www.a.com").into_host(),
        String::new(),
    );
    writer.insert(
        Node::from("https://www.a.com").into_host(),
        Node::from("https://www.e.com").into_host(),
        String::new(),
    );

    writer.insert(
        Node::from("https://www.c.com").into_host(),
        Node::from("https://www.c.com").into_host(),
        String::new(),
    );

    writer.insert(
        Node::from("https://www.b.com").into_host(),
        Node::from("https://www.e.com").into_host(),
        String::new(),
    );
    writer.insert(
        Node::from("https://www.e.com").into_host(),
        Node::from("https://www.b.com").into_host(),
        String::new(),
    );

    let graph = writer.finalize();

    index
        .insert(Webpage {
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
                    rand_words(100)
                ),
                "https://www.a.com/this/is/a/pattern",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            dmoz_description: None,
            safety_classification: None,
            node_id: Some(Node::from("www.a.com").into_host().id()),
        })
        .expect("failed to insert webpage");
    index
        .insert(Webpage {
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
                    rand_words(100)
                ),
                "https://www.b.com/this/is/b/pattern",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0001,
            page_centrality: 0.0,

            pre_computed_score: 0.0,
            fetch_time_ms: 500,
            dmoz_description: None,
            safety_classification: None,
            node_id: Some(Node::from("www.b.com").into_host().id()),
        })
        .expect("failed to insert webpage");
    index
        .insert(Webpage {
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
                    rand_words(100)
                ),
                "https://www.c.com/this/is/c/pattern",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0002,
            page_centrality: 0.0,

            pre_computed_score: 0.0,
            fetch_time_ms: 500,
            dmoz_description: None,
            safety_classification: None,
            node_id: Some(Node::from("www.c.com").into_host().id()),
        })
        .expect("failed to insert webpage");

    index.commit().expect("failed to commit index");
    let mut searcher = LocalSearcher::from(index);

    searcher.set_inbound_similarity(InboundSimilarity::build(&graph));

    let res = searcher
        .search(&SearchQuery {
            query: "website".to_string(),
            optic: Some(
                Optic::parse(
                    r#"
                    Like(Site("www.a.com"));
                    Like(Site("www.b.com"));
                    Dislike(Site("www.c.com"));
                "#,
                )
                .unwrap(),
            ),
            ..Default::default()
        })
        .unwrap()
        .webpages;

    assert_eq!(res.len(), 3);
    assert_eq!(res[0].url, "https://www.b.com/this/is/b/pattern");
    assert_eq!(res[1].url, "https://www.a.com/this/is/a/pattern");
    assert_eq!(res[2].url, "https://www.c.com/this/is/c/pattern");
}

#[test]
fn schema_org_search() {
    let mut index = temporary_index().expect("Unable to open index");

    index
        .insert(Webpage {
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
                                "name": "Beach in Mexico"
                                }}
                            </script>
                        </head>
                        <body>
                            {CONTENT} {}
                            example example example
                        </body>
                    </html>
                "#,
                    rand_words(100)
                ),
                "https://www.a.com/",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");
    index
            .insert(Webpage {
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
                        rand_words(100)
                    ),
                    "https://www.b.com/",
                ).unwrap(),
                backlink_labels: vec![],
                host_centrality: 0.0001,
                page_centrality: 0.0,

                pre_computed_score: 0.0,
                fetch_time_ms: 500,
                node_id: None,
                dmoz_description: None,
                safety_classification: None,
            })
            .expect("failed to insert webpage");

    index.commit().unwrap();
    let searcher = LocalSearcher::from(index);

    let res = searcher
        .search(&SearchQuery {
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
        .search(&SearchQuery {
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
        .search(&SearchQuery {
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
        .search(&SearchQuery {
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
    let mut index = temporary_index().expect("Unable to open index");

    index
        .insert(Webpage {
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
                    rand_words(100)
                ),
                "https://chat.stackoverflow.com",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");

    index.commit().expect("failed to commit index");
    let searcher = LocalSearcher::from(index);

    let res = searcher
        .search(&SearchQuery {
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
                        Action(Boost(1))
                    };
                    Rule {
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
    let mut index = temporary_index().expect("Unable to open index");

    index
        .insert(Webpage {
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
                    rand_words(100)
                ),
                "https://a.com",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");
    index
        .insert(Webpage {
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
                    rand_words(100)
                ),
                "https://b.com/",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");

    index.commit().expect("failed to commit index");
    let searcher = LocalSearcher::from(index);

    let res = searcher
        .search(&SearchQuery {
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
            site_rankings: Some(SiteRankings {
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
fn discussion_optic() {
    let mut index = temporary_index().expect("Unable to open index");

    index
        .insert(Webpage {
            html: Html::parse(
                include_str!("../testcases/schema_org/infinity_war.html"),
                "https://a.com",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");
    index.commit().expect("failed to commit index");

    let searcher = LocalSearcher::from(index);
    let res = searcher
        .search(&SearchQuery {
            query: "avengers endgame".to_string(),
            ..Default::default()
        })
        .unwrap()
        .webpages;

    assert!(!res.is_empty());
    assert_eq!(&res[0].url, "https://a.com/");

    let res = searcher
        .search(&SearchQuery {
            query: "avengers endgame".to_string(),
            optic: Some(Optic::parse(include_str!("../src/searcher/discussions.optic")).unwrap()),
            ..Default::default()
        })
        .unwrap()
        .webpages;
    assert!(res.is_empty());
}

#[test]
fn special_pattern_syntax() {
    let mut index = temporary_index().expect("Unable to open index");

    index
        .insert(Webpage {
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
                    rand_words(1000)
                ),
                "https://example.com",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");
    index.commit().expect("failed to commit index");

    let searcher = LocalSearcher::from(index);
    let res = searcher
        .search(&SearchQuery {
            query: "example".to_string(),
            ..Default::default()
        })
        .unwrap()
        .webpages;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].url, "https://example.com/");

    let res = searcher
        .search(&SearchQuery {
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
        .search(&SearchQuery {
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
        .search(&SearchQuery {
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
        .search(&SearchQuery {
            query: "example".to_string(),
            optic: Some(
                Optic::parse("Rule { Matches { Title(\"|This an\") }, Action(Discard) }").unwrap(),
            ),
            ..Default::default()
        })
        .unwrap()
        .webpages;
    assert_eq!(res.len(), 1);

    let res = searcher
        .search(&SearchQuery {
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
        .search(&SearchQuery {
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
        .search(&SearchQuery {
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
        .search(&SearchQuery {
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
        .search(&SearchQuery {
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
fn active_optic_with_blocked_sites() {
    let mut index = temporary_index().expect("Unable to open index");

    index
        .insert(Webpage {
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
                    rand_words(1000)
                ),
                "https://example.com",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");
    index.commit().expect("failed to commit index");

    let searcher = LocalSearcher::from(index);

    let res = searcher
        .search(&SearchQuery {
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
        .search(&SearchQuery {
            query: "example".to_string(),
            optic: Some(
                Optic::parse(
                    "DiscardNonMatching; Rule { Matches { Title(\"is\") }, Action(Boost(0)) }",
                )
                .unwrap(),
            ),
            site_rankings: Some(SiteRankings {
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
    let mut index = temporary_index().expect("Unable to open index");

    index
        .insert(Webpage {
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
                    rand_words(1000)
                ),
                "https://example.com",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");
    index.commit().expect("failed to commit index");

    let searcher = LocalSearcher::from(index);

    let res = searcher
        .search(&SearchQuery {
            query: "example".to_string(),
            optic: Some(Optic::parse("").unwrap()),
            ..Default::default()
        })
        .unwrap()
        .webpages;
    assert_eq!(res.len(), 1);

    let res = searcher
        .search(&SearchQuery {
            query: "example".to_string(),
            optic: Some(Optic::parse("Rule { Matches { Title(\"\") }, Action(Discard) }").unwrap()),
            ..Default::default()
        })
        .unwrap()
        .webpages;
    assert_eq!(res.len(), 1);
}

#[test]
fn wildcard_edge_cases() {
    let mut index = temporary_index().expect("Unable to open index");

    index
        .insert(Webpage {
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
                    rand_words(1000)
                ),
                "https://example.com",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");
    index
        .insert(Webpage {
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
                    rand_words(1000)
                ),
                "https://example.com",
            )
            .unwrap(),
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
        .expect("failed to insert webpage");
    index.commit().expect("failed to commit index");

    let searcher = LocalSearcher::from(index);

    let res = searcher
        .search(&SearchQuery {
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
        .search(&SearchQuery {
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
        .search(&SearchQuery {
            query: "example".to_string(),
            optic: Some(
                Optic::parse("Rule { Matches { Title(\"* This is\") }, Action(Discard) }").unwrap(),
            ),
            ..Default::default()
        })
        .unwrap()
        .webpages;
    assert_eq!(res.len(), 1);

    let res = searcher
        .search(&SearchQuery {
            query: "example".to_string(),
            optic: Some(
                Optic::parse("Rule { Matches { Title(\"example *\") }, Action(Discard) }").unwrap(),
            ),
            ..Default::default()
        })
        .unwrap()
        .webpages;
    assert_eq!(res.len(), 1);

    let res = searcher
        .search(&SearchQuery {
            query: "example".to_string(),
            optic: Some(
                Optic::parse("Rule { Matches { Title(\"example website *\") }, Action(Discard) }")
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
    let mut index = temporary_index().expect("Unable to open index");

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
        backlink_labels: vec![],
        host_centrality: 0.0,
        page_centrality: 0.0,
        fetch_time_ms: 500,
        pre_computed_score: 0.0,

        node_id: None,
        dmoz_description: None,
        safety_classification: None,
    };

    page.html.set_clean_text("".to_string());

    index.insert(page).expect("failed to insert webpage");
    index.commit().expect("failed to commit index");

    let searcher = LocalSearcher::from(index);

    let res = searcher
        .search(&SearchQuery {
            query: "example".to_string(),
            ..Default::default()
        })
        .unwrap()
        .webpages;
    assert_eq!(res.len(), 1);

    let res = searcher
        .search(&SearchQuery {
            query: "example".to_string(),
            optic: Some(
                Optic::parse(
                    "DiscardNonMatching; Rule { Matches { Content(\"||\") }, Action(Boost(0)) }",
                )
                .unwrap(),
            ),
            ..Default::default()
        })
        .unwrap()
        .webpages;
    assert_eq!(res.len(), 1);

    let res = searcher
        .search(&SearchQuery {
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
    let mut index = temporary_index().expect("Unable to open index");

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
            backlink_labels: vec![],
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,

            node_id: None,
            dmoz_description: None,
                safety_classification: None,
        };

    page.html.set_clean_text("".to_string());

    index.insert(page).expect("failed to insert webpage");

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
        backlink_labels: vec![],
        host_centrality: 0.0,
        page_centrality: 0.0,
        fetch_time_ms: 500,
        pre_computed_score: 0.0,

        node_id: None,
        dmoz_description: None,
        safety_classification: None,
    };

    page.html.set_clean_text("".to_string());

    index.insert(page).expect("failed to insert webpage");
    index.commit().expect("failed to commit index");

    let searcher = LocalSearcher::from(index);

    let res = searcher
        .search(&SearchQuery {
            query: "example".to_string(),
            ..Default::default()
        })
        .unwrap()
        .webpages;
    assert_eq!(res.len(), 2);

    let res = searcher
        .search(&SearchQuery {
            query: "example".to_string(),
            optic: Some(
                Optic::parse("DiscardNonMatching; Rule { Matches { MicroformatTag(\"|h-*\") } }")
                    .unwrap(),
            ),
            ..Default::default()
        })
        .unwrap()
        .webpages;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].domain, "example.com");
}
