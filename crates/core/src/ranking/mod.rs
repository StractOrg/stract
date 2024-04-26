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

pub mod bitvec_similarity;
pub mod bm25;
pub mod inbound_similarity;
pub mod initial;
pub mod models;
pub mod optics;
pub mod pipeline;
pub mod signal;

use initial::InitialScoreTweaker;

use crate::{
    collector::{MainCollector, MaxDocsConsidered, TopDocs},
    config::CollectorConfig,
    fastfield_reader::FastFieldReader,
    search_ctx::Ctx,
    searcher::NUM_RESULTS_PER_PAGE,
};

pub use self::signal::*;

#[derive(Clone)]
pub struct Ranker {
    max_docs: Option<MaxDocsConsidered>,
    offset: Option<usize>,
    computer: SignalComputer,
    fastfield_reader: FastFieldReader,
    de_rank_similar: bool,
    num_results: Option<usize>,
    collector_config: CollectorConfig,
}

impl Ranker {
    pub fn new(
        computer: SignalComputer,
        fastfield_reader: FastFieldReader,
        collector_config: CollectorConfig,
    ) -> Self {
        Ranker {
            offset: None,
            computer,
            max_docs: None,
            de_rank_similar: true,
            fastfield_reader,
            num_results: None,
            collector_config,
        }
    }

    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn with_max_docs(mut self, total_docs: usize, segments: usize) -> Self {
        self.max_docs = Some(MaxDocsConsidered {
            total_docs,
            segments,
        });
        self
    }

    pub fn with_num_results(mut self, num_results: usize) -> Self {
        self.num_results = Some(num_results);
        self
    }

    pub fn de_rank_similar(&mut self, de_rank_similar: bool) {
        self.de_rank_similar = de_rank_similar;
    }

    pub fn computer(&self) -> SignalComputer {
        self.computer.clone()
    }

    pub fn collector(&self, ctx: Ctx) -> MainCollector {
        let computer = self.computer();

        let score_tweaker =
            InitialScoreTweaker::new(ctx.tv_searcher, computer, self.fastfield_reader.clone());

        let mut collector = TopDocs::with_limit(
            self.num_results.unwrap_or(NUM_RESULTS_PER_PAGE),
            self.fastfield_reader.clone(),
        );

        if self.de_rank_similar {
            collector = collector.and_de_rank_similar()
        }

        if let Some(offset) = self.offset {
            collector = collector.and_offset(offset);
        }

        if let Some(max_docs) = &self.max_docs {
            collector = collector.and_max_docs(max_docs.clone());
        }

        collector = collector.and_collector_config(self.collector_config.clone());

        collector.main_collector(score_tweaker)
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use optics::{
        ast::{RankingCoeff, RankingTarget},
        Optic,
    };

    use crate::{
        config::{IndexingDualEncoderConfig, IndexingLocalConfig, WarcSource},
        entrypoint::indexer::IndexingWorker,
        index::Index,
        models::dual_encoder::DualEncoder,
        searcher::{LocalSearcher, SearchQuery},
        webpage::{Html, Webpage},
    };

    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";
    // const CONTENT_2: &str = "what should i write in this text what should i write in this text what should i write in this text what should i write in this text what should i write in this text what should i write in this text what should i write in this text";

    #[test]
    fn host_centrality_ranking() {
        let mut index = Index::temporary().expect("Unable to open index");

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
                host_centrality: 5.0,
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);
        let result = searcher
            .search(&SearchQuery {
                query: "example".to_string(),
                ..Default::default()
            })
            .expect("Search failed");
        assert_eq!(result.webpages.len(), 2);
        assert_eq!(result.webpages[0].url, "https://www.b.com/");
        assert_eq!(result.webpages[1].url, "https://www.a.com/");
    }

    #[test]
    fn page_centrality_ranking() {
        let mut index = Index::temporary().expect("Unable to open index");

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
                backlink_labels: vec![],
                fetch_time_ms: 500,
                page_centrality: 5.0,
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);
        let result = searcher
            .search(&SearchQuery {
                query: "example".to_string(),
                ..Default::default()
            })
            .expect("Search failed");
        assert_eq!(result.webpages.len(), 2);
        assert_eq!(result.webpages[0].url, "https://www.b.com/");
        assert_eq!(result.webpages[1].url, "https://www.a.com/");
    }

    #[test]
    fn freshness_ranking() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Title</title>
                            <meta property="og:updated_time" content="1999-06-22T19:37:34+00:00" />
                        </head>
                        <body>
                            {CONTENT} {}
                        </body>
                    </html>
                "#,
                        crate::rand_words(100),
                    ),
                    "https://www.old.com",
                )
                .unwrap(),
                host_centrality: 1.0,
                fetch_time_ms: 4999,
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
                            <title>Title</title>
                            <meta property="og:updated_time" content="2023-06-22T19:37:34+00:00" />
                        </head>
                        <body>
                            {CONTENT} {}
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://www.new.com",
                )
                .unwrap(),
                host_centrality: 1.0,
                fetch_time_ms: 5000,
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);
        let result = searcher
            .search(&SearchQuery {
                query: "title".to_string(),
                return_ranking_signals: true,
                optic: Some(Optic {
                    rankings: vec![RankingCoeff {
                        target: RankingTarget::Signal("update_timestamp".to_string()),
                        value: 1_000_000.0,
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.webpages[0].url, "https://www.new.com/");
    }

    #[test]
    fn derank_trackers() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    r#"
                    <html>
                        <head>
                            <title>Test site</title>
                        </head>
                        <body>
                            test
                        </body>
                    </html>
                "#,
                    "https://www.first.com",
                )
                .unwrap(),
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index
            .insert(&Webpage {
                    html: Html::parse(r#"
                    <html>
                        <head>
                            <script>
                                !function(){var analytics=window.analytics=window.analytics||[];if(!analytics.initialize)if(analytics.invoked)window.console&&console.error&&console.error("Segment snippet included twice.");else{analytics.invoked=!0;analytics.methods=["trackSubmit","trackClick","trackLink","trackForm","pageview","identify","reset","group","track","ready","alias","debug","page","once","off","on","addSourceMiddleware","addIntegrationMiddleware","setAnonymousId","addDestinationMiddleware"];analytics.factory=function(e){return function(){var t=Array.prototype.slice.call(arguments);t.unshift(e);analytics.push(t);return analytics}};for(var e=0;e<analytics.methods.length;e++){var key=analytics.methods[e];analytics[key]=analytics.factory(key)}analytics.load=function(key,e){var t=document.createElement("script");t.type="text/javascript";t.async=!0;t.src="https://cdn.segment.com/analytics.js/v1/" + key + "/analytics.min.js";var n=document.getElementsByTagName("script")[0];n.parentNode.insertBefore(t,n);analytics._loadOptions=e};analytics._writeKey="";analytics.SNIPPET_VERSION="4.13.2";
                                analytics.load("");
                                analytics.page();
                                }}();
                            </script>
                            <script>
                                (function(h,o,t,j,a,r){
                                    h.hj=h.hj||function(){(h.hj.q=h.hj.q||[]).push(arguments)};
                                    a.appendChild(r);
                                })(window,document,'https://static.hotjar.com/c/hotjar-','.js?sv=');
                            </script>
                            <script src="https://thirdparty.com/js"></script>
                            <link href='//securepubads.g.doubleclick.net' rel='preconnect'>
                            <title>Test site</title>
                        </head>
                        <body>
                            test
                        </body>
                    </html>
                "#,
                "https://www.second.com").unwrap(),
                host_centrality: 0.00003,
                fetch_time_ms: 500,
                ..Default::default()
        })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);
        let result = searcher
            .search(&SearchQuery {
                query: "test".to_string(),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.webpages.len(), 2);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");
    }

    #[test]
    fn backlink_text() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    r#"
                    <html>
                        <head>
                            <title>Test site</title>
                        </head>
                        <body>
                            test
                        </body>
                    </html>
                "#,
                    "https://www.first.com",
                )
                .unwrap(),
                backlink_labels: vec!["test this is the best test site".to_string()],
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index
            .insert(&Webpage {
                html: Html::parse(
                    r#"
                    <html>
                        <head>
                            <title>Second test site</title>
                        </head>
                        <body>
                            test test test test test test test
                        </body>
                    </html>
                "#,
                    "https://www.second.com",
                )
                .unwrap(),
                host_centrality: 0.00003,
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);
        let result = searcher
            .search(&SearchQuery {
                query: "test".to_string(),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.webpages.len(), 2);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");
    }

    #[test]
    fn custom_signal_aggregation() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    r#"
            <html>
                <head>
                    <title>Test website</title>
                </head>
                <body>
                    example
                </body>
            </html>
            "#,
                    "https://www.body.com",
                )
                .unwrap(),
                host_centrality: 1.0,
                fetch_time_ms: 20,
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index
            .insert(&Webpage {
                html: Html::parse(
                    r#"
            <html>
                <head>
                    <title>Example website</title>
                </head>
                <body>
                    test
                </body>
            </html>
            "#,
                    "https://www.title.com",
                )
                .unwrap(),
                host_centrality: 1.0,
                fetch_time_ms: 20,
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index
            .insert(&Webpage {
                html: Html::parse(
                    r#"
            <html>
                <head>
                    <title>Test website</title>
                    <meta property="og:description" content="example" />
                </head>
                <body>
                    test example
                </body>
            </html>
            "#,
                    "https://www.centrality.com",
                )
                .unwrap(),
                host_centrality: 1.02,
                fetch_time_ms: 500,
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index.commit().unwrap();

        let searcher = LocalSearcher::new(index);

        let res = searcher
            .search(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse(
                        r#"
                        Ranking(Signal("bm25_title"), 20000000);
                        Ranking(Signal("host_centrality"), 0);
                    "#,
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap();

        assert_eq!(res.webpages.len(), 3);
        assert_eq!(&res.webpages[0].url, "https://www.title.com/");

        let res = searcher
            .search(&SearchQuery {
                query: "example".to_string(),
                optic: Some(
                    Optic::parse(
                        r#"
                        Ranking(Signal("host_centrality"), 2000000)
                    "#,
                    )
                    .unwrap(),
                ),
                ..Default::default()
            })
            .unwrap();

        assert_eq!(res.webpages.len(), 3);
        assert_eq!(&res.webpages[0].url, "https://www.centrality.com/");
    }

    #[test]
    fn fetch_time_ranking() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                {CONTENT} {}
                            </body>
                        </html>
                    "#,
                        crate::rand_words(100)
                    ),
                    "https://www.first.com",
                )
                .unwrap(),
                host_centrality: 1.0,
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
                                <title>Test website</title>
                            </head>
                            <body>
                                {CONTENT} {}
                            </body>
                        </html>
                    "#,
                        crate::rand_words(100)
                    ),
                    "https://www.second.com",
                )
                .unwrap(),
                host_centrality: 1.0,
                fetch_time_ms: 5000,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::new(index);

        let result = searcher
            .search(&SearchQuery {
                query: "test".to_string(),
                optic: Some(Optic {
                    rankings: vec![RankingCoeff {
                        target: RankingTarget::Signal("fetch_time_ms".to_string()),
                        value: 100_000.0,
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.webpages.len(), 2);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");
        assert_eq!(result.webpages[1].url, "https://www.second.com/");
    }

    #[test]
    fn num_slashes_and_digits() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                {CONTENT} {}
                            </body>
                        </html>
                    "#,
                        crate::rand_words(100)
                    ),
                    "https://www.first.com/one",
                )
                .unwrap(),
                host_centrality: 1.0,
                fetch_time_ms: 2,
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
                                <title>Test website</title>
                            </head>
                            <body>
                                {CONTENT} {}
                            </body>
                        </html>
                    "#,
                        crate::rand_words(100)
                    ),
                    "https://www.second.com/one/two",
                )
                .unwrap(),
                host_centrality: 1.0,
                fetch_time_ms: 1,
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
                                <title>Test website</title>
                            </head>
                            <body>
                                {CONTENT} {}
                            </body>
                        </html>
                    "#,
                        crate::rand_words(100)
                    ),
                    "https://www.third.com/one/two123",
                )
                .unwrap(),
                host_centrality: 1.0,
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::new(index);

        let result = searcher
            .search(&SearchQuery {
                query: "test".to_string(),
                optic: Some(Optic {
                    rankings: vec![
                        RankingCoeff {
                            target: RankingTarget::Signal("url_slashes".to_string()),
                            value: 100_000.0,
                        },
                        RankingCoeff {
                            target: RankingTarget::Signal("url_digits".to_string()),
                            value: 100_000.0,
                        },
                    ],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.webpages.len(), 3);
        assert_eq!(result.webpages[0].url, "https://www.first.com/one");
        assert_eq!(result.webpages[1].url, "https://www.second.com/one/two");
        assert_eq!(result.webpages[2].url, "https://www.third.com/one/two123");
    }

    fn setup_worker(data_path: &Path) -> IndexingWorker {
        IndexingWorker::new(IndexingLocalConfig {
            host_centrality_store_path: crate::gen_temp_path().to_str().unwrap().to_string(),
            page_centrality_store_path: None,
            page_webgraph: None,
            topics_path: None,
            safety_classifier_path: None,
            dual_encoder: Some(IndexingDualEncoderConfig {
                model_path: data_path.to_str().unwrap().to_string(),
                page_centrality_rank_threshold: None,
            }),
            output_path: crate::gen_temp_path().to_str().unwrap().to_string(),
            limit_warc_files: None,
            skip_warc_files: None,
            warc_source: WarcSource::Local(crate::config::LocalConfig {
                folder: crate::gen_temp_path().to_str().unwrap().to_string(),
                names: vec!["".to_string()],
            }),
            host_centrality_threshold: None,
            minimum_clean_words: None,
            batch_size: 10,
        })
    }

    #[test]
    fn title_embeddings() {
        let data_path = Path::new("../../data/summarizer/dual_encoder");
        if !data_path.exists() {
            // Skip the test if the test data is not available
            return;
        }

        let worker = setup_worker(data_path);

        let mut index = Index::temporary().expect("Unable to open index");

        let mut pages = vec![
            Webpage::test_parse(
                &format!(
                    r#"
                <html>
                    <head>
                        <title>Homemade Heart Brownie Recipe</title>
                    </head>
                    <body>
                        best chocolate cake {CONTENT} {}
                    </body>
                </html>
            "#,
                    crate::rand_words(100)
                ),
                "https://www.a.com/",
            )
            .unwrap(),
            Webpage::test_parse(
                &format!(
                    r#"
                <html>
                    <head>
                        <title>How To Best Use an iMac as a Monitor for a PC</title>
                    </head>
                    <body>
                        best chocolate cake {CONTENT} {}
                    </body>
                </html>
            "#,
                    crate::rand_words(100)
                ),
                "https://www.b.com/",
            )
            .unwrap(),
        ];

        worker.set_title_embeddings(&mut pages);
        assert!(pages.iter().all(|p| p.title_embedding.is_some()));

        for page in pages {
            index.insert(&page).expect("failed to insert webpage");
        }

        index.commit().expect("failed to commit index");

        let mut searcher = LocalSearcher::new(index);
        searcher
            .set_dual_encoder(DualEncoder::open(data_path).expect("failed to open dual encoder"));

        let result = searcher
            .search(&SearchQuery {
                query: "best chocolate cake".to_string(),
                optic: Some(Optic {
                    rankings: vec![RankingCoeff {
                        target: RankingTarget::Signal("title_embedding_similarity".to_string()),
                        value: 100_000.0,
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.webpages.len(), 2);
        assert_eq!(result.webpages[0].url, "https://www.a.com/");
    }

    #[test]
    fn keyword_embeddings() {
        let data_path = Path::new("../../data/summarizer/dual_encoder");
        if !data_path.exists() {
            // Skip the test if the test data is not available
            return;
        }

        let worker = setup_worker(data_path);

        let mut index = Index::temporary().expect("Unable to open index");

        let mut a = Webpage::test_parse(
            &format!(
                r#"
                <html>
                    <head>
                        <title>Homemade Heart Brownie Recipe</title>
                    </head>
                    <body>
                        best chocolate cake {CONTENT} {}
                    </body>
                </html>
            "#,
                crate::rand_words(100)
            ),
            "https://www.a.com/",
        )
        .unwrap();

        a.keywords = vec![
            "chocolate".to_string(),
            "cake".to_string(),
            "recipe".to_string(),
        ];

        let mut b = Webpage::test_parse(
            &format!(
                r#"
                <html>
                    <head>
                        <title>How To Best Use an iMac as a Monitor for a PC</title>
                    </head>
                    <body>
                        best chocolate cake {CONTENT} {}
                    </body>
                </html>
            "#,
                crate::rand_words(100)
            ),
            "https://www.b.com/",
        )
        .unwrap();

        b.keywords = vec!["imac".to_string()];

        let mut pages = vec![a, b];

        worker.set_keyword_embeddings(&mut pages);

        assert!(pages.iter().all(|p| p.keyword_embedding.is_some()));

        for page in pages {
            index.insert(&page).expect("failed to insert webpage");
        }

        index.commit().expect("failed to commit index");

        let mut searcher = LocalSearcher::new(index);
        searcher
            .set_dual_encoder(DualEncoder::open(data_path).expect("failed to open dual encoder"));

        let result = searcher
            .search(&SearchQuery {
                query: "best chocolate cake".to_string(),
                optic: Some(Optic {
                    rankings: vec![RankingCoeff {
                        target: RankingTarget::Signal("keyword_embedding_similarity".to_string()),
                        value: 100_000.0,
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.webpages.len(), 2);
        assert_eq!(result.webpages[0].url, "https://www.a.com/");
    }
}
