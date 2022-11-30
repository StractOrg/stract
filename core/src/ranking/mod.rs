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

pub mod bm25;
pub mod centrality_store;
pub mod initial;
pub mod optics;
pub mod signal;

use std::sync::Arc;

use initial::InitialScoreTweaker;
use tantivy::collector::Collector;

use crate::{
    collector::{MaxDocsConsidered, TopDocs},
    fastfield_cache::FastFieldCache,
    inverted_index,
    searcher::NUM_RESULTS_PER_PAGE,
    webgraph::centrality::{approximate_harmonic, topic},
    webpage::region::{Region, RegionCount},
};

pub use self::signal::*;

pub struct Ranker {
    region_count: Arc<RegionCount>,
    selected_region: Option<Region>,
    max_docs: Option<MaxDocsConsidered>,
    offset: Option<usize>,
    aggregator: SignalAggregator,
    fastfield_cache: Arc<FastFieldCache>,
    de_rank_similar: bool,
    topic_scorer: Option<topic::Scorer>,
    num_results: Option<usize>,
}

impl Ranker {
    pub fn new(
        region_count: RegionCount,
        aggregator: SignalAggregator,
        fastfield_cache: Arc<FastFieldCache>,
    ) -> Self {
        Ranker {
            region_count: Arc::new(region_count),
            selected_region: None,
            offset: None,
            aggregator,
            max_docs: None,
            de_rank_similar: true,
            fastfield_cache,
            topic_scorer: None,
            num_results: None,
        }
    }

    pub fn with_region(mut self, region: Region) -> Self {
        self.selected_region = Some(region);
        self
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

    pub fn collector(&self) -> impl Collector<Fruit = Vec<inverted_index::WebsitePointer>> {
        let mut aggregator = self.aggregator.clone();

        if let Some(topic_scorer) = self.topic_scorer.clone() {
            aggregator.set_topic_scorer(topic_scorer);
        }

        let score_tweaker = InitialScoreTweaker::new(
            Arc::clone(&self.region_count),
            self.selected_region,
            aggregator,
            Arc::clone(&self.fastfield_cache),
        );

        let mut collector = TopDocs::with_limit(
            self.num_results.unwrap_or(NUM_RESULTS_PER_PAGE),
            Arc::clone(&self.fastfield_cache),
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

        collector.tweak_score(score_tweaker)
    }

    pub fn set_topic_scorer(&mut self, topic_scorer: topic::Scorer) {
        self.topic_scorer = Some(topic_scorer);
    }

    pub fn set_query_centrality(&mut self, approx: approximate_harmonic::Scorer) {
        self.aggregator.set_query_centrality(approx);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        human_website_annotations::{self, Info, Topic},
        index::Index,
        searcher::{LocalSearcher, SearchQuery},
        webgraph::{
            centrality::{
                approximate_harmonic::ApproximatedHarmonicCentrality, topic::TopicCentrality,
            },
            Node, WebgraphBuilder,
        },
        webpage::{Html, Link, Webpage},
    };

    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";
    const CONTENT_2: &str = "what should i write in this text what should i write in this text what should i write in this text what should i write in this text what should i write in this text what should i write in this text what should i write in this text";

    #[test]
    fn host_centrality_ranking() {
        let mut index = Index::temporary().expect("Unable to open index");

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
                        crate::rand_words(100)
                    ),
                    "https://www.a.com",
                ),
                backlinks: vec![],
                host_centrality: 0.0,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
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
                        crate::rand_words(100)
                    ),
                    "https://www.b.com",
                ),
                backlinks: vec![],
                host_centrality: 5.0,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);
        let result = searcher
            .search(&SearchQuery {
                original: "example".to_string(),
                selected_region: None,
                optic_program: None,
                skip_pages: None,
                site_rankings: None,
            })
            .expect("Search failed")
            .into_websites()
            .unwrap();
        assert_eq!(result.webpages.documents.len(), 2);
        assert_eq!(result.webpages.documents[0].url, "https://www.b.com");
        assert_eq!(result.webpages.documents[1].url, "https://www.a.com");
    }

    #[test]
    fn page_centrality_ranking() {
        let mut index = Index::temporary().expect("Unable to open index");

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
                        crate::rand_words(100)
                    ),
                    "https://www.a.com",
                ),
                backlinks: vec![],
                host_centrality: 0.0,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
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
                        crate::rand_words(100)
                    ),
                    "https://www.b.com",
                ),
                backlinks: vec![],
                host_centrality: 0.0,
                fetch_time_ms: 500,
                page_centrality: 5.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);
        let result = searcher
            .search(&SearchQuery {
                original: "example".to_string(),
                selected_region: None,
                optic_program: None,
                skip_pages: None,
                site_rankings: None,
            })
            .expect("Search failed")
            .into_websites()
            .unwrap();
        assert_eq!(result.webpages.documents.len(), 2);
        assert_eq!(result.webpages.documents[0].url, "https://www.b.com");
        assert_eq!(result.webpages.documents[1].url, "https://www.a.com");
    }

    #[test]
    fn navigational_search() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>DR Homepage</title>
                        </head>
                        <body>
                            {CONTENT} {}
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://www.dr.dk",
                ),
                backlinks: vec![],
                host_centrality: 0.0,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index
            .insert(Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Subsite dr</title>
                        </head>
                        <body>
                            {CONTENT} {}
                        </body>
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://www.dr.dk/whatever",
                ),
                backlinks: vec![],
                host_centrality: 0.0,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
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
                        dr and some other text {CONTENT} dk {}
                    </html>
                "#,
                        crate::rand_words(100)
                    ),
                    "https://www.b.com",
                ),
                backlinks: vec![],
                host_centrality: 0.003,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);
        let result = searcher
            .search(&SearchQuery {
                original: "dr dk".to_string(),
                selected_region: None,
                optic_program: None,
                skip_pages: None,
                site_rankings: None,
            })
            .expect("Search failed")
            .into_websites()
            .unwrap();

        assert_eq!(result.webpages.documents.len(), 3);
        assert_eq!(result.webpages.documents[0].url, "https://www.dr.dk");
    }

    #[test]
    fn freshness_ranking() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Title</title>
                            <meta property="og:updated_time" content="1999-06-22T19:37:34+00:00" />
                        </head>
                        <body>
                            {CONTENT}
                        </body>
                    </html>
                "#
                    ),
                    "https://www.old.com",
                ),
                backlinks: vec![],
                host_centrality: 0.092,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index
            .insert(Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Title</title>
                            <meta property="og:updated_time" content="2022-06-22T19:37:34+00:00" />
                        </head>
                        <body>
                            {CONTENT}
                        </body>
                    </html>
                "#
                    ),
                    "https://www.new.com",
                ),
                backlinks: vec![],
                host_centrality: 0.09,
                fetch_time_ms: 500,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                page_centrality: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);
        let result = searcher
            .search(&SearchQuery {
                original: "title".to_string(),
                selected_region: None,
                optic_program: None,
                skip_pages: None,
                site_rankings: None,
            })
            .expect("Search failed")
            .into_websites()
            .unwrap();

        assert_eq!(result.webpages.documents[0].url, "https://www.new.com");
    }

    #[test]
    fn derank_trackers() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage {
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
                ),
                backlinks: vec![],
                host_centrality: 0.0,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index
            .insert(Webpage {
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
                "https://www.second.com"),
                backlinks: vec![],
                host_centrality: 0.003,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
        })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);
        let result = searcher
            .search(&SearchQuery {
                original: "test".to_string(),
                selected_region: None,
                optic_program: None,
                skip_pages: None,
                site_rankings: None,
            })
            .expect("Search failed")
            .into_websites()
            .unwrap();

        assert_eq!(result.webpages.documents.len(), 2);
        assert_eq!(result.webpages.documents[0].url, "https://www.first.com");
    }

    #[test]
    fn backlink_text() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage {
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
                ),
                backlinks: vec![Link {
                    source: "https://www.second.com".to_string().into(),
                    destination: "https://www.first.com".to_string().into(),
                    text: "test this is the best test site".to_string(),
                }],
                host_centrality: 0.0,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index
            .insert(Webpage {
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
                ),
                backlinks: vec![],
                host_centrality: 0.00003,
                fetch_time_ms: 500,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                page_centrality: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);
        let result = searcher
            .search(&SearchQuery {
                original: "test".to_string(),
                selected_region: None,
                optic_program: None,
                skip_pages: None,
                site_rankings: None,
            })
            .expect("Search failed")
            .into_websites()
            .unwrap();

        assert_eq!(result.webpages.documents.len(), 2);
        assert_eq!(result.webpages.documents[0].url, "https://www.first.com");
    }

    #[test]
    fn custom_signal_aggregation() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage {
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
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                fetch_time_ms: 20,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                page_centrality: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");

        index
            .insert(Webpage {
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
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                fetch_time_ms: 20,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");

        index
            .insert(Webpage {
                html: Html::parse(
                    r#"
            <html>
                <head>
                    <title>Test website</title>
                    <meta property="og:description" content="example" />
                </head>
                <body>
                    test
                </body>
            </html>
            "#,
                    "https://www.centrality.com",
                ),
                backlinks: vec![],
                host_centrality: 1.02,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");

        index.commit().unwrap();

        let searcher = LocalSearcher::new(index);

        let res = searcher
            .search(&SearchQuery {
                original: "example".to_string(),
                selected_region: None,
                optic_program: Some(
                    r#"
                        Ranking{Field("title"), 20000000};
                        Ranking{Signal("host_centrality"), 0};
                    "#
                    .to_string(),
                ),
                skip_pages: None,
                site_rankings: None,
            })
            .unwrap()
            .into_websites()
            .unwrap();

        assert_eq!(res.webpages.num_docs, 3);
        assert_eq!(&res.webpages.documents[0].url, "https://www.title.com");

        let res = searcher
            .search(&SearchQuery {
                original: "example".to_string(),
                selected_region: None,
                optic_program: Some(
                    r#"
                        Ranking{Signal("host_centrality"), 2000000};
                    "#
                    .to_string(),
                ),
                skip_pages: None,
                site_rankings: None,
            })
            .unwrap()
            .into_websites()
            .unwrap();

        assert_eq!(res.webpages.num_docs, 3);
        assert_eq!(&res.webpages.documents[0].url, "https://www.centrality.com");
    }

    #[test]
    fn term_proximity_ranking() {
        let mut index = Index::temporary().expect("Unable to open index");
        index
            .insert(Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                {CONTENT} termA termB d d d d d d d d d {}
                            </body>
                        </html>
                    "#,
                        crate::rand_words(100)
                    ),
                    "https://www.first.com",
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index
            .insert(Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                {CONTENT} termA d d d d d d d d d termB {}
                            </body>
                        </html>
                    "#,
                        crate::rand_words(100)
                    ),
                    "https://www.third.com",
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                fetch_time_ms: 500,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                page_centrality: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index
            .insert(Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                {CONTENT} termA d d d d termB d d d d d {}
                            </body>
                        </html>
                    "#,
                        crate::rand_words(100)
                    ),
                    "https://www.second.com",
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                fetch_time_ms: 500,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                page_centrality: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::new(index);

        let result = searcher
            .search(&SearchQuery {
                original: "termA termB".to_string(),
                selected_region: None,
                optic_program: None,
                skip_pages: None,
                site_rankings: None,
            })
            .expect("Search failed")
            .into_websites()
            .unwrap()
            .webpages;

        assert_eq!(result.num_docs, 3);
        assert_eq!(result.documents.len(), 3);
        assert_eq!(result.documents[0].url, "https://www.first.com");
        assert_eq!(result.documents[1].url, "https://www.second.com");
        assert_eq!(result.documents[2].url, "https://www.third.com");
    }

    #[test]
    fn fetch_time_ranking() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage {
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
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                fetch_time_ms: 0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                page_centrality: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index
            .insert(Webpage {
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
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                fetch_time_ms: 5000,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::new(index);

        let result = searcher
            .search(&SearchQuery {
                original: "test".to_string(),
                selected_region: None,
                optic_program: None,
                skip_pages: None,
                site_rankings: None,
            })
            .expect("Search failed")
            .into_websites()
            .unwrap()
            .webpages;

        assert_eq!(result.num_docs, 2);
        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://www.first.com");
        assert_eq!(result.documents[1].url, "https://www.second.com");
    }

    #[test]
    fn crawl_stability() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage {
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
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                fetch_time_ms: 1000,
                pre_computed_score: 0.0,
                crawl_stability: 1.0,
                page_centrality: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index
            .insert(Webpage {
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
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                fetch_time_ms: 0,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::new(index);

        let result = searcher
            .search(&SearchQuery {
                original: "test".to_string(),
                selected_region: None,
                optic_program: None,
                skip_pages: None,
                site_rankings: None,
            })
            .expect("Search failed")
            .into_websites()
            .unwrap()
            .webpages;

        assert_eq!(result.num_docs, 2);
        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://www.first.com");
        assert_eq!(result.documents[1].url, "https://www.second.com");
    }

    #[test]
    fn topic_score() {
        let mut index = Index::temporary().expect("Unable to open index");
        let topic_a = Topic::from_string("/root/topic_a".to_string());
        let topic_b = Topic::from_string("/root/topic_b".to_string());

        index
            .insert(Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                <div>
                                    {CONTENT} topic_a
                                </div>
                                <div>
                                    {}
                                </div>
                            </body>
                        </html>
                    "#,
                        crate::rand_words(100)
                    ),
                    "https://www.a.com",
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                fetch_time_ms: 1000,
                pre_computed_score: 0.0,
                crawl_stability: 1.0,
                page_centrality: 0.0,
                primary_image: None,
                host_topic: Some(topic_a.clone()),
                node_id: Some(1),
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index
            .insert(Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                <div>
                                    {CONTENT} topic_b
                                </div>
                                <div>
                                    {}
                                </div>
                            </body>
                        </html>
                    "#,
                        crate::rand_words(100)
                    ),
                    "https://www.b.com",
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                fetch_time_ms: 1000,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: Some(topic_b.clone()),
                node_id: Some(2),
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index
            .insert(Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                <div>
                                    {CONTENT_2} topic_a topic_b
                                </div>
                                <div>
                                    {}
                                </div>
                            </body>
                        </html>
                    "#,
                        crate::rand_words(100)
                    ),
                    "https://www.wrong.com",
                ),
                backlinks: vec![],
                host_centrality: 1.01,
                fetch_time_ms: 0,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                node_id: Some(0),
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let mut mapper = HashMap::new();
        mapper.insert(
            "www.a.com".to_string(),
            Info {
                description: String::new(),
                topic: topic_a,
            },
        );
        mapper.insert(
            "www.b.com".to_string(),
            Info {
                description: String::new(),
                topic: topic_b,
            },
        );
        let topics = human_website_annotations::Mapper::from(mapper);

        let mut webgraph = WebgraphBuilder::new(crate::gen_temp_path())
            .with_host_graph()
            .open();

        webgraph.insert(
            Node::from("www.wrong.com"),
            Node::from("www.a.com"),
            String::new(),
        );
        webgraph.insert(
            Node::from("www.wrong.com"),
            Node::from("www.b.com"),
            String::new(),
        );

        webgraph.flush();

        let approx = ApproximatedHarmonicCentrality::new(&webgraph);

        let topic_centrality = TopicCentrality::build(&index, topics, webgraph, approx);

        let mut searcher = LocalSearcher::new(index);
        searcher.set_topic_centrality(topic_centrality);

        let result = searcher
            .search(&SearchQuery {
                original: "topic_a".to_string(),
                selected_region: None,
                optic_program: None,
                skip_pages: None,
                site_rankings: None,
            })
            .expect("Search failed")
            .into_websites()
            .unwrap()
            .webpages;

        assert_eq!(result.num_docs, 2);
        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://www.a.com");
        assert_eq!(result.documents[1].url, "https://www.wrong.com");

        let result = searcher
            .search(&SearchQuery {
                original: "topic_b".to_string(),
                selected_region: None,
                optic_program: None,
                skip_pages: None,
                site_rankings: None,
            })
            .expect("Search failed")
            .into_websites()
            .unwrap()
            .webpages;

        assert_eq!(result.num_docs, 2);
        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://www.b.com");
        assert_eq!(result.documents[1].url, "https://www.wrong.com");
    }
}
