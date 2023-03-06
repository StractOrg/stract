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

use optics::ast::RankingStage;

use crate::schema::Field;

use super::{Signal, SignalAggregator};

pub const SCALE: f32 = 10.0;

pub trait CreateAggregator {
    fn aggregator(&self) -> SignalAggregator;
}

impl CreateAggregator for RankingStage {
    fn aggregator(&self) -> SignalAggregator {
        let aggregator = SignalAggregator::new(
            self.coefficients
                .iter()
                .filter_map(|coeff| match &coeff.target {
                    optics::ast::RankingTarget::Signal(s) => {
                        Signal::from_string(s.clone()).map(|s| (s, coeff.score))
                    }
                    optics::ast::RankingTarget::Field(_) => None,
                }),
            self.coefficients
                .iter()
                .filter_map(|coeff| match &coeff.target {
                    optics::ast::RankingTarget::Signal(_) => None,
                    optics::ast::RankingTarget::Field(f) => Field::from_name(f.clone())
                        .and_then(|f| f.as_text())
                        .map(|f| (f, coeff.score)),
                }),
        );

        aggregator
    }
}

#[cfg(test)]
mod tests {
    use optics::SiteRankings;

    use crate::{
        gen_temp_path,
        index::Index,
        ranking::centrality_store::CentralityStore,
        searcher::{LocalSearcher, SearchQuery},
        webgraph::{Node, WebgraphBuilder},
        webpage::{Html, Webpage},
    };
    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[test]
    fn site_rankings() {
        let mut index = Index::temporary().expect("Unable to open index");

        let mut graph = WebgraphBuilder::new_memory().open();

        graph.insert(
            Node::from("https://www.first.com").into_host(),
            Node::from("https://www.second.com").into_host(),
            String::new(),
        );
        graph.insert(
            Node::from("https://www.third.com").into_host(),
            Node::from("https://www.third.com").into_host(),
            String::new(),
        );

        graph.commit();

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
                fetch_time_ms: 50,
                pre_computed_score: 0.0,
                page_centrality: 0.0,
                primary_image: None,
                crawl_stability: 0.0,
                dmoz_description: None,
                host_topic: None,
                node_id: Some(
                    graph
                        .node2id(&Node::from("https://www.first.com").into_host())
                        .unwrap(),
                ),
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
                fetch_time_ms: 49,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                primary_image: None,
                crawl_stability: 0.0,
                host_topic: None,
                dmoz_description: None,
                node_id: Some(
                    graph
                        .node2id(&Node::from("https://www.second.com").into_host())
                        .unwrap(),
                ),
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
                    "https://www.third.com",
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                fetch_time_ms: 0,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                primary_image: None,
                crawl_stability: 0.0,
                dmoz_description: None,
                host_topic: None,
                node_id: Some(
                    graph
                        .node2id(&Node::from("https://www.third.com").into_host())
                        .unwrap(),
                ),
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let mut searcher = LocalSearcher::new(index);

        searcher.set_centrality_store(CentralityStore::build(&graph, gen_temp_path()).into());

        let result = searcher
            .search(&SearchQuery {
                query: "test".to_string(),
                site_rankings: Some(SiteRankings {
                    liked: vec!["www.first.com".to_string()],
                    disliked: vec!["www.third.com".to_string()],
                    blocked: vec![],
                }),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.num_hits, 3);
        assert_eq!(result.webpages.len(), 3);
        assert_eq!(result.webpages[0].url, "https://www.first.com");
        assert_eq!(result.webpages[1].url, "https://www.second.com");
        assert_eq!(result.webpages[2].url, "https://www.third.com");

        let result = searcher
            .search(&SearchQuery {
                query: "test".to_string(),
                site_rankings: Some(SiteRankings {
                    liked: vec!["www.first.com".to_string()],
                    disliked: vec![],
                    blocked: vec![],
                }),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.num_hits, 3);
        assert_eq!(result.webpages.len(), 3);
        assert_eq!(result.webpages[0].url, "https://www.first.com");
        assert_eq!(result.webpages[1].url, "https://www.second.com");
        assert_eq!(result.webpages[2].url, "https://www.third.com");

        let result = searcher
            .search(&SearchQuery {
                query: "test".to_string(),
                site_rankings: Some(SiteRankings {
                    liked: vec![],
                    disliked: vec!["www.second.com".to_string()],
                    blocked: vec!["www.first.com".to_string()],
                }),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.num_hits, 2);
        assert_eq!(result.webpages.len(), 2);
        assert_eq!(result.webpages[0].url, "https://www.third.com");
        assert_eq!(result.webpages[1].url, "https://www.second.com");
    }
}
