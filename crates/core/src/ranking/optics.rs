// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use optics::HostRankings;
    use tokio::sync::RwLock;

    use crate::{
        bangs::Bangs,
        index::Index,
        searcher::{api::ApiSearcher, LocalSearchClient, LocalSearcher, SearchQuery},
        webgraph::{Edge, Node, Webgraph},
        webpage::{Html, Webpage},
    };
    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn host_rankings() {
        let dir = crate::gen_temp_dir().unwrap();
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        let mut graph = Webgraph::open(&dir, 0u64.into()).unwrap();

        graph
            .insert(Edge::new_test(
                Node::from("https://www.first.com").into_host(),
                Node::from("https://www.nan.com").into_host(),
            ))
            .unwrap();
        graph
            .insert(Edge::new_test(
                Node::from("https://www.nan.com").into_host(),
                Node::from("https://www.first.com").into_host(),
            ))
            .unwrap();
        graph
            .insert(Edge::new_test(
                Node::from("https://www.third.com").into_host(),
                Node::from("https://www.third.com").into_host(),
            ))
            .unwrap();
        graph
            .insert(Edge::new_test(
                Node::from("https://www.nan.com").into_host(),
                Node::from("https://www.second.com").into_host(),
            ))
            .unwrap();
        graph
            .insert(Edge::new_test(
                Node::from("https://www.second.com").into_host(),
                Node::from("https://www.nan.com").into_host(),
            ))
            .unwrap();
        graph
            .insert(Edge::new_test(
                Node::from("https://www.second.com").into_host(),
                Node::from("https://www.third.com").into_host(),
            ))
            .unwrap();
        graph
            .insert(Edge::new_test(
                Node::from("https://www.extra.com").into_host(),
                Node::from("https://www.first.com").into_host(),
            ))
            .unwrap();
        graph
            .insert(Edge::new_test(
                Node::from("https://www.second.com").into_host(),
                Node::from("https://www.extra.com").into_host(),
            ))
            .unwrap();
        graph.commit().unwrap();

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
                fetch_time_ms: 500,
                node_id: Some(Node::from("https://www.first.com").into_host().id()),
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
                fetch_time_ms: 499,
                node_id: Some(Node::from("https://www.second.com").into_host().id()),
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
                    "https://www.third.com",
                )
                .unwrap(),
                host_centrality: 1.0,
                node_id: Some(Node::from("https://www.third.com").into_host().id()),
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher: ApiSearcher<_, _> = ApiSearcher::new(
            LocalSearchClient::from(LocalSearcher::builder(Arc::new(RwLock::new(index))).build()),
            None,
            Bangs::empty(),
            crate::searcher::api::Config::default(),
        )
        .await
        .with_webgraph(graph);

        let result = searcher
            .search(&SearchQuery {
                query: "test".to_string(),
                host_rankings: Some(HostRankings {
                    liked: vec!["www.first.com".to_string()],
                    disliked: vec![],
                    blocked: vec![],
                }),

                signal_coefficients: crate::enum_map! {
                    crate::ranking::SignalEnum::from(crate::ranking::signals::InboundSimilarity) => 100_000_000.0,
                }.into(),

                ..Default::default()
            })
            .await
            .expect("Search failed")
            .into_websites_result();

        assert_eq!(result.webpages.len(), 3);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");
        assert_eq!(result.webpages[1].url, "https://www.second.com/");
        assert_eq!(result.webpages[2].url, "https://www.third.com/");

        let result = searcher
            .search(&SearchQuery {
                query: "test".to_string(),
                host_rankings: Some(HostRankings {
                    liked: vec![],
                    disliked: vec!["www.second.com".to_string()],
                    blocked: vec!["first.com".to_string()],
                }),

                signal_coefficients: crate::enum_map! {
                    crate::ranking::SignalEnum::from(crate::ranking::signals::InboundSimilarity) => 100_000_000.0,
                }.into(),

                return_ranking_signals: true,
                ..Default::default()
            })
            .await
            .expect("Search failed")
            .into_websites_result();

        assert_eq!(result.webpages.len(), 2);
        assert_eq!(result.webpages[0].url, "https://www.third.com/");
        assert_eq!(result.webpages[1].url, "https://www.second.com/");

        let result = searcher
            .search(&SearchQuery {
                query: "test".to_string(),
                host_rankings: Some(HostRankings {
                    liked: vec!["first.com".to_string()],
                    disliked: vec![],
                    blocked: vec!["abc.first.com".to_string()],
                }),

                signal_coefficients: crate::enum_map! {
                    crate::ranking::SignalEnum::from(crate::ranking::signals::InboundSimilarity) => 100_000_000.0,
                }.into(),

                return_ranking_signals: true,
                ..Default::default()
            })
            .await
            .expect("Search failed")
            .into_websites_result();

        assert_eq!(result.webpages.len(), 3);
        assert_eq!(result.webpages[0].url, "https://www.first.com/");
        assert_eq!(result.webpages[1].url, "https://www.second.com/");
        assert_eq!(result.webpages[2].url, "https://www.third.com/");
    }
}
