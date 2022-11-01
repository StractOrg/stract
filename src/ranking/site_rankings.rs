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

use serde::{Deserialize, Serialize};

use crate::{
    webgraph::{
        centrality::approximate_harmonic::{ApproximatedHarmonicCentrality, Scorer},
        Node,
    },
    webpage::Url,
};

use super::goggles::{Action, Goggle, Instruction, PatternOption};

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct SiteRankings {
    pub liked: Vec<String>,
    pub disliked: Vec<String>,
    pub blocked: Vec<String>,
}

impl SiteRankings {
    pub fn instructions(&self) -> Vec<Instruction> {
        let mut instructions = Vec::new();

        for site in &self.liked {
            instructions.push(Instruction {
                patterns: Vec::new(),
                options: vec![
                    PatternOption::Site(site.clone()),
                    PatternOption::Action(Action::Boost(5)),
                ],
            });
        }

        for site in &self.disliked {
            instructions.push(Instruction {
                patterns: Vec::new(),
                options: vec![
                    PatternOption::Site(site.clone()),
                    PatternOption::Action(Action::Downrank(5)),
                ],
            });
        }

        for site in &self.blocked {
            instructions.push(Instruction {
                patterns: Vec::new(),
                options: vec![
                    PatternOption::Site(site.clone()),
                    PatternOption::Action(Action::Discard),
                ],
            });
        }

        instructions
    }

    pub fn centrality_scorer(&self, approx_harmonic: &ApproximatedHarmonicCentrality) -> Scorer {
        let mut liked_nodes = Vec::new();
        let mut disliked_nodes = Vec::new();

        for site in &self.liked {
            liked_nodes.push(Node::from_url(&Url::from(site.clone())).into_host());
        }

        for site in &self.disliked {
            disliked_nodes.push(Node::from_url(&Url::from(site.clone())).into_host());
        }

        approx_harmonic.scorer(&liked_nodes, &disliked_nodes)
    }

    pub fn into_goggle(self) -> Goggle {
        Goggle {
            site_rankings: self,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        gen_temp_path,
        index::Index,
        ranking::{centrality_store::CentralityStore, site_rankings::SiteRankings},
        searcher::{LocalSearcher, SearchQuery},
        webgraph::{Node, WebgraphBuilder},
        webpage::{Html, Webpage},
    };
    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[test]
    fn site_rankings() {
        let mut index = Index::temporary().expect("Unable to open index");

        let mut graph = WebgraphBuilder::new_memory()
            .with_full_graph()
            .with_host_graph()
            .open();

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

        graph.flush();

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
                fetch_time_ms: 5000,
                pre_computed_score: 0.0,
                page_centrality: 0.0,
                primary_image: None,
                crawl_stability: 0.0,
                dmoz_description: None,
                host_topic: None,
                node_id: Some(
                    graph
                        .host
                        .as_ref()
                        .unwrap()
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
                fetch_time_ms: 2000,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                primary_image: None,
                crawl_stability: 0.0,
                host_topic: None,
                dmoz_description: None,
                node_id: Some(
                    graph
                        .host
                        .as_ref()
                        .unwrap()
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
                        .host
                        .as_ref()
                        .unwrap()
                        .node2id(&Node::from("https://www.third.com").into_host())
                        .unwrap(),
                ),
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let mut searcher = LocalSearcher::new(index);

        searcher.set_centrality_store(CentralityStore::build(&graph, gen_temp_path()));

        let result = searcher
            .search(&SearchQuery {
                original: "test".to_string(),
                selected_region: None,
                goggle_program: None,
                skip_pages: None,
                site_rankings: Some(SiteRankings {
                    liked: vec!["first.com".to_string()],
                    disliked: vec!["third.com".to_string()],
                    blocked: vec![],
                }),
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

        let result = searcher
            .search(&SearchQuery {
                original: "test".to_string(),
                selected_region: None,
                goggle_program: None,
                skip_pages: None,
                site_rankings: Some(SiteRankings {
                    liked: vec!["first.com".to_string()],
                    disliked: vec![],
                    blocked: vec![],
                }),
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

        let result = searcher
            .search(&SearchQuery {
                original: "test".to_string(),
                selected_region: None,
                goggle_program: None,
                skip_pages: None,
                site_rankings: Some(SiteRankings {
                    liked: vec![],
                    disliked: vec!["second.com".to_string()],
                    blocked: vec!["first.com".to_string()],
                }),
            })
            .expect("Search failed")
            .into_websites()
            .unwrap()
            .webpages;

        assert_eq!(result.num_docs, 2);
        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://www.third.com");
        assert_eq!(result.documents[1].url, "https://www.second.com");
    }
}
