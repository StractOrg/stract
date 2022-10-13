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

use super::{
    goggles::{Action, Goggle, Instruction, PatternOption},
    SignalAggregator,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SiteRankings {
    pub preferred: Vec<String>,
    pub disliked: Vec<String>,
    pub blocked: Vec<String>,
}

impl SiteRankings {
    pub fn into_goggle(self) -> Goggle {
        let mut instructions = Vec::new();

        for site in self.preferred {
            instructions.push(Instruction {
                patterns: Vec::new(),
                options: vec![
                    PatternOption::Site(site),
                    PatternOption::Action(Action::Boost(5)),
                ],
            });
        }

        for site in self.disliked {
            instructions.push(Instruction {
                patterns: Vec::new(),
                options: vec![
                    PatternOption::Site(site),
                    PatternOption::Action(Action::Downrank(5)),
                ],
            });
        }

        for site in self.blocked {
            instructions.push(Instruction {
                patterns: Vec::new(),
                options: vec![
                    PatternOption::Site(site),
                    PatternOption::Action(Action::Discard),
                ],
            });
        }

        Goggle {
            aggregator: SignalAggregator::default(),
            instructions,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        index::Index,
        ranking::site_rankings::SiteRankings,
        searcher::{LocalSearcher, SearchQuery},
        webpage::{Html, Webpage},
    };
    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[test]
    fn site_rankings() {
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
                                {CONTENT}
                            </body>
                        </html>
                    "#
                    ),
                    "https://www.first.com",
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                fetch_time_ms: 5000,
                pre_computed_score: 0.0,
                page_centrality: 0.0,
                primary_image: None,
                node_id: None,
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
                                {CONTENT}
                            </body>
                        </html>
                    "#
                    ),
                    "https://www.second.com",
                ),
                backlinks: vec![],
                host_centrality: 1.0,
                fetch_time_ms: 0,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                primary_image: None,
                node_id: None,
            })
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::new(index, None, None);

        let result = searcher
            .search(&SearchQuery {
                original: "test".to_string(),
                selected_region: None,
                goggle_program: None,
                skip_pages: None,
                site_rankings: Some(SiteRankings {
                    preferred: vec!["first.com".to_string()],
                    disliked: vec!["second.com".to_string()],
                    blocked: vec![],
                }),
            })
            .expect("Search failed")
            .into_websites()
            .unwrap()
            .webpages;

        assert_eq!(result.num_docs, 2);
        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://www.first.com");
        assert_eq!(result.documents[1].url, "https://www.second.com");

        let result = searcher
            .search(&SearchQuery {
                original: "test".to_string(),
                selected_region: None,
                goggle_program: None,
                skip_pages: None,
                site_rankings: Some(SiteRankings {
                    preferred: vec![],
                    disliked: vec!["second.com".to_string()],
                    blocked: vec!["first.com".to_string()],
                }),
            })
            .expect("Search failed")
            .into_websites()
            .unwrap()
            .webpages;

        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.second.com");
    }
}
