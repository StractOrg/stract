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

pub mod centrality_store;
mod initial;

use crate::query::Query;
use initial::InitialScoreTweaker;
use tantivy::collector::{Collector, TopDocs};

pub struct Ranker {}

impl Ranker {
    pub fn new(_query: Query) -> Self {
        Ranker {}
    }

    pub fn collector(&self) -> impl Collector<Fruit = Vec<(f64, tantivy::DocAddress)>> {
        let score_tweaker = InitialScoreTweaker::default();
        TopDocs::with_limit(20).tweak_score(score_tweaker)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        index::Index,
        searcher::Searcher,
        webpage::{Link, Webpage},
    };

    #[test]
    fn harmonic_ranking() {
        for _ in 0..10 {
            let mut index = Index::temporary().expect("Unable to open index");

            index
                .insert(Webpage::new(
                    r#"
                        <html>
                            <head>
                                <title>Website A</title>
                            </head>
                            <a href="https://www.b.com">B site is great</a>
                        </html>
                    "#,
                    "https://www.a.com",
                    vec![],
                    0.0,
                ))
                .expect("failed to parse webpage");
            index
                .insert(Webpage::new(
                    r#"
                        <html>
                            <head>
                                <title>Website B</title>
                            </head>
                            <body>
                                body
                            </body>
                        </html>
                    "#,
                    "https://www.b.com",
                    vec![Link {
                        source: "https://www.a.com".to_string(),
                        destination: "https://www.b.com".to_string(),
                        text: "B site is great".to_string(),
                    }],
                    5.0,
                ))
                .expect("failed to parse webpage");

            index.commit().expect("failed to commit index");
            let searcher = Searcher::from(index);
            let result = searcher.search("great site").expect("Search failed");
            assert_eq!(result.documents.len(), 2);
            assert_eq!(result.documents[0].url, "https://www.b.com");
            assert_eq!(result.documents[1].url, "https://www.a.com");
        }
    }

    #[test]
    fn navigational_search() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
                r#"
                    <html>
                        <head>
                            <title>DR Homepage</title>
                        </head>
                        <body>
                            body
                        </body>
                    </html>
                "#,
                "https://www.dr.dk",
                vec![],
                0.0,
            ))
            .expect("failed to parse webpage");
        index
            .insert(Webpage::new(
                r#"
                    <html>
                        <head>
                            <title>Subsite dr</title>
                        </head>
                        <body>
                            body
                        </body>
                    </html>
                "#,
                "https://www.dr.dk/whatever",
                vec![],
                0.0,
            ))
            .expect("failed to parse webpage");
        index
            .insert(Webpage::new(
                r#"
                    <html>
                        <head>
                            <title>Website B</title>
                        </head>
                        dr dk dr dk and some other text
                    </html>
                "#,
                "https://www.b.com",
                vec![],
                0.003,
            ))
            .expect("failed to parse webpage");

        index.commit().expect("failed to commit index");
        let searcher = Searcher::from(index);
        let result = searcher.search("dr dk").expect("Search failed");

        assert_eq!(result.documents.len(), 3);
        assert_eq!(result.documents[0].url, "https://www.dr.dk");
        assert_eq!(result.documents[1].url, "https://www.b.com");
        assert_eq!(result.documents[2].url, "https://www.dr.dk/whatever");
    }
}
