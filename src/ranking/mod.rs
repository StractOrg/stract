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

    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[test]
    fn harmonic_ranking() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
                &format!(
                    r#"
                    <html>
                        <head>
                            <title>Website A</title>
                        </head>
                        {CONTENT}
                        <a href="https://www.b.com">B site is great</a>
                    </html>
                "#
                ),
                "https://www.a.com",
                vec![],
                0.0,
                500,
            ))
            .expect("failed to parse webpage");
        index
            .insert(Webpage::new(
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
                "https://www.b.com",
                vec![Link {
                    source: "https://www.a.com".to_string().into(),
                    destination: "https://www.b.com".to_string().into(),
                    text: "B site is great".to_string(),
                }],
                5.0,
                500,
            ))
            .expect("failed to parse webpage");

        index.commit().expect("failed to commit index");
        let searcher = Searcher::from(index);
        let result = searcher.search("great site").expect("Search failed");
        assert_eq!(result.webpages.documents.len(), 2);
        assert_eq!(result.webpages.documents[0].url, "https://www.b.com");
        assert_eq!(result.webpages.documents[1].url, "https://www.a.com");
    }

    #[test]
    fn navigational_search() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
                &format!(
                    r#"
                    <html>
                        <head>
                            <title>DR Homepage</title>
                        </head>
                        <body>
                            {CONTENT}
                        </body>
                    </html>
                "#
                ),
                "https://www.dr.dk",
                vec![],
                0.0,
                500,
            ))
            .expect("failed to parse webpage");
        index
            .insert(Webpage::new(
                &format!(
                    r#"
                    <html>
                        <head>
                            <title>Subsite dr</title>
                        </head>
                        <body>
                            {CONTENT}
                        </body>
                    </html>
                "#
                ),
                "https://www.dr.dk/whatever",
                vec![],
                0.0,
                500,
            ))
            .expect("failed to parse webpage");
        index
            .insert(Webpage::new(
                &format!(
                    r#"
                    <html>
                        <head>
                            <title>Website B</title>
                        </head>
                        dr dk dr dk and some other text {CONTENT}
                    </html>
                "#
                ),
                "https://www.b.com",
                vec![],
                0.003,
                500,
            ))
            .expect("failed to parse webpage");

        index.commit().expect("failed to commit index");
        let searcher = Searcher::from(index);
        let result = searcher.search("dr dk").expect("Search failed");

        assert_eq!(result.webpages.documents.len(), 3);
        assert_eq!(result.webpages.documents[0].url, "https://www.dr.dk");
    }

    #[test]
    fn freshness_ranking() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
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
                vec![],
                0.092,
                500,
            ))
            .expect("failed to parse webpage");
        index
            .insert(Webpage::new(
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
                vec![],
                0.09,
                500,
            ))
            .expect("failed to parse webpage");

        index.commit().expect("failed to commit index");
        let searcher = Searcher::from(index);
        let result = searcher.search("title").expect("Search failed");

        assert_eq!(result.webpages.documents[0].url, "https://www.new.com");
    }
}
