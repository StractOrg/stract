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

const SCALE: f32 = 500.0;

pub mod ast;
mod const_query;
mod lexer;
mod pattern_query;

use std::{collections::HashMap, convert::TryFrom};

use crate::{
    query::union::UnionQuery,
    schema::{Field, TextField},
    webgraph::centrality::approximate_harmonic::ApproximatedHarmonicCentrality,
    Result,
};
use itertools::Itertools;
use logos::Logos;
use tantivy::{
    query::{BooleanQuery, BoostQuery, Occur, QueryClone},
    schema::Schema,
};

use self::{
    ast::{RankingTarget, RawAction, RawMatchPart, RawOptic, RawRule},
    const_query::ConstQuery,
    pattern_query::PatternQuery,
};

use crate::ranking::{signal::SignalAggregator, site_rankings::SiteRankings, Signal};

pub fn parse(optic: &str) -> Result<Optic> {
    let raw_optic = ast::parse(optic)?;

    Optic::try_from(raw_optic)
}

impl TryFrom<RawOptic> for Optic {
    type Error = crate::Error;

    fn try_from(raw: RawOptic) -> Result<Self> {
        let mut rules = Vec::new();

        for rule in raw.rules {
            rules.push(Rule::try_from(rule)?);
        }

        let mut boosts = HashMap::new();
        let mut coefficients = HashMap::new();

        for ranking in raw.rankings {
            match ranking.target {
                RankingTarget::Signal(name) => {
                    if let Some(signal) = Signal::from_string(name) {
                        coefficients.insert(signal, ranking.score);
                    }
                }
                RankingTarget::Field(name) => {
                    if let Some(field) = Field::from_name(name) {
                        if let Some(text_field) = field.as_text() {
                            boosts.insert(text_field, ranking.score);
                        }
                    }
                }
            }
        }

        let mut liked_sites = Vec::new();
        let mut disliked_sites = Vec::new();

        for pref in raw.site_preferences {
            match pref {
                ast::RawSitePreference::Like(site) => liked_sites.push(site),
                ast::RawSitePreference::Dislike(site) => disliked_sites.push(site),
            }
        }

        Ok(Self {
            rules,
            coefficients,
            boosts,
            discard_non_matching: raw.discard_non_matching,
            site_rankings: SiteRankings {
                liked: liked_sites,
                disliked: disliked_sites,
                blocked: Vec::new(), // blocked sites are handled by `$discard` syntax.
            },
        })
    }
}

impl TryFrom<RawRule> for Rule {
    type Error = crate::Error;

    fn try_from(raw: RawRule) -> Result<Self> {
        let mut matches = Vec::new();

        for matching in raw.matches.0 {
            matches.push(matching.try_into()?);
        }

        Ok(Rule {
            matches,
            action: raw.action.map(Action::from).unwrap_or(Action::Boost(1)),
        })
    }
}

impl From<RawAction> for Action {
    fn from(value: RawAction) -> Self {
        match value {
            RawAction::Boost(boost) => Action::Boost(boost),
            RawAction::Downrank(down_boost) => Action::Downrank(down_boost),
            RawAction::Discard => Action::Discard,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Matching {
    pub pattern: Vec<PatternPart>,
    pub location: MatchLocation,
}

impl Matching {
    fn pattern_query(&self, schema: &tantivy::schema::Schema) -> Box<dyn tantivy::query::Query> {
        match &self.location {
            MatchLocation::Site => {
                let site_field = schema
                    .get_field(Field::Text(TextField::Site).name())
                    .unwrap();

                PatternQuery::new(self.pattern.clone(), site_field).box_clone()
            }
            MatchLocation::Url => {
                let field = schema
                    .get_field(Field::Text(TextField::Url).name())
                    .unwrap();

                PatternQuery::new(self.pattern.clone(), field).box_clone()
            }
            MatchLocation::Domain => {
                let field = schema
                    .get_field(Field::Text(TextField::Domain).name())
                    .unwrap();

                PatternQuery::new(self.pattern.clone(), field).box_clone()
            }
            MatchLocation::Title => {
                let field = schema
                    .get_field(Field::Text(TextField::Title).name())
                    .unwrap();

                PatternQuery::new(self.pattern.clone(), field).box_clone()
            }
            MatchLocation::Description => {
                let desc_field = schema
                    .get_field(Field::Text(TextField::Description).name())
                    .unwrap();

                let dmoz_desc_field = schema
                    .get_field(Field::Text(TextField::DmozDescription).name())
                    .unwrap();

                UnionQuery::from(vec![
                    PatternQuery::new(self.pattern.clone(), desc_field).box_clone(),
                    PatternQuery::new(self.pattern.clone(), dmoz_desc_field).box_clone(),
                ])
                .box_clone()
            }
            MatchLocation::Content => {
                let field = schema
                    .get_field(Field::Text(TextField::CleanBody).name())
                    .unwrap();

                PatternQuery::new(self.pattern.clone(), field).box_clone()
            }
        }
    }
}

impl TryFrom<RawMatchPart> for Matching {
    type Error = crate::Error;

    fn try_from(raw: RawMatchPart) -> Result<Self> {
        let (s, loc) = match raw {
            RawMatchPart::Site(s) => (s, MatchLocation::Site),
            RawMatchPart::Url(s) => (s, MatchLocation::Url),
            RawMatchPart::Domain(s) => (s, MatchLocation::Domain),
            RawMatchPart::Title(s) => (s, MatchLocation::Title),
            RawMatchPart::Description(s) => (s, MatchLocation::Description),
            RawMatchPart::Content(s) => (s, MatchLocation::Content),
        };

        let mut pattern = Vec::new();

        for tok in PatternToken::lexer(&s) {
            match tok {
                PatternToken::Raw(s) => pattern.push(PatternPart::Raw(s)),
                PatternToken::Wildcard => pattern.push(PatternPart::Wildcard),
                PatternToken::Anchor => pattern.push(PatternPart::Anchor),
                PatternToken::Error => return Err(crate::Error::Parse),
            }
        }

        Ok(Self {
            location: loc,
            pattern,
        })
    }
}

#[derive(Logos, Debug)]
enum PatternToken {
    #[regex(".*", |lex| lex.slice().to_string())]
    Raw(String),

    #[token("*")]
    Wildcard,

    #[token("|")]
    Anchor,

    #[error]
    Error,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PatternPart {
    Raw(String),
    Wildcard,
    Anchor,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum MatchLocation {
    Site,
    Url,
    Domain,
    Title,
    Description,
    Content,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Action {
    Boost(u64),
    Downrank(u64),
    Discard,
}

#[derive(Debug, Default, Clone)]
pub struct Optic {
    pub coefficients: HashMap<Signal, f64>,
    pub boosts: HashMap<TextField, f64>,
    pub site_rankings: SiteRankings,
    pub rules: Vec<Rule>,
    pub discard_non_matching: bool,
}

impl Optic {
    pub fn as_tantivy(&self, schema: &Schema) -> Vec<(Occur, Box<dyn tantivy::query::Query>)> {
        if self.discard_non_matching {
            vec![(
                Occur::Must,
                UnionQuery::from(
                    self.rules
                        .iter()
                        .chain(self.site_rankings.rules().iter())
                        .filter_map(|rule| rule.as_tantivy(schema))
                        .map(|query| BooleanQuery::from(vec![query]).box_clone())
                        .collect_vec(),
                )
                .box_clone(),
            )]
        } else {
            self.rules
                .iter()
                .chain(self.site_rankings.rules().iter())
                .filter_map(|rule| rule.as_tantivy(schema))
                .collect()
        }
    }

    pub fn merge(mut self, mut other: Self) -> Self {
        self.rules.append(&mut other.rules);
        self.coefficients.extend(other.coefficients.into_iter());
        self.boosts.extend(other.boosts.into_iter());

        self.discard_non_matching |= other.discard_non_matching;

        self.site_rankings
            .liked
            .append(&mut other.site_rankings.liked);

        self.site_rankings
            .disliked
            .append(&mut other.site_rankings.disliked);

        self.site_rankings
            .blocked
            .append(&mut other.site_rankings.blocked);

        self
    }

    pub fn aggregator(&self, approx: Option<&ApproximatedHarmonicCentrality>) -> SignalAggregator {
        let mut aggregator = SignalAggregator::new(
            self.coefficients.clone().into_iter(),
            self.boosts.clone().into_iter(),
        );

        if let Some(approx) = approx {
            aggregator.add_personal_harmonic(self.site_rankings.centrality_scorer(approx));
        }

        aggregator
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Rule {
    pub matches: Vec<Matching>,
    pub action: Action,
}

impl Rule {
    pub fn as_tantivy(&self, schema: &Schema) -> Option<(Occur, Box<dyn tantivy::query::Query>)> {
        let mut subqueries: Vec<_> = self
            .matches
            .iter()
            .map(|matching| (Occur::Must, matching.pattern_query(schema)))
            .collect();

        if subqueries.is_empty() {
            return None;
        }

        let subquery = if subqueries.len() == 1 {
            subqueries.pop().unwrap().1
        } else {
            BooleanQuery::from(subqueries).box_clone()
        };

        match &self.action {
            Action::Boost(boost) => Some((
                Occur::Should,
                BoostQuery::new(
                    ConstQuery::new(subquery, 1.0).box_clone(),
                    *boost as f32 * SCALE,
                )
                .box_clone(),
            )),
            Action::Downrank(boost) => Some((
                Occur::Should,
                BoostQuery::new(
                    ConstQuery::new(subquery, 1.0).box_clone(),
                    *boost as f32 * -SCALE,
                )
                .box_clone(),
            )),
            Action::Discard => Some((Occur::MustNot, subquery)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        gen_temp_path,
        index::Index,
        ranking::centrality_store::CentralityStore,
        schema::create_schema,
        searcher::{LocalSearcher, SearchQuery},
        webgraph::{Node, WebgraphBuilder},
        webpage::{Html, Webpage},
    };

    use super::*;
    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[test]
    fn discard_and_boost_sites() {
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
                page_centrality: 0.0,
                fetch_time_ms: 500,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                node_id: None,
                host_topic: None,
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
                host_centrality: 0.01,
                page_centrality: 0.0,
                primary_image: None,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                fetch_time_ms: 500,
                node_id: None,
                host_topic: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search(&SearchQuery {
                original: "website".to_string(),
                selected_region: None,
                optic_program: None,
                skip_pages: None,
                site_rankings: None,
            })
            .unwrap()
            .into_websites()
            .unwrap()
            .webpages
            .documents;

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].url, "https://www.b.com");
        assert_eq!(res[1].url, "https://www.a.com");

        let res = searcher
            .search(&SearchQuery {
                original: "website".to_string(),
                selected_region: None,
                optic_program: Some(
                    r#"
                        Rule {
                            Matches {
                                Domain("b.com")
                            },
                            Action(Discard)
                        }
                    "#
                    .to_string(),
                ),
                skip_pages: None,
                site_rankings: None,
            })
            .unwrap()
            .into_websites()
            .unwrap()
            .webpages
            .documents;

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].url, "https://www.a.com");

        let res = searcher
            .search(&SearchQuery {
                original: "website".to_string(),
                selected_region: None,
                optic_program: Some(
                    r#"
                        Rule {
                            Matches {
                                Domain("a.com")
                            },
                            Action(Boost(10))
                        }
                    "#
                    .to_string(),
                ),
                skip_pages: None,
                site_rankings: None,
            })
            .unwrap()
            .into_websites()
            .unwrap()
            .webpages
            .documents;

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].url, "https://www.a.com");
        assert_eq!(res[1].url, "https://www.b.com");
    }

    #[test]
    fn quickstart_as_query() {
        parse(include_str!("../../testcases/optics/quickstart.optic"))
            .unwrap()
            .as_tantivy(&create_schema());
    }

    #[test]
    fn example_optics_dont_crash() {
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
                            {CONTENT}
                            example example example
                        </body>
                    </html>
                "#
                    ),
                    "https://www.a.com/this/is/a/pattern",
                ),
                backlinks: vec![],
                host_centrality: 0.0,
                page_centrality: 0.0,
                fetch_time_ms: 500,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                node_id: None,
                host_topic: None,
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
                            {CONTENT}
                        </body>
                    </html>
                "#
                    ),
                    "https://www.b.com/this/is/b/pattern",
                ),
                backlinks: vec![],
                host_centrality: 0.0001,
                page_centrality: 0.0,
                primary_image: None,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                fetch_time_ms: 500,
                node_id: None,
                host_topic: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let _ = searcher
            .search(&SearchQuery {
                original: "website".to_string(),
                selected_region: None,
                optic_program: Some(
                    include_str!("../../testcases/optics/quickstart.optic").to_string(),
                ),
                skip_pages: None,
                site_rankings: None,
            })
            .unwrap()
            .into_websites()
            .unwrap()
            .webpages
            .documents;

        let _ = searcher
            .search(&SearchQuery {
                original: "website".to_string(),
                selected_region: None,
                optic_program: Some(
                    include_str!("../../testcases/optics/hacker_news.optic").to_string(),
                ),
                skip_pages: None,
                site_rankings: None,
            })
            .unwrap()
            .into_websites()
            .unwrap()
            .webpages
            .documents;

        let _ = searcher
            .search(&SearchQuery {
                original: "website".to_string(),
                selected_region: None,
                optic_program: Some(
                    include_str!("../../testcases/optics/copycats_removal.optic").to_string(),
                ),
                skip_pages: None,
                site_rankings: None,
            })
            .unwrap()
            .into_websites()
            .unwrap()
            .webpages
            .documents;
    }

    #[test]
    fn empty_discard() {
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
                    "https://www.a.com/this/is/a/pattern",
                ),
                backlinks: vec![],
                host_centrality: 0.0,
                page_centrality: 0.0,
                fetch_time_ms: 500,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                node_id: None,
                host_topic: None,
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
                    "https://www.b.com/this/is/b/pattern",
                ),
                backlinks: vec![],
                host_centrality: 0.0001,
                page_centrality: 0.0,
                primary_image: None,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                fetch_time_ms: 500,
                node_id: None,
                dmoz_description: None,
                host_topic: None,
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
                    "https://www.c.com/this/is/c/pattern",
                ),
                backlinks: vec![],
                host_centrality: 0.0001,
                page_centrality: 0.0,
                primary_image: None,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                fetch_time_ms: 500,
                node_id: None,
                host_topic: None,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let searcher = LocalSearcher::from(index);

        let res = searcher
            .search(&SearchQuery {
                original: "website".to_string(),
                selected_region: None,
                optic_program: Some(
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
                "#
                    .to_string(),
                ),
                skip_pages: None,
                site_rankings: None,
            })
            .unwrap()
            .into_websites()
            .unwrap()
            .webpages
            .documents;

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].url, "https://www.a.com/this/is/a/pattern");
    }

    #[test]
    fn liked_sites() {
        let mut index = Index::temporary().expect("Unable to open index");

        let mut graph = WebgraphBuilder::new_memory()
            .with_full_graph()
            .with_host_graph()
            .open();

        graph.insert(
            Node::from("https://www.a.com").into_host(),
            Node::from("https://www.b.com").into_host(),
            String::new(),
        );

        graph.insert(
            Node::from("https://www.c.com").into_host(),
            Node::from("https://www.c.com").into_host(),
            String::new(),
        );

        graph.flush();

        let centrality_store = CentralityStore::build(&graph, gen_temp_path());

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
                    "https://www.a.com/this/is/a/pattern",
                ),
                backlinks: vec![],
                host_centrality: 0.0,
                page_centrality: 0.0,
                fetch_time_ms: 500,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
                dmoz_description: None,
                node_id: Some(
                    *centrality_store
                        .approx_harmonic
                        .node2id
                        .get(&Node::from("www.a.com").into_host())
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
                ),
                backlinks: vec![],
                host_centrality: 0.0001,
                page_centrality: 0.0,
                primary_image: None,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                fetch_time_ms: 500,
                dmoz_description: None,
                host_topic: None,
                node_id: Some(
                    *centrality_store
                        .approx_harmonic
                        .node2id
                        .get(&Node::from("www.b.com").into_host())
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
                ),
                backlinks: vec![],
                host_centrality: 0.0002,
                page_centrality: 0.0,
                primary_image: None,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                fetch_time_ms: 500,
                host_topic: None,
                dmoz_description: None,
                node_id: Some(
                    *centrality_store
                        .approx_harmonic
                        .node2id
                        .get(&Node::from("www.c.com").into_host())
                        .unwrap(),
                ),
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");
        let mut searcher = LocalSearcher::from(index);

        searcher.set_centrality_store(centrality_store);

        let res = searcher
            .search(&SearchQuery {
                original: "website".to_string(),
                selected_region: None,
                optic_program: Some(
                    r#"
                    Like(Site("www.a.com"));
                    Like(Site("www.b.com"));
                    Dislike(Site("www.c.com"));
                "#
                    .to_string(),
                ),
                skip_pages: None,
                site_rankings: None,
            })
            .unwrap()
            .into_websites()
            .unwrap()
            .webpages
            .documents;

        assert_eq!(res.len(), 3);
        assert_eq!(res[0].url, "https://www.b.com/this/is/b/pattern");
        assert_eq!(res[1].url, "https://www.a.com/this/is/a/pattern");
        assert_eq!(res[2].url, "https://www.c.com/this/is/c/pattern");
    }
}
