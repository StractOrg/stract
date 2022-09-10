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

mod ast;
mod pattern_query;
mod signal;

use std::convert::TryFrom;

use crate::{schema::Field, tokenizer, Result};
use itertools::Itertools;
use tantivy::{
    query::{BooleanQuery, BoostQuery, Occur, PhraseQuery, QueryClone, TermQuery},
    schema::{IndexRecordOption, Schema},
    tokenizer::TextAnalyzer,
};

pub use self::signal::*;
use self::{
    ast::{RawAction, RawGoggle, RawInstruction, RawPatternOption, RawPatternPart},
    pattern_query::PatternQuery,
};

pub fn parse(goggle: &str) -> Result<Goggle> {
    let raw_goggle = ast::parse(goggle)?;

    Goggle::try_from(raw_goggle)
}

impl TryFrom<RawGoggle> for Goggle {
    type Error = crate::Error;

    fn try_from(raw: RawGoggle) -> Result<Self> {
        let mut instructions = Vec::new();

        for inst in raw.instructions {
            instructions.push(Instruction::try_from(inst)?);
        }

        Ok(Self {
            aggregator: SignalAggregator::try_from(raw.alterations)?,
            instructions,
        })
    }
}

impl TryFrom<RawInstruction> for Instruction {
    type Error = crate::Error;

    fn try_from(value: RawInstruction) -> Result<Self> {
        let mut patterns = Vec::new();

        for pattern in value.patterns {
            patterns.push(pattern.into());
        }

        let mut options = Vec::new();

        for option in value.options {
            options.push(option.try_into()?);
        }

        Ok(Instruction { patterns, options })
    }
}

impl From<RawPatternPart> for PatternPart {
    fn from(value: RawPatternPart) -> Self {
        match value {
            RawPatternPart::Raw(text) => PatternPart::Raw(text),
            RawPatternPart::Wildcard => PatternPart::Wildcard,
            RawPatternPart::Delimeter => PatternPart::Delimeter,
            RawPatternPart::Anchor => PatternPart::Anchor,
        }
    }
}
impl TryFrom<RawPatternOption> for PatternOption {
    type Error = crate::Error;

    fn try_from(value: RawPatternOption) -> Result<Self> {
        let res = match value {
            RawPatternOption::Site(site) => PatternOption::Site(site),
            RawPatternOption::InUrl => PatternOption::InUrl,
            RawPatternOption::InTitle => PatternOption::InTitle,
            RawPatternOption::InDescription => PatternOption::InDescription,
            RawPatternOption::InContent => PatternOption::InContent,
            RawPatternOption::Action(action) => PatternOption::Action(action.try_into()?),
        };

        Ok(res)
    }
}

impl TryFrom<RawAction> for Action {
    type Error = crate::Error;

    fn try_from(value: RawAction) -> Result<Self> {
        let res = match value {
            RawAction::Boost(boost) => Action::Boost(boost.parse()?),
            RawAction::Downrank(down_boost) => Action::Downrank(down_boost.parse()?),
            RawAction::Discard => Action::Discard,
        };

        Ok(res)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Instruction {
    pub patterns: Vec<PatternPart>,
    pub options: Vec<PatternOption>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PatternPart {
    Raw(String),
    Wildcard,
    Delimeter,
    Anchor,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PatternOption {
    Site(String),
    InUrl,
    InTitle,
    InDescription,
    InContent,
    Action(Action),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Action {
    Boost(u64),
    Downrank(u64),
    Discard,
}

#[derive(Debug, Default)]
pub struct Goggle {
    pub aggregator: SignalAggregator,
    instructions: Vec<Instruction>,
}

impl Goggle {
    pub fn as_tantivy(&self, schema: &Schema) -> Vec<(Occur, Box<dyn tantivy::query::Query>)> {
        if self
            .instructions
            .iter()
            .any(|instruction| instruction.is_empty_discard())
        {
            vec![(
                Occur::Must,
                BooleanQuery::from(
                    self.instructions
                        .iter()
                        .map(|instruction| instruction.as_tantivy(schema))
                        .collect_vec(),
                )
                .box_clone(),
            )]
        } else {
            self.instructions
                .iter()
                .map(|instruction| instruction.as_tantivy(schema))
                .collect()
        }
    }
}

fn process_tantivy_term(
    term: &str,
    analyzer: TextAnalyzer,
    tantivy_field: tantivy::schema::Field,
) -> Vec<tantivy::Term> {
    let mut terms: Vec<tantivy::Term> = Vec::new();
    let mut token_stream = analyzer.token_stream(term);
    token_stream.process(&mut |token| {
        let term = tantivy::Term::from_field_text(tantivy_field, &token.text);
        terms.push(term);
    });

    terms
}

fn process_site(site: &str, field: tantivy::schema::Field) -> Box<dyn tantivy::query::Query> {
    let mut terms = process_tantivy_term(
        site,
        TextAnalyzer::new(tokenizer::Normal::default(), vec![]),
        field,
    );

    if terms.len() > 1 {
        Box::new(PhraseQuery::new(terms)) as Box<dyn tantivy::query::Query>
    } else {
        let term = terms.pop().unwrap();
        Box::new(TermQuery::new(
            term,
            IndexRecordOption::WithFreqsAndPositions,
        ))
    }
}

impl Instruction {
    pub fn as_tantivy(&self, schema: &Schema) -> (Occur, Box<dyn tantivy::query::Query>) {
        let mut subqueries = Vec::new();

        let mut field = None;
        let mut action = None;

        for option in &self.options {
            match option {
                PatternOption::Site(site) if field.is_none() => {
                    field = Some(schema.get_field(Field::Site.as_str()).unwrap());

                    let domain_field = schema.get_field(Field::Domain.as_str()).unwrap();
                    let site_field = schema.get_field(Field::Site.as_str()).unwrap();

                    subqueries.push((
                        Occur::Must,
                        BooleanQuery::new(vec![
                            (Occur::Should, process_site(site, domain_field)),
                            (Occur::Should, process_site(site, site_field)),
                        ])
                        .box_clone(),
                    ));
                }
                PatternOption::InUrl if field.is_none() => {
                    field = Some(schema.get_field(Field::Url.as_str()).unwrap())
                }
                PatternOption::InTitle if field.is_none() => {
                    field = Some(schema.get_field(Field::Title.as_str()).unwrap())
                }
                PatternOption::InDescription if field.is_none() => {
                    field = Some(schema.get_field(Field::Description.as_str()).unwrap())
                }
                PatternOption::InContent if field.is_none() => {
                    field = Some(schema.get_field(Field::CleanBody.as_str()).unwrap())
                }
                PatternOption::Action(pattern_action) if action.is_none() => {
                    action = Some(*pattern_action)
                }
                _ => {}
            }
        }

        let action = action.unwrap_or(Action::Boost(1));
        let field = field.unwrap_or_else(|| schema.get_field(Field::Url.as_str()).unwrap());

        if !self.patterns.is_empty() {
            let query = self.pattern_query(field);
            subqueries.push((Occur::Must, query));
        }

        match action {
            Action::Boost(boost) => (
                Occur::Should,
                BoostQuery::new(
                    BooleanQuery::from(subqueries).box_clone(),
                    boost as f32 + 1.0,
                )
                .box_clone(),
            ),
            Action::Downrank(boost) => (
                Occur::Should,
                BoostQuery::new(
                    BooleanQuery::from(subqueries).box_clone(),
                    1.0 / (boost as f32 + 1.0),
                )
                .box_clone(),
            ),
            Action::Discard => (Occur::MustNot, BooleanQuery::from(subqueries).box_clone()),
        }
    }

    fn pattern_query(&self, field: tantivy::schema::Field) -> Box<dyn tantivy::query::Query> {
        PatternQuery::new(self.patterns.clone(), field).box_clone()
    }

    fn is_empty_discard(&self) -> bool {
        self.patterns.is_empty()
            && self.options.len() == 1
            && matches!(
                self.options.first(),
                Some(PatternOption::Action(Action::Discard))
            )
    }
}

#[cfg(test)]
mod tests {
    use crate::{index::Index, schema::create_schema, searcher::Searcher, webpage::Webpage};

    use super::*;
    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[test]
    fn discard_and_boost_sites() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
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
                vec![],
                0.0001,
                500,
            ))
            .expect("failed to parse webpage");

        index.commit().expect("failed to commit index");
        let searcher = Searcher::from(index);

        let res = searcher
            .search("website", None, None, None)
            .unwrap()
            .into_websites()
            .unwrap()
            .webpages
            .documents;

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].url, "https://www.b.com");
        assert_eq!(res[1].url, "https://www.a.com");

        let res = searcher
            .search(
                "website",
                None,
                Some(
                    r#"
                $discard,site=b.com
            "#
                    .to_string(),
                ),
                None,
            )
            .unwrap()
            .into_websites()
            .unwrap()
            .webpages
            .documents;

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].url, "https://www.a.com");

        let res = searcher
            .search(
                "website",
                None,
                Some(
                    r#"
                $boost=10,site=a.com
            "#
                    .to_string(),
                ),
                None,
            )
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
        parse(include_str!("../../../testcases/goggles/quickstart.goggle"))
            .unwrap()
            .as_tantivy(&create_schema());
    }

    #[test]
    fn example_goggles_dont_crash() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
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
                "https://www.b.com/this/is/b/pattern",
                vec![],
                0.0001,
                500,
            ))
            .expect("failed to parse webpage");

        index.commit().expect("failed to commit index");
        let searcher = Searcher::from(index);

        let _ = searcher
            .search(
                "website",
                None,
                Some(include_str!("../../../testcases/goggles/quickstart.goggle").to_string()),
                None,
            )
            .unwrap()
            .into_websites()
            .unwrap()
            .webpages
            .documents;

        let _ = searcher
            .search(
                "website",
                None,
                Some(include_str!("../../../testcases/goggles/hacker_news.goggle").to_string()),
                None,
            )
            .unwrap()
            .into_websites()
            .unwrap()
            .webpages
            .documents;

        let _ = searcher
            .search(
                "website",
                None,
                Some(
                    include_str!("../../../testcases/goggles/copycats_removal.goggle").to_string(),
                ),
                None,
            )
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
            .insert(Webpage::new(
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
                "https://www.b.com/this/is/b/pattern",
                vec![],
                0.0001,
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
                "https://www.c.com/this/is/c/pattern",
                vec![],
                0.0001,
                500,
            ))
            .expect("failed to parse webpage");

        index.commit().expect("failed to commit index");
        let searcher = Searcher::from(index);

        let res = searcher
            .search(
                "website",
                None,
                Some(
                    r#"
                $discard
                $site=a.com,boost=6
                $site=b.com,boost=1
                "#
                    .to_string(),
                ),
                None,
            )
            .unwrap()
            .into_websites()
            .unwrap()
            .webpages
            .documents;

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].url, "https://www.a.com/this/is/a/pattern");
    }
}