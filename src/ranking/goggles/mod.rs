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
mod signal;

use crate::{schema::Field, tokenizer, Error, Result};
use tantivy::{
    query::{BooleanQuery, BoostQuery, Occur, PhraseQuery, QueryClone, RegexQuery, TermQuery},
    schema::{IndexRecordOption, Schema},
    tokenizer::TextAnalyzer,
};

use self::ast::{Action, Instruction, PatternOption, PatternPart, RawGoggle};
pub use self::signal::*;

pub fn parse(goggle: &str) -> Result<Goggle> {
    let raw_goggle = ast::parse(goggle)?;

    Ok(Goggle::from(raw_goggle))
}

impl From<RawGoggle> for Goggle {
    fn from(raw: RawGoggle) -> Self {
        Self {
            aggregator: SignalAggregator::from(raw.alterations),
            instructions: raw.instructions,
        }
    }
}

#[derive(Debug, Default)]
pub struct Goggle {
    pub aggregator: SignalAggregator,
    instructions: Vec<Instruction>,
}

impl Goggle {
    pub fn as_tantivy(&self, schema: &Schema) -> Vec<(Occur, Box<dyn tantivy::query::Query>)> {
        self.instructions
            .iter()
            .map(|instruction| instruction.as_tantivy(schema))
            .collect()
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
            if let Ok(regex) = self.pattern_regex() {
                subqueries.push((
                    Occur::Must,
                    RegexQuery::from_pattern(&regex, field).unwrap().box_clone(),
                ))
            }
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

    fn pattern_regex(&self) -> Result<String> {
        let mut regex = String::new();

        if !matches!(self.patterns.first(), Some(PatternPart::Anchor)) {
            regex.push_str(".*");
        }

        for (i, pattern) in self.patterns.iter().enumerate() {
            match pattern {
                PatternPart::Raw(string) => regex.push_str(&regex::escape(string)),
                PatternPart::Wildcard => regex.push_str(".*"),
                PatternPart::Delimeter => regex.push_str("([^\\w\\d._%-]|$)"),
                PatternPart::Anchor if i == 0 => regex.push('^'),
                PatternPart::Anchor if i == self.patterns.len() - 1 => regex.push('$'),
                PatternPart::Anchor => return Err(Error::Parse), // TODO: make this less generic error
            }
        }

        if !matches!(self.patterns.last(), Some(PatternPart::Anchor)) {
            regex.push_str(".*");
        }

        Ok(regex)
    }
}

#[cfg(test)]
mod tests {
    use crate::{index::Index, searcher::Searcher, webpage::Webpage};

    use super::*;
    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[test]
    fn simple_pattern_regex() {
        let instr = Instruction {
            patterns: vec![PatternPart::Raw("test/url".to_string())],
            options: vec![],
        };

        assert_eq!(&instr.pattern_regex().unwrap(), ".*test/url.*");
    }

    #[test]
    fn wildcard_pattern_regex() {
        let instr = Instruction {
            patterns: vec![
                PatternPart::Raw("test".to_string()),
                PatternPart::Wildcard,
                PatternPart::Raw("url".to_string()),
            ],
            options: vec![],
        };

        assert_eq!(&instr.pattern_regex().unwrap(), ".*test.*url.*");
    }

    #[test]
    fn delimeter_pattern_regex() {
        let instr = Instruction {
            patterns: vec![
                PatternPart::Raw("test".to_string()),
                PatternPart::Delimeter,
                PatternPart::Raw("url".to_string()),
            ],
            options: vec![],
        };

        assert_eq!(
            &instr.pattern_regex().unwrap(),
            ".*test([^\\w\\d._%-]|$)url.*"
        );
    }

    #[test]
    fn anchor_regex() {
        let instr = Instruction {
            patterns: vec![
                PatternPart::Anchor,
                PatternPart::Raw("test/url".to_string()),
            ],
            options: vec![],
        };

        assert_eq!(&instr.pattern_regex().unwrap(), "^test/url.*");

        let instr = Instruction {
            patterns: vec![
                PatternPart::Raw("test/url".to_string()),
                PatternPart::Anchor,
            ],
            options: vec![],
        };

        assert_eq!(&instr.pattern_regex().unwrap(), ".*test/url$");

        let instr = Instruction {
            patterns: vec![
                PatternPart::Anchor,
                PatternPart::Raw("test/url".to_string()),
                PatternPart::Anchor,
            ],
            options: vec![],
        };

        assert_eq!(&instr.pattern_regex().unwrap(), "^test/url$");
    }

    #[test]
    fn escape_pattern() {
        let instr = Instruction {
            patterns: vec![PatternPart::Raw("test\\d+".to_string())],
            options: vec![],
        };

        assert_eq!(&instr.pattern_regex().unwrap(), ".*test\\\\d\\+.*");
    }

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
            "#,
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
            "#,
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
}
