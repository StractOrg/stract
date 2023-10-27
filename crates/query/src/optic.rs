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

use itertools::Itertools;
use optics::{Action, MatchLocation, Matching, Optic, Rule};
use schema::{fastfield_reader::FastFieldReader, TextField};
use tantivy::{
    query::{BooleanQuery, Occur, QueryClone},
    schema::Schema,
};

use super::{const_query::ConstQuery, pattern_query::PatternQuery, union::UnionQuery};

pub trait AsTantivyQuery {
    fn as_tantivy(
        &self,
        schema: &Schema,
        fastfield_reader: &FastFieldReader,
    ) -> Box<dyn tantivy::query::Query>;
}

pub trait AsMultipleTantivyQuery {
    fn as_multiple_tantivy(
        &self,
        schema: &Schema,
        fastfield_reader: &FastFieldReader,
    ) -> Vec<(Occur, Box<dyn tantivy::query::Query>)>;
}

impl AsMultipleTantivyQuery for Optic {
    fn as_multiple_tantivy(
        &self,
        schema: &Schema,
        fastfields: &FastFieldReader,
    ) -> Vec<(Occur, Box<dyn tantivy::query::Query>)> {
        if self.discard_non_matching {
            vec![(
                Occur::Must,
                UnionQuery::from(
                    self.rules
                        .iter()
                        .chain(self.site_rankings.rules().iter())
                        .filter_map(|rule| rule.as_searchable_rule(schema, fastfields))
                        .map(|(occur, rule)| {
                            BooleanQuery::from(vec![(occur, rule.query)]).box_clone()
                        })
                        .collect_vec(),
                )
                .box_clone(),
            )]
        } else {
            self.rules
                .iter()
                .chain(self.site_rankings.rules().iter())
                .filter_map(|rule| rule.as_searchable_rule(schema, fastfields))
                .map(|(occur, rule)| (occur, rule.query))
                .collect()
        }
    }
}

pub struct SearchableRule {
    pub query: Box<dyn tantivy::query::Query>,
    pub boost: f64,
}

pub trait AsSearchableRule {
    fn as_searchable_rule(
        &self,
        schema: &Schema,
        fastfield_reader: &FastFieldReader,
    ) -> Option<(Occur, SearchableRule)>;
}

impl AsSearchableRule for Rule {
    fn as_searchable_rule(
        &self,
        schema: &Schema,
        fastfield_reader: &FastFieldReader,
    ) -> Option<(Occur, SearchableRule)> {
        let mut subqueries: Vec<_> = self
            .matches
            .iter()
            .map(|matching| (Occur::Must, matching.as_tantivy(schema, fastfield_reader)))
            .collect();

        if subqueries.is_empty() {
            return None;
        }

        let subquery = if subqueries.len() == 1 {
            subqueries.pop().unwrap().1
        } else {
            Box::new(BooleanQuery::from(subqueries))
        };

        match &self.action {
            Action::Boost(boost) => Some((
                Occur::Should,
                SearchableRule {
                    query: Box::new(ConstQuery::new(subquery, 1.0)),
                    boost: *boost as f64,
                },
            )),
            Action::Downrank(boost) => Some((
                Occur::Should,
                SearchableRule {
                    query: Box::new(ConstQuery::new(subquery, 1.0)),
                    boost: *boost as f64 * -1.0,
                },
            )),
            Action::Discard => Some((
                Occur::MustNot,
                SearchableRule {
                    query: subquery,
                    boost: 0.0,
                },
            )),
        }
    }
}

impl AsTantivyQuery for Matching {
    fn as_tantivy(
        &self,
        schema: &Schema,
        fastfield_reader: &FastFieldReader,
    ) -> Box<dyn tantivy::query::Query> {
        match &self.location {
            MatchLocation::Site => ConstQuery::new(
                PatternQuery::new(
                    self.pattern.clone(),
                    TextField::UrlForSiteOperator,
                    schema,
                    fastfield_reader.clone(),
                )
                .box_clone(),
                1.0,
            )
            .box_clone(),
            MatchLocation::Url => Box::new(ConstQuery::new(
                Box::new(PatternQuery::new(
                    self.pattern.clone(),
                    TextField::Url,
                    schema,
                    fastfield_reader.clone(),
                )),
                1.0,
            )),
            MatchLocation::Domain => Box::new(ConstQuery::new(
                Box::new(PatternQuery::new(
                    self.pattern.clone(),
                    TextField::Domain,
                    schema,
                    fastfield_reader.clone(),
                )),
                1.0,
            )),
            MatchLocation::Title => Box::new(ConstQuery::new(
                Box::new(PatternQuery::new(
                    self.pattern.clone(),
                    TextField::Title,
                    schema,
                    fastfield_reader.clone(),
                )),
                1.0,
            )),
            MatchLocation::Description => UnionQuery::from(vec![
                Box::new(ConstQuery::new(
                    Box::new(PatternQuery::new(
                        self.pattern.clone(),
                        TextField::Description,
                        schema,
                        fastfield_reader.clone(),
                    )),
                    1.0,
                )) as Box<dyn tantivy::query::Query>,
                Box::new(ConstQuery::new(
                    Box::new(PatternQuery::new(
                        self.pattern.clone(),
                        TextField::DmozDescription,
                        schema,
                        fastfield_reader.clone(),
                    )),
                    1.0,
                )) as Box<dyn tantivy::query::Query>,
            ])
            .box_clone(),
            MatchLocation::Content => Box::new(ConstQuery::new(
                Box::new(PatternQuery::new(
                    self.pattern.clone(),
                    TextField::CleanBody,
                    schema,
                    fastfield_reader.clone(),
                )),
                1.0,
            )),
            MatchLocation::MicroformatTag => Box::new(ConstQuery::new(
                Box::new(PatternQuery::new(
                    self.pattern.clone(),
                    TextField::MicroformatTags,
                    schema,
                    fastfield_reader.clone(),
                )),
                1.0,
            )),
            MatchLocation::Schema => Box::new(ConstQuery::new(
                Box::new(PatternQuery::new(
                    self.pattern.clone(),
                    TextField::FlattenedSchemaOrgJson,
                    schema,
                    fastfield_reader.clone(),
                )),
                1.0,
            )),
        }
    }
}
