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

use tantivy::query::{BooleanQuery, Query, Weight};

use crate::webgraph::{
    schema::{Field, FieldEnum},
    tokenizer::Tokenizer,
};

#[derive(Clone, Debug)]
pub struct PhraseOrTermQuery {
    text: String,
    field: FieldEnum,
}

impl PhraseOrTermQuery {
    pub fn new<F: Field>(text: String, field: F) -> Self {
        Self {
            text,
            field: field.into(),
        }
    }
}

impl Query for PhraseOrTermQuery {
    fn weight(
        &self,
        scoring: tantivy::query::EnableScoring<'_>,
    ) -> tantivy::Result<Box<dyn Weight>> {
        let schema = scoring.schema();

        let tv_field = schema.get_field(self.field.name())?;
        let mut tokenizer = self.field.tokenizer();
        let mut token_stream = tokenizer.token_stream(&self.text);

        let mut terms = Vec::new();

        while token_stream.advance() {
            terms.push(tantivy::Term::from_field_text(
                tv_field,
                &token_stream.token().text,
            ));
        }

        if terms.is_empty() {
            Ok(Box::new(tantivy::query::EmptyWeight))
        } else if terms.len() == 1 {
            tantivy::query::TermQuery::new(
                terms.remove(0),
                tantivy::schema::IndexRecordOption::Basic,
            )
            .weight(scoring)
        } else {
            let index_record_option = schema
                .get_field_entry(tv_field)
                .field_type()
                .index_record_option()
                .unwrap();

            match index_record_option {
                tantivy::schema::IndexRecordOption::WithFreqsAndPositions => {
                    tantivy::query::PhraseQuery::new(terms).weight(scoring)
                }
                _ => {
                    let queries = terms
                        .into_iter()
                        .map(|term| {
                            Box::new(tantivy::query::TermQuery::new(
                                term,
                                tantivy::schema::IndexRecordOption::Basic,
                            )) as Box<dyn Query>
                        })
                        .collect();

                    BooleanQuery::intersection(queries).weight(scoring)
                }
            }
        }
    }
}
