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

use super::field_union::FieldUnion;
use super::term_intersection::intersect_terms;
use super::term_scorer::TermScorerForField;
use super::FieldData;
use tantivy::fieldnorm::FieldNormReader;
use tantivy::postings::SegmentPostings;
use tantivy::query::Explanation;
use tantivy::{DocId, Score, SegmentReader};

pub struct Weight {
    terms: Vec<String>,
    fields: Vec<FieldData>,
    scoring_enabled: bool,
}

impl tantivy::query::Weight for Weight {
    fn scorer(
        &self,
        reader: &SegmentReader,
        boost: Score,
    ) -> tantivy::Result<Box<dyn tantivy::query::Scorer>> {
        // each term has a union over the fields
        // e.g. if the term both occurs in the title and body
        let mut term_scorers: Vec<FieldUnion> = Vec::new();

        for term_text in &self.terms {
            let mut field_scorers: Vec<TermScorerForField> = Vec::new();

            for field_data in &self.fields {
                let inverted_index = reader.inverted_index(field_data.tantivy)?;

                let processed_terms: Vec<String> = field_data
                    .analyzer
                    .as_ref()
                    .map(|analyzer| {
                        let mut terms = Vec::new();

                        let mut stream = analyzer.token_stream(term_text);
                        while let Some(token) = stream.next() {
                            terms.push(token.text.clone());
                        }

                        terms
                    })
                    .unwrap_or_else(|| vec![term_text.clone()]);

                for term in processed_terms {
                    let fieldnorm_reader_opt = if self.scoring_enabled {
                        reader.get_fieldnorms_reader(field_data.tantivy).ok()
                    } else {
                        None
                    };
                    let fieldnorm_reader = fieldnorm_reader_opt
                        .unwrap_or_else(|| FieldNormReader::constant(reader.max_doc(), 1));
                    let similarity_weight = field_data
                        .scoring
                        .boost_by(field_data.boost.unwrap_or(1.0) * boost);

                    let term = tantivy::Term::from_field_text(field_data.tantivy, &term);

                    let postings_opt: Option<SegmentPostings> = field_data
                        .index_record_option
                        .and_then(|index_record_option| {
                            inverted_index
                                .read_postings(&term, index_record_option)
                                .unwrap()
                        });

                    let posting = postings_opt.unwrap_or_else(SegmentPostings::empty);

                    field_scorers.push(TermScorerForField::new(
                        posting,
                        fieldnorm_reader,
                        similarity_weight,
                    ));
                }
            }

            term_scorers.push(FieldUnion::from(field_scorers));
        }

        Ok(intersect_terms(term_scorers))
    }

    fn explain(&self, _reader: &SegmentReader, _doc: DocId) -> tantivy::Result<Explanation> {
        unimplemented!();
    }
}

impl Weight {
    pub fn new(terms: Vec<String>, fields: Vec<FieldData>, scoring_enabled: bool) -> Weight {
        Weight {
            terms,
            fields,
            scoring_enabled,
        }
    }
}
