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

mod scorer;
mod weight;

use optics::PatternPart;

use tantivy::tokenizer::Tokenizer;

use crate::{
    fastfield_reader::FastFieldReader,
    schema::{Field, TextField},
};

use self::weight::{FastSiteDomainPatternWeight, PatternWeight};

#[derive(Clone)]
pub struct PatternQuery {
    patterns: Vec<PatternPart>,
    can_optimize_site_domain: bool,
    field: tantivy::schema::Field,
    raw_terms: Vec<tantivy::Term>,
    fastfield_reader: FastFieldReader,
}

impl std::fmt::Debug for PatternQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PatternQuery")
            .field("patterns", &self.patterns)
            .field("field", &self.field)
            .field("raw_terms", &self.raw_terms)
            .finish()
    }
}

impl PatternQuery {
    pub fn new(
        patterns: Vec<PatternPart>,
        field: TextField,
        schema: &tantivy::schema::Schema,
        fastfield_reader: FastFieldReader,
    ) -> Self {
        let field = Field::Text(field);
        let tv_field = schema.get_field(field.name()).unwrap();

        if can_optimize_site_domain(&patterns, field) {
            if patterns.len() == 3 {
                let PatternPart::Raw(term) = &patterns[1] else {
                    unreachable!()
                };

                return Self {
                    patterns: Vec::new(),
                    field: tv_field,
                    can_optimize_site_domain: true,
                    raw_terms: vec![tantivy::Term::from_field_text(tv_field, term.as_str())],
                    fastfield_reader,
                };
            } else {
                let term: String = patterns
                    .iter()
                    .filter_map(|p| match p {
                        PatternPart::Raw(s) => Some(s.clone()),
                        PatternPart::Wildcard => None,
                        PatternPart::Anchor => None,
                    })
                    .collect();

                return Self {
                    patterns,
                    field: tv_field,
                    can_optimize_site_domain: true,
                    raw_terms: vec![tantivy::Term::from_field_text(tv_field, &term)],
                    fastfield_reader,
                };
            }
        }

        let mut raw_terms = Vec::with_capacity(patterns.len());
        let mut new_patterns = Vec::with_capacity(patterns.len());

        for pattern in &patterns {
            match pattern {
                PatternPart::Raw(text) => {
                    let mut tokenizer = field.as_text().unwrap().indexing_tokenizer();
                    let mut stream = tokenizer.token_stream(text);

                    while let Some(token) = stream.next() {
                        new_patterns.push(PatternPart::Raw(token.text.clone()));
                        let term = tantivy::Term::from_field_text(tv_field, &token.text);
                        raw_terms.push(term);
                    }
                }
                PatternPart::Wildcard => new_patterns.push(PatternPart::Wildcard),
                PatternPart::Anchor => new_patterns.push(PatternPart::Anchor),
            }
        }

        raw_terms.shrink_to_fit();

        Self {
            patterns: new_patterns,
            field: tv_field,
            raw_terms,
            can_optimize_site_domain: false,
            fastfield_reader,
        }
    }
}

impl tantivy::query::Query for PatternQuery {
    fn weight(
        &self,
        _scoring: tantivy::query::EnableScoring<'_>,
    ) -> tantivy::Result<Box<dyn tantivy::query::Weight>> {
        if self.can_optimize_site_domain {
            return Ok(Box::new(FastSiteDomainPatternWeight {
                term: self.raw_terms[0].clone(),
                field: self.field,
            }));
        }

        Ok(Box::new(PatternWeight {
            raw_terms: self.raw_terms.clone(),
            patterns: self.patterns.clone(),
            field: self.field,
            fastfield_reader: self.fastfield_reader.clone(),
        }))
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a tantivy::Term, bool)) {
        for term in &self.raw_terms {
            visitor(term, true);
        }
    }
}

#[derive(Debug)]
pub enum SmallPatternPart {
    Term,
    Wildcard,
    Anchor,
}

/// if pattern is of form Site("|site|") or Domain("|domain|")
/// we can use the field without tokenization to speed up the query significantly
fn can_optimize_site_domain(patterns: &[PatternPart], field: Field) -> bool {
    patterns.len() >= 2
        && matches!(&patterns[0], PatternPart::Anchor)
        && matches!(&patterns[patterns.len() - 1], PatternPart::Anchor)
        && patterns[1..patterns.len() - 1]
            .iter()
            .all(|pattern| matches!(pattern, PatternPart::Raw(_)))
        && (matches!(field, Field::Text(TextField::UrlForSiteOperator))
            || matches!(field, Field::Text(TextField::Domain)))
}
