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

use optics::PatternPart;
use tantivy::{
    query::{EmptyScorer, Explanation, Scorer},
    schema::IndexRecordOption,
    DocSet, TantivyError,
};

use crate::{
    numericalfield_reader::NumericalFieldReader,
    schema::{
        numerical_field,
        text_field::{self, TextField},
        Field, TextFieldEnum,
    },
};

use super::scorer::{
    AllScorer, EmptyFieldScorer, FastSiteDomainPatternScorer, NormalPatternScorer, PatternScorer,
};
use super::SmallPatternPart;

pub struct FastSiteDomainPatternWeight {
    pub term: tantivy::Term,
    pub field: tantivy::schema::Field,
}

impl FastSiteDomainPatternWeight {
    fn pattern_scorer(
        &self,
        reader: &tantivy::SegmentReader,
    ) -> tantivy::Result<Option<FastSiteDomainPatternScorer>> {
        let field_no_tokenizer = match Field::get(self.field.field_id() as usize) {
            Some(Field::Text(TextFieldEnum::UrlForSiteOperator(_))) => {
                Field::Text(text_field::SiteNoTokenizer.into())
            }
            Some(Field::Text(TextFieldEnum::Domain(_))) => {
                Field::Text(text_field::DomainNoTokenizer.into())
            }
            _ => unreachable!(),
        };

        let tv_field = reader
            .schema()
            .get_field(field_no_tokenizer.name())
            .unwrap();

        let opt = match field_no_tokenizer {
            Field::Text(t) => t.record_option(),
            Field::Numerical(_) => unreachable!(),
        };

        match reader
            .inverted_index(tv_field)?
            .read_postings(&self.term, opt)?
        {
            Some(posting) => Ok(Some(FastSiteDomainPatternScorer { posting })),
            None => Ok(None),
        }
    }
}

impl tantivy::query::Weight for FastSiteDomainPatternWeight {
    fn scorer(
        &self,
        reader: &tantivy::SegmentReader,
        _boost: tantivy::Score,
    ) -> tantivy::Result<Box<dyn tantivy::query::Scorer>> {
        if let Some(scorer) = self.pattern_scorer(reader)? {
            Ok(Box::new(PatternScorer::FastSiteDomain(Box::new(scorer))))
        } else {
            Ok(Box::new(EmptyScorer))
        }
    }

    fn explain(
        &self,
        reader: &tantivy::SegmentReader,
        doc: tantivy::DocId,
    ) -> tantivy::Result<tantivy::query::Explanation> {
        let scorer_opt = self.pattern_scorer(reader)?;
        if scorer_opt.is_none() {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match (empty scorer)"
            )));
        }
        let mut scorer = scorer_opt.unwrap();
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match"
            )));
        }
        let explanation = Explanation::new("Pattern Scorer", scorer.score());
        Ok(explanation)
    }
}

pub struct PatternWeight {
    pub patterns: Vec<PatternPart>,
    pub raw_terms: Vec<tantivy::Term>,
    pub field: tantivy::schema::Field,
    pub columnfield_reader: NumericalFieldReader,
}

impl PatternWeight {
    pub(crate) fn pattern_scorer(
        &self,
        reader: &tantivy::SegmentReader,
    ) -> tantivy::Result<Option<PatternScorer>> {
        if self.patterns.is_empty() {
            return Ok(None);
        }

        let num_tokens_columnfield = match Field::get(self.field.field_id() as usize) {
            Some(Field::Text(TextFieldEnum::Title(_))) => {
                Ok(numerical_field::NumTitleTokens.into())
            }
            Some(Field::Text(TextFieldEnum::CleanBody(_))) => {
                Ok(numerical_field::NumCleanBodyTokens.into())
            }
            Some(Field::Text(TextFieldEnum::Url(_))) => Ok(numerical_field::NumUrlTokens.into()),
            Some(Field::Text(TextFieldEnum::Domain(_))) => {
                Ok(numerical_field::NumDomainTokens.into())
            }
            Some(Field::Text(TextFieldEnum::UrlForSiteOperator(_))) => {
                Ok(numerical_field::NumUrlForSiteOperatorTokens.into())
            }
            Some(Field::Text(TextFieldEnum::Description(_))) => {
                Ok(numerical_field::NumDescriptionTokens.into())
            }
            Some(Field::Text(TextFieldEnum::MicroformatTags(_))) => {
                Ok(numerical_field::NumMicroformatTagsTokens.into())
            }
            Some(Field::Text(TextFieldEnum::FlattenedSchemaOrgJson(_))) => {
                Ok(numerical_field::NumFlattenedSchemaTokens.into())
            }
            Some(field) => Err(TantivyError::InvalidArgument(format!(
                "{} is not supported in pattern query",
                field.name()
            ))),
            None => Err(TantivyError::InvalidArgument(format!(
                "Field with id {} is not supported in pattern query",
                self.field.field_id()
            ))),
        }?;

        // "*" matches everything
        if self.raw_terms.is_empty()
            && self
                .patterns
                .iter()
                .any(|p| matches!(p, PatternPart::Wildcard))
        {
            return Ok(Some(PatternScorer::Everything(AllScorer {
                doc: 0,
                max_doc: reader.max_doc(),
            })));
        }

        // "||" and "|" matches empty string

        if self.raw_terms.is_empty()
            && self
                .patterns
                .iter()
                .all(|p| matches!(p, PatternPart::Anchor))
        {
            return Ok(Some(PatternScorer::EmptyField(EmptyFieldScorer {
                num_tokens_columnfield,
                segment_reader: self
                    .columnfield_reader
                    .borrow_segment(&reader.segment_id())
                    .clone(),
                all_scorer: AllScorer {
                    doc: 0,
                    max_doc: reader.max_doc(),
                },
            })));
        }

        let mut term_postings_list = Vec::with_capacity(self.raw_terms.len());
        for term in &self.raw_terms {
            if let Some(postings) = reader
                .inverted_index(term.field())?
                .read_postings(term, IndexRecordOption::WithFreqsAndPositions)?
            {
                term_postings_list.push(postings);
            } else {
                return Ok(None);
            }
        }

        let small_patterns = self
            .patterns
            .iter()
            .map(|pattern| match pattern {
                PatternPart::Raw(_) => SmallPatternPart::Term,
                PatternPart::Wildcard => SmallPatternPart::Wildcard,
                PatternPart::Anchor => SmallPatternPart::Anchor,
            })
            .collect();

        Ok(Some(PatternScorer::Normal(NormalPatternScorer::new(
            term_postings_list,
            small_patterns,
            reader.segment_id(),
            num_tokens_columnfield,
            self.columnfield_reader.clone(),
        ))))
    }
}

impl tantivy::query::Weight for PatternWeight {
    fn scorer(
        &self,
        reader: &tantivy::SegmentReader,
        _boost: tantivy::Score,
    ) -> tantivy::Result<Box<dyn tantivy::query::Scorer>> {
        if let Some(scorer) = self.pattern_scorer(reader)? {
            Ok(Box::new(scorer))
        } else {
            Ok(Box::new(EmptyScorer))
        }
    }

    fn explain(
        &self,
        reader: &tantivy::SegmentReader,
        doc: tantivy::DocId,
    ) -> tantivy::Result<tantivy::query::Explanation> {
        let scorer_opt = self.pattern_scorer(reader)?;
        if scorer_opt.is_none() {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match (empty scorer)"
            )));
        }
        let mut scorer = scorer_opt.unwrap();
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match"
            )));
        }
        let explanation = Explanation::new("Pattern Scorer", scorer.score());
        Ok(explanation)
    }
}
