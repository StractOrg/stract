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
    fieldnorm::FieldNormReader,
    query::{EmptyScorer, Explanation, Scorer},
    schema::IndexRecordOption,
    DocSet, TantivyError,
};

use crate::{
    fastfield_reader::FastFieldReader,
    ranking::bm25::Bm25Weight,
    schema::{FastField, Field, TextField},
};

use super::scorer::{
    AllScorer, EmptyFieldScorer, FastSiteDomainPatternScorer, NormalPatternScorer, PatternScorer,
};
use super::SmallPatternPart;

pub struct FastSiteDomainPatternWeight {
    pub term: tantivy::Term,
    pub field: tantivy::schema::Field,
    pub similarity_weight: Option<Bm25Weight>,
}

impl FastSiteDomainPatternWeight {
    fn fieldnorm_reader(
        &self,
        reader: &tantivy::SegmentReader,
    ) -> tantivy::Result<FieldNormReader> {
        if self.similarity_weight.is_some() {
            if let Some(fieldnorm_reader) = reader.fieldnorms_readers().get_field(self.field)? {
                return Ok(fieldnorm_reader);
            }
        }
        Ok(FieldNormReader::constant(reader.max_doc(), 1))
    }

    fn pattern_scorer(
        &self,
        reader: &tantivy::SegmentReader,
        boost: tantivy::Score,
    ) -> tantivy::Result<Option<FastSiteDomainPatternScorer>> {
        let similarity_weight = self
            .similarity_weight
            .as_ref()
            .map(|weight| weight.boost_by(boost));

        let fieldnorm_reader = self.fieldnorm_reader(reader)?;

        let field_no_tokenizer = match Field::get(self.field.field_id() as usize) {
            Some(Field::Text(TextField::UrlForSiteOperator)) => {
                Field::Text(TextField::SiteNoTokenizer)
            }
            Some(Field::Text(TextField::Domain)) => Field::Text(TextField::DomainNoTokenizer),
            _ => unreachable!(),
        };

        let tv_field = reader
            .schema()
            .get_field(field_no_tokenizer.name())
            .unwrap();

        let opt = match field_no_tokenizer {
            Field::Text(t) => t.index_option(),
            Field::Fast(_) => unreachable!(),
        };

        match reader
            .inverted_index(tv_field)?
            .read_postings(&self.term, opt)?
        {
            Some(posting) => Ok(Some(FastSiteDomainPatternScorer {
                similarity_weight,
                posting,
                fieldnorm_reader,
            })),
            None => Ok(None),
        }
    }
}

impl tantivy::query::Weight for FastSiteDomainPatternWeight {
    fn scorer(
        &self,
        reader: &tantivy::SegmentReader,
        boost: tantivy::Score,
    ) -> tantivy::Result<Box<dyn tantivy::query::Scorer>> {
        if let Some(scorer) = self.pattern_scorer(reader, boost)? {
            Ok(Box::new(PatternScorer::FastSiteDomain(scorer)))
        } else {
            Ok(Box::new(EmptyScorer))
        }
    }

    fn explain(
        &self,
        reader: &tantivy::SegmentReader,
        doc: tantivy::DocId,
    ) -> tantivy::Result<tantivy::query::Explanation> {
        let scorer_opt = self.pattern_scorer(reader, 1.0)?;
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
        let fieldnorm_reader = self.fieldnorm_reader(reader)?;
        let fieldnorm_id = fieldnorm_reader.fieldnorm_id(doc);
        let term_freq = scorer.term_freq();
        let mut explanation = Explanation::new("Pattern Scorer", scorer.score());
        explanation.add_detail(
            self.similarity_weight
                .as_ref()
                .unwrap()
                .explain(fieldnorm_id, term_freq),
        );
        Ok(explanation)
    }
}

pub struct PatternWeight {
    pub similarity_weight: Option<Bm25Weight>,
    pub patterns: Vec<PatternPart>,
    pub raw_terms: Vec<tantivy::Term>,
    pub field: tantivy::schema::Field,
    pub fastfield_reader: FastFieldReader,
}

impl PatternWeight {
    fn fieldnorm_reader(
        &self,
        reader: &tantivy::SegmentReader,
    ) -> tantivy::Result<FieldNormReader> {
        if self.similarity_weight.is_some() {
            if let Some(fieldnorm_reader) = reader.fieldnorms_readers().get_field(self.field)? {
                return Ok(fieldnorm_reader);
            }
        }
        Ok(FieldNormReader::constant(reader.max_doc(), 1))
    }

    pub(crate) fn pattern_scorer(
        &self,
        reader: &tantivy::SegmentReader,
        boost: tantivy::Score,
    ) -> tantivy::Result<Option<PatternScorer>> {
        if self.patterns.is_empty() {
            return Ok(None);
        }

        let num_tokens_fastfield = match Field::get(self.field.field_id() as usize) {
            Some(Field::Text(TextField::Title)) => Ok(FastField::NumTitleTokens),
            Some(Field::Text(TextField::CleanBody)) => Ok(FastField::NumCleanBodyTokens),
            Some(Field::Text(TextField::Url)) => Ok(FastField::NumUrlTokens),
            Some(Field::Text(TextField::Domain)) => Ok(FastField::NumDomainTokens),
            Some(Field::Text(TextField::UrlForSiteOperator)) => {
                Ok(FastField::NumUrlForSiteOperatorTokens)
            }
            Some(Field::Text(TextField::Description)) => Ok(FastField::NumDescriptionTokens),
            Some(Field::Text(TextField::MicroformatTags)) => {
                Ok(FastField::NumMicroformatTagsTokens)
            }
            Some(Field::Text(TextField::FlattenedSchemaOrgJson)) => {
                Ok(FastField::NumFlattenedSchemaTokens)
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
                num_tokens_fastfield,
                segment_reader: self.fastfield_reader.get_segment(&reader.segment_id()),
                all_scorer: AllScorer {
                    doc: 0,
                    max_doc: reader.max_doc(),
                },
            })));
        }

        let similarity_weight = self
            .similarity_weight
            .as_ref()
            .map(|weight| weight.boost_by(boost));

        let fieldnorm_reader = self.fieldnorm_reader(reader)?;

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
            similarity_weight,
            term_postings_list,
            fieldnorm_reader,
            small_patterns,
            reader.segment_id(),
            num_tokens_fastfield,
            self.fastfield_reader.clone(),
        ))))
    }
}

impl tantivy::query::Weight for PatternWeight {
    fn scorer(
        &self,
        reader: &tantivy::SegmentReader,
        boost: tantivy::Score,
    ) -> tantivy::Result<Box<dyn tantivy::query::Scorer>> {
        if let Some(scorer) = self.pattern_scorer(reader, boost)? {
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
        let scorer_opt = self.pattern_scorer(reader, 1.0)?;
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
        let fieldnorm_reader = self.fieldnorm_reader(reader)?;
        let fieldnorm_id = fieldnorm_reader.fieldnorm_id(doc);
        let term_freq = scorer.term_freq();
        let mut explanation = Explanation::new("Pattern Scorer", scorer.score());
        explanation.add_detail(
            self.similarity_weight
                .as_ref()
                .unwrap()
                .explain(fieldnorm_id, term_freq),
        );
        Ok(explanation)
    }
}
