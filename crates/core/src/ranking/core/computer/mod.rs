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

use crate::query::optic::AsSearchableRule;
use crate::query::{Query, MAX_TERMS_FOR_NGRAM_LOOKUPS};
use crate::ranking::bm25f::MultiBm25FWeight;
use crate::schema::text_field::TextField;
use crate::Result;
use crate::{enum_map::EnumMap, numericalfield_reader, schema::TextFieldEnum, webpage::Webpage};

use std::cell::RefCell;

use std::sync::Arc;

use itertools::Itertools;
use lending_iter::LendingIterator;
use tantivy::fieldnorm::FieldNormReader;
use tantivy::postings::SegmentPostings;
use tantivy::query::{Query as _, Scorer};
use tantivy::tokenizer::Tokenizer as _;

use tantivy::DocSet;
use tantivy::{postings::Postings, DocId};

use crate::webpage::region::RegionCount;

use crate::ranking::bm25::MultiBm25Weight;
use crate::ranking::models::linear::LinearRegression;

use super::{ComputedSignal, Signal, SignalCoefficient, SignalEnum};

mod order;
pub use order::SignalComputeOrder;

#[derive(Clone)]
pub struct TextFieldData {
    postings: Vec<SegmentPostings>,
    bm25: MultiBm25Weight,
    bm25f: MultiBm25FWeight,
    fieldnorm_reader: FieldNormReader,
    signal_coefficient: f64,
}

impl TextFieldData {
    pub fn bm25(&mut self, doc: DocId) -> f64 {
        if self.postings.is_empty() {
            return 0.0;
        }

        let fieldnorm_id = self.fieldnorm_reader.fieldnorm_id(doc);

        self.bm25
            .score(self.postings.iter_mut().map(move |posting| {
                if posting.doc() == doc || (posting.doc() < doc && posting.seek(doc) == doc) {
                    (fieldnorm_id, posting.term_freq())
                } else {
                    (fieldnorm_id, 0)
                }
            })) as f64
    }

    pub fn idf_sum(&mut self, doc: DocId) -> f64 {
        if self.postings.is_empty() {
            return 0.0;
        }
        let idf = self.bm25.idf();

        self.postings
            .iter_mut()
            .zip_eq(idf)
            .filter_map(|(posting, idf)| {
                if posting.doc() == doc || (posting.doc() < doc && posting.seek(doc) == doc) {
                    Some(idf)
                } else {
                    None
                }
            })
            .sum::<f32>() as f64
    }

    pub fn bm25f(&mut self, doc: DocId) -> f64 {
        if self.postings.is_empty() {
            return 0.0;
        }

        let fieldnorm_id = self.fieldnorm_reader.fieldnorm_id(doc);

        self.bm25f.score(
            self.signal_coefficient as f32,
            self.postings.iter_mut().map(move |posting| {
                if posting.doc() == doc || (posting.doc() < doc && posting.seek(doc) == doc) {
                    (fieldnorm_id, posting.term_freq())
                } else {
                    (fieldnorm_id, 0)
                }
            }),
        ) as f64
    }
}
pub struct RuleBoost {
    docset: Box<dyn Scorer>,
    boost: f64,
}

pub struct OpticBoosts {
    rules: Vec<RuleBoost>,
}

pub struct SegmentReader {
    text_fields: EnumMap<TextFieldEnum, TextFieldData>,
    optic_boosts: OpticBoosts,
    numericalfield_reader: Arc<numericalfield_reader::SegmentReader>,
}

impl SegmentReader {
    pub fn text_fields_mut(&mut self) -> &mut EnumMap<TextFieldEnum, TextFieldData> {
        &mut self.text_fields
    }

    pub fn numericalfield_reader(&self) -> &numericalfield_reader::SegmentReader {
        &self.numericalfield_reader
    }
}

#[derive(Clone)]
pub struct QueryData {
    simple_terms: Vec<String>,
    optic_rules: Vec<optics::Rule>,
    selected_region: Option<crate::webpage::Region>,
    lang: Option<whatlang::Lang>,
}
impl QueryData {
    pub fn selected_region(&self) -> Option<crate::webpage::Region> {
        self.selected_region
    }

    pub fn simple_terms(&self) -> &[String] {
        &self.simple_terms
    }
}

pub struct SignalComputer {
    query_data: Option<QueryData>,
    query_signal_coefficients: Option<SignalCoefficient>,
    segment_reader: Option<RefCell<SegmentReader>>,
    fetch_time_ms_cache: Vec<f64>,
    update_time_cache: Vec<f64>,
    region_count: Option<Arc<RegionCount>>,
    current_timestamp: Option<usize>,
    linear_regression: Option<Arc<LinearRegression>>,
    order: SignalComputeOrder,
}

impl Clone for SignalComputer {
    fn clone(&self) -> Self {
        Self {
            query_data: self.query_data.clone(),
            query_signal_coefficients: self.query_signal_coefficients.clone(),
            segment_reader: None,
            fetch_time_ms_cache: self.fetch_time_ms_cache.clone(),
            update_time_cache: self.update_time_cache.clone(),
            region_count: self.region_count.clone(),
            current_timestamp: self.current_timestamp,
            linear_regression: self.linear_regression.clone(),
            order: self.order.clone(),
        }
    }
}

impl std::fmt::Debug for SignalComputer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignalComputer")
            .field(
                "query",
                &self
                    .query_data
                    .as_ref()
                    .map(|q| q.simple_terms.clone())
                    .unwrap_or_default(),
            )
            .finish()
    }
}

impl SignalComputer {
    pub fn new(query: Option<&Query>) -> Self {
        let query_signal_coefficients = query.as_ref().map(|q| q.signal_coefficients());

        let fetch_time_ms_cache: Vec<_> = (0..1000)
            .map(|fetch_time| 1.0 / (fetch_time as f64 + 1.0))
            .collect();

        let update_time_cache = (0..(3 * 365 * 24))
            .map(|hours_since_update| 1.0 / ((hours_since_update as f64 + 1.0).log2()))
            .collect();

        let query = query.as_ref().map(|q| QueryData {
            simple_terms: q.simple_terms().to_vec(),
            optic_rules: q
                .optics()
                .iter()
                .flat_map(|o| o.rules.iter())
                .filter(|rule| match rule.action {
                    optics::Action::Downrank(b) | optics::Action::Boost(b) => b != 0,
                    optics::Action::Discard => false,
                })
                .cloned()
                .collect(),
            selected_region: q.region().cloned(),
            lang: q.lang(),
        });

        let mut s = Self {
            segment_reader: None,
            query_signal_coefficients,
            fetch_time_ms_cache,
            update_time_cache,
            region_count: None,
            current_timestamp: None,
            linear_regression: None,
            query_data: query,
            order: SignalComputeOrder::new(),
        };

        s.set_current_timestamp(chrono::Utc::now().timestamp() as usize);

        s
    }

    fn prepare_textfields(
        &self,
        tv_searcher: &tantivy::Searcher,
        segment_reader: &tantivy::SegmentReader,
    ) -> Result<EnumMap<TextFieldEnum, TextFieldData>> {
        let mut text_fields = EnumMap::new();
        let schema = tv_searcher.schema();

        if let Some(query) = &self.query_data {
            if !query.simple_terms.is_empty() {
                for signal in SignalEnum::all() {
                    if let Some((text_field, tv_field)) = signal
                        .as_textfield()
                        .and_then(|f| (f.tantivy_field(schema).map(|tv_field| (f, tv_field))))
                    {
                        if text_field.ngram_size() > 1
                            && query.simple_terms.len() > MAX_TERMS_FOR_NGRAM_LOOKUPS
                        {
                            continue;
                        }

                        let simple_query = itertools::intersperse(
                            query.simple_terms.iter().map(|s| s.as_str()),
                            " ",
                        )
                        .collect::<String>();

                        let mut terms = Vec::new();
                        let mut tokenizer = text_field.tokenizer(query.lang.as_ref());
                        let mut stream = tokenizer.token_stream(&simple_query);
                        let mut it = tantivy::tokenizer::TokenStream::iter(&mut stream);

                        while let Some(token) = it.next() {
                            let term = tantivy::Term::from_field_text(tv_field, &token.text);
                            terms.push(term);
                        }

                        if terms.is_empty() {
                            continue;
                        }

                        let fieldnorm_reader = segment_reader.get_fieldnorms_reader(tv_field)?;
                        let inverted_index = segment_reader.inverted_index(tv_field)?;

                        let mut matching_terms = Vec::with_capacity(terms.len());
                        let mut postings = Vec::with_capacity(terms.len());
                        for term in &terms {
                            if let Some(p) =
                                inverted_index.read_postings(term, text_field.record_option())?
                            {
                                postings.push(p);
                                matching_terms.push(term.clone());
                            }
                        }
                        let bm25 = MultiBm25Weight::for_terms(
                            tv_searcher,
                            &matching_terms,
                            text_field.bm25_constants(),
                        )?;
                        let bm25f = MultiBm25FWeight::for_terms(
                            tv_searcher,
                            &matching_terms,
                            text_field.bm25_constants(),
                        );

                        text_fields.insert(
                            text_field,
                            TextFieldData {
                                postings,
                                bm25,
                                bm25f,
                                fieldnorm_reader,
                                signal_coefficient: self.coefficient(&signal),
                            },
                        );
                    }
                }
            }
        }

        Ok(text_fields)
    }

    fn prepare_optic(
        &self,
        tv_searcher: &tantivy::Searcher,
        segment_reader: &tantivy::SegmentReader,
        numericalfield_reader: &numericalfield_reader::NumericalFieldReader,
    ) -> Vec<RuleBoost> {
        let mut optic_rule_boosts = Vec::new();

        if let Some(query) = &self.query_data {
            optic_rule_boosts = query
                .optic_rules
                .iter()
                .filter_map(|rule| {
                    rule.as_searchable_rule(tv_searcher.schema(), numericalfield_reader)
                })
                .map(|(_, rule)| RuleBoost {
                    docset: rule
                        .query
                        .weight(tantivy::query::EnableScoring::Enabled {
                            searcher: tv_searcher,
                            statistics_provider: tv_searcher,
                        })
                        .unwrap()
                        .scorer(segment_reader, 0.0)
                        .unwrap(),
                    boost: rule.boost,
                })
                .collect();
        }

        optic_rule_boosts
    }

    pub fn register_segment(
        &mut self,
        tv_searcher: &tantivy::Searcher,
        segment_reader: &tantivy::SegmentReader,
        numericalfield_reader: &numericalfield_reader::NumericalFieldReader,
    ) -> Result<()> {
        let numericalfield_segment_reader =
            numericalfield_reader.get_segment(&segment_reader.segment_id());
        let text_fields = self.prepare_textfields(tv_searcher, segment_reader)?;
        let optic_rule_boosts =
            self.prepare_optic(tv_searcher, segment_reader, numericalfield_reader);

        self.segment_reader = Some(RefCell::new(SegmentReader {
            text_fields,
            numericalfield_reader: numericalfield_segment_reader,
            optic_boosts: OpticBoosts {
                rules: optic_rule_boosts,
            },
        }));

        Ok(())
    }

    pub fn set_region_count(&mut self, region_count: RegionCount) {
        self.region_count = Some(Arc::new(region_count));
    }

    pub fn set_current_timestamp(&mut self, current_timestamp: usize) {
        self.current_timestamp = Some(current_timestamp);
    }

    pub fn set_linear_model(&mut self, linear_model: Arc<LinearRegression>) {
        self.linear_regression = Some(linear_model);
    }

    /// Computes the scored signals for a given document.
    ///
    /// Important: This function assumes that the docs a scored in ascending order of docid
    /// within their segment. If this invariant is not upheld, the documents will not have
    /// scores calculated for their text related signals. The wrong ranking will most likely
    /// be returned.
    /// This function also assumes that the segment reader has been set.
    pub fn compute_signals(&self, doc: DocId) -> impl Iterator<Item = ComputedSignal> + '_ {
        self.order.compute(doc, self)
    }

    pub fn boosts(&mut self, doc: DocId) -> Option<f64> {
        self.segment_reader.as_ref().map(|segment_reader| {
            let mut downrank = 0.0;
            let mut boost = 0.0;

            for rule in &mut segment_reader.borrow_mut().optic_boosts.rules {
                if rule.docset.doc() > doc {
                    continue;
                }

                if rule.docset.doc() == doc || rule.docset.seek(doc) == doc {
                    if rule.boost < 0.0 {
                        downrank += rule.boost.abs();
                    } else {
                        boost += rule.boost;
                    }
                }
            }

            if downrank > boost {
                let diff = downrank - boost;
                1.0 / (1.0 + diff)
            } else {
                boost - downrank + 1.0
            }
        })
    }

    pub fn precompute_score(&self, webpage: &Webpage) -> f64 {
        SignalEnum::all()
            .filter_map(|signal| {
                signal
                    .precompute(webpage, self)
                    .map(|score| ComputedSignal { signal, score })
            })
            .map(|computed| self.coefficient(&computed.signal) * computed.score)
            .sum()
    }

    pub fn coefficient(&self, signal: &SignalEnum) -> f64 {
        self.query_signal_coefficients
            .as_ref()
            .map(|coefficients| coefficients.get(signal))
            .or_else(|| {
                self.linear_regression
                    .as_ref()
                    .and_then(|model| model.weights.get(*signal).copied())
            })
            .unwrap_or(signal.default_coefficient())
    }

    pub fn segment_reader(&self) -> Option<&RefCell<SegmentReader>> {
        self.segment_reader.as_ref()
    }

    pub fn fetch_time_ms_cache(&self) -> &[f64] {
        &self.fetch_time_ms_cache
    }

    pub fn current_timestamp(&self) -> Option<usize> {
        self.current_timestamp
    }

    pub fn update_time_cache(&self) -> &[f64] {
        &self.update_time_cache
    }

    pub fn region_count(&self) -> Option<&RegionCount> {
        self.region_count.as_deref()
    }

    pub fn query_data(&self) -> Option<&QueryData> {
        self.query_data.as_ref()
    }
}
