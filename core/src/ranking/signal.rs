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

use crate::fastfield_reader::FieldValue;
use crate::query::optic::AsSearchableRule;
use crate::query::Query;
use crate::Result;
use crate::{
    enum_map::EnumMap,
    fastfield_reader,
    schema::{FastField, TextField},
    webgraph::{
        centrality::{online_harmonic, topic},
        NodeID,
    },
    webpage::Webpage,
};
use std::str::FromStr;
use std::sync::Arc;
use tantivy::fieldnorm::FieldNormReader;
use tantivy::postings::SegmentPostings;
use tantivy::query::Scorer;
use thiserror::Error;

use tantivy::DocSet;
use tantivy::{DocId, Postings};

use crate::{
    schema::FLOAT_SCALING,
    webpage::region::{Region, RegionCount},
};

use super::bm25::Bm25Weight;
use super::inbound_similarity;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unknown signal: {0}")]
    UnknownSignal(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Signal {
    Bm25,
    Bm25Title,
    Bm25CleanBody,
    Bm25StemmedTitle,
    Bm25StemmedCleanBody,
    Bm25AllBody,
    Bm25Url,
    Bm25Site,
    Bm25Domain,
    Bm25SiteNoTokenizer,
    Bm25DomainNoTokenizer,
    Bm25DomainIfHomepage,
    Bm25DomainNameIfHomepageNoTokenizer,
    Bm25TitleIfHomepage,
    Bm25BacklinkText,
    Bm25Description,
    CrossEncoder,
    HostCentrality,
    PageCentrality,
    IsHomepage,
    FetchTimeMs,
    UpdateTimestamp,
    TrackerScore,
    Region,
    PersonalCentrality,
    CrawlStability,
    TopicCentrality,
    QueryCentrality,
    InboundSimilarity,
    LambdaMART,
}

impl From<Signal> for usize {
    fn from(signal: Signal) -> Self {
        signal as usize
    }
}

pub const ALL_SIGNALS: [Signal; 30] = [
    Signal::Bm25,
    Signal::Bm25Title,
    Signal::Bm25CleanBody,
    Signal::Bm25StemmedTitle,
    Signal::Bm25StemmedCleanBody,
    Signal::Bm25AllBody,
    Signal::Bm25Url,
    Signal::Bm25Site,
    Signal::Bm25Domain,
    Signal::Bm25SiteNoTokenizer,
    Signal::Bm25DomainNoTokenizer,
    Signal::Bm25DomainIfHomepage,
    Signal::Bm25DomainNameIfHomepageNoTokenizer,
    Signal::Bm25TitleIfHomepage,
    Signal::Bm25BacklinkText,
    Signal::Bm25Description,
    Signal::CrossEncoder,
    Signal::HostCentrality,
    Signal::PageCentrality,
    Signal::IsHomepage,
    Signal::FetchTimeMs,
    Signal::UpdateTimestamp,
    Signal::TrackerScore,
    Signal::Region,
    Signal::PersonalCentrality,
    Signal::CrawlStability,
    Signal::TopicCentrality,
    Signal::QueryCentrality,
    Signal::InboundSimilarity,
    Signal::LambdaMART,
];

fn score_timestamp(timestamp: usize, signal_aggregator: &SignalAggregator) -> f64 {
    if timestamp >= signal_aggregator.current_timestamp.unwrap_or(0) {
        return 0.0;
    }

    let hours_since_update =
        (signal_aggregator.current_timestamp.unwrap() - timestamp).max(1) / 3600;

    if hours_since_update < signal_aggregator.update_time_cache.len() {
        signal_aggregator.update_time_cache[hours_since_update]
    } else {
        0.0
    }
}

fn score_trackers(num_trackers: f64) -> f64 {
    1.0 / (num_trackers + 1.0)
}

fn bm25(field: &mut TextFieldData, doc: DocId) -> f64 {
    let mut term_freq = 0;
    for posting in &mut field.postings {
        if posting.doc() == doc || (posting.doc() < doc && posting.seek(doc) == doc) {
            term_freq += posting.term_freq();
        }
    }

    if term_freq == 0 {
        return 0.0;
    }

    let fieldnorm_id = field.fieldnorm_reader.fieldnorm_id(doc);
    field.weight.score(fieldnorm_id, term_freq) as f64
}

impl Signal {
    fn is_computable_before_search(&self) -> bool {
        self.as_fastfield().is_some()
    }

    fn default_coefficient(&self) -> f64 {
        match self {
            Signal::Bm25 => 1.0,
            Signal::Bm25Title => 15.0,
            Signal::Bm25CleanBody => 4.0,
            Signal::Bm25StemmedTitle => 0.5,
            Signal::Bm25StemmedCleanBody => 0.5,
            Signal::Bm25AllBody => 1.0,
            Signal::Bm25Url => 1.0,
            Signal::Bm25Site => 1.0,
            Signal::Bm25Domain => 1.0,
            Signal::Bm25SiteNoTokenizer => 1.0,
            Signal::Bm25DomainNoTokenizer => 1.0,
            Signal::Bm25DomainIfHomepage => 6.0,
            Signal::Bm25DomainNameIfHomepageNoTokenizer => 30.0,
            Signal::Bm25TitleIfHomepage => 3.0,
            Signal::Bm25BacklinkText => 4.0,
            Signal::Bm25Description => 1.0,
            Signal::CrossEncoder => 100.0,
            Signal::HostCentrality => 2_500.0,
            Signal::PageCentrality => 4_500.0,
            Signal::TopicCentrality => 2_500.0,
            Signal::QueryCentrality => 1_000.0,
            Signal::IsHomepage => 0.1,
            Signal::FetchTimeMs => 0.001,
            Signal::UpdateTimestamp => 80.0,
            Signal::TrackerScore => 20.0,
            Signal::Region => 60.0,
            Signal::CrawlStability => 20.0,
            Signal::PersonalCentrality => 5_000.0,
            Signal::InboundSimilarity => 5_000.0,
            Signal::LambdaMART => 10_000.0,
        }
    }

    fn host_id(&self, aggregator: &SignalAggregator, doc: DocId) -> Option<NodeID> {
        aggregator.segment_reader.as_ref().and_then(|reader| {
            let node_id: Option<u64> = reader
                .fastfield_reader
                .get_field_reader(&FastField::HostNodeID)
                .get(&doc)
                .into();
            let node_id = node_id.unwrap();

            if node_id == u64::MAX {
                None
            } else {
                Some(NodeID::from(node_id))
            }
        })
    }

    fn fastfield_value(&self, aggregator: &SignalAggregator, doc: DocId) -> Option<FieldValue> {
        aggregator.segment_reader.as_ref().and_then(|reader| {
            self.as_fastfield().map(|fast_field| {
                reader
                    .fastfield_reader
                    .get_field_reader(&fast_field)
                    .get(&doc)
            })
        })
    }

    pub fn compute(
        self,
        signal_aggregator: &mut SignalAggregator,
        doc: DocId,
    ) -> Option<ComputedSignal> {
        let value: Option<f64> = match self {
            Signal::HostCentrality | Signal::PageCentrality | Signal::CrawlStability => {
                let field_value: Option<u64> = self
                    .fastfield_value(signal_aggregator, doc)
                    .and_then(|val| val.into());

                field_value.map(|val| val as f64 / FLOAT_SCALING as f64)
            }
            Signal::IsHomepage => {
                let field_value: Option<u64> = self
                    .fastfield_value(signal_aggregator, doc)
                    .and_then(|val| val.into());

                field_value.map(|val| val as f64)
            }
            Signal::FetchTimeMs => {
                let field_value: Option<u64> = self
                    .fastfield_value(signal_aggregator, doc)
                    .and_then(|val| val.into());

                field_value.map(|v| v as usize).map(|fetch_time_ms| {
                    if fetch_time_ms >= signal_aggregator.fetch_time_ms_cache.len() {
                        0.0
                    } else {
                        signal_aggregator.fetch_time_ms_cache[fetch_time_ms]
                    }
                })
            }
            Signal::UpdateTimestamp => {
                let field_value: Option<u64> = self
                    .fastfield_value(signal_aggregator, doc)
                    .and_then(|val| val.into());

                field_value
                    .map(|v| v as usize)
                    .map(|update_timestamp| score_timestamp(update_timestamp, signal_aggregator))
            }
            Signal::TrackerScore => {
                let field_value: Option<u64> = self
                    .fastfield_value(signal_aggregator, doc)
                    .and_then(|val| val.into());

                field_value.map(|num_trackers| score_trackers(num_trackers as f64))
            }
            Signal::Region => {
                let field_value: Option<u64> = self
                    .fastfield_value(signal_aggregator, doc)
                    .and_then(|val| val.into());

                field_value
                    .map(Region::from_id)
                    .map(|region| score_region(region, signal_aggregator))
            }
            Signal::PersonalCentrality => {
                let host_id = self.host_id(signal_aggregator, doc);
                host_id.map(|host_id| signal_aggregator.personal_centrality(host_id))
            }
            Signal::TopicCentrality => {
                let host_id = self.host_id(signal_aggregator, doc);
                host_id.and_then(|host_id| signal_aggregator.topic_centrality(host_id))
            }
            Signal::QueryCentrality => {
                let host_id = self.host_id(signal_aggregator, doc);
                host_id.and_then(|host_id| signal_aggregator.query_centrality(host_id))
            }
            Signal::InboundSimilarity => {
                let host_id = self.host_id(signal_aggregator, doc);
                host_id.map(|host_id| signal_aggregator.inbound_similarity(host_id))
            }
            Signal::Bm25 => signal_aggregator.segment_reader.as_mut().map(|reader| {
                reader
                    .text_fields
                    .values_mut()
                    .map(|field| bm25(field, doc))
                    .sum()
            }),
            Signal::Bm25Title
            | Signal::Bm25CleanBody
            | Signal::Bm25StemmedTitle
            | Signal::Bm25StemmedCleanBody
            | Signal::Bm25AllBody
            | Signal::Bm25Url
            | Signal::Bm25Site
            | Signal::Bm25Domain
            | Signal::Bm25SiteNoTokenizer
            | Signal::Bm25DomainNoTokenizer
            | Signal::Bm25DomainIfHomepage
            | Signal::Bm25DomainNameIfHomepageNoTokenizer
            | Signal::Bm25TitleIfHomepage
            | Signal::Bm25BacklinkText
            | Signal::Bm25Description => signal_aggregator.segment_reader.as_mut().map(|reader| {
                reader
                    .text_fields
                    .get_mut(self.as_textfield().unwrap())
                    .map(|field| bm25(field, doc))
                    .unwrap_or(0.0)
            }),
            Signal::CrossEncoder => None, // this is calculated in a later step
            Signal::LambdaMART => None,
        };

        value.map(|value| ComputedSignal {
            signal: self,
            coefficient: signal_aggregator.coefficients().get(&self),
            value,
        })
    }

    pub fn precompute(
        self,
        signal_aggregator: &SignalAggregator,
        webpage: &Webpage,
    ) -> Option<ComputedSignal> {
        if !self.is_computable_before_search() {
            return None;
        }

        let value = match self {
            Signal::HostCentrality => Some(webpage.host_centrality),
            Signal::PageCentrality => Some(webpage.page_centrality),
            Signal::IsHomepage => Some(webpage.html.url().is_homepage().into()),
            Signal::CrawlStability => Some(webpage.crawl_stability),
            Signal::FetchTimeMs => {
                let fetch_time_ms = webpage.fetch_time_ms as usize;
                if fetch_time_ms >= signal_aggregator.fetch_time_ms_cache.len() {
                    Some(0.0)
                } else {
                    Some(signal_aggregator.fetch_time_ms_cache[fetch_time_ms])
                }
            }
            Signal::UpdateTimestamp => {
                let update_timestamp = webpage
                    .html
                    .updated_time()
                    .map(|date| date.timestamp().max(0))
                    .unwrap_or(0) as usize;

                Some(score_timestamp(update_timestamp, signal_aggregator))
            }
            Signal::TrackerScore => {
                let num_trackers = webpage.html.trackers().len() as f64;
                Some(score_trackers(num_trackers))
            }
            Signal::Region => {
                let region = Region::guess_from(webpage).unwrap_or(Region::All);
                Some(score_region(region, signal_aggregator))
            }
            Signal::Bm25
            | Signal::Bm25Title
            | Signal::Bm25CleanBody
            | Signal::Bm25StemmedTitle
            | Signal::Bm25StemmedCleanBody
            | Signal::Bm25AllBody
            | Signal::Bm25Url
            | Signal::Bm25Site
            | Signal::Bm25Domain
            | Signal::Bm25SiteNoTokenizer
            | Signal::Bm25DomainNoTokenizer
            | Signal::Bm25DomainIfHomepage
            | Signal::Bm25DomainNameIfHomepageNoTokenizer
            | Signal::Bm25TitleIfHomepage
            | Signal::Bm25BacklinkText
            | Signal::Bm25Description
            | Signal::CrossEncoder
            | Signal::PersonalCentrality
            | Signal::TopicCentrality
            | Signal::InboundSimilarity
            | Signal::LambdaMART
            | Signal::QueryCentrality => {
                tracing::error!("signal {self:?} cannot be precomputed");
                None
            }
        };

        value.map(|value| ComputedSignal {
            signal: self,
            coefficient: signal_aggregator.coefficients().get(&self),
            value,
        })
    }

    fn as_fastfield(&self) -> Option<FastField> {
        match self {
            Signal::HostCentrality => Some(FastField::HostCentrality),
            Signal::PageCentrality => Some(FastField::PageCentrality),
            Signal::IsHomepage => Some(FastField::IsHomepage),
            Signal::FetchTimeMs => Some(FastField::FetchTimeMs),
            Signal::UpdateTimestamp => Some(FastField::LastUpdated),
            Signal::TrackerScore => Some(FastField::TrackerScore),
            Signal::Region => Some(FastField::Region),
            Signal::CrawlStability => Some(FastField::CrawlStability),
            _ => None,
        }
    }

    fn as_textfield(&self) -> Option<TextField> {
        match self {
            Signal::Bm25Title => Some(TextField::Title),
            Signal::Bm25CleanBody => Some(TextField::CleanBody),
            Signal::Bm25StemmedTitle => Some(TextField::StemmedTitle),
            Signal::Bm25StemmedCleanBody => Some(TextField::StemmedCleanBody),
            Signal::Bm25AllBody => Some(TextField::AllBody),
            Signal::Bm25Url => Some(TextField::Url),
            Signal::Bm25Site => Some(TextField::Site),
            Signal::Bm25Domain => Some(TextField::Domain),
            Signal::Bm25SiteNoTokenizer => Some(TextField::SiteNoTokenizer),
            Signal::Bm25DomainNoTokenizer => Some(TextField::DomainNoTokenizer),
            Signal::Bm25DomainIfHomepage => Some(TextField::DomainIfHomepage),
            Signal::Bm25DomainNameIfHomepageNoTokenizer => {
                Some(TextField::DomainNameIfHomepageNoTokenizer)
            }
            Signal::Bm25TitleIfHomepage => Some(TextField::TitleIfHomepage),
            Signal::Bm25BacklinkText => Some(TextField::BacklinkText),
            Signal::Bm25Description => Some(TextField::Description),
            _ => None,
        }
    }
}

impl FromStr for Signal {
    type Err = Error;

    fn from_str(name: &str) -> std::result::Result<Self, Self::Err> {
        match name {
            "bm25" => Ok(Signal::Bm25),
            "bm25_title" => Ok(Signal::Bm25Title),
            "bm25_clean_body" => Ok(Signal::Bm25CleanBody),
            "bm25_stemmed_title" => Ok(Signal::Bm25StemmedTitle),
            "bm25_stemmed_clean_body" => Ok(Signal::Bm25StemmedCleanBody),
            "bm25_all_body" => Ok(Signal::Bm25AllBody),
            "bm25_url" => Ok(Signal::Bm25Url),
            "bm25_site" => Ok(Signal::Bm25Site),
            "bm25_domain" => Ok(Signal::Bm25Domain),
            "bm25_site_no_tokenizer" => Ok(Signal::Bm25SiteNoTokenizer),
            "bm25_domain_no_tokenizer" => Ok(Signal::Bm25DomainNoTokenizer),
            "bm25_domain_if_homepage" => Ok(Signal::Bm25DomainIfHomepage),
            "bm25_domain_name_if_homepage_no_tokenizer" => {
                Ok(Signal::Bm25DomainNameIfHomepageNoTokenizer)
            }
            "bm25_title_if_homepage" => Ok(Signal::Bm25TitleIfHomepage),
            "bm25_backlink_text" => Ok(Signal::Bm25BacklinkText),
            "bm25_description" => Ok(Signal::Bm25Description),
            "cross_encoder" => Ok(Signal::CrossEncoder),
            "host_centrality" => Ok(Signal::HostCentrality),
            "page_centrality" => Ok(Signal::PageCentrality),
            "is_homepage" => Ok(Signal::IsHomepage),
            "fetch_time_ms" => Ok(Signal::FetchTimeMs),
            "update_timestamp" => Ok(Signal::UpdateTimestamp),
            "tracker_score" => Ok(Signal::TrackerScore),
            "region" => Ok(Signal::Region),
            "personal_centrality" => Ok(Signal::PersonalCentrality),
            "topic_centrality" => Ok(Signal::TopicCentrality),
            "query_centrality" => Ok(Signal::QueryCentrality),
            "inbound_similarity" => Ok(Signal::InboundSimilarity),
            "crawl_stability" => Ok(Signal::CrawlStability),
            "lambda_mart" => Ok(Signal::LambdaMART),
            _ => Err(Error::UnknownSignal(name.to_string())),
        }
    }
}

fn score_region(webpage_region: Region, aggregator: &SignalAggregator) -> f64 {
    match aggregator.region_count.as_ref() {
        Some(region_count) => {
            let boost = aggregator.selected_region.map_or(0.0, |region| {
                if region == webpage_region {
                    50.0
                } else {
                    0.0
                }
            });

            boost + region_count.score(&webpage_region)
        }
        None => 0.0,
    }
}

#[derive(Debug, Clone, Default)]
pub struct SignalCoefficient(Vec<Option<f64>>);

impl SignalCoefficient {
    pub fn get(&self, signal: &Signal) -> f64 {
        self.0
            .get((*signal) as usize)
            .copied()
            .flatten()
            .unwrap_or_else(|| signal.default_coefficient())
    }

    pub fn new(coefficients: impl Iterator<Item = (Signal, f64)>) -> Self {
        let mut fast_coefficients = Vec::new();

        for (signal, coefficient) in coefficients {
            let idx = signal as usize;

            while idx >= fast_coefficients.len() {
                fast_coefficients.push(None);
            }

            fast_coefficients[idx] = Some(coefficient);
        }

        Self(fast_coefficients)
    }
}

#[derive(Clone)]
struct TextFieldData {
    postings: Vec<SegmentPostings>,
    weight: Bm25Weight,
    fieldnorm_reader: FieldNormReader,
}

struct RuleBoost {
    docset: Box<dyn Scorer>,
    boost: f64,
}

struct SegmentReader {
    text_fields: EnumMap<TextField, TextFieldData>,
    optic_rule_boosts: Vec<RuleBoost>,
    fastfield_reader: Arc<fastfield_reader::SegmentReader>,
}

pub struct SignalAggregator {
    query: Option<Query>,
    signal_coefficients: SignalCoefficient,
    segment_reader: Option<SegmentReader>,
    personal_centrality: Option<Arc<online_harmonic::Scorer>>,
    inbound_similariy: Option<Arc<inbound_similarity::Scorer>>,
    fetch_time_ms_cache: Vec<f64>,
    update_time_cache: Vec<f64>,
    topic_scorer: Option<topic::Scorer>,
    query_centrality: Option<Arc<online_harmonic::Scorer>>,
    region_count: Option<Arc<RegionCount>>,
    selected_region: Option<Region>,
    current_timestamp: Option<usize>,
}

impl Clone for SignalAggregator {
    fn clone(&self) -> Self {
        Self {
            query: self.query.clone(),
            signal_coefficients: self.signal_coefficients.clone(),
            segment_reader: None,
            personal_centrality: self.personal_centrality.clone(),
            inbound_similariy: self.inbound_similariy.clone(),
            fetch_time_ms_cache: self.fetch_time_ms_cache.clone(),
            update_time_cache: self.update_time_cache.clone(),
            topic_scorer: self.topic_scorer.clone(),
            query_centrality: self.query_centrality.clone(),
            region_count: self.region_count.clone(),
            selected_region: self.selected_region,
            current_timestamp: self.current_timestamp,
        }
    }
}

impl std::fmt::Debug for SignalAggregator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignalAggregator")
            .field("query", &self.query)
            .finish()
    }
}

impl SignalAggregator {
    pub fn new(query: Option<Query>) -> Self {
        let signal_coefficients = query
            .as_ref()
            .map(|q| q.signal_coefficients())
            .unwrap_or_default();

        let fetch_time_ms_cache: Vec<_> = (0..1000)
            .map(|fetch_time| 1.0 / (fetch_time as f64 + 1.0))
            .collect();

        let update_time_cache = (0..(3 * 365 * 24))
            .map(|hours_since_update| 1.0 / ((hours_since_update as f64 + 1.0).log2()))
            .collect();

        Self {
            segment_reader: None,
            personal_centrality: None,
            inbound_similariy: None,
            signal_coefficients,
            fetch_time_ms_cache,
            update_time_cache,
            topic_scorer: None,
            query_centrality: None,
            region_count: None,
            selected_region: None,
            current_timestamp: None,
            query,
        }
    }

    pub fn register_segment(
        &mut self,
        tv_searcher: &tantivy::Searcher,
        segment_reader: &tantivy::SegmentReader,
        fastfield_reader: &fastfield_reader::FastFieldReader,
    ) -> Result<()> {
        let fastfield_segment_reader = fastfield_reader.get_segment(&segment_reader.segment_id());
        let mut text_fields = EnumMap::new();
        let schema = tv_searcher.schema();
        let mut optic_rule_boosts = Vec::new();

        if let Some(query) = &self.query {
            if !query.simple_terms().is_empty() {
                for signal in ALL_SIGNALS {
                    if let Some(text_field) = signal.as_textfield() {
                        let tv_field = schema.get_field(text_field.name()).unwrap();
                        let terms: Vec<_> = query
                            .simple_terms()
                            .iter()
                            .map(|text| tantivy::Term::from_field_text(tv_field, text))
                            .collect();

                        let weight = Bm25Weight::for_terms(tv_searcher, &terms)?;

                        let fieldnorm_reader = segment_reader.get_fieldnorms_reader(tv_field)?;
                        let inverted_index = segment_reader.inverted_index(tv_field)?;

                        let mut postings = Vec::with_capacity(terms.len());
                        for term in &terms {
                            if let Some(p) =
                                inverted_index.read_postings(term, text_field.index_option())?
                            {
                                postings.push(p);
                            }
                        }

                        text_fields.insert(
                            text_field,
                            TextFieldData {
                                postings,
                                weight,
                                fieldnorm_reader,
                            },
                        );
                    }
                }
            }

            if let Some(optic) = query.optic() {
                optic_rule_boosts = optic
                    .rules
                    .iter()
                    .filter_map(|rule| {
                        rule.as_searchable_rule(tv_searcher.schema(), fastfield_reader)
                    })
                    .map(|(_, rule)| RuleBoost {
                        docset: rule
                            .query
                            .weight(tantivy::query::EnableScoring::Enabled(tv_searcher))
                            .unwrap()
                            .scorer(segment_reader, 0.0)
                            .unwrap(),
                        boost: rule.boost,
                    })
                    .collect();
            }
        }

        self.segment_reader = Some(SegmentReader {
            text_fields,
            fastfield_reader: fastfield_segment_reader,
            optic_rule_boosts,
        });

        Ok(())
    }

    pub fn set_topic_scorer(&mut self, topic_scorer: topic::Scorer) {
        self.topic_scorer = Some(topic_scorer);
    }

    pub fn set_query_centrality(&mut self, query_centrality: online_harmonic::Scorer) {
        self.query_centrality = Some(Arc::new(query_centrality));
    }

    pub fn set_personal_harmonic(&mut self, personal_centrality: online_harmonic::Scorer) {
        self.personal_centrality = Some(Arc::new(personal_centrality));
    }

    pub fn set_inbound_similarity(&mut self, scorer: inbound_similarity::Scorer) {
        self.inbound_similariy = Some(Arc::new(scorer));
    }

    pub fn set_region_count(&mut self, region_count: RegionCount) {
        self.region_count = Some(Arc::new(region_count));
    }

    pub fn set_selected_region(&mut self, region: Region) {
        self.selected_region = Some(region);
    }

    pub fn set_current_timestamp(&mut self, current_timestamp: usize) {
        self.current_timestamp = Some(current_timestamp);
    }

    pub fn topic_centrality(&self, host_id: NodeID) -> Option<f64> {
        self.topic_scorer
            .as_ref()
            .map(|scorer| scorer.score(host_id))
    }

    pub fn query_centrality(&self, host_id: NodeID) -> Option<f64> {
        self.query_centrality
            .as_ref()
            .map(|scorer| scorer.score(host_id))
    }

    pub fn personal_centrality(&self, host_id: NodeID) -> f64 {
        self.personal_centrality
            .as_ref()
            .map(|scorer| scorer.score(host_id))
            .unwrap_or_default()
    }

    pub fn inbound_similarity(&self, host_id: NodeID) -> f64 {
        self.inbound_similariy
            .as_ref()
            .map(|scorer| scorer.score(&host_id))
            .unwrap_or_default()
    }

    pub fn compute_signals(
        &mut self,
        doc: DocId,
    ) -> impl Iterator<Item = Option<ComputedSignal>> + '_ {
        ALL_SIGNALS
            .into_iter()
            .map(move |signal| signal.compute(self, doc))
    }

    pub fn boosts(&mut self, doc: DocId) -> Option<f64> {
        self.segment_reader.as_mut().and_then(|segment_reader| {
            let mut res = 0.0;
            for rule in &mut segment_reader.optic_rule_boosts {
                if rule.docset.doc() > doc {
                    continue;
                }

                if rule.docset.doc() == doc || rule.docset.seek(doc) == doc {
                    res += rule.boost;
                }
            }

            if res == 0.0 {
                None
            } else {
                Some(res)
            }
        })
    }

    pub fn precompute_score(&self, webpage: &Webpage) -> f64 {
        ALL_SIGNALS
            .into_iter()
            .filter_map(|signal| signal.precompute(self, webpage))
            .map(|computed| computed.coefficient * computed.value)
            .sum()
    }

    fn coefficients(&self) -> &SignalCoefficient {
        &self.signal_coefficients
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ComputedSignal {
    pub signal: Signal,
    pub coefficient: f64,
    pub value: f64,
}
