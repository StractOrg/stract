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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

use crate::schema::{self, Field};
use itertools::Itertools;

use tantivy::DocSet;
use tantivy::{DocId, Postings};

use super::computer::TextFieldData;
use super::{Signal, SignalComputer};

fn bm25(field: &mut TextFieldData, doc: DocId) -> f64 {
    if field.postings.is_empty() {
        return 0.0;
    }

    let fieldnorm_id = field.fieldnorm_reader.fieldnorm_id(doc);

    field
        .weight
        .score(field.postings.iter_mut().map(move |posting| {
            if posting.doc() == doc || (posting.doc() < doc && posting.seek(doc) == doc) {
                (fieldnorm_id, posting.term_freq())
            } else {
                (fieldnorm_id, 0)
            }
        })) as f64
}

fn idf_sum(field: &mut TextFieldData, doc: DocId) -> f64 {
    if field.postings.is_empty() {
        return 0.0;
    }
    let idf = field.weight.idf();

    field
        .postings
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

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Bm25Title;
impl Signal for Bm25Title {
    fn default_coefficient(&self) -> f64 {
        0.0063
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::Title.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Bm25TitleBigrams;
impl Signal for Bm25TitleBigrams {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::TitleBigrams.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Bm25TitleTrigrams;
impl Signal for Bm25TitleTrigrams {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::TitleTrigrams.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Bm25CleanBody;
impl Signal for Bm25CleanBody {
    fn default_coefficient(&self) -> f64 {
        0.0063
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::CleanBody.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Bm25CleanBodyBigrams;
impl Signal for Bm25CleanBodyBigrams {
    fn default_coefficient(&self) -> f64 {
        0.005
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::CleanBodyBigrams.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Bm25CleanBodyTrigrams;
impl Signal for Bm25CleanBodyTrigrams {
    fn default_coefficient(&self) -> f64 {
        0.005
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::CleanBodyTrigrams.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Bm25StemmedTitle;
impl Signal for Bm25StemmedTitle {
    fn default_coefficient(&self) -> f64 {
        0.003
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::StemmedTitle.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Bm25StemmedCleanBody;
impl Signal for Bm25StemmedCleanBody {
    fn default_coefficient(&self) -> f64 {
        0.001
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::StemmedCleanBody.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Bm25AllBody;
impl Signal for Bm25AllBody {
    fn default_coefficient(&self) -> f64 {
        0.0
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::AllBody.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Bm25Keywords;
impl Signal for Bm25Keywords {
    fn default_coefficient(&self) -> f64 {
        0.001
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::Keywords.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Bm25BacklinkText;
impl Signal for Bm25BacklinkText {
    fn default_coefficient(&self) -> f64 {
        0.003
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::BacklinkText.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| bm25(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct IdfSumUrl;
impl Signal for IdfSumUrl {
    fn default_coefficient(&self) -> f64 {
        0.0003
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::Url.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct IdfSumSite;
impl Signal for IdfSumSite {
    fn default_coefficient(&self) -> f64 {
        0.00015
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::SiteWithout.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct IdfSumDomain;
impl Signal for IdfSumDomain {
    fn default_coefficient(&self) -> f64 {
        0.0003
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::Domain.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct IdfSumSiteNoTokenizer;
impl Signal for IdfSumSiteNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.00015
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::SiteNoTokenizer.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct IdfSumDomainNoTokenizer;
impl Signal for IdfSumDomainNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.0002
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::DomainNoTokenizer.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct IdfSumDomainNameNoTokenizer;
impl Signal for IdfSumDomainNameNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.0002
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(
            schema::text_field::DomainNameNoTokenizer.into(),
        ))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct IdfSumDomainIfHomepage;
impl Signal for IdfSumDomainIfHomepage {
    fn default_coefficient(&self) -> f64 {
        0.0004
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::DomainIfHomepage.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct IdfSumDomainNameIfHomepageNoTokenizer;
impl Signal for IdfSumDomainNameIfHomepageNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.0036
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(
            schema::text_field::DomainNameIfHomepageNoTokenizer.into(),
        ))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct IdfSumDomainIfHomepageNoTokenizer;
impl Signal for IdfSumDomainIfHomepageNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.0036
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(
            schema::text_field::DomainIfHomepageNoTokenizer.into(),
        ))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct IdfSumTitleIfHomepage;
impl Signal for IdfSumTitleIfHomepage {
    fn default_coefficient(&self) -> f64 {
        0.00022
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::TitleIfHomepage.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> Option<f64> {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| idf_sum(field, doc))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct CrossEncoderSnippet;
impl Signal for CrossEncoderSnippet {
    fn default_coefficient(&self) -> f64 {
        0.17
    }

    fn as_field(&self) -> Option<Field> {
        None
    }

    fn compute(&self, _: DocId, _: &SignalComputer) -> Option<f64> {
        None // computed in later ranking stage
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct CrossEncoderTitle;
impl Signal for CrossEncoderTitle {
    fn default_coefficient(&self) -> f64 {
        0.17
    }

    fn as_field(&self) -> Option<Field> {
        None
    }

    fn compute(&self, _: DocId, _: &SignalComputer) -> Option<f64> {
        None // computed in later ranking stage
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct TitleEmbeddingSimilarity;
impl Signal for TitleEmbeddingSimilarity {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::TitleEmbeddings.into()))
    }

    fn compute(&self, _: DocId, _: &SignalComputer) -> Option<f64> {
        None // computed in later ranking stage
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct KeywordEmbeddingSimilarity;
impl Signal for KeywordEmbeddingSimilarity {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Fast(schema::fast_field::KeywordEmbeddings.into()))
    }

    fn compute(&self, _: DocId, _: &SignalComputer) -> Option<f64> {
        None // computed in later ranking stage
    }
}
