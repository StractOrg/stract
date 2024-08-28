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

use tantivy::DocId;

use crate::ranking::{CoreSignal, SignalCalculation, SignalComputer};

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
pub struct Bm25F;
impl CoreSignal for Bm25F {
    fn default_coefficient(&self) -> f64 {
        0.1
    }

    fn as_field(&self) -> Option<Field> {
        None
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        SignalCalculation::new_symmetrical(
            seg_reader
                .text_fields_mut()
                .values_mut()
                .map(|field| field.bm25f(doc))
                .sum(),
        )
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
pub struct Bm25Title;
impl CoreSignal for Bm25Title {
    fn default_coefficient(&self) -> f64 {
        0.0063
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::Title.into()))
    }

    fn has_sibling_ngrams(&self) -> bool {
        true
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.bm25(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
pub struct TitleCoverage;
impl CoreSignal for TitleCoverage {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::Title.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.coverage(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for Bm25TitleBigrams {
    fn default_coefficient(&self) -> f64 {
        0.005
    }

    fn has_sibling_ngrams(&self) -> bool {
        true
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::TitleBigrams.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.bm25(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for Bm25TitleTrigrams {
    fn default_coefficient(&self) -> f64 {
        0.005
    }

    fn has_sibling_ngrams(&self) -> bool {
        true
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::TitleTrigrams.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.bm25(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for Bm25CleanBody {
    fn default_coefficient(&self) -> f64 {
        0.005
    }

    fn has_sibling_ngrams(&self) -> bool {
        true
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::CleanBody.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.bm25(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
pub struct CleanBodyCoverage;
impl CoreSignal for CleanBodyCoverage {
    fn default_coefficient(&self) -> f64 {
        0.01
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::CleanBody.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.coverage(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for Bm25CleanBodyBigrams {
    fn default_coefficient(&self) -> f64 {
        0.005
    }

    fn has_sibling_ngrams(&self) -> bool {
        true
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::CleanBodyBigrams.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.bm25(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for Bm25CleanBodyTrigrams {
    fn default_coefficient(&self) -> f64 {
        0.005
    }

    fn has_sibling_ngrams(&self) -> bool {
        true
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::CleanBodyTrigrams.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.bm25(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for Bm25StemmedTitle {
    fn default_coefficient(&self) -> f64 {
        0.003
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::StemmedTitle.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.bm25(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for Bm25StemmedCleanBody {
    fn default_coefficient(&self) -> f64 {
        0.001
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::StemmedCleanBody.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.bm25(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for Bm25AllBody {
    fn default_coefficient(&self) -> f64 {
        0.0
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::AllBody.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.bm25(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for Bm25Keywords {
    fn default_coefficient(&self) -> f64 {
        0.001
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::Keywords.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.bm25(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for Bm25BacklinkText {
    fn default_coefficient(&self) -> f64 {
        0.003
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::BacklinkText.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.bm25(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for IdfSumUrl {
    fn default_coefficient(&self) -> f64 {
        0.0006
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::Url.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.idf_sum(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for IdfSumSite {
    fn default_coefficient(&self) -> f64 {
        0.00015
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::SiteWithout.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.idf_sum(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for IdfSumDomain {
    fn default_coefficient(&self) -> f64 {
        0.0003
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::Domain.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.idf_sum(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for IdfSumSiteNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.00015
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::SiteNoTokenizer.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.idf_sum(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for IdfSumDomainNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.0036
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::DomainNoTokenizer.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.idf_sum(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for IdfSumDomainNameNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.0002
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(
            schema::text_field::DomainNameNoTokenizer.into(),
        ))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.idf_sum(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for IdfSumDomainIfHomepage {
    fn default_coefficient(&self) -> f64 {
        0.0004
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::DomainIfHomepage.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.idf_sum(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for IdfSumDomainNameIfHomepageNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.0036
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(
            schema::text_field::DomainNameIfHomepageNoTokenizer.into(),
        ))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.idf_sum(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for IdfSumDomainIfHomepageNoTokenizer {
    fn default_coefficient(&self) -> f64 {
        0.0036
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(
            schema::text_field::DomainIfHomepageNoTokenizer.into(),
        ))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.idf_sum(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
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
impl CoreSignal for IdfSumTitleIfHomepage {
    fn default_coefficient(&self) -> f64 {
        0.001
    }

    fn as_field(&self) -> Option<Field> {
        Some(Field::Text(schema::text_field::TitleIfHomepage.into()))
    }

    fn compute(&self, doc: DocId, signal_computer: &SignalComputer) -> SignalCalculation {
        let mut seg_reader = signal_computer.segment_reader().unwrap().borrow_mut();

        let val = seg_reader
            .text_fields_mut()
            .get_mut(self.as_textfield().unwrap())
            .map(|field| field.idf_sum(doc))
            .unwrap_or(0.0);

        SignalCalculation::new_symmetrical(val)
    }
}
