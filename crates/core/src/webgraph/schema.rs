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

use enum_dispatch::enum_dispatch;
use strum::{EnumDiscriminants, VariantArray};
use tantivy::schema::FieldType;
use tantivy::schema::NumericOptions;
use tantivy::schema::OwnedValue;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::schema::Value;

use crate::enum_dispatch_from_discriminant;
use crate::Result;

use super::document::{Edge, ReferenceValue};
use super::tokenizer::Tokenizer;
use super::tokenizer::TokenizerEnum;
use super::Node;

pub fn create_schema() -> tantivy::schema::Schema {
    let mut schema_builder = tantivy::schema::Schema::builder();

    for field in FieldEnum::iter() {
        schema_builder.add_field(tantivy::schema::FieldEntry::new(
            field.name().to_string(),
            field.field_type(),
        ));
    }

    schema_builder.build()
}

#[enum_dispatch]
pub trait Field:
    Into<FieldEnum> + Clone + Copy + std::fmt::Debug + bincode::Encode + bincode::Decode
{
    fn name(&self) -> &'static str;
    fn document_value<'a>(&self, edge: &'a Edge) -> ReferenceValue<'a>;
    fn set_value(&self, edge: &mut Edge, value: OwnedValue) -> Result<()>;
    fn field_type(&self) -> tantivy::schema::FieldType;
    fn tokenizer(&self) -> TokenizerEnum {
        TokenizerEnum::default()
    }
}

#[derive(Clone, Copy, Debug, bincode::Encode, bincode::Decode)]
pub struct FromUrl;
impl Field for FromUrl {
    fn name(&self) -> &'static str {
        "from_url"
    }

    fn document_value<'a>(&self, edge: &'a Edge) -> ReferenceValue<'a> {
        ReferenceValue::Str(edge.from.as_str())
    }

    fn set_value(&self, edge: &mut Edge, value: OwnedValue) -> Result<()> {
        let url = value
            .as_ref()
            .as_str()
            .ok_or(anyhow::anyhow!("Invalid URL"))?;
        edge.from = Node::from_str_not_validated(url);
        Ok(())
    }

    fn field_type(&self) -> FieldType {
        FieldType::Str(
            TextOptions::default().set_stored().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(self.tokenizer().name())
                    .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions),
            ),
        )
    }
}

#[derive(Clone, Copy, Debug, bincode::Encode, bincode::Decode)]
pub struct ToUrl;
impl Field for ToUrl {
    fn name(&self) -> &'static str {
        "to_url"
    }

    fn document_value<'a>(&self, edge: &'a Edge) -> ReferenceValue<'a> {
        ReferenceValue::Str(edge.to.as_str())
    }

    fn set_value(&self, edge: &mut Edge, value: OwnedValue) -> Result<()> {
        let url = value
            .as_ref()
            .as_str()
            .ok_or(anyhow::anyhow!("Invalid URL"))?;
        edge.to = Node::from_str_not_validated(url);
        Ok(())
    }

    fn field_type(&self) -> FieldType {
        FieldType::Str(
            TextOptions::default().set_stored().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(self.tokenizer().name())
                    .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions),
            ),
        )
    }
}

#[derive(Clone, Copy, Debug, bincode::Encode, bincode::Decode)]
pub struct FromId;
impl Field for FromId {
    fn name(&self) -> &'static str {
        "from_id"
    }

    fn document_value<'a>(&self, edge: &'a Edge) -> ReferenceValue<'a> {
        ReferenceValue::U64(edge.from.id().as_u64())
    }

    fn set_value(&self, _: &mut Edge, _: OwnedValue) -> Result<()> {
        Ok(())
    }

    fn field_type(&self) -> FieldType {
        FieldType::U64(
            NumericOptions::default()
                .set_indexed()
                .set_stored()
                .set_columnar(),
        )
    }
}

#[derive(Clone, Copy, Debug, bincode::Encode, bincode::Decode)]
pub struct ToId;
impl Field for ToId {
    fn name(&self) -> &'static str {
        "to_id"
    }

    fn document_value<'a>(&self, edge: &'a Edge) -> ReferenceValue<'a> {
        ReferenceValue::U64(edge.to.id().as_u64())
    }

    fn set_value(&self, _: &mut Edge, _: OwnedValue) -> Result<()> {
        Ok(())
    }

    fn field_type(&self) -> FieldType {
        FieldType::U64(
            NumericOptions::default()
                .set_indexed()
                .set_stored()
                .set_columnar(),
        )
    }
}

#[derive(Clone, Copy, Debug, bincode::Encode, bincode::Decode)]
pub struct FromHostId;
impl Field for FromHostId {
    fn name(&self) -> &'static str {
        "from_host_id"
    }

    fn document_value<'a>(&self, edge: &'a Edge) -> ReferenceValue<'a> {
        ReferenceValue::U64(edge.from.clone().into_host().id().as_u64())
    }

    fn set_value(&self, _: &mut Edge, _: OwnedValue) -> Result<()> {
        Ok(())
    }

    fn field_type(&self) -> FieldType {
        FieldType::U64(
            NumericOptions::default()
                .set_indexed()
                .set_stored()
                .set_columnar(),
        )
    }
}

#[derive(Clone, Copy, Debug, bincode::Encode, bincode::Decode)]
pub struct ToHostId;
impl Field for ToHostId {
    fn name(&self) -> &'static str {
        "to_host_id"
    }

    fn document_value<'a>(&self, edge: &'a Edge) -> ReferenceValue<'a> {
        ReferenceValue::U64(edge.to.clone().into_host().id().as_u64())
    }

    fn set_value(&self, _: &mut Edge, _: OwnedValue) -> Result<()> {
        Ok(())
    }

    fn field_type(&self) -> FieldType {
        FieldType::U64(
            NumericOptions::default()
                .set_indexed()
                .set_stored()
                .set_columnar(),
        )
    }
}

#[derive(Clone, Copy, Debug, bincode::Encode, bincode::Decode)]
pub struct RelFlags;
impl Field for RelFlags {
    fn name(&self) -> &'static str {
        "rel_flags"
    }

    fn document_value<'a>(&self, edge: &'a Edge) -> ReferenceValue<'a> {
        ReferenceValue::U64(edge.rel_flags.as_u64())
    }

    fn set_value(&self, edge: &mut Edge, value: OwnedValue) -> Result<()> {
        edge.rel_flags = value
            .as_ref()
            .as_u64()
            .ok_or(anyhow::anyhow!("Rel flags should be a u64"))?
            .into();

        Ok(())
    }

    fn field_type(&self) -> FieldType {
        FieldType::U64(
            NumericOptions::default()
                .set_indexed()
                .set_stored()
                .set_columnar(),
        )
    }
}

#[derive(Clone, Copy, Debug, bincode::Encode, bincode::Decode)]
pub struct Label;
impl Field for Label {
    fn name(&self) -> &'static str {
        "label"
    }

    fn document_value<'a>(&self, edge: &'a Edge) -> ReferenceValue<'a> {
        ReferenceValue::Str(edge.label.as_str())
    }

    fn set_value(&self, edge: &mut Edge, value: OwnedValue) -> Result<()> {
        edge.label = value
            .as_ref()
            .as_str()
            .ok_or(anyhow::anyhow!("Invalid label"))?
            .to_string();
        Ok(())
    }

    fn field_type(&self) -> FieldType {
        FieldType::Str(
            TextOptions::default().set_stored().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(self.tokenizer().name())
                    .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions),
            ),
        )
    }
}

#[derive(Clone, Copy, Debug, bincode::Encode, bincode::Decode)]
pub struct SortScore;
impl Field for SortScore {
    fn name(&self) -> &'static str {
        "sort_score"
    }

    fn document_value<'a>(&self, edge: &'a Edge) -> ReferenceValue<'a> {
        ReferenceValue::F64(edge.sort_score)
    }

    fn set_value(&self, edge: &mut Edge, value: OwnedValue) -> Result<()> {
        let sort_score = value
            .as_ref()
            .as_f64()
            .ok_or(anyhow::anyhow!("Invalid sort score"))?;
        edge.sort_score = sort_score;

        Ok(())
    }

    fn field_type(&self) -> FieldType {
        FieldType::F64(
            NumericOptions::default()
                .set_indexed()
                .set_stored()
                .set_columnar(),
        )
    }
}

#[enum_dispatch(Field)]
#[derive(Clone, Copy, Debug, EnumDiscriminants, bincode::Encode, bincode::Decode)]
#[strum_discriminants(derive(VariantArray))]
pub enum FieldEnum {
    FromUrl,
    ToUrl,
    FromId,
    ToId,
    FromHostId,
    ToHostId,
    RelFlags,
    Label,
    SortScore,
}

impl FieldEnum {
    pub fn iter() -> impl Iterator<Item = FieldEnum> {
        FieldEnumDiscriminants::VARIANTS
            .iter()
            .copied()
            .map(FieldEnum::from)
    }
}

enum_dispatch_from_discriminant!(FieldEnumDiscriminants => FieldEnum,
[
  FromUrl,
  ToUrl,
  FromId,
  ToId,
  FromHostId,
  ToHostId,
  RelFlags,
  Label,
  SortScore,
]);

impl crate::enum_map::InsertEnumMapKey for FieldEnumDiscriminants {
    fn into_usize(self) -> usize {
        self as usize
    }
}
