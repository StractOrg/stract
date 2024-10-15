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

use strum::VariantArray;
use tantivy::{
    schema::{document::DocumentDeserialize, OwnedValue},
    Document,
};

use crate::webpage::html::links::RelFlags;

use super::{
    schema::{Field, FieldEnum, FieldEnumDiscriminants},
    Node,
};

pub struct Edge {
    pub from: Node,
    pub to: Node,
    pub rel_flags: RelFlags,
    pub label: String,
}

impl Edge {
    pub fn empty() -> Self {
        Self {
            from: Node::empty(),
            to: Node::empty(),
            rel_flags: RelFlags::default(),
            label: String::default(),
        }
    }
}

impl Document for Edge {
    type Value<'a> = ReferenceValue<'a>;

    type FieldsValuesIter<'a> = FieldsIter<'a>;

    fn iter_fields_and_values(&self) -> Self::FieldsValuesIter<'_> {
        FieldsIter::new(self)
    }
}

impl DocumentDeserialize for Edge {
    fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Self, tantivy::schema::document::DeserializeError>
    where
        D: tantivy::schema::document::DocumentDeserializer<'de>,
    {
        let mut deserializer = deserializer;
        let mut edge = Edge::empty();

        while let Some((field, value)) = deserializer.next_field::<OwnedValue>()? {
            let field =
                FieldEnum::from(FieldEnumDiscriminants::VARIANTS[field.field_id() as usize]);
            field
                .set_value(&mut edge, value)
                .map_err(|e| tantivy::schema::document::DeserializeError::custom(e.to_string()))?;
        }

        Ok(edge)
    }
}

pub struct FieldsIter<'a> {
    edge: &'a Edge,
    index: usize,
}

impl<'a> FieldsIter<'a> {
    pub fn new(edge: &'a Edge) -> Self {
        Self { edge, index: 0 }
    }
}

impl<'a> Iterator for FieldsIter<'a> {
    type Item = (tantivy::schema::Field, ReferenceValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        FieldEnumDiscriminants::VARIANTS
            .get(self.index)
            .map(|field| {
                let field = FieldEnum::from(*field);
                let tv_field = tantivy::schema::Field::from_field_id(self.index as u32);

                let value = field.document_value(self.edge);
                self.index += 1;

                (tv_field, value)
            })
    }
}

/// A enum representing a value for tantivy to index.
#[derive(Clone, Debug, PartialEq)]
pub enum ReferenceValue<'a> {
    Str(&'a str),
    U64(u64),
    I64(i64),
    F64(f64),
    Date(tantivy::DateTime),
    Bytes(&'a [u8]),
    Bool(bool),
}

impl<'a> tantivy::schema::Value<'a> for ReferenceValue<'a> {
    type ArrayIter = std::iter::Empty<Self>;
    type ObjectIter = std::iter::Empty<(&'a str, Self)>;

    fn as_value(&self) -> tantivy::schema::document::ReferenceValue<'a, Self> {
        match self {
            ReferenceValue::Str(s) => tantivy::schema::document::ReferenceValue::Leaf(
                tantivy::schema::document::ReferenceValueLeaf::Str(*s),
            ),
            ReferenceValue::U64(u) => tantivy::schema::document::ReferenceValue::Leaf(
                tantivy::schema::document::ReferenceValueLeaf::U64(*u),
            ),
            ReferenceValue::I64(i) => tantivy::schema::document::ReferenceValue::Leaf(
                tantivy::schema::document::ReferenceValueLeaf::I64(*i),
            ),
            ReferenceValue::F64(f) => tantivy::schema::document::ReferenceValue::Leaf(
                tantivy::schema::document::ReferenceValueLeaf::F64(*f),
            ),
            ReferenceValue::Date(d) => tantivy::schema::document::ReferenceValue::Leaf(
                tantivy::schema::document::ReferenceValueLeaf::Date(*d),
            ),
            ReferenceValue::Bytes(b) => tantivy::schema::document::ReferenceValue::Leaf(
                tantivy::schema::document::ReferenceValueLeaf::Bytes(*b),
            ),
            ReferenceValue::Bool(b) => tantivy::schema::document::ReferenceValue::Leaf(
                tantivy::schema::document::ReferenceValueLeaf::Bool(*b),
            ),
        }
    }
}
