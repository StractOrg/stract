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
    Node, NodeID,
};

#[derive(Debug, Clone, Copy, bincode::Encode, bincode::Decode)]
pub struct SmallEdge {
    pub from: NodeID,
    pub to: NodeID,
    pub rel_flags: RelFlags,
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct SmallEdgeWithLabel {
    pub from: NodeID,
    pub to: NodeID,
    pub rel_flags: RelFlags,
    pub label: String,
}

#[derive(
    Debug,
    Clone,
    bincode::Encode,
    bincode::Decode,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum PrettyRelFlag {
    Alternate,
    Author,
    Canonical,
    Help,
    Icon,
    License,
    Me,
    Next,
    NoFollow,
    Prev,
    PrivacyPolicy,
    Search,
    Stylesheet,
    Tag,
    TermsOfService,
    Sponsored,
    IsInFooter,
    IsInNavigation,
    LinkTag,
    ScriptTag,
    MetaTag,
    SameIcannDomain,
}

impl From<RelFlags> for Option<PrettyRelFlag> {
    fn from(flags: RelFlags) -> Self {
        if flags.contains(RelFlags::ALTERNATE) {
            Some(PrettyRelFlag::Alternate)
        } else if flags.contains(RelFlags::AUTHOR) {
            Some(PrettyRelFlag::Author)
        } else if flags.contains(RelFlags::CANONICAL) {
            Some(PrettyRelFlag::Canonical)
        } else if flags.contains(RelFlags::HELP) {
            Some(PrettyRelFlag::Help)
        } else if flags.contains(RelFlags::ICON) {
            Some(PrettyRelFlag::Icon)
        } else if flags.contains(RelFlags::LICENSE) {
            Some(PrettyRelFlag::License)
        } else if flags.contains(RelFlags::ME) {
            Some(PrettyRelFlag::Me)
        } else if flags.contains(RelFlags::NEXT) {
            Some(PrettyRelFlag::Next)
        } else if flags.contains(RelFlags::NOFOLLOW) {
            Some(PrettyRelFlag::NoFollow)
        } else if flags.contains(RelFlags::PREV) {
            Some(PrettyRelFlag::Prev)
        } else if flags.contains(RelFlags::PRIVACY_POLICY) {
            Some(PrettyRelFlag::PrivacyPolicy)
        } else if flags.contains(RelFlags::SEARCH) {
            Some(PrettyRelFlag::Search)
        } else if flags.contains(RelFlags::STYLESHEET) {
            Some(PrettyRelFlag::Stylesheet)
        } else if flags.contains(RelFlags::TAG) {
            Some(PrettyRelFlag::Tag)
        } else if flags.contains(RelFlags::TERMS_OF_SERVICE) {
            Some(PrettyRelFlag::TermsOfService)
        } else if flags.contains(RelFlags::SPONSORED) {
            Some(PrettyRelFlag::Sponsored)
        } else if flags.contains(RelFlags::IS_IN_FOOTER) {
            Some(PrettyRelFlag::IsInFooter)
        } else if flags.contains(RelFlags::IS_IN_NAVIGATION) {
            Some(PrettyRelFlag::IsInNavigation)
        } else if flags.contains(RelFlags::LINK_TAG) {
            Some(PrettyRelFlag::LinkTag)
        } else if flags.contains(RelFlags::SCRIPT_TAG) {
            Some(PrettyRelFlag::ScriptTag)
        } else if flags.contains(RelFlags::META_TAG) {
            Some(PrettyRelFlag::MetaTag)
        } else if flags.contains(RelFlags::SAME_ICANN_DOMAIN) {
            Some(PrettyRelFlag::SameIcannDomain)
        } else {
            None
        }
    }
}

impl From<RelFlags> for Vec<PrettyRelFlag> {
    fn from(flags: RelFlags) -> Self {
        flags
            .iter_names()
            .flat_map(|(_, flag)| <Option<PrettyRelFlag>>::from(flag))
            .collect()
    }
}

#[derive(
    Debug,
    Clone,
    bincode::Encode,
    bincode::Decode,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
pub struct PrettyEdge {
    pub from: String,
    pub to: String,
    pub rel_flags: Vec<PrettyRelFlag>,
    pub label: String,
}

impl From<Edge> for PrettyEdge {
    fn from(edge: Edge) -> Self {
        Self {
            from: edge.from.as_str().to_string(),
            to: edge.to.as_str().to_string(),
            rel_flags: edge.rel_flags.into(),
            label: edge.label,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct Edge {
    pub from: Node,
    pub to: Node,
    pub rel_flags: RelFlags,
    pub label: String,
    pub sort_score: f64,
}

impl Edge {
    pub fn empty() -> Self {
        Self {
            from: Node::empty(),
            to: Node::empty(),
            rel_flags: RelFlags::default(),
            label: String::default(),
            sort_score: 0.0,
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
    F64(f64),
}

impl<'a> tantivy::schema::Value<'a> for ReferenceValue<'a> {
    type ArrayIter = std::iter::Empty<Self>;
    type ObjectIter = std::iter::Empty<(&'a str, Self)>;

    fn as_value(&self) -> tantivy::schema::document::ReferenceValue<'a, Self> {
        match self {
            ReferenceValue::Str(s) => tantivy::schema::document::ReferenceValue::Leaf(
                tantivy::schema::document::ReferenceValueLeaf::Str(s),
            ),
            ReferenceValue::U64(u) => tantivy::schema::document::ReferenceValue::Leaf(
                tantivy::schema::document::ReferenceValueLeaf::U64(*u),
            ),
            ReferenceValue::F64(f) => tantivy::schema::document::ReferenceValue::Leaf(
                tantivy::schema::document::ReferenceValueLeaf::F64(*f),
            ),
        }
    }
}
