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
use tantivy::schema::OwnedValue;
use tantivy::schema::Value;

use crate::enum_dispatch_from_discriminant;
use crate::Result;

use super::document::{Edge, ReferenceValue};
use super::Node;

#[enum_dispatch]
pub trait Field:
    Into<FieldEnum> + Clone + Copy + std::fmt::Debug + bincode::Encode + bincode::Decode
{
    fn name(&self) -> &'static str;
    fn document_value<'a>(&self, edge: &'a Edge) -> ReferenceValue<'a>;
    fn set_value<'a>(&self, edge: &'a mut Edge, value: OwnedValue) -> Result<()>;
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

    fn set_value<'a>(&self, edge: &'a mut Edge, value: OwnedValue) -> Result<()> {
        let url = value
            .as_ref()
            .as_str()
            .ok_or(anyhow::anyhow!("Invalid URL"))?;
        edge.from = Node::from_str(url);
        Ok(())
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

    fn set_value<'a>(&self, edge: &'a mut Edge, value: OwnedValue) -> Result<()> {
        let url = value
            .as_ref()
            .as_str()
            .ok_or(anyhow::anyhow!("Invalid URL"))?;
        edge.to = Node::from_str(url);
        Ok(())
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

    fn set_value<'a>(&self, _: &'a mut Edge, _: OwnedValue) -> Result<()> {
        Ok(())
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

    fn set_value<'a>(&self, _: &'a mut Edge, _: OwnedValue) -> Result<()> {
        Ok(())
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

    fn set_value<'a>(&self, _: &'a mut Edge, _: OwnedValue) -> Result<()> {
        Ok(())
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

    fn set_value<'a>(&self, _: &'a mut Edge, _: OwnedValue) -> Result<()> {
        Ok(())
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

    fn set_value<'a>(&self, _: &'a mut Edge, _: OwnedValue) -> Result<()> {
        Ok(())
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

    fn set_value<'a>(&self, edge: &'a mut Edge, value: OwnedValue) -> Result<()> {
        edge.label = value
            .as_ref()
            .as_str()
            .ok_or(anyhow::anyhow!("Invalid label"))?
            .to_string();
        Ok(())
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
]);
