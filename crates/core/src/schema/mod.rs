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

pub mod numerical_field;
pub mod text_field;

use tantivy::schema::{BytesOptions, DateOptions, NumericOptions, TextOptions};

pub use numerical_field::{DataType, NumericalFieldEnum};
pub use text_field::TextFieldEnum;

use self::{numerical_field::NumericalField, text_field::TextField};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Field {
    Numerical(NumericalFieldEnum),
    Text(TextFieldEnum),
}

impl Field {
    #[inline]
    pub fn get(field_id: usize) -> Option<Field> {
        if field_id < TextFieldEnum::num_variants() {
            return Some(Field::Text(TextFieldEnum::get(field_id).unwrap()));
        }
        let field_id = field_id - TextFieldEnum::num_variants();

        if field_id < NumericalFieldEnum::num_variants() {
            return Some(Field::Numerical(NumericalFieldEnum::get(field_id).unwrap()));
        }
        let _field_id = field_id - NumericalFieldEnum::num_variants();

        None
    }

    #[inline]
    pub fn all() -> impl Iterator<Item = Field> {
        TextFieldEnum::all()
            .map(Field::Text)
            .chain(NumericalFieldEnum::all().map(Field::Numerical))
    }

    pub fn has_pos(&self) -> bool {
        match self {
            Field::Numerical(_) => false,
            Field::Text(text) => text.has_pos(),
        }
    }

    pub fn indexing_option(&self) -> IndexingOption {
        match self {
            Field::Text(f) => f.indexing_option(),
            Field::Numerical(f) => f.indexing_option(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Field::Text(f) => f.name(),
            Field::Numerical(f) => f.name(),
        }
    }

    pub fn is_searchable(&self) -> bool {
        match self {
            Field::Text(f) => f.is_searchable(),
            Field::Numerical(_) => false,
        }
    }

    pub fn as_text(&self) -> Option<TextFieldEnum> {
        match self {
            Field::Numerical(_) => None,
            Field::Text(field) => Some(*field),
        }
    }

    pub fn as_numerical(&self) -> Option<NumericalFieldEnum> {
        match self {
            Field::Numerical(field) => Some(*field),
            Field::Text(_) => None,
        }
    }
}

pub fn create_schema() -> tantivy::schema::Schema {
    let mut builder = tantivy::schema::Schema::builder();

    for field in Field::all() {
        match field.indexing_option() {
            IndexingOption::Text(options) => builder.add_text_field(field.name(), options),
            IndexingOption::Integer(options) => {
                let fast = field.as_numerical().expect("Expected fast field");
                match fast.data_type() {
                    DataType::U64 => builder.add_u64_field(field.name(), options),
                    DataType::U128 => builder.add_u128_field(field.name(), options),
                    DataType::F64 => builder.add_f64_field(field.name(), options),
                    DataType::Bool => builder.add_bool_field(field.name(), options),
                    DataType::Bytes => {
                        panic!("bytes field should have a `Bytes` variant as indexing option")
                    }
                }
            }
            IndexingOption::DateTime(options) => builder.add_date_field(field.name(), options),
            IndexingOption::Bytes(options) => builder.add_bytes_field(field.name(), options),
        };
    }

    builder.build()
}

pub enum IndexingOption {
    Text(TextOptions),
    Integer(NumericOptions),
    DateTime(DateOptions),
    Bytes(BytesOptions),
}
