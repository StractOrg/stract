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

use enum_dispatch::enum_dispatch;
use strum::{EnumDiscriminants, VariantArray};
use tantivy::tokenizer::{BoxTokenStream, Tokenizer as _};

#[enum_dispatch]
pub trait Tokenizer: Into<TokenizerEnum> + Clone {
    fn name(&self) -> &'static str;
    fn token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a>;

    fn into_tantivy(self) -> TantivyBridge {
        TantivyBridge::from(self.into())
    }
}

#[derive(Clone, Default)]
pub struct SimpleTokenizer(tantivy::tokenizer::SimpleTokenizer);

impl Tokenizer for SimpleTokenizer {
    fn name(&self) -> &'static str {
        "simple"
    }

    fn token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a> {
        BoxTokenStream::new(self.0.token_stream(text))
    }
}

#[enum_dispatch(Tokenizer)]
#[derive(Clone, EnumDiscriminants)]
#[strum_discriminants(derive(VariantArray))]
pub enum TokenizerEnum {
    SimpleTokenizer,
}

impl TokenizerEnum {
    pub fn iter() -> impl Iterator<Item = Self> {
        TokenizerEnumDiscriminants::VARIANTS
            .iter()
            .map(|d| (*d).into())
    }
}

impl Default for TokenizerEnum {
    fn default() -> Self {
        TokenizerEnum::SimpleTokenizer(SimpleTokenizer::default())
    }
}

impl From<TokenizerEnumDiscriminants> for TokenizerEnum {
    fn from(value: TokenizerEnumDiscriminants) -> Self {
        match value {
            TokenizerEnumDiscriminants::SimpleTokenizer => SimpleTokenizer::default().into(),
        }
    }
}

#[derive(Clone)]
pub struct TantivyBridge(TokenizerEnum);

impl<T> From<T> for TantivyBridge
where
    T: Tokenizer + Sync + Send + 'static,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl tantivy::tokenizer::Tokenizer for TantivyBridge {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.0.token_stream(text)
    }
}
