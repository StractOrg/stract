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

use tantivy::tokenizer::BoxTokenStream;

use crate::tokenizer::{self, normalizer, split_with_range::SplitWithRange, Normalize};

#[derive(Clone)]
pub struct PredTokenizer<P>(pub P)
where
    P: Fn(char) -> bool + Send + Sync + Clone + 'static;

impl<P> tantivy::tokenizer::Tokenizer for PredTokenizer<P>
where
    P: Fn(char) -> bool + Send + Sync + Clone + 'static,
{
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let stream = Box::new(
            text.split_with_range(|c| self.0(c))
                .map(|(s, range)| tokenizer::Token::new(s, range))
                .normalize(&normalizer::Lowercase)
                .normalize(&normalizer::UnicodeNFKD)
                .normalize(&normalizer::UnicodeDiacritics),
        );

        BoxTokenStream::new(PredTokenStream::new_boxed(stream))
    }
}

pub struct PredTokenStream<'a> {
    stream: Box<dyn Iterator<Item = tokenizer::Token<'a>> + 'a>,
    token: Option<tantivy::tokenizer::Token>,
    next_position: usize,
}

impl tantivy::tokenizer::TokenStream for PredTokenStream<'_> {
    fn advance(&mut self) -> bool {
        self.token = self.stream.next().map(|token| {
            let span = token.span();
            let pos = self.next_position;
            self.next_position += 1;
            tantivy::tokenizer::Token {
                offset_from: span.start,
                offset_to: span.end,
                position: pos,
                text: token.text().to_string(),
                ..Default::default()
            }
        });

        self.token.is_some()
    }

    fn token(&self) -> &tantivy::tokenizer::Token {
        self.token.as_ref().unwrap()
    }

    fn token_mut(&mut self) -> &mut tantivy::tokenizer::Token {
        self.token.as_mut().unwrap()
    }
}

impl<'a> PredTokenStream<'a> {
    fn new_boxed(
        stream: Box<dyn Iterator<Item = tokenizer::Token<'a>> + 'a>,
    ) -> BoxTokenStream<'a> {
        BoxTokenStream::new(Self {
            stream,
            token: None,
            next_position: 0,
        })
    }
}
