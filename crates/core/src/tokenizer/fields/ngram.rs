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

pub struct NGramTokenStream<'a, const N: usize> {
    inner: BoxTokenStream<'a>,
    token: tantivy::tokenizer::Token,
    token_window: [tantivy::tokenizer::Token; N],
    next_pos: usize,
}

impl<'a, const N: usize> NGramTokenStream<'a, N> {
    pub fn new(inner: BoxTokenStream<'a>) -> Self {
        Self {
            inner,
            token: tantivy::tokenizer::Token::default(),
            token_window: std::array::from_fn(|_| tantivy::tokenizer::Token::default()),
            next_pos: 0,
        }
    }
}

fn reuse_token_alloc(token: &mut tantivy::tokenizer::Token, new_token: &tantivy::tokenizer::Token) {
    token.text.clear();
    token.text += new_token.text.as_str();
    token.offset_from = new_token.offset_from;
    token.offset_to = new_token.offset_to;
    token.position = new_token.position;
    token.position_length = new_token.position_length;
}

impl<'a, const N: usize> tantivy::tokenizer::TokenStream for NGramTokenStream<'a, N> {
    fn advance(&mut self) -> bool {
        if !self.inner.advance() {
            return false;
        }

        self.token_window.rotate_left(1);
        reuse_token_alloc(&mut self.token_window[N - 1], self.inner.token());

        while self.token_window[0].text.is_empty() {
            if !self.inner.advance() {
                break;
            }

            self.token_window.rotate_left(1);
            reuse_token_alloc(&mut self.token_window[N - 1], self.inner.token());
        }

        self.next_pos += 1;

        let begin = self
            .token_window
            .iter()
            .position(|token| !token.text.is_empty())
            .unwrap_or(N - 1);

        self.token.position = self.next_pos;
        self.token.offset_from = self.token_window[begin].offset_from;
        self.token.offset_to = self.token_window[N - 1].offset_to;
        self.token.position_length = N - begin;

        self.token.text.clear();
        for token in &self.token_window {
            self.token.text += token.text.as_str();
        }

        true
    }

    fn token(&self) -> &tantivy::tokenizer::Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut tantivy::tokenizer::Token {
        &mut self.token
    }
}
