use tantivy::tokenizer::BoxTokenStream;

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
#[derive(Clone, Default, Debug)]
pub struct Identity {}

impl Identity {
    pub fn as_str() -> &'static str {
        "identity_tokenizer"
    }
}
impl tantivy::tokenizer::Tokenizer for Identity {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&mut self, text: &'a str) -> Self::TokenStream<'a> {
        BoxTokenStream::new(IdentityTokenStream::from(text.to_string()))
    }
}
pub struct IdentityTokenStream {
    num_advances: usize,
    token: Option<tantivy::tokenizer::Token>,
}

impl From<String> for IdentityTokenStream {
    fn from(text: String) -> Self {
        Self {
            num_advances: 0,
            token: Some(tantivy::tokenizer::Token {
                offset_from: 0,
                offset_to: text.len(),
                position: 0,
                text,
                ..Default::default()
            }),
        }
    }
}
impl tantivy::tokenizer::TokenStream for IdentityTokenStream {
    fn advance(&mut self) -> bool {
        self.num_advances += 1;

        if self.num_advances == 1 {
            true
        } else {
            self.token = None;
            false
        }
    }

    fn token(&self) -> &tantivy::tokenizer::Token {
        self.token.as_ref().unwrap()
    }

    fn token_mut(&mut self) -> &mut tantivy::tokenizer::Token {
        self.token.as_mut().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lending_iter::LendingIterator;
    use tantivy::tokenizer::Tokenizer as _;

    fn tokenize_identity(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut tokenizer = Identity {};
        let mut stream = tokenizer.token_stream(s);
        let mut it = tantivy::tokenizer::TokenStream::iter(&mut stream);

        while let Some(token) = it.next() {
            res.push(token.text.clone());
        }

        res
    }

    #[test]
    fn identity() {
        assert_eq!(tokenize_identity("this is a test"), vec!["this is a test"]);
        assert_eq!(tokenize_identity("a-b"), vec!["a-b"]);
    }
}
