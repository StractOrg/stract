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

use std::str::CharIndices;

use tantivy::tokenizer::BoxTokenStream;

use crate::{ceil_char_boundary, floor_char_boundary};

pub struct FlattenedJson {
    flattened_json: String,
    inner_tokenizer: JsonField,
}

struct IntermediateFlatValue {
    parent_keys: Vec<String>,
    val: serde_json::Value,
}

fn flatten(val: serde_json::Value) -> Vec<String> {
    let mut res = Vec::new();

    let mut stack = Vec::new();
    stack.push(IntermediateFlatValue {
        parent_keys: Vec::new(),
        val,
    });

    while let Some(elem) = stack.pop() {
        match elem.val {
            serde_json::Value::Null => {
                res.push(itertools::intersperse(elem.parent_keys, ".".to_string()).collect())
            }
            serde_json::Value::Bool(b) => {
                let key: String =
                    itertools::intersperse(elem.parent_keys, ".".to_string()).collect();
                res.push(format!("{key}=\"{b}\""))
            }
            serde_json::Value::Number(n) => {
                let key: String =
                    itertools::intersperse(elem.parent_keys, ".".to_string()).collect();
                res.push(format!("{key}=\"{n}\""))
            }
            serde_json::Value::String(s) => {
                let key: String =
                    itertools::intersperse(elem.parent_keys, ".".to_string()).collect();
                res.push(format!("{key}=\"{}\"", s.replace('"', "\\\"")))
            }
            serde_json::Value::Array(arr) => {
                for item in arr {
                    stack.push(IntermediateFlatValue {
                        parent_keys: elem.parent_keys.clone(),
                        val: item,
                    });
                }
            }
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    let mut parent_keys = elem.parent_keys.clone();
                    parent_keys.push(key);

                    stack.push(IntermediateFlatValue { parent_keys, val });
                }
            }
        }
    }

    res.reverse();

    res
}

impl FlattenedJson {
    pub fn new<T>(value: &T) -> crate::Result<Self>
    where
        T: serde::Serialize,
    {
        let json = serde_json::to_string(value)?;
        let val: serde_json::Value = serde_json::from_str(&json)?;

        let flattened_json = itertools::intersperse(flatten(val), "\n".to_string()).collect();

        Ok(Self {
            flattened_json,
            inner_tokenizer: JsonField,
        })
    }

    pub fn token_stream(&mut self) -> BoxTokenStream {
        tantivy::tokenizer::Tokenizer::token_stream(&mut self.inner_tokenizer, &self.flattened_json)
    }

    pub fn text(&self) -> &str {
        &self.flattened_json
    }
}

#[derive(Clone, Debug)]
pub struct JsonField;

impl JsonField {
    pub fn as_str() -> &'static str {
        "json_tokenizer"
    }
}

impl tantivy::tokenizer::Tokenizer for JsonField {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&mut self, text: &'a str) -> Self::TokenStream<'a> {
        BoxTokenStream::new(JsonFieldTokenStream {
            text,
            chars: text.char_indices(),
            token: tantivy::tokenizer::Token::default(),
        })
    }
}

pub struct JsonFieldTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: tantivy::tokenizer::Token,
}

impl JsonFieldTokenStream<'_> {
    // search for the end of the current token.
    fn search_token_end(&mut self, is_quote: bool) -> usize {
        let mut escaped = false;
        for (offset, c) in self.chars.by_ref() {
            if is_quote {
                if c == '\\' {
                    escaped = true;
                } else {
                    if c == '"' && !escaped {
                        return offset;
                    }

                    escaped = false;
                }
            } else if !c.is_alphanumeric() {
                return offset;
            }
        }

        self.text.len()
    }
}

impl tantivy::tokenizer::TokenStream for JsonFieldTokenStream<'_> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        let mut prev_was_quote = false;

        while let Some((offset_from, c)) = self.chars.next() {
            if !matches!(c, '.' | '\n' | '"') {
                let offset_to = self.search_token_end(prev_was_quote);
                self.token.offset_from = offset_from;
                self.token.offset_to = offset_to;

                if prev_was_quote {
                    self.token.offset_from -= 1;
                    self.token.offset_to += 1;

                    self.token.offset_from = floor_char_boundary(self.text, self.token.offset_from);
                    self.token.offset_to =
                        ceil_char_boundary(self.text, self.token.offset_to).min(self.text.len());
                }

                self.token
                    .text
                    .push_str(&self.text[self.token.offset_from..self.token.offset_to]);
                return true;
            }

            prev_was_quote = c == '"';
        }
        false
    }

    fn token(&self) -> &tantivy::tokenizer::Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut tantivy::tokenizer::Token {
        &mut self.token
    }
}

#[cfg(test)]
mod tests {
    use lending_iter::LendingIterator;
    use tantivy::tokenizer::Tokenizer;

    use super::*;

    fn tokenize_json(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut tokenizer = JsonField;
        let mut stream = tokenizer.token_stream(s);

        let mut it = tantivy::tokenizer::TokenStream::iter(&mut stream);
        while let Some(token) = it.next() {
            res.push(token.text.clone());
        }

        res
    }

    #[test]
    fn tokenize_json_field() {
        assert_eq!(
            tokenize_json(r#"Test.field="value""#),
            vec!["Test", "field", "\"value\"",]
        );
        assert_eq!(
            tokenize_json(r#"Test.field="this is the value""#),
            vec!["Test", "field", "\"this is the value\"",]
        );
        assert_eq!(
            tokenize_json(r#"Test.field="this is\" the value""#),
            vec!["Test", "field", "\"this is\\\" the value\"",]
        );
        assert_eq!(
            tokenize_json("Test.field=\"this*@# is\\\" the\\\" \nvalue\""),
            vec!["Test", "field", "\"this*@# is\\\" the\\\" \nvalue\"",]
        );
    }

    fn flattened_json_helper(json: &str, expected: &str) {
        let parsed: serde_json::Value = serde_json::from_str(json).unwrap();
        let flattened = FlattenedJson::new(&parsed).unwrap();
        let flat = flattened.text();

        assert_eq!(flat, expected);
    }

    #[test]
    fn flatten_json_object() {
        let json = r#"
        {
            "key1": "val1",
            "key2": "val2"
        }
        "#;
        let expected = r#"key1="val1"
key2="val2""#;

        flattened_json_helper(json, expected);

        let json = r#"
        {
            "key1": 1,
            "key2": 2
        }
        "#;
        let expected = r#"key1="1"
key2="2""#;

        flattened_json_helper(json, expected);

        let json = r#"
        {
            "key1": {
                "key2": "value1",
                "key3": "value2"
            }
        }
        "#;
        let expected = r#"key1.key2="value1"
key1.key3="value2""#;

        flattened_json_helper(json, expected);

        let json = r#"
        {
            "$key1": {
                "$key2": "value1",
                "key3": "value2"
            }
        }
        "#;
        let expected = r#"$key1.$key2="value1"
$key1.key3="value2""#;

        flattened_json_helper(json, expected);

        let json = r#"
        {
            "key1": [
                "value1",
                "value2"
            ]
        }
        "#;
        let expected = r#"key1="value1"
key1="value2""#;

        flattened_json_helper(json, expected);

        let json = r#"
        {
            "key1": [
                "value1",
                {
                    "key2": "value2",
                    "key3": 123
                }
            ]
        }
        "#;
        let expected = r#"key1="value1"
key1.key2="value2"
key1.key3="123""#;

        flattened_json_helper(json, expected);

        let json = r#"
        {
            "key1": [
                "value1",
                {
                    "key2": "this\" is @ a # test"
                }
            ]
        }
        "#;
        let expected = r#"key1="value1"
key1.key2="this\" is @ a # test""#;

        flattened_json_helper(json, expected);
    }

    #[test]
    fn out_of_bounds_crash() {
        tokenize_json(
            r#"
Breadcrumb.title="Home"
Breadcrumb.url="https://www.eurotecnicaservice.it/?lang=en"
Breadcrumb.title="Fuser Pur"
Breadcrumb.url="https://www.eurotecnicaservice.it/testing\"
"#,
        );
    }
}
