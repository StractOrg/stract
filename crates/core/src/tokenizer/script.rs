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

use super::script_tokenizer::ScriptTokenizer;

#[derive(Debug, PartialEq, Default, Clone, Copy)]
pub enum Script {
    Latin,

    #[default]
    Other,
}

impl From<char> for Script {
    fn from(c: char) -> Self {
        if c.is_ascii() {
            Script::Latin
        } else {
            Script::Other
        }
    }
}

impl Script {
    pub fn tokenizer(self) -> Box<dyn ScriptTokenizer> {
        match self {
            Script::Latin => Box::new(super::script_tokenizer::Latin),
            Script::Other => Box::new(super::script_tokenizer::Latin),
        }
    }
}
