// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use lalrpop_util::lalrpop_mod;

lalrpop_mod!(pub parser, "/ranking/signal_aggregator/parser.rs");

pub static PARSER: once_cell::sync::Lazy<parser::AlterationsParser> =
    once_cell::sync::Lazy::new(parser::AlterationsParser::new);

#[derive(Debug, PartialEq, Eq)]
pub enum Target {
    Signal(String),
    Field(String),
}

#[derive(Debug, PartialEq)]
pub struct Alteration {
    pub target: Target,
    pub score: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let res: Vec<Alteration> = PARSER
            .parse(
                r#"
            @host_centrality = 3
            @bm25 = 100
            @field_url = 2
        "#,
            )
            .unwrap();

        assert_eq!(
            res,
            vec![
                Alteration {
                    target: Target::Signal("host_centrality".to_string()),
                    score: 3.0,
                },
                Alteration {
                    target: Target::Signal("bm25".to_string()),
                    score: 100.0
                },
                Alteration {
                    target: Target::Field("url".to_string()),
                    score: 2.0
                }
            ]
        )
    }
}
