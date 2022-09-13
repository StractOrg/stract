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

use std::{collections::HashMap, fs, path::Path};

use itertools::intersperse;
use serde::{Deserialize, Serialize};

use crate::{
    query::{parser::Term, Query},
    webpage::Url,
};

pub const BANG_PREFIX: char = '!';

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bang {
    #[serde(rename = "c")]
    category: Option<String>,

    #[serde(rename = "sc")]
    sub_category: Option<String>,

    #[serde(rename = "d")]
    domain: Option<String>,

    #[serde(rename = "r")]
    ranking: Option<usize>,

    #[serde(rename = "s")]
    site: Option<String>,

    #[serde(rename = "t")]
    tag: String,

    #[serde(rename = "u")]
    url: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct BangHit {
    bang: Bang,
    pub redirect_to: Url,
}

pub struct Bangs {
    bangs: HashMap<String, Bang>,
}

impl Bangs {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        let json = fs::read_to_string(path).unwrap();

        Self::from_json(json.as_str())
    }

    pub fn from_json(json: &str) -> Self {
        let all_bangs: Vec<Bang> = serde_json::from_str(json).unwrap();

        Self {
            bangs: all_bangs
                .into_iter()
                .map(|bang| (bang.tag.clone(), bang))
                .collect(),
        }
    }

    pub fn get(&self, query: &Query) -> Option<BangHit> {
        for possible_bang in query.terms().iter().filter_map(|term| {
            if let Term::PossibleBang(possible_bang) = term.as_ref() {
                Some(possible_bang)
            } else {
                None
            }
        }) {
            if let Some(bang) = self.bangs.get(possible_bang) {
                return Some(BangHit {
                    bang: bang.clone(),
                    redirect_to: bang
                        .url
                        .replace(
                            "{{{s}}}",
                            intersperse(
                                query
                                    .terms()
                                    .iter()
                                    .filter(|term| {
                                        if let Term::PossibleBang(bang) = term.as_ref() {
                                            bang != possible_bang
                                        } else {
                                            true
                                        }
                                    })
                                    .map(|term| term.as_ref().to_string()),
                                " ".to_string(),
                            )
                            .collect::<String>()
                            .as_str(),
                        )
                        .into(),
                });
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tantivy::tokenizer::TokenizerManager;

    use crate::{ranking::SignalAggregator, schema::create_schema};

    use super::*;

    #[test]
    fn simple_bang() {
        let schema = Arc::new(create_schema());
        let tokenizer_manager = TokenizerManager::new();

        let bangs = Bangs::from_json(
            r#"[{
            "c": "Multimedia",
            "d": "www.youtube.com",
            "r": 1646,
            "s": "Youtube",
            "sc": "Video",
            "t": "ty",
            "u": "https://www.youtube.com/results?search_query={{{s}}}"
        }]"#,
        );

        let query = Query::parse(
            "no bangs",
            schema.clone(),
            &tokenizer_manager,
            &SignalAggregator::default(),
        )
        .unwrap();
        assert_eq!(bangs.get(&query), None);

        let query = Query::parse(
            "!no bangs",
            schema.clone(),
            &tokenizer_manager,
            &SignalAggregator::default(),
        )
        .unwrap();
        assert_eq!(bangs.get(&query), None);

        let query = Query::parse(
            "!ty bangs",
            schema,
            &tokenizer_manager,
            &SignalAggregator::default(),
        )
        .unwrap();
        assert_eq!(
            bangs.get(&query),
            Some(BangHit {
                bang: Bang {
                    category: Some("Multimedia".to_string()),
                    sub_category: Some("Video".to_string()),
                    domain: Some("www.youtube.com".to_string()),
                    ranking: Some(1646),
                    site: Some("Youtube".to_string()),
                    tag: "ty".to_string(),
                    url: "https://www.youtube.com/results?search_query={{{s}}}".to_string()
                },
                redirect_to: "https://www.youtube.com/results?search_query=bangs"
                    .to_string()
                    .into()
            })
        );
    }
}
