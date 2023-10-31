// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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

use std::{
    collections::HashMap,
    fs,
    ops::{Deref, DerefMut},
    path::Path,
};

use itertools::intersperse;
use serde::{Deserialize, Serialize};
use url::Url;
use utoipa::ToSchema;

use crate::query::parser::Term;

pub const BANG_PREFIX: char = '!';

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Bang {
    #[serde(rename = "c")]
    pub(crate) category: Option<String>,

    #[serde(rename = "sc")]
    pub(crate) sub_category: Option<String>,

    #[serde(rename = "d")]
    pub(crate) domain: Option<String>,

    #[serde(rename = "r")]
    pub(crate) ranking: Option<usize>,

    #[serde(rename = "s")]
    pub(crate) site: Option<String>,

    #[serde(rename = "t")]
    pub(crate) tag: String,

    #[serde(rename = "u")]
    pub(crate) url: String,
}

/// Wrapper around `Url` that implements `ToSchema` for `Url`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, ToSchema)]
#[schema(value_type = String, title = "Url")]
pub struct UrlWrapper(Url);

impl DerefMut for UrlWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deref for UrlWrapper {
    type Target = Url;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Url> for UrlWrapper {
    fn from(url: Url) -> Self {
        Self(url)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BangHit {
    pub bang: Bang,
    pub redirect_to: UrlWrapper,
}

pub struct Bangs {
    bangs: HashMap<String, Bang>,
}

impl Bangs {
    pub fn from_path(path: &Path) -> Self {
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

    pub fn get(&self, terms: &[Box<Term>]) -> Option<BangHit> {
        for possible_bang in terms.iter().filter_map(|term| {
            if let Term::PossibleBang(possible_bang) = term.as_ref() {
                Some(possible_bang)
            } else {
                None
            }
        }) {
            if let Some(bang) = self.bangs.get(possible_bang) {
                let query = intersperse(
                    terms
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
                .collect::<String>();

                let query = urlencoding::encode(query.as_str()).to_string();
                let mut url = bang.url.replace("{{{s}}}", query.as_str());

                if !url.contains("://") {
                    url = "http://".to_string() + url.as_str();
                }

                return Some(BangHit {
                    bang: bang.clone(),
                    redirect_to: Url::parse(url.as_str()).unwrap().into(),
                });
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {

    use crate::query::parser::parse;

    use super::*;

    #[test]
    fn simple_bang() {
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

        assert_eq!(bangs.get(&parse("no bangs")), None);
        assert_eq!(bangs.get(&parse("!no bangs")), None);

        assert_eq!(
            bangs.get(&parse("!ty bangs")),
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
                redirect_to: Url::parse("https://www.youtube.com/results?search_query=bangs")
                    .unwrap()
                    .into()
            })
        );
    }
}
