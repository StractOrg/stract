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

//! WASM Bindings for Client Side JavaScript
//!
//! To be packaged with wasm-pack + vite and served to the browser

use thiserror::Error;
use wasm_bindgen::prelude::*;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to serialize")]
    Serialization(#[from] serde_wasm_bindgen::Error),

    #[error("Optics error: {0}")]
    OpticParse(#[from] optics::Error),

    #[error("Json serialization error: {0}")]
    SerdeJson(#[from] serde_json::Error),
}

impl From<Error> for JsValue {
    fn from(val: Error) -> Self {
        JsValue::from_str(&format!("{val:?}"))
    }
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_log(s: &str);
}

/// WASM Bindings for Optics
///
/// Used to prevent recreation of the parsing methods in JS.
#[wasm_bindgen]
pub struct Optic;

#[wasm_bindgen]
impl Optic {
    /// Takes the contents of a .optic file and converts it to a Result containing either an error or a JSON serialized [`HostRankings`]
    #[wasm_bindgen(js_name = parsePreferenceOptic)]
    pub fn parse_preference_optic(contents: JsValue) -> Result<JsValue, Error> {
        let optic_contents: String = serde_wasm_bindgen::from_value(contents)?;
        let host_rankings = optics::Optic::parse(&optic_contents)?.host_rankings;

        let rankings_json = serde_json::to_string(&host_rankings)?;

        console_log(&("Parsed rankings to JSON: ".to_owned() + &rankings_json));

        Ok(serde_wasm_bindgen::to_value(&rankings_json)?)
    }
}
