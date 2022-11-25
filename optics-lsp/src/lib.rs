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

use std::collections::HashMap;

// use cuely::optics::ast::RawOptic;
use lsp_types::{
    notification::{DidChangeTextDocument, DidOpenTextDocument, DidSaveTextDocument, Notification},
    Hover, HoverContents, MarkedString, Url,
};
use thiserror::Error;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_log(s: &str);
}

fn log(s: &str) {
    console_log(&("[optics_lsp] ".to_owned() + s))
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to serialize")]
    Serialization(#[from] serde_wasm_bindgen::Error),
}

impl From<Error> for JsValue {
    fn from(val: Error) -> Self {
        JsValue::from_str(&format!("{:?}", val))
    }
}

struct File {
    source: String,
    // raw_optic: RawOptic,
}

#[wasm_bindgen]
pub struct OpticsBackend {
    diagnostic_callback: js_sys::Function,
    files: HashMap<Url, File>,
}

#[wasm_bindgen]
impl OpticsBackend {
    #[wasm_bindgen(constructor)]
    pub fn new(diagnostic_callback: &js_sys::Function) -> Self {
        console_error_panic_hook::set_once();

        Self {
            diagnostic_callback: diagnostic_callback.clone(),
            files: HashMap::new(),
        }
    }

    #[wasm_bindgen(js_name = onNotification)]
    pub fn on_notification(&mut self, method: &str, params: JsValue) {
        match method {
            DidOpenTextDocument::METHOD => log("OPEN"),
            DidChangeTextDocument::METHOD => log("CHANGE"),
            DidSaveTextDocument::METHOD => log("SAVE"),
            _ => log(&format!("on_notification {} {:?}", method, params)),
        }
    }

    #[wasm_bindgen(js_name = onHover)]
    pub fn on_hover(&mut self, params: JsValue) -> Result<JsValue, Error> {
        log(&format!("on_hover {:?}", params));

        Ok(serde_wasm_bindgen::to_value(&Hover {
            contents: HoverContents::Scalar(MarkedString::String("You're hovering!".to_string())),
            range: None,
        })?)
    }
}
