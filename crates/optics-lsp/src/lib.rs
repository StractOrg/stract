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

mod docs;

use std::collections::HashMap;

use itertools::Itertools;
use lsp_types::{
    notification::{DidChangeTextDocument, DidOpenTextDocument, Notification},
    Diagnostic, DiagnosticSeverity, DidChangeTextDocumentParams, DidOpenTextDocumentParams, Hover,
    HoverContents, HoverParams, LanguageString, MarkedString, Position, PublishDiagnosticsParams,
    Range, Url,
};
use optics::Optic;
use thiserror::Error;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_log(s: &str);
}

fn log(s: &str) {
    #[allow(unused_unsafe)]
    unsafe {
        console_log(&("[optics_lsp] ".to_owned() + s))
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to serialize")]
    Serialization(#[from] serde_wasm_bindgen::Error),
}

impl From<Error> for JsValue {
    fn from(val: Error) -> Self {
        JsValue::from_str(&format!("{val:?}"))
    }
}

#[derive(Debug)]
struct File {
    source: String,
    optic: Result<Optic, optics::Error>,
}
impl File {
    fn new(source: String) -> Self {
        File {
            optic: optics::parse(&source),
            source,
        }
    }

    fn error(&self) -> Option<optics::Error> {
        if let Err(err) = &self.optic {
            Some(err.clone())
        } else {
            None
        }
    }

    fn tokens(&self) -> impl Iterator<Item = (usize, optics::Token<'_>, usize)> {
        optics::lex(&self.source).filter_map(|elem| elem.ok())
    }

    fn token_at_offset(&self, offset: usize) -> Option<(usize, optics::Token<'_>, usize)> {
        self.tokens()
            .find(|(start, _, end)| offset >= *start && offset <= *end)
    }
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
            DidOpenTextDocument::METHOD => {
                let DidOpenTextDocumentParams { text_document } =
                    serde_wasm_bindgen::from_value(params).unwrap();

                self.handle_change(text_document.uri.clone(), text_document.text);
                self.send_diagnostics(text_document.uri);
            }
            DidChangeTextDocument::METHOD => {
                let params: DidChangeTextDocumentParams =
                    serde_wasm_bindgen::from_value(params).unwrap();

                self.handle_change(
                    params.text_document.uri.clone(),
                    params.content_changes[0].text.clone(),
                );
                self.send_diagnostics(params.text_document.uri);
            }
            _ => log(&format!("on_notification {method} {params:?}")),
        }
    }

    #[wasm_bindgen(js_name = onHover)]
    pub fn on_hover(&mut self, params: JsValue) -> Result<JsValue, Error> {
        log(&format!("on_hover {params:?}"));
        let params: HoverParams = serde_wasm_bindgen::from_value(params).unwrap();

        Ok(serde_wasm_bindgen::to_value(&self.handle_hover(params))?)
    }
}

impl OpticsBackend {
    fn handle_hover(&self, params: HoverParams) -> Option<Hover> {
        self.files
            .get(&params.text_document_position_params.text_document.uri)
            .and_then(|file| {
                position_to_byte_offset(
                    &params.text_document_position_params.position,
                    &file.source,
                )
                .and_then(|offset| {
                    file.token_at_offset(offset)
                        .and_then(|(start, token, end)| {
                            let msg = docs::token_docs(&token);
                            msg.map(|msg| Hover {
                                contents: HoverContents::Array(vec![
                                    MarkedString::LanguageString(LanguageString {
                                        language: "optic".to_string(),
                                        value: token.to_string(),
                                    }),
                                    MarkedString::String(msg.to_string()),
                                ]),
                                range: Some(Range {
                                    start: offset_to_pos(start, &file.source),
                                    end: offset_to_pos(end, &file.source),
                                }),
                            })
                        })
                })
            })
    }

    fn handle_change(&mut self, url: Url, source: String) {
        self.files.insert(url, File::new(source));
    }

    fn send_diagnostics(&self, url: Url) {
        if let Some(f) = self.files.get(&url) {
            self.send_diagnostic(url, f.error().map(|err| err_to_diagnostic(err, &f.source)));
        }
    }

    fn send_diagnostic(&self, url: Url, diagnostic: Option<Diagnostic>) {
        let this = &JsValue::null();

        let params = PublishDiagnosticsParams {
            uri: url,
            diagnostics: diagnostic
                .map(|diagnostic| vec![diagnostic])
                .unwrap_or_default(),
            version: None,
        };
        log(&format!("Sending diagnostic {params:?}"));

        let params = &serde_wasm_bindgen::to_value(&params).unwrap();
        if let Err(e) = self.diagnostic_callback.call1(this, params) {
            log(&format!(
                "send_diagnostics params:\n\t{params:?}\n\tJS error: {e:?}",
            ));
        }
    }
}

fn err_to_diagnostic(err: optics::Error, source: &str) -> Diagnostic {
    match err {
        optics::Error::UnexpectedEof { expected } => {
            let message = [
                "Unexpected EOF.".to_string(),
                "Expected one of the following tokens:".to_string(),
            ]
            .into_iter()
            .chain(expected.iter().map(|ex| format!(" - {ex}")))
            .join("\n");

            let eof_position = Position {
                line: source.lines().count() as u32,
                character: source.lines().last().unwrap_or_default().len() as u32,
            };

            Diagnostic {
                range: Range {
                    start: eof_position,
                    end: eof_position,
                },
                severity: Some(DiagnosticSeverity::ERROR),
                message,
                ..Default::default()
            }
        }
        optics::Error::UnexpectedToken {
            token: (start, tok, end),
            expected,
        } => {
            let message = [
                format!("Unexpected token \"{tok}\""),
                "Expected one of the following tokens:".to_string(),
            ]
            .into_iter()
            .chain(expected.iter().map(|ex| format!(" - {ex}")))
            .join("\n");

            Diagnostic {
                range: Range {
                    start: offset_to_pos(start, source),
                    end: offset_to_pos(end, source),
                },
                severity: Some(DiagnosticSeverity::ERROR),
                message,
                ..Default::default()
            }
        }
        optics::Error::UnrecognizedToken {
            token: (start, tok, end),
        } => {
            let message = format!("Unrecognized token \"{tok}\"");
            Diagnostic {
                range: Range {
                    start: offset_to_pos(start, source),
                    end: offset_to_pos(end, source),
                },
                severity: Some(DiagnosticSeverity::ERROR),
                message,
                ..Default::default()
            }
        }
        optics::Error::NumberParse {
            token: (start, tok, end),
        } => {
            let message = format!("Failed to parse token \"{tok}\" as a number");
            Diagnostic {
                range: Range {
                    start: offset_to_pos(start, source),
                    end: offset_to_pos(end, source),
                },
                severity: Some(DiagnosticSeverity::ERROR),
                message,
                ..Default::default()
            }
        }
        optics::Error::Unknown(start, end) => {
            let message = "We encountered an unknown error".to_string();
            Diagnostic {
                range: Range {
                    start: offset_to_pos(start, source),
                    end: offset_to_pos(end, source),
                },
                severity: Some(DiagnosticSeverity::ERROR),
                message,
                ..Default::default()
            }
        }
        optics::Error::Pattern => {
            let message = "One of your patterns are unsupported".to_string();
            Diagnostic {
                range: Range {
                    start: offset_to_pos(0, source),
                    end: offset_to_pos(source.len(), source),
                },
                severity: Some(DiagnosticSeverity::ERROR),
                message,
                ..Default::default()
            }
        }
        optics::Error::RankingStagesMismatch => {
            unreachable!("this error cannot occur at compile time")
        }
    }
}

fn offset_to_pos(offset: usize, src: &str) -> Position {
    if src[..offset].is_empty() {
        return Position::new(0, 0);
    }

    if src[..offset].ends_with('\n') {
        let l = src[..offset].lines().count();
        Position::new(l as _, 0)
    } else {
        let l = src[..offset].lines().count() - 1;
        let c = src[..offset].lines().last().unwrap_or_default().len();
        Position::new(l as _, c as _)
    }
}

fn position_to_byte_offset(pos: &Position, src: &str) -> Option<usize> {
    let mut lines = pos.line;
    let mut columns = pos.character;
    src.char_indices()
        .find(|&(_, c)| {
            if lines == 0 {
                if columns == 0 {
                    return true;
                } else {
                    columns -= 1
                }
            } else if c == '\n' {
                lines -= 1;
            }
            false
        })
        .map(|(idx, _)| idx)
}
