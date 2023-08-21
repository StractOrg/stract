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

use std::collections::HashMap;

use lsp_types::{
    notification::{DidChangeTextDocument, DidOpenTextDocument, Notification},
    Diagnostic, DiagnosticSeverity, DidChangeTextDocumentParams, DidOpenTextDocumentParams,
     Hover, HoverContents, HoverParams, MarkedString, Position,
    PublishDiagnosticsParams, Range, Url,
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
                            let msg = match token {
                                optics::Token::DiscardNonMatching => {
                                    Some("All results that does not match any of the rules in the optic will be discarded.".to_string())
                                }

                                optics::Token::Rule => Some("A rule specifies how a particular search result should be treated. \
                                It consists of a `Matches` block and an optional `Action`. Any search result that matches the `Matches` block \
                                will have the `Action` applied to it. The action can either `Boost`, `Downrank` or `Discard` a result. An empty `Action` is \
                                equivalent to a `Boost` of 0.".to_string()),

                                optics::Token::RankingPipeline => Some("The final ranking consists of multiple stages in a pipeline. Each stage receives the 
                                best scoring webpages from the stage before it, and uses more accurate yet computationally expensive algorithms to rank 
                                the pages for the next stage.".to_string()),

                                optics::Token::Stage => Some("The final ranking consists of multiple stages in a pipeline. Each stage has different fields and 
                                signals available to it.".to_string()),
                                
                                optics::Token::Matches => Some("`Matches` dictates the set of criteria a search result should match in order to have the action applied to it. \
                                A search result must match all the parts of the `Matches` block in order to match the specific rule.".to_string()),

                               optics::Token::Site => Some("`Site(\"...\")` matches any search result where the pattern defined in `\"...\"` matches the site of the result. \
                               Note that when `Site` is used inside `Like` or `Dislike`, the pattern can only contain simple terms (no `*` and `|`). \n\n\
                               When the site is used in a `Matches` block, you can use `*` as a wildcard term and `|` to indicate either the end or start of a string. \n\
                               Consider the pattern `\"|sub.*.com|\"`. This will ensure that the terms `sub` and `.` must appear at the beggining of the site, then followed by any \
                               domain-name that ends in `.` and `com`. Note that `|` can only be used in the beggining or end of a pattern and the pattern will only match full terms (no substring matching). \n\n\
                               This example illustrates the difference between `Domain`, `Site` and `Url`:\n\
                               Assume a search result has the url `https://sub.example.org/page`. the domain here is `example.org`, the site is `sub.example.org` and the url is the entire url (with protocol).\
                               ".to_string()),

                                optics::Token::Url => Some("`Url(\"...\")` matches any search result where the pattern defined in `\"...\"` matches the url of the result. \
                                You can use `*` as a wildcard term and `|` to indicate either the end or start of a url. \n\
                                Consider the pattern `\"https://sub.*.com|\"`. This will ensure that the terms `https`, `:`, `/`, `/`, `sub` and `.` must appear before any term that ends with `.` and `com` \
                                in the url. Note that `|` can only be used in the beggining or end of a pattern and the pattern will only match full terms (no substring matching). \n\n\
                                This example illustrates the difference between `Domain`, `Site` and `Url`:\n\
                                Assume a search result has the url `https://sub.example.org/page`. the domain here is `example.org`, the site is `sub.example.org` and the url is the entire url (with protocol).\
                                ".to_string()),

                                optics::Token::Domain => Some("`Domain(\"...\")` matches any search result where the pattern defined in `\"...\"` matches the domain of the result. \
                                You can use `*` as a wildcard term and `|` to indicate either the end or start of a domain. \n\
                                Consider the pattern `\"example.org\"`. This is equivalent to doing a phrase search for `\"example.org\"` in the domain. Note that the pattern will only match full terms (no substring matching). \n\n\
                                This example illustrates the difference between `Domain`, `Site` and `Url`:\n\
                                Assume a search result has the url `https://sub.example.org/page`. the domain here is `example.org`, the site is `sub.example.org` and the url is the entire url (with protocol).\
                                ".to_string()),

                                optics::Token::Title => Some("`Title(\"...\")` matches any search result where the pattern defined in `\"...\"` matches the title of the web page. \
                                You can use `*` as a wildcard term and `|` to indicate either the end or start of a title. \n\
                                Consider the pattern `\"|Best * ever\"`. This will match any result where the title starts with `Best` followed by any term(s) and then followed by the term `ever`. \
                                Note that the pattern will only match full terms (no substring matching) and the modifier `|` can only be used at the end or beggining of the pattern.".to_string()),

                                optics::Token::Description => Some("`Description(\"...\")` matches any search result where the pattern defined in `\"...\"` matches the description of the web page. \
                                You can use `*` as a wildcard term and `|` to indicate either the end or start of a description. \n\
                                Consider the pattern `\"|Best * ever\"`. This will match any result where the description starts with `Best` followed by any term(s) and then followed by the term `ever`. \
                                Note that the pattern will only match full terms (no substring matching) and the modifier `|` can only be used at the end or beggining of the pattern.".to_string()),

                                optics::Token::Content => Some("`Content(\"...\")` matches any search result where the pattern defined in `\"...\"` matches the content of the web page. \
                                The content of a webpage is all the text that is not part of navigational menues, footers etc. \n\
                                You can use `*` as a wildcard term and `|` to indicate either the end or start of the content. \n\
                                Consider the pattern `\"Best * ever\"`. This will match any result where the description starts with `Best` followed by any term(s) and then followed by the term `ever`. \
                                Note that the pattern will only match full terms (no substring matching) and the modifier `|` can only be used at the end or beggining of the pattern.".to_string()),

                                optics::Token::MicroformatTag => Some("`MicroformatTag(\"...\")` matches any search result that contains the microformat tag defined in `\"...\"`. \
                                This is useful when looking for indieweb pages.".to_string()),

                                optics::Token::Schema => Some("`Schema(\"...\")` matches any search result that contains the https://schema.org entity defined in `\"...\"`. \
                                As an example, `Schema(\"BlogPosting\")` matches all pages that contains the https://schema.org/BlogPosting entity. Note that `Schema` \
                                does not support the pattern syntax, but only simple strings.".to_string()),

                                optics::Token::Ranking => Some("When results are ranked we take a weighted sum of various signals to give each webpage a score for the specific query. \
                                The top scored results are then presented to the user. `Ranking` allows you to alter the weight of all the `Signal`s and text `Field`s.".to_string()),

                                optics::Token::Signal => Some("During ranking of the search results, a number of signals are combined in a weighted sum to create the final score for each search result. \
                                `Signal` allows you to change the coefficient used for each signal, and thereby alter the search result ranking. Some supported signals are e.g. \"host_centrality\", \"bm25\" and \"tracker_score\". \
                                A complete list of the available signals can be found in the code (https://github.com/StractOrg/Stract/blob/main/src/ranking/signal.rs)".to_string()),

                                optics::Token::Field => Some("`Field` lets you change how the various text fields are prioritized during ranking (e.g. a search result matching text in the title is probably more relevant than a result where only the body matches). \
                                Some supported fields are e.g. \"title\", \"body\", \"backlink_text\" and \"site\". \
                                A complete list of available fields can be seen in the code (https://github.com/StractOrg/Stract/blob/main/src/schema.rs)".to_string()),

                                optics::Token::Action => Some("`Action` defines which action should be applied to the matching search result. The result can either be boosted, downranked or discarded.".to_string()),

                                optics::Token::Boost => Some("`Boost(...)` boosts the search result by the number specified in `...`.".to_string()),

                                optics::Token::Downrank => Some("`Downrank(...)` downranks the search result by the number specified in `...`. A higher number further downranks the search result.".to_string()),

                                optics::Token::Discard => Some("`Discard` discards the matching search result completely from the results page.".to_string()),

                                optics::Token::Like => Some("`Like(Site(...))` lets you like specific sites. During ranking, we will calculate a centrality meassure from all you liked sites \
                                so results that are heavily linked to from your liked sites will be favored. Note therefore, that `Like` not only alters the ranking of the specifc site, \
                                but also sites that are heavily linked to from the liked site.".to_string()),

                                optics::Token::Dislike => Some("`Dislike(Site(...))` lets you dislike specifc sites. During ranking, we will calculate a centrality meassure from all you dislike sites \
                                so results that are heavily linked to from your disliked sites will be downranked. Note therefore, that `Dislike` not only alters the ranking of the specifc site, \
                                but also sites that are heavily linked to from the disliked site.".to_string()),


                                _ => None,
                            };

                            msg.map(|msg| Hover {
                                contents: HoverContents::Scalar(MarkedString::String(msg)),
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
        optics::Error::UnexpectedEOF { expected } => {
            let mut message = String::new();

            message.push_str("Unexpected EOF.\n");
            message.push_str("Expected one of the following tokens:");

            for ex in expected {
                message.push('\n');
                message.push_str(" - ");
                message.push_str(ex.as_str());
            }

            Diagnostic {
                range: Range {
                    start: Position {
                        line: source.lines().count() as u32,
                        character: source.lines().last().map(|l| l.len()).unwrap_or_default()
                            as u32,
                    },
                    end: Position {
                        line: source.lines().count() as u32,
                        character: source.lines().last().map(|l| l.len()).unwrap_or_default()
                            as u32,
                    },
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
            let mut message = String::new();
            message.push_str(&format!("Unexpected token \"{tok}\"\n"));
            message.push_str("Expected one of the following tokens:");

            for ex in expected {
                message.push('\n');
                message.push_str(" - ");
                message.push_str(ex.as_str());
            }

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
        },
        optics::Error::RankingStagesMismatch => unreachable!("this error cannot occur at compile time"),
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
        let c = src[..offset].lines().last().unwrap().len();
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
