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

use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::notification::PublishDiagnostics;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};
use wasm_bindgen::prelude::*;

#[derive(Debug)]
struct Backend {
    client: Client,
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, _: InitializeParams) -> Result<InitializeResult> {
        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                document_formatting_provider: Some(OneOf::Left(true)),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "Server initialized!")
            .await;
    }
    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        Ok(Some(Hover {
            contents: HoverContents::Scalar(MarkedString::String("You're hovering!".to_string())),
            range: None,
        }))
        // if let Some(msg) = self.find(
        //     params.text_document_position_params.position,
        //     &params.text_document_position_params.text_document.uri,
        // ) {
        //     Ok(Some(Hover {
        //         contents: HoverContents::Scalar(MarkedString::String(
        //             msg, // "You're hovering!".to_string(),
        //         )),
        //         range: None,
        //     }))
        // } else {
        //     Ok(None)
        // }
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        // self.handle_change(params.text_document.uri, params.text_document.text)
        //     .await
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        // self.handle_change(
        //     params.text_document.uri,
        //     params.content_changes[0].text.clone(),
        // )
        // .await
    }

    async fn formatting(&self, params: DocumentFormattingParams) -> Result<Option<Vec<TextEdit>>> {
        Ok(None)
        // let src = self.sources.get(&params.text_document.uri).unwrap();
        // let range = Range::new(
        //     byte_offset_to_position(&src, 0),
        //     byte_offset_to_position(&src, src.len()),
        // );

        // match macor_fmt::prettify(&src).unwrap() {
        //     Some(text) => Ok(Some(vec![TextEdit::new(range, text)])),
        //     _ => Ok(None),
        // }
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

#[wasm_bindgen]
pub async fn run() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(|client| Backend { client });
    Server::new(stdin, stdout, socket).serve(service).await;
}
