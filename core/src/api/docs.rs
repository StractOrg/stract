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

use axum::Router;
use utoipa::{Modify, OpenApi};
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
        paths(),
        modifiers(&ApiModifier),
        tags(
            (name = "stract"),
        )
    )]
struct ApiDoc;

struct ApiModifier;

impl Modify for ApiModifier {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi.info.description = Some(
            r#"Stract is an open source web search engine. The API is totally free while in beta, but some endpoints will most likely be paid by consumption in the future.
The API might also change quite a bit during the beta period, but we will try to keep it as stable as possible. We look forward to see what you will build!"#.to_string(),
        );
    }
}

pub fn router<S: Clone + Send + Sync + 'static, B: axum::body::HttpBody + Send + Sync + 'static>(
) -> impl Into<Router<S, B>> {
    SwaggerUi::new("/beta/api/docs")
        .url("/beta/api/docs/openapi.json", ApiDoc::openapi())
        .config(utoipa_swagger_ui::Config::default().use_base_layout())
}
