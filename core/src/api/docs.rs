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

use super::{alice, autosuggest, fact_check, search, summarize, webgraph};
use axum::Router;
use utoipa::{Modify, OpenApi};
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
        paths(
            search::api,
            webgraph::similar_sites,
            webgraph::knows_site,
            autosuggest::route,
            summarize::summarize_route,
            fact_check::fact_check_route,
            alice::alice_route,
        ),
        components(
            schemas(
                crate::webpage::region::Region,
                optics::SiteRankings,
                search::ApiSearchQuery,
                search::ApiSearchResult,
                crate::searcher::WebsitesResult,
                crate::search_prettifier::HighlightedSpellCorrection,
                crate::search_prettifier::DisplayedWebpage,
                crate::search_prettifier::DisplayedEntity,
                crate::search_prettifier::DisplayedAnswer,
                crate::search_prettifier::Sidebar,
                crate::search_prettifier::Snippet,
                crate::search_prettifier::StackOverflowAnswer,
                crate::search_prettifier::StackOverflowQuestion,
                crate::search_prettifier::CodeOrText,

                crate::bangs::UrlWrapper,

                crate::widgets::Widget,
                crate::widgets::calculator::Calculation,
                crate::widgets::calculator::Expr,
                crate::ranking::signal::SignalScore,
                crate::bangs::BangHit,
                crate::bangs::Bang,

                webgraph::SimilarSitesParams,
                webgraph::KnowsSite,
                crate::entrypoint::webgraph_server::ScoredSite,

                autosuggest::Suggestion,
                fact_check::FactCheckParams,
                fact_check::FactCheckResponse,

                crate::alice::SimplifiedWebsite,
                crate::alice::ExecutionState,
                crate::alice::EncodedEncryptedState,
                alice::EncodedSavedState,
            ),
        ),
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
        .config(
            utoipa_swagger_ui::Config::default()
                .use_base_layout()
                .default_models_expand_depth(0),
        )
}
