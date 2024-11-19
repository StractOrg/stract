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

use super::{autosuggest, explore, hosts, search, webgraph};
use axum::Router;
use utoipa::{Modify, OpenApi};
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
        paths(
            search::search,
            search::widget,
            search::sidebar,
            search::spellcheck,
            webgraph::host::similar,
            webgraph::host::knows,
            webgraph::host::ingoing_hosts,
            webgraph::host::outgoing_hosts,
            webgraph::page::ingoing_pages,
            webgraph::page::outgoing_pages,
            autosuggest::route,
            hosts::hosts_export_optic,
            explore::explore_export_optic,
        ),
        components(
            schemas(
                crate::webpage::region::Region,
                optics::HostRankings,
                search::ApiSearchQuery,
                search::ApiSearchResult,
                search::WidgetQuery,
                search::SidebarQuery,
                search::SpellcheckQuery,
                search::ReturnBody,
                autosuggest::AutosuggestQuery,
                crate::searcher::WebsitesResult,
                crate::search_prettifier::HighlightedSpellCorrection,
                crate::search_prettifier::DisplayedWebpage,
                crate::search_prettifier::DisplayedEntity,
                crate::search_prettifier::DisplayedAnswer,
                crate::search_prettifier::DisplayedSidebar,
                crate::search_prettifier::Snippet,
                crate::search_prettifier::RichSnippet,
                crate::search_prettifier::StackOverflowAnswer,
                crate::search_prettifier::StackOverflowQuestion,
                crate::search_prettifier::CodeOrText,

                crate::snippet::TextSnippet,
                crate::highlighted::HighlightedFragment,
                crate::highlighted::HighlightedKind,

                crate::entity_index::entity::EntitySnippet,
                crate::entity_index::entity::EntitySnippetFragment,

                crate::bangs::UrlWrapper,

                crate::widgets::Widget,
                crate::widgets::calculator::Calculation,
                crate::widgets::thesaurus::ThesaurusWidget,
                crate::widgets::thesaurus::Lemma,
                crate::widgets::thesaurus::WordMeaning,
                crate::widgets::thesaurus::Definition,
                crate::widgets::thesaurus::Example,
                crate::widgets::thesaurus::PartOfSpeech,
                crate::widgets::thesaurus::PartOfSpeechMeaning,

                crate::ranking::SignalEnumDiscriminants,
                crate::ranking::SignalScore,
                
                crate::bangs::BangHit,
                crate::bangs::Bang,

                webgraph::host::SimilarHostsQuery,
                webgraph::KnowsHost,
                crate::entrypoint::webgraph_server::ScoredHost,

                autosuggest::Suggestion,

                hosts::HostsExportOpticParams,
                explore::ExploreExportOpticParams,

                crate::webgraph::Node,
                crate::webgraph::PrettyRelFlag,
                crate::webgraph::PrettyEdge,

                crate::search_prettifier::StructuredData,
                crate::search_prettifier::OneOrManyString,
                crate::search_prettifier::OneOrManyProperty,
                crate::search_prettifier::Property,

                crate::collector::approx_count::Count,
            ),
        ),
        modifiers(&ApiModifier),
        tags(
            (name = "stract"),
        )
    )]
struct ApiDoc;

struct ApiModifier;

fn mark_internal(path: &mut utoipa::openapi::path::PathItem) {
        let internal_extensions = utoipa::openapi::extensions::ExtensionsBuilder::new()
            .add("x-internal", true)
            .build();

    let mut current_extensions = path.extensions.clone().unwrap_or_default();
    current_extensions.merge(internal_extensions.clone());
    path.extensions = Some(current_extensions);

    if let Some(operation) = path.post.as_mut() {
        let mut current_extensions = operation.extensions.clone().unwrap_or_default();
        current_extensions.merge(internal_extensions.clone());
        operation.extensions = Some(current_extensions);
    }

    if let Some(operation) = path.get.as_mut() {
        let mut current_extensions = operation.extensions.clone().unwrap_or_default();
        current_extensions.merge(internal_extensions.clone());
        operation.extensions = Some(current_extensions);
    }
}

impl Modify for ApiModifier {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi.info.description = Some(
            "Stract is an open source web search engine. The API is totally free while in beta, but some endpoints will be paid by consumption in the future.
The API might also change quite a bit during the beta period, but we will try to keep it as stable as possible. We look forward to see what you will build!

Remember to always give proper attributions to the sources you use from the search results.".to_string(),
        );

        mark_internal(openapi.paths.paths.get_mut("/beta/api/explore/export").unwrap());
        mark_internal(openapi.paths.paths.get_mut("/beta/api/hosts/export").unwrap());
        mark_internal(openapi.paths.paths.get_mut("/beta/api/webgraph/host/knows").unwrap());
    }
}

pub fn router<S: Clone + Send + Sync + 'static>() -> impl Into<Router<S>> {
    SwaggerUi::new("/beta/api/docs/swagger")
        .url("/beta/api/docs/openapi.json", ApiDoc::openapi())
        .config(
            utoipa_swagger_ui::Config::default()
                .use_base_layout()
                .default_models_expand_depth(0),
        )
}
