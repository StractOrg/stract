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

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract;
use axum::response::sse::KeepAlive;
use axum::response::{sse::Event, Sse};
use futures::stream::Stream;
use http::StatusCode;
use serde::Deserialize;
use tokio_stream::StreamExt as _;
use utoipa::IntoParams;

use super::State;
use crate::Result;

#[derive(Deserialize, Debug, IntoParams)]
pub struct SummarizeParams {
    pub url: String,
    pub query: String,
}

#[cfg(feature = "libtorch")]
async fn summarize(
    params: SummarizeParams,
    state: Arc<State>,
) -> Result<Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>> {
    let webpage = state
        .searcher
        .get_webpage(&params.url)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Webpage not found: {}", params.url))?;

    let stream = state
        .summarizer
        .summarize(&params.query, &webpage.body)
        .await?;

    Ok(
        Sse::new(stream.map(|term| Event::default().data(term)).map(Ok))
            .keep_alive(KeepAlive::default()),
    )
}

#[cfg(not(feature = "libtorch"))]
async fn summarize(
    params: SummarizeParams,
    state: Arc<State>,
) -> Result<Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>> {
    use futures::stream::BoxStream;

    Err::<Sse<BoxStream<_>>, anyhow::Error>(anyhow::anyhow!("Summarization is not enabled"))
}

#[utoipa::path(
    get,
    path = "/beta/api/summarize",
    params(SummarizeParams),
    responses(
        (status = 200, description = "Summarize a website", body = String, content_type = "text/event-stream"),
    )
)]
pub async fn summarize_route(
    extract::Query(params): extract::Query<SummarizeParams>,
    extract::State(state): extract::State<Arc<State>>,
) -> std::result::Result<Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>, StatusCode>
{
    // err might actually happen if url contains more than 255 tokens
    // as these might be dropped by tantivy.
    match summarize(params, state).await {
        Ok(stream) => Ok(stream),
        Err(_) => Err(StatusCode::NO_CONTENT),
    }
}
