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

use std::{convert::Infallible, sync::Arc};

use axum::{
    extract,
    response::{sse::Event, IntoResponse, Sse},
};
use eventsource_stream::Eventsource;
use http::StatusCode;
use rand::seq::SliceRandom;
use tokio_stream::{Stream, StreamExt};

use crate::{
    distributed::member::Service,
    entrypoint::{self, alice::SaveStateParams},
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SavedState {
    pub id: String,
    pub uuid: uuid::Uuid,
}

pub async fn save_state(
    extract::State(state): extract::State<Arc<super::State>>,
    extract::Json(params): extract::Json<SaveStateParams>,
) -> Result<impl IntoResponse, http::StatusCode> {
    let client = reqwest::ClientBuilder::default()
        .build()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut members = state.cluster.members().await;

    members.shuffle(&mut rand::thread_rng());

    let (alice_addr, id) = members
        .into_iter()
        .find_map(|m| {
            if let Service::Alice { host } = m.service {
                Some((host, m.id))
            } else {
                None
            }
        })
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let alice_url = format!("http://{}/save_state", alice_addr);

    let res = client
        .post(alice_url)
        .json(&params)
        .send()
        .await
        .map_err(|e| {
            tracing::error!("Error contacting alice: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let uuid = res.text().await.map_err(|e| {
        tracing::error!("Error contacting alice: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let saved_state = SavedState {
        id,
        uuid: uuid::Uuid::parse_str(&uuid).map_err(|e| {
            tracing::error!("Error parsing uuid: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?,
    };

    Ok(EncodedSavedState::encode(saved_state).0)
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct EncodedSavedState(String);

impl EncodedSavedState {
    fn encode(saved_state: SavedState) -> Self {
        let saved_state = bincode::serialize(&saved_state).unwrap();
        let saved_state = base64::encode(saved_state);
        Self(saved_state)
    }

    fn decode(self) -> Result<SavedState, crate::alice::Error> {
        let saved_state = base64::decode(self.0)?;
        let saved_state = bincode::deserialize(&saved_state)?;
        Ok(saved_state)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    message: String,
    optic: Option<String>,
    prev_state: Option<EncodedSavedState>,
}

pub async fn route(
    extract::State(state): extract::State<Arc<super::State>>,
    extract::Query(params): extract::Query<Params>,
) -> std::result::Result<
    Sse<impl Stream<Item = std::result::Result<axum::response::sse::Event, Infallible>>>,
    StatusCode,
> {
    let mut saved_state: Option<SavedState> = None;

    if let Some(prev_state) = params.prev_state {
        saved_state = Some(prev_state.decode().map_err(|e| {
            tracing::error!("Error decoding saved state: {}", e);
            StatusCode::BAD_REQUEST
        })?);
    }

    let client = reqwest::ClientBuilder::default()
        .build()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut members = state.cluster.members().await;

    members.shuffle(&mut rand::thread_rng());

    let alice_addr = if let Some(id) = saved_state.as_ref().map(|s| s.id.clone()) {
        members
            .into_iter()
            .find_map(|m| {
                if m.id == id {
                    if let Service::Alice { host } = m.service {
                        Some(host)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        members
            .into_iter()
            .find_map(|m| {
                if let Service::Alice { host } = m.service {
                    Some(host)
                } else {
                    None
                }
            })
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
    };

    let params = entrypoint::alice::Params {
        message: params.message,
        prev_state: saved_state.map(|s| s.uuid),
        optic: params.optic,
    };

    let mut events = client
        .get(format!("http://{}", alice_addr))
        .query(&params)
        .send()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .bytes_stream()
        .eventsource();

    let stream = async_stream::stream! {
        while let Some(Ok(item)) = events.next().await {
            yield Ok(Event::default().data(item.data));
        }
    };

    Ok(Sse::new(stream))
}

// for openai alice
// #[derive(serde::Serialize, serde::Deserialize, Debug)]
// #[serde(rename_all = "camelCase")]
// pub struct Params {
//     optic: Option<String>,
//     conv: EncodedSavedState,
// }
//
// pub async fn route(
//     extract::State(state): extract::State<Arc<super::State>>,
//     extract::Query(params): extract::Query<Params>,
// ) -> std::result::Result<
//     Sse<impl Stream<Item = std::result::Result<axum::response::sse::Event, Infallible>>>,
//     StatusCode,
// > {
//     let saved_state: SavedState = params.conv.decode().map_err(|e| {
//         tracing::error!("Error decoding saved state: {}", e);
//         StatusCode::BAD_REQUEST
//     })?;
//
//     let client = reqwest::ClientBuilder::default()
//         .build()
//         .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
//
//     let mut members = state.cluster.members().await;
//
//     members.shuffle(&mut rand::thread_rng());
//
//     let id = saved_state.id.clone();
//     let alice_addr = members
//         .into_iter()
//         .find_map(|m| {
//             if m.id == id {
//                 if let Service::Alice { host } = m.service {
//                     Some(host)
//                 } else {
//                     None
//                 }
//             } else {
//                 None
//             }
//         })
//         .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
//
//     let params = entrypoint::alice::openai::Params {
//         conversation: saved_state.uuid,
//         optic: params.optic,
//     };
//
//     let mut events = client
//         .get(format!("http://{}", alice_addr))
//         .query(&params)
//         .send()
//         .await
//         .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
//         .bytes_stream()
//         .eventsource();
//
//     let stream = async_stream::stream! {
//         while let Some(Ok(item)) = events.next().await {
//             yield Ok(Event::default().data(item.data));
//         }
//     };
//
//     Ok(Sse::new(stream))
// }
