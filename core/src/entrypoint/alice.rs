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

use aes_gcm::{aead::OsRng, Aes256Gcm, KeyInit};
use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    extract,
    response::{
        sse::{Event, KeepAlive},
        IntoResponse, Sse,
    },
    routing::{get, post},
    Router,
};
use base64::Engine;
use tokio::sync::Mutex;
use tokio_stream::Stream;
use tokio_stream::StreamExt as _;
use tracing::info;

use crate::{
    alice::{Alice, EncodedEncryptedState, EncryptedState, BASE64_ENGINE},
    config::AliceLocalConfig,
    distributed::{
        cluster::Cluster,
        member::{Member, Service},
    },
    ttl_cache::TTLCache,
};

pub struct State {
    pub alice: Alice,
    pub cluster: Cluster,
    pub conv_states: Arc<Mutex<TTLCache<uuid::Uuid, EncryptedState>>>,
}

fn router(alice: Alice, cluster: Cluster) -> Router {
    let state = Arc::new(State {
        alice,
        cluster,
        conv_states: Arc::new(Mutex::new(TTLCache::with_ttl(Duration::from_secs(10)))),
    });

    Router::new()
        .route("/", get(route))
        .route("/save_state", post(save_state))
        .with_state(state)
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SaveStateParams {
    pub state: EncodedEncryptedState,
}

pub async fn save_state(
    extract::State(state): extract::State<Arc<State>>,
    extract::Json(params): extract::Json<SaveStateParams>,
) -> Result<impl IntoResponse, http::StatusCode> {
    let encrypted_state = params.state.decode().map_err(|e| {
        info!("error decoding state: {}", e);
        http::StatusCode::BAD_REQUEST
    })?;

    let uuid = uuid::Uuid::new_v4();

    state.conv_states.lock().await.insert(uuid, encrypted_state);

    Ok(uuid.to_string())
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AliceParams {
    pub message: String,
    pub optic: Option<String>,
    pub prev_state: Option<uuid::Uuid>,
}

pub async fn route(
    extract::State(state): extract::State<Arc<State>>,
    extract::Query(params): extract::Query<AliceParams>,
) -> std::result::Result<
    Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>,
    http::StatusCode,
> {
    let mut prev_state = None;
    if let Some(p) = params.prev_state {
        prev_state = Some(
            state
                .conv_states
                .lock()
                .await
                .get(&p)
                .cloned()
                .ok_or(http::StatusCode::BAD_REQUEST)?,
        );
    }

    let search_addr = state
        .cluster
        .members()
        .await
        .into_iter()
        .find_map(|m| {
            if let Service::Api { host } = m.service {
                Some(host)
            } else {
                None
            }
        })
        .ok_or_else(|| {
            info!("no api found");

            http::StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let executor = state
        .alice
        .new_executor(
            &params.message,
            prev_state,
            format!("http://{}/beta/api/search", search_addr),
            params.optic,
        )
        .map_err(|e| {
            info!("error creating executor: {}", e);
            http::StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    tokio::task::spawn_blocking(move || {
        for msg in executor {
            let msg = serde_json::to_string(&msg)
                .map_err(|e| {
                    info!("error serializing message: {}", e);
                    e
                })
                .unwrap();

            tx.send(msg).ok();
        }
    });

    let stream = async_stream::stream! {
        while let Some(item) = rx.recv().await {
            yield item;
        }
    };

    Ok(
        Sse::new(stream.map(|term| Event::default().data(term)).map(Ok))
            .keep_alive(KeepAlive::default()),
    )
}

pub async fn run(config: AliceLocalConfig) -> Result<(), anyhow::Error> {
    let addr: SocketAddr = config.host;
    let key = BASE64_ENGINE.decode(config.encryption_key)?;

    info!("starting alice");
    let alice = Alice::open(
        config.alice_path.as_ref(),
        config.accelerator.clone().map(|acc| acc.into()),
        &key,
    )?;

    let cluster = Cluster::join(
        Member {
            id: config.cluster_id,
            service: Service::Alice { host: config.host },
        },
        config.gossip_addr,
        config.gossip_seed_nodes.unwrap_or_default(),
    )
    .await?;

    let app = router(alice, cluster);

    info!("alice is ready to accept requests on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}

pub fn generate_key() {
    let key = Aes256Gcm::generate_key(OsRng);
    println!("{}", BASE64_ENGINE.encode(key.as_slice()));
}
