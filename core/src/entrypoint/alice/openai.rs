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
use tokio::sync::Mutex;
use tokio_stream::{Stream, StreamExt};
use tracing::info;

use crate::{
    alice::openai::{Alice, Message},
    distributed::{
        cluster::Cluster,
        member::{Member, Service},
    },
    ttl_cache::TTLCache,
    AliceOpenaiConfig,
};

pub struct State {
    pub alice: Alice,
    pub cluster: Cluster,
    pub conv_states: Arc<Mutex<TTLCache<uuid::Uuid, Vec<Message>>>>,
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

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SaveStateParams {
    pub conversation: Vec<Message>,
}

pub async fn save_state(
    extract::State(state): extract::State<Arc<State>>,
    extract::Json(params): extract::Json<SaveStateParams>,
) -> impl IntoResponse {
    let uuid = uuid::Uuid::new_v4();

    state
        .conv_states
        .lock()
        .await
        .insert(uuid, params.conversation);

    uuid.to_string()
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    pub optic: Option<String>,
    pub conversation: uuid::Uuid,
}

pub async fn route(
    extract::State(state): extract::State<Arc<State>>,
    extract::Query(params): extract::Query<Params>,
) -> std::result::Result<
    Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>,
    http::StatusCode,
> {
    let conversation = state
        .conv_states
        .lock()
        .await
        .get(&params.conversation)
        .cloned()
        .ok_or(http::StatusCode::BAD_REQUEST)?;

    let search_addr = state
        .cluster
        .members()
        .await
        .into_iter()
        .find_map(|m| {
            if let Service::Frontend { host } = m.service {
                Some(host)
            } else {
                None
            }
        })
        .ok_or_else(|| {
            info!("no frontend found");

            http::StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut executor = state
        .alice
        .new_executor(
            conversation,
            params.optic,
            format!("http://{}/beta/api/search", search_addr),
        )
        .map_err(|e| {
            info!("error creating executor: {}", e);
            http::StatusCode::BAD_REQUEST
        })?;

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    tokio::task::spawn(async move {
        while let Ok(Some(msg)) = executor.next().await {
            let msg = serde_json::to_string(&msg)
                .map_err(|e| {
                    info!("error serializing message: {}", e);
                    e
                })
                .unwrap();

            tx.send(msg).await.ok();
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

/// This is currently not used, but let's keep it around for now
/// in case we want to use it later.
pub async fn run(config: AliceOpenaiConfig) -> Result<(), crate::alice::Error> {
    let addr: SocketAddr = config.host;

    let alice = Alice::open(&config.summarizer_path, &config.api_key)?;

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
