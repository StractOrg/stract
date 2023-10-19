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

use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Timelike, Utc};
use scylla::{prepared_statement::PreparedStatement, SessionBuilder};
use stdx::leaky_queue::LeakyQueue;
use thiserror::Error;
use tokio::{sync::Mutex, time};
use url::Url;
use uuid::Uuid;

#[derive(Debug, Error)]
enum Error {
    #[error("scylla query")]
    ScyllaQuery(#[from] scylla::transport::errors::QueryError),

    #[error("scylla new session")]
    ScyllaNewSess(#[from] scylla::transport::errors::NewSessionError),
}

/// Note that we don't store any information that can be used to link
/// the query and result back to the user performing the query. This is extremely important!
#[derive(Debug, Clone)]
pub struct StoredQuery {
    qid: Uuid,
    query: String,
    result_urls: Vec<Url>,
    timestamp: Option<DateTime<Utc>>, // it is extremely important that we strip minutes, seconds and nanoseconds here for privacy
}

#[derive(Clone)]
pub enum ImprovementEvent {
    StoreQuery(StoredQuery),
    Click { qid: Uuid, idx: usize },
}

impl StoredQuery {
    pub fn new(query: String, urls: Vec<Url>) -> Self {
        let timestamp = Utc::now()
            .with_minute(0)
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0));

        let qid = Uuid::new_v4();

        Self {
            qid,
            query,
            result_urls: urls,
            timestamp,
        }
    }

    pub fn qid(&self) -> &Uuid {
        &self.qid
    }
}

async fn dump_queue(queue: &Mutex<LeakyQueue<ImprovementEvent>>) -> Vec<ImprovementEvent> {
    let mut res = Vec::new();
    let mut lock = queue.lock().await;

    while let Some(query) = lock.pop() {
        res.push(query);
    }

    res
}

pub async fn store_improvements_loop(
    queue: Arc<Mutex<LeakyQueue<ImprovementEvent>>>,
    scylla_host: String,
) {
    let scylla = ScyllaConn::new(scylla_host.as_str()).await.unwrap();
    let mut interval = time::interval(Duration::from_secs(30));

    loop {
        interval.tick().await;
        let events = dump_queue(&queue).await;

        for event in events {
            match event {
                ImprovementEvent::StoreQuery(query) => scylla.store_query(query).await,
                ImprovementEvent::Click { qid, idx } => {
                    if let Ok(idx) = idx.try_into() {
                        scylla.store_click(qid, idx).await
                    }
                }
            }
        }
    }
}

struct ScyllaConn {
    session: scylla::Session,
    prepared_insert: PreparedStatement,
    prepared_click: PreparedStatement,
}

impl ScyllaConn {
    async fn new(seed_node: &str) -> Result<Self, Error> {
        let session = SessionBuilder::new().known_node(seed_node).build().await?;

        session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;
        session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.queries (qid uuid, query text, urls text, timestamp timestamp, primary key (qid, timestamp))",
            &[],
        )
        .await?;

        session
            .query(
                "CREATE TABLE IF NOT EXISTS ks.clicks (qid uuid, click tinyint, primary key (qid))",
                &[],
            )
            .await?;

        let prepared_insert: PreparedStatement = session
            .prepare("INSERT INTO ks.queries (qid, query, urls, timestamp) VALUES(?, ?, ?, ?)")
            .await?;

        let prepared_click: PreparedStatement = session
            .prepare("INSERT INTO ks.clicks (qid, click) VALUES(?, ?)")
            .await?;

        Ok(Self {
            session,
            prepared_insert,
            prepared_click,
        })
    }

    async fn store_query(&self, query: StoredQuery) {
        let urls = serde_json::to_string(&query.result_urls).unwrap();
        let timestamp = query
            .timestamp
            .map(|timestamp| chrono::Duration::seconds(timestamp.timestamp()))
            .unwrap_or_else(|| chrono::Duration::seconds(0));
        let qid = query.qid;

        let res = self
            .session
            .execute(
                &self.prepared_insert,
                (
                    qid,
                    query.query,
                    urls,
                    scylla::frame::value::Timestamp(timestamp),
                ),
            )
            .await;

        if let Err(err) = res {
            tracing::error!("scylla insert error: {err}");
        }
    }

    async fn store_click(&self, qid: Uuid, idx: i8) {
        let res = self.session.execute(&self.prepared_click, (qid, idx)).await;

        if let Err(err) = res {
            tracing::error!("scylla store_click error: {err}");
        }
    }
}
