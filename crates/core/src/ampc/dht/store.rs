// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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
// along with this program.  If not, see <https://www.gnu.org/licenses/>
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;

use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::RaftSnapshotBuilder;
use openraft::RaftTypeConfig;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::ampc::dht::network::api;

use super::upsert::UpsertEnum;
use super::upsert::UpsertFn;
use super::NodeId;
use super::TypeConfig;
use super::{Request, Response};

#[derive(Serialize, Deserialize, Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
#[serde(transparent)]
pub struct Table(String);

impl Table {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for Table {
    fn from(v: String) -> Self {
        Self(v)
    }
}

impl From<&str> for Table {
    fn from(v: &str) -> Self {
        Self(v.to_string())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
#[serde(transparent)]
pub struct Key(Vec<u8>);

impl Key {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Key {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl From<&[u8]> for Key {
    fn from(v: &[u8]) -> Self {
        Self(v.to_vec())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(transparent)]
pub struct Value(Vec<u8>);

impl Value {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl From<&[u8]> for Value {
    fn from(v: &[u8]) -> Self {
        Self(v.to_vec())
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Db {
    data: BTreeMap<Table, BTreeMap<Key, Value>>,
}

impl Db {
    pub fn drop_table(&mut self, table: &Table) {
        self.data.remove(table);
    }

    pub fn get(&self, table: &Table, key: &Key) -> Option<Value> {
        self.data.get(table).and_then(|m| m.get(key).cloned())
    }

    pub fn set(&mut self, table: Table, key: Key, value: Value) {
        self.data.entry(table).or_default().insert(key, value);
    }

    pub fn batch_set(&mut self, table: Table, values: Vec<(Key, Value)>) {
        self.data.entry(table).or_default().extend(values);
    }

    pub fn upsert(&mut self, table: Table, upsert_fn: &UpsertEnum, key: Key, value: Value) {
        let table = self.data.entry(table).or_default();

        match table.get(&key).cloned() {
            Some(old) => {
                let merged = upsert_fn.upsert(old, value);
                table.insert(key, merged)
            }
            None => table.insert(key, value),
        };
    }

    pub fn batch_upsert(
        &mut self,
        table: Table,
        upsert_fn: &UpsertEnum,
        values: Vec<(Key, Value)>,
    ) {
        let table = self.data.entry(table).or_default();

        for (key, value) in values {
            match table.get(&key).cloned() {
                Some(old) => {
                    let merged = upsert_fn.upsert(old, value);
                    table.insert(key, merged);
                }
                None => {
                    table.insert(key, value);
                }
            }
        }
    }

    pub fn clone_table(&mut self, from: &Table, to: Table) {
        let data = self.data.get(from).cloned().unwrap_or_default();
        self.data.insert(to, data);
    }

    pub fn new_table(&mut self, table: Table) {
        self.data.insert(table, BTreeMap::new());
    }

    pub fn tables(&self) -> Vec<Table> {
        self.data.keys().cloned().collect()
    }

    pub fn batch_get(&self, table: &Table, keys: &[Key]) -> Vec<(Key, Value)> {
        match self.data.get(table) {
            None => Vec::new(),
            Some(table) => keys
                .iter()
                .filter_map(|key| table.get(key).map(|value| (key.clone(), value.clone())))
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,

    /// Application data.
    pub db: Db,
}

#[derive(Debug, Default)]
pub struct StateMachineStore {
    pub state_machine: RwLock<StateMachineData>,
    snapshot_idx: Arc<Mutex<u64>>,

    /// The last received snapshot.
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // Serialize the data of the state machine.
            let state_machine = self.state_machine.read().await;
            data = bincode::serialize(&*state_machine)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership.clone();
        }

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock().unwrap();
            *l += 1;
            *l
        };

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut res = Vec::new();
        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            if let Some(ref last) = sm.last_applied_log {
                if last >= &entry.log_id {
                    res.push(Response::Empty);
                    continue;
                }
            }

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(Response::Empty),
                EntryPayload::Normal(ref req) => match req {
                    Request::Set(api::Set { table, key, value }) => {
                        sm.db.set(table.clone(), key.clone(), value.clone());
                        res.push(Response::Set(Ok(())))
                    }
                    Request::BatchSet(api::BatchSet { table, values }) => {
                        sm.db.batch_set(table.clone(), values.clone());
                        res.push(Response::Set(Ok(())))
                    }
                    Request::Upsert(api::Upsert {
                        table,
                        key,
                        value,
                        upsert_fn,
                    }) => {
                        sm.db
                            .upsert(table.clone(), upsert_fn, key.clone(), value.clone());
                        res.push(Response::Upsert(Ok(())))
                    }
                    Request::BatchUpsert(api::BatchUpsert {
                        table,
                        upsert_fn,
                        values,
                    }) => {
                        sm.db.batch_upsert(table.clone(), upsert_fn, values.clone());
                        res.push(Response::Upsert(Ok(())))
                    }
                    Request::CreateTable(api::CreateTable { table }) => {
                        sm.db.new_table(table.clone());
                        res.push(Response::CreateTable(Ok(())))
                    }
                    Request::DropTable(api::DropTable { table }) => {
                        sm.db.drop_table(table);
                        res.push(Response::DropTable(Ok(())))
                    }
                    Request::AllTables(api::AllTables) => {
                        res.push(Response::AllTables(Ok(sm.db.tables())))
                    }
                    Request::CloneTable(api::CloneTable { from, to }) => {
                        sm.db.clone_table(from, to.clone());
                        res.push(Response::CloneTable(Ok(())))
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(Response::Empty)
                }
            };
        }
        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let data: Db = bincode::deserialize(&new_snapshot.data).map_err(|e| {
                StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e)
            })?;

            let mut state_machine = self.state_machine.write().await;
            state_machine.db = data;
            state_machine.last_applied_log = meta.last_log_id;
            state_machine.last_membership = meta.last_membership.clone();
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Arc::clone(self)
    }
}

#[cfg(test)]
mod tests {
    use openraft::testing::{StoreBuilder, Suite};

    type LogStore = crate::ampc::dht::log_store::LogStore<TypeConfig>;

    use super::*;

    struct MemStoreBuilder {}

    impl StoreBuilder<TypeConfig, LogStore, Arc<StateMachineStore>, ()> for MemStoreBuilder {
        async fn build(
            &self,
        ) -> Result<((), LogStore, Arc<StateMachineStore>), StorageError<NodeId>> {
            let log_store = LogStore::default();
            let sm = Arc::new(StateMachineStore::default());

            Ok(((), log_store, sm))
        }
    }

    #[test]
    pub fn test_raft_impl() -> Result<(), StorageError<NodeId>> {
        Suite::test_all(MemStoreBuilder {})?;
        Ok(())
    }
}
