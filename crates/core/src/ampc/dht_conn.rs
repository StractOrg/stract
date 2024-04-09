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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::net::SocketAddr;

use super::{
    block_on,
    dht::{self, store::UpsertAction, upsert::UpsertEnum},
};

pub trait DhtTables
where
    Self: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    fn drop_tables(&self);
    fn next(&self) -> Self;
    fn cleanup_prev_tables(&self);
}

// TODO: this could be a derive proc macro instead
macro_rules! impl_dht_tables {
    ($struct:ty, [$($field:ident),*$(,)?]) => {
        #[allow(unused_imports)]
        use $crate::ampc::dht_conn::DhtTable as _;

        impl $crate::ampc::dht_conn::DhtTables for $struct {
            fn drop_tables(&self) {
                $(self.$field.drop_table();)*
            }

            fn next(&self) -> Self {
                Self {
                    $($field: self.$field.next(),)*
                }
            }

            fn cleanup_prev_tables(&self) {
                $(
                    let tables = $crate::ampc::block_on(self.$field.client().all_tables()).unwrap();

                    for table in tables {
                        if table.as_str().starts_with(&self.$field.table().prefix()) {
                            $crate::ampc::block_on(self.$field.client().drop_table(table)).unwrap();
                        }
                    }
                )*
            }
        }
    };
}

pub(crate) use impl_dht_tables;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Table {
    prefix: String,
    round: u64,
}

impl Table {
    pub fn new<S: AsRef<str>>(prefix: S) -> Self {
        Self {
            prefix: prefix.as_ref().to_string(),
            round: 0,
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    fn dht(&self) -> dht::Table {
        format!("{}-{}", self.prefix, self.round).into()
    }

    fn next(&self) -> Self {
        Self {
            prefix: self.prefix.clone(),
            round: self.round + 1,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct DefaultDhtTable<K, V> {
    table: Table,
    client: dht::Client,
    _maker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Clone for DefaultDhtTable<K, V> {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            client: self.client.clone(),
            _maker: std::marker::PhantomData,
        }
    }
}
impl<K, V> DefaultDhtTable<K, V>
where
    K: serde::Serialize + serde::de::DeserializeOwned,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    pub fn new<S: AsRef<str>>(members: &[(dht::ShardId, SocketAddr)], prefix: S) -> Self {
        Self {
            table: Table::new(prefix),
            client: dht::Client::new(members),
            _maker: std::marker::PhantomData,
        }
    }
}

pub trait DhtTable: Clone + serde::Serialize + serde::de::DeserializeOwned {
    type Key: serde::Serialize + serde::de::DeserializeOwned;
    type Value: serde::Serialize + serde::de::DeserializeOwned;

    fn client(&self) -> &dht::Client;
    fn table(&self) -> &Table;
    fn next(&self) -> Self;

    fn get(&self, key: Self::Key) -> Option<Self::Value> {
        let key = bincode::serialize(&key).unwrap();

        block_on(self.client().get(self.table().dht(), key.into()))
            .unwrap()
            .map(|v| bincode::deserialize(v.as_bytes()).unwrap())
    }

    fn batch_get(&self, keys: Vec<Self::Key>) -> Vec<(Self::Key, Self::Value)> {
        let keys: Vec<dht::Key> = keys
            .into_iter()
            .map(|k| bincode::serialize(&k).unwrap().into())
            .collect::<Vec<_>>();
        let values = block_on(self.client().batch_get(self.table().dht(), keys)).unwrap();

        values
            .into_iter()
            .map(|(k, v)| {
                (
                    bincode::deserialize(k.as_bytes()).unwrap(),
                    bincode::deserialize(v.as_bytes()).unwrap(),
                )
            })
            .collect()
    }

    fn set(&self, key: Self::Key, value: Self::Value) {
        let key = bincode::serialize(&key).unwrap();
        let value = bincode::serialize(&value).unwrap();

        block_on(
            self.client()
                .set(self.table().dht(), key.into(), value.into()),
        )
        .unwrap();
    }

    fn batch_set(&self, pairs: Vec<(Self::Key, Self::Value)>) {
        let pairs: Vec<(dht::Key, dht::Value)> = pairs
            .into_iter()
            .map(|(k, v)| {
                (
                    bincode::serialize(&k).unwrap().into(),
                    bincode::serialize(&v).unwrap().into(),
                )
            })
            .collect();

        block_on(self.client().batch_set(self.table().dht(), pairs)).unwrap();
    }

    fn upsert<F: Into<UpsertEnum>>(
        &self,
        upsert: F,
        key: Self::Key,
        value: Self::Value,
    ) -> UpsertAction {
        let key = bincode::serialize(&key).unwrap();
        let value = bincode::serialize(&value).unwrap();

        block_on(
            self.client()
                .upsert(self.table().dht(), upsert, key.into(), value.into()),
        )
        .unwrap()
    }

    fn batch_upsert<F: Into<UpsertEnum> + Clone>(
        &self,
        upsert: F,
        pairs: Vec<(Self::Key, Self::Value)>,
    ) -> Vec<(Self::Key, UpsertAction)> {
        let pairs: Vec<(dht::Key, dht::Value)> = pairs
            .into_iter()
            .map(|(k, v)| {
                (
                    bincode::serialize(&k).unwrap().into(),
                    bincode::serialize(&v).unwrap().into(),
                )
            })
            .collect();

        block_on(
            self.client()
                .batch_upsert(self.table().dht(), upsert, pairs),
        )
        .unwrap()
        .into_iter()
        .map(|(k, did_upsert)| (bincode::deserialize(k.as_bytes()).unwrap(), did_upsert))
        .collect()
    }

    fn init_from(&self, prev: &DefaultDhtTable<Self::Key, Self::Value>) {
        block_on(
            self.client()
                .clone_table(prev.table().dht(), self.table().dht()),
        )
        .unwrap();
    }

    fn drop_table(&self) {
        block_on(self.client().drop_table(self.table().dht())).unwrap();
    }
}

impl<K, V> DhtTable for DefaultDhtTable<K, V>
where
    K: serde::Serialize + serde::de::DeserializeOwned,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    type Key = K;
    type Value = V;

    fn client(&self) -> &dht::Client {
        &self.client
    }

    fn table(&self) -> &Table {
        &self.table
    }

    fn next(&self) -> DefaultDhtTable<Self::Key, Self::Value> {
        let new = Self {
            table: self.table().next(),
            client: self.client().clone(),
            _maker: std::marker::PhantomData,
        };

        new.init_from(self);

        new
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct DhtConn<T> {
    prev: T,
    next: T,
}

impl<T> DhtConn<T>
where
    T: DhtTables,
{
    pub fn new(initial: T) -> Self {
        let next = initial.next();

        Self {
            prev: initial,
            next,
        }
    }

    pub(super) fn cleanup_prev_tables(&self) {
        self.prev.cleanup_prev_tables();
    }

    pub(super) fn next_round(&mut self) {
        self.prev.drop_tables();
        self.prev = self.next.clone();

        self.next = self.prev.next();
    }

    pub fn prev(&self) -> &T {
        &self.prev
    }

    pub fn next(&self) -> &T {
        &self.next
    }

    pub fn take_prev(self) -> T {
        self.prev
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use openraft::{error::InitializeError, BasicNode};
    use tracing_test::traced_test;

    use self::dht::upsert;

    use super::*;

    #[derive(
        Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord,
    )]
    struct Id(u64);
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
    struct Counter(u64);

    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct Tables {
        id: DefaultDhtTable<Id, Counter>,
    }

    impl_dht_tables!(Tables, [id]);

    pub fn start_dht_background() -> SocketAddr {
        let (tx, rx) = crossbeam_channel::unbounded();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let (raft, server, addr) = dht::tests::server(1).await.unwrap();

                let members: BTreeMap<u64, _> = vec![(1, addr)]
                    .into_iter()
                    .map(|(id, addr)| (id, BasicNode::new(addr)))
                    .collect();

                if let Err(e) = raft.initialize(members.clone()).await {
                    match e {
                        openraft::error::RaftError::APIError(e) => match e {
                            InitializeError::NotAllowed(_) => {}
                            InitializeError::NotInMembers(_) => panic!("{:?}", e),
                        },
                        openraft::error::RaftError::Fatal(_) => panic!("{:?}", e),
                    }
                };

                tx.send(addr).unwrap();

                loop {
                    server.accept().await.unwrap();
                }
            })
        });

        rx.recv().unwrap()
    }

    #[test]
    #[traced_test]
    fn test_dht_conn() -> anyhow::Result<()> {
        let addr = start_dht_background();

        let tables = Tables {
            id: DefaultDhtTable::new(&[(1.into(), addr)], "id"),
        };

        tables.id.set(Id(0), Counter(0));

        assert_eq!(tables.id.get(Id(0)), Some(Counter(0)));

        tables
            .id
            .batch_set(vec![(Id(1), Counter(0)), (Id(2), Counter(0))]);

        let mut res = tables.id.batch_get(vec![Id(1), Id(2)]);
        res.sort_by(|(a, _), (b, _)| a.cmp(&b));

        assert_eq!(res, vec![(Id(1), Counter(0)), (Id(2), Counter(0))]);

        tables.id.upsert(upsert::U64Add, Id(0), Counter(1));
        assert_eq!(tables.id.get(Id(0)), Some(Counter(1)));

        tables.id.batch_upsert(
            upsert::U64Add,
            vec![(Id(1), Counter(1)), (Id(2), Counter(1))],
        );

        let mut res = tables.id.batch_get(vec![Id(0), Id(1), Id(2)]);
        res.sort_by(|(a, _), (b, _)| a.cmp(&b));

        assert_eq!(
            res,
            vec![
                (Id(0), Counter(1)),
                (Id(1), Counter(1)),
                (Id(2), Counter(1))
            ]
        );

        Ok(())
    }
}
