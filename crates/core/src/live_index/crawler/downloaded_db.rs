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
// along with this program.  If not, see <https://www.gnu.org/licenses/

use redb::ReadableTable;
use url::Url;

use crate::Result;
use std::{path::Path, time::Duration};

use crate::live_index::TTL;

#[derive(Debug, Clone)]
struct InsertionTime {
    time: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
struct TruncatedUrl(String);

impl TruncatedUrl {
    fn new(url: &Url) -> Self {
        let mut url = url.clone().to_string();

        // redb has a hard limit of 4gb per value
        // and is better suited for smaller values.
        // we therefore truncate to ~8kb
        if url.len() > 8192 {
            url = url.chars().take(8192).collect();
        }

        Self(url)
    }
}

impl redb::Key for TruncatedUrl {
    fn compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }
}

impl redb::Value for TruncatedUrl {
    type SelfType<'a> = TruncatedUrl
    where
        Self: 'a;

    type AsBytes<'a> = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let s = std::str::from_utf8(data).unwrap();
        Self(s.to_string())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        value.0.as_bytes()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("TruncatedUrl")
    }
}

impl redb::Key for InsertionTime {
    fn compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }
}

impl redb::Value for InsertionTime {
    type SelfType<'a> = InsertionTime
    where
        Self: 'a;

    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let time = chrono::DateTime::parse_from_rfc3339(std::str::from_utf8(data).unwrap())
            .unwrap()
            .with_timezone(&chrono::Utc);

        Self { time }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        let s = value.time.to_rfc3339();
        s.as_bytes().to_vec()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("InsertionTime")
    }
}

struct InnerDb {
    db: redb::Database,
    last_truncate: chrono::DateTime<chrono::Utc>,
}

impl InnerDb {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().join("downloaded.db");

        let db = if !path.exists() {
            if let Some(parent) = path.parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent)?;
                }
            }

            redb::Database::create(&path)?
        } else {
            redb::Database::open(&path)?
        };

        // make sure the tables exist
        let txn = db.begin_write()?;

        txn.open_table(Self::urls_table_definition())?;
        txn.open_table(Self::times_table_definition())?;

        txn.commit()?;

        Ok(Self {
            db,
            last_truncate: chrono::Utc::now(),
        })
    }

    fn urls_table_definition() -> redb::TableDefinition<'static, TruncatedUrl, ()> {
        redb::TableDefinition::new("urls")
    }

    fn times_table_definition() -> redb::TableDefinition<'static, InsertionTime, TruncatedUrl> {
        redb::TableDefinition::new("times")
    }

    fn has_downloaded(&self, url: &Url) -> Result<bool> {
        let url = TruncatedUrl::new(url);

        Ok(self
            .db
            .begin_read()?
            .open_table(Self::urls_table_definition())?
            .get(&url)?
            .is_some())
    }

    fn truncate(&mut self, ttl: Duration) -> Result<()> {
        let now = chrono::Utc::now();
        let thresh = now - ttl;

        let thresh = InsertionTime { time: thresh };
        let txn = self.db.begin_write()?;

        let to_remove = {
            let times = txn.open_table(Self::times_table_definition())?;

            let to_remove: Vec<_> = times
                .range(..thresh)?
                .map(|r| {
                    let (time, url) = r.unwrap();

                    (time.value().clone(), url.value().clone())
                })
                .collect();

            to_remove
        };

        {
            let mut times = txn.open_table(Self::times_table_definition())?;
            let mut urls = txn.open_table(Self::urls_table_definition())?;

            for (time, url) in to_remove {
                times.remove(time)?;
                urls.remove(url)?;
            }
        }

        txn.commit()?;

        self.last_truncate = chrono::Utc::now();

        Ok(())
    }

    fn maybe_truncate(&mut self) -> Result<()> {
        let now = chrono::Utc::now();

        if self.last_truncate.signed_duration_since(now).num_seconds() < 60 {
            return Ok(());
        }

        self.truncate(TTL)
    }

    pub fn insert(&mut self, url: &Url) -> Result<()> {
        {
            let key = TruncatedUrl::new(url);
            let txn = self.db.begin_write()?;
            {
                let mut table = txn.open_table(Self::urls_table_definition())?;
                table.insert(&key, ())?;
            }

            let time = InsertionTime {
                time: chrono::Utc::now(),
            };
            {
                let mut table = txn.open_table(Self::times_table_definition())?;
                table.insert(time, key)?;
            }

            txn.commit()?;
        }

        self.maybe_truncate()?;

        Ok(())
    }
}

pub struct DownloadedDb {
    inner: std::sync::Mutex<InnerDb>,
}

impl DownloadedDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = InnerDb::open(path)?;

        Ok(Self {
            inner: std::sync::Mutex::new(db),
        })
    }

    pub fn has_downloaded(&self, url: &Url) -> Result<bool> {
        self.inner.lock().unwrap().has_downloaded(url)
    }

    pub fn insert(&self, url: &Url) -> Result<()> {
        self.inner.lock().unwrap().insert(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_downloaded_db() {
        let db = DownloadedDb::open(crate::gen_temp_path()).unwrap();

        let url = Url::parse("https://example.com").unwrap();
        assert!(!db.has_downloaded(&url).unwrap());

        db.insert(&url).unwrap();
        assert!(db.has_downloaded(&url).unwrap());
    }

    #[test]
    fn test_truncate_ttl() {
        let db = DownloadedDb::open(crate::gen_temp_path()).unwrap();

        let url = Url::parse("https://example.com").unwrap();
        db.insert(&url).unwrap();

        assert!(db.has_downloaded(&url).unwrap());

        let ttl = Duration::from_secs(1);
        std::thread::sleep(ttl + Duration::from_secs(1));

        db.inner.lock().unwrap().truncate(ttl).unwrap();

        assert!(!db.has_downloaded(&url).unwrap());
    }
}
