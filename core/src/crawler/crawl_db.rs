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

use std::{collections::HashSet, path::Path, rc::Rc, sync::Arc};

use itertools::Itertools;
use rusqlite::{
    types::{FromSql, FromSqlError, FromSqlResult, Value, ValueRef},
    ToSql,
};

use crate::webpage::Url;

use super::{Domain, Job, Result, UrlResponse};

pub enum UrlStatus {
    Pending,
    Crawling,
    Failed,
    Done,
}

impl ToSql for UrlStatus {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            UrlStatus::Pending => Ok(rusqlite::types::ToSqlOutput::Owned(
                rusqlite::types::Value::Integer(0),
            )),
            UrlStatus::Crawling => Ok(rusqlite::types::ToSqlOutput::Owned(
                rusqlite::types::Value::Integer(1),
            )),
            UrlStatus::Failed => Ok(rusqlite::types::ToSqlOutput::Owned(
                rusqlite::types::Value::Integer(2),
            )),
            UrlStatus::Done => Ok(rusqlite::types::ToSqlOutput::Owned(
                rusqlite::types::Value::Integer(3),
            )),
        }
    }
}

impl FromSql for UrlStatus {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(0) => Ok(UrlStatus::Pending),
            ValueRef::Integer(1) => Ok(UrlStatus::Crawling),
            ValueRef::Integer(2) => Ok(UrlStatus::Failed),
            ValueRef::Integer(3) => Ok(UrlStatus::Done),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

pub enum DomainStatus {
    Pending,
    CrawlInProgress,
}

impl ToSql for DomainStatus {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            DomainStatus::Pending => Ok(rusqlite::types::ToSqlOutput::Owned(
                rusqlite::types::Value::Integer(0),
            )),
            DomainStatus::CrawlInProgress => Ok(rusqlite::types::ToSqlOutput::Owned(
                rusqlite::types::Value::Integer(1),
            )),
        }
    }
}

impl FromSql for DomainStatus {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(0) => Ok(DomainStatus::Pending),
            ValueRef::Integer(1) => Ok(DomainStatus::CrawlInProgress),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

pub struct CrawlDb {
    conn: Arc<rusqlite::Connection>,
}

impl CrawlDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Arc::new(rusqlite::Connection::open(path)?);
        // create tables if not exists
        // there should be one table that contains all known URLs
        // when a URL is added to the table, it should be marked as `UrlStatus::Pending`
        // when a URL is being fetched, it should be marked as `UrlStatus::Crawling`
        // when a URL has been fetched, it should be marked as `UrlStatus::Done`.
        // Each URL should have a counter that counts the number of incoming links
        // from URLs on other domains.
        conn.execute(
            "CREATE TABLE IF NOT EXISTS url(
            url TEXT NOT NULL UNIQUE,
            domain TEXT NOT NULL,
            status INTEGER NOT NULL,
            error_code INTEGER,
            incoming_links INTEGER NOT NULL,
            PRIMARY KEY (url)
        );",
            [],
        )?;

        // index by url and status
        conn.execute(
            "CREATE INDEX IF NOT EXISTS url_url_status ON url (url, status);",
            [],
        )?;

        // there should be one table that contains all known domains
        // and whether a crawl is in progress for that domain.
        // It should also contain a copy of the maximum count of incoming links
        // for any URL in that domain.
        conn.execute(
            "CREATE TABLE IF NOT EXISTS domain (
            domain TEXT NOT NULL UNIQUE,
            max_incoming_links INTEGER NOT NULL,
            status INTEGER NOT NULL,
            PRIMARY KEY (domain)
        );",
            [],
        )?;

        // index by domain and status
        conn.execute(
            "CREATE INDEX IF NOT EXISTS domain_domain_status ON domain (domain, status);",
            [],
        )?;

        // store redirects
        conn.execute(
            "CREATE TABLE IF NOT EXISTS redirect (
            from_url INTEGER NOT NULL,
            to_url INTEGER NOT NULL,
            PRIMARY KEY (from_url, to_url)
        );",
            [],
        )?;

        // update performance stuff
        // WAL mode
        conn.pragma_update(None, "journal_mode", "'WAL'")?;

        // sync OFF - if coordinator crashes, we are SOL anyway and will have to restart the crawl.
        conn.pragma_update(None, "synchronous", 0)?;

        // store temp tables in memory
        conn.pragma_update(None, "temp_store", 2)?;

        // set cache size to 64 MB
        conn.pragma_update(None, "cache_size", -64_000)?; // negative value means kilobytes (https://www.sqlite.org/pragma.html#pragma_cache_size)

        rusqlite::vtab::array::load_module(&conn)?;

        Ok(Self { conn })
    }

    pub fn transaction(&self) -> Result<Transaction<'_>> {
        Ok(Transaction {
            tx: Some(self.conn.unchecked_transaction()?),
        })
    }
}

pub struct Transaction<'a> {
    tx: Option<rusqlite::Transaction<'a>>,
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        self.tx.take().unwrap().commit().unwrap();
    }
}

impl Transaction<'_> {
    fn tx(&self) -> &rusqlite::Transaction<'_> {
        self.tx.as_ref().unwrap()
    }

    pub fn insert_seed_urls(&mut self, urls: &[Url]) -> Result<()> {
        let mut prepared_insert_url = self.tx().prepare(
            "INSERT INTO url (url, domain, status, incoming_links)
        VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT (url) DO NOTHING;",
        )?;
        let mut prepared_insert_domain = self.tx().prepare(
            "INSERT INTO domain (domain, max_incoming_links, status)
        VALUES (?1, 0, ?2)
        ON CONFLICT (domain) DO NOTHING;",
        )?;

        for url in urls {
            prepared_insert_url.execute((
                url.full(),
                url.domain().to_string(),
                UrlStatus::Pending,
                0,
            ))?;
            prepared_insert_domain.execute((url.domain().to_string(), DomainStatus::Pending))?;
        }

        Ok(())
    }

    pub fn update_max_inlinks_domains<'a, I: Iterator<Item = &'a Domain>>(
        &self,
        mut domains: I,
    ) -> Result<()> {
        for chunk in domains.by_ref().chunks(32_784).into_iter() {
            self.update_max_inlinks_domains_chunk(chunk)?;
        }

        Ok(())
    }

    fn update_max_inlinks_domains_chunk<'a, I: Iterator<Item = &'a Domain>>(
        &self,
        domains: I,
    ) -> Result<()> {
        // Collect domains into a Vec to use them later in the SQL
        let domains: Rc<Vec<Value>> =
            Rc::new(domains.map(|d| d.0.to_string()).map(Value::from).collect());

        // Start transaction
        let tx = self.tx();

        // Create a temporary table to hold the max incoming links for each domain
        tx.execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS temp_domain AS 
            SELECT domain, MAX(incoming_links) as max_incoming_links
            FROM url 
            WHERE domain IN ?1 AND status = ?2
            GROUP BY domain;",
            (domains.clone(), UrlStatus::Pending),
        )?;

        // Update the domain table
        tx.execute(
            "UPDATE domain 
            SET max_incoming_links = IFNULL(
                (SELECT max_incoming_links FROM temp_domain WHERE temp_domain.domain = domain.domain), 0
            ) 
            WHERE domain IN ?1;",
            (domains, ),
        )?;

        // Drop the temporary table
        tx.execute("DROP TABLE temp_domain;", [])?;

        Ok(())
    }

    pub fn insert_urls(&self, crawled_domain: &Domain, urls: &[Url]) -> Result<()> {
        // insert discovered URLs if not already in database
        // update incoming link counts for discovered URLs
        // if count is above prev max for domain, update max
        let domain = Url::from(crawled_domain.0.clone());
        let mut prepared_diff_domain = self.tx().prepare(
            "INSERT INTO url (url, domain, status, incoming_links)
            VALUES (?1, ?2, ?3, 1)
            ON CONFLICT (url) DO UPDATE SET incoming_links = incoming_links + 1;",
        )?;
        let mut prepared_same_domain = self.tx().prepare(
            "INSERT INTO url (url, domain, status, incoming_links)
            VALUES (?1, ?2, ?3, 0)
            ON CONFLICT (url) DO NOTHING;",
        )?;
        let mut prepared_insert_domain = self.tx().prepare(
            "INSERT INTO domain (domain, max_incoming_links, status)
            VALUES (?1, 0, ?2)
            ON CONFLICT (domain) DO NOTHING;",
        )?;

        let mut unique_domains = HashSet::new();

        for url in urls {
            if domain.domain() != url.domain() {
                prepared_diff_domain.execute((url.full(), url.domain(), UrlStatus::Pending))?;
            } else {
                prepared_same_domain.execute((url.full(), url.domain(), UrlStatus::Pending))?;
            }

            unique_domains.insert(url.domain().to_string());
        }

        for domain in unique_domains {
            prepared_insert_domain.execute((domain, DomainStatus::Pending))?;
        }

        Ok(())
    }

    pub fn update_url_status(&self, url_responses: &[UrlResponse]) -> Result<()> {
        let mut prepared_update_status_url = self
            .tx()
            .prepare("UPDATE url SET status = ?1, error_code = ?2 WHERE url = ?3;")?;

        let mut prepared_insert_redirect = self.tx().prepare(
            "INSERT INTO redirect (from_url, to_url)
            VALUES (?1, ?2)
            ON CONFLICT (from_url, to_url) DO NOTHING;",
        )?;

        let mut prepared_insert_url = self.tx().prepare(
            "INSERT INTO url (url, domain, status, incoming_links)
            VALUES (?1, ?2, ?3, 0)
            ON CONFLICT (url) DO NOTHING;",
        )?;

        // update status of URLs
        for url_res in url_responses {
            match url_res {
                UrlResponse::Success { url } => {
                    prepared_update_status_url.execute((
                        UrlStatus::Done,
                        None::<u16>,
                        url.full(),
                    ))?;
                }
                UrlResponse::Failed { url, status_code } => {
                    prepared_update_status_url.execute((
                        UrlStatus::Failed,
                        status_code,
                        url.full(),
                    ))?;
                }
                UrlResponse::Redirected { url, new_url } => {
                    prepared_update_status_url.execute((
                        UrlStatus::Done,
                        None::<u16>,
                        url.full(),
                    ))?;
                    prepared_insert_redirect.execute((url.full(), new_url.full()))?;
                    prepared_insert_url.execute((
                        new_url.full(),
                        new_url.domain(),
                        UrlStatus::Done,
                    ))?;
                }
            }
        }

        Ok(())
    }

    pub fn set_domain_status(&self, domain: &Domain, status: DomainStatus) -> Result<()> {
        self.tx().execute(
            "UPDATE domain SET status = ?1 WHERE domain = ?2;",
            (status, &domain.0),
        )?;

        Ok(())
    }

    pub fn sample_domains(&self, num_jobs: usize) -> Result<Vec<Domain>> {
        // weighted sample from domains that are not currently being crawled
        // see https://www.kaggle.com/code/kotamori/random-sample-with-weights-on-sql/notebook for details on math
        let mut stmt = self.tx().prepare(
            "SELECT domain, -log((abs(random()) % 1000000 + 0.5) / 1000000.0) / (max_incoming_links + 1) as priority FROM domain
            WHERE status = ?1
            ORDER BY priority DESC
            LIMIT ?2;",
        )?;

        // dfs sample
        // let mut stmt = self.tx().prepare(
        //     "SELECT domain FROM domain
        //     WHERE status = ?1
        //     LIMIT ?2;",
        // )?;

        let rows = stmt.query_map((DomainStatus::Pending, num_jobs), |row| {
            row.get::<_, String>(0)
        })?;

        let mut domains = Vec::new();
        for domain in rows {
            domains.push(Domain(domain?));
        }

        Ok(domains)
    }

    pub fn prepare_jobs(&self, domains: &[Domain], urls_per_job: usize) -> Result<Vec<Job>> {
        let mut jobs = Vec::new();
        // get URLs from sampled domains
        let mut stmt = self.tx().prepare(
            "SELECT url FROM url
            WHERE domain = ?1 AND status = ?2
            ORDER BY incoming_links DESC
            LIMIT ?3;",
        )?;

        let mut prepared_update_status_url = self
            .tx()
            .prepare("UPDATE url SET status = ?1 WHERE url = ?2;")?;
        let mut prepared_update_status_domain = self
            .tx()
            .prepare("UPDATE domain SET status = ?1 WHERE domain = ?2;")?;

        for domain in domains {
            let mut urls = Vec::new();

            let rows = stmt.query_map((&domain.0, UrlStatus::Pending, urls_per_job), |row| {
                row.get::<_, String>(0)
            })?;

            prepared_update_status_domain.execute((DomainStatus::CrawlInProgress, &domain.0))?;

            for url in rows {
                let url = url?;

                prepared_update_status_url.execute((UrlStatus::Crawling, url.clone()))?;

                urls.push(url.into());
            }

            jobs.push(Job {
                domain: domain.clone(),
                fetch_sitemap: true, // TODO: only fetch sitemap if we haven't already
                urls: urls.into(),
            });
        }

        Ok(jobs)
    }
}
