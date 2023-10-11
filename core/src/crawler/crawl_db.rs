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

use hashbrown::HashMap;
use rand::Rng;
use std::hash::Hash;
use std::path::PathBuf;
use std::{collections::BinaryHeap, path::Path};
use url::Url;

use super::{Domain, DomainCrawled, Job, Result, UrlToInsert, MAX_URL_LEN_BYTES};

const MAX_URL_DB_SIZE_BYTES: u64 = 20 * 1024 * 1024 * 1024; // 20GB

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum UrlStatus {
    Pending,
    Crawling,
    Failed { status_code: Option<u16> },
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DomainStatus {
    Pending,
    CrawlInProgress,
}

struct SampledItem<T> {
    item: T,
    priority: f64,
}

impl<T> PartialEq for SampledItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl<T> Eq for SampledItem<T> {}

impl<T> PartialOrd for SampledItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for SampledItem<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.total_cmp(&other.priority)
    }
}

fn weighted_sample<T>(items: impl Iterator<Item = (T, f64)>, num_items: usize) -> Vec<(T, f64)> {
    let mut sampled_items: BinaryHeap<SampledItem<T>> = BinaryHeap::with_capacity(num_items);

    let mut rng = rand::thread_rng();

    for (item, weight) in items {
        // see https://www.kaggle.com/code/kotamori/random-sample-with-weights-on-sql/notebook for details on math
        let priority = -(rng.gen::<f64>().abs() + f64::EPSILON).ln() / (weight + 1.0);

        if sampled_items.len() < num_items {
            sampled_items.push(SampledItem { item, priority });
        } else if let Some(mut max) = sampled_items.peek_mut() {
            if priority < max.priority {
                max.item = item;
                max.priority = priority;
            }
        }
    }

    sampled_items
        .into_iter()
        .map(|s| (s.item, s.priority))
        .collect()
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct UrlState {
    weight: f64,
    status: UrlStatus,
}

impl Default for UrlState {
    fn default() -> Self {
        Self {
            weight: 0.0,
            status: UrlStatus::Pending,
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct DomainState {
    weight: f64,
    status: DomainStatus,
}

impl Default for DomainState {
    fn default() -> Self {
        Self {
            weight: 1.0,
            status: DomainStatus::Pending,
        }
    }
}
struct CachedValue<T> {
    value: T,
    last_updated: std::time::Instant,
}

impl<T> From<T> for CachedValue<T> {
    fn from(value: T) -> Self {
        Self {
            value,
            last_updated: std::time::Instant::now(),
        }
    }
}

struct UrlStateDbShard {
    db: rocksdb::DB,
    /// from rocksdb docs: "Cache must outlive DB instance which uses it."
    _cache: rocksdb::Cache,
    approx_size_bytes: CachedValue<u64>,
}

impl UrlStateDbShard {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut options = rocksdb::Options::default();

        options.create_if_missing(true);

        options.set_max_background_jobs(8);
        options.increase_parallelism(8);
        options.set_max_subcompactions(8);
        options.set_write_buffer_size(512 * 1024 * 1024);
        options.set_allow_mmap_reads(true);
        options.set_allow_mmap_writes(true);
        options.set_target_file_size_base(512 * 1024 * 1024); // 512 MB
        options.set_target_file_size_multiplier(10);

        options.set_max_write_buffer_number(4);
        options.set_min_write_buffer_number_to_merge(2);
        options.set_level_zero_slowdown_writes_trigger(-1);
        options.set_level_zero_stop_writes_trigger(-1);

        // some recommended settings (https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
        options.set_level_compaction_dynamic_level_bytes(true);
        options.set_bytes_per_sync(1048576);
        let mut block_options = rocksdb::BlockBasedOptions::default();
        let cache = rocksdb::Cache::new_lru_cache(1024 * 1024 * 1024); // 1GB
        block_options.set_block_cache(&cache);
        block_options.set_ribbon_filter(10.0);
        block_options.set_format_version(5);
        block_options.set_block_size(16 * 1024);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);

        options.set_block_based_table_factory(&block_options);
        options.set_optimize_filters_for_hits(true);

        options.set_compression_type(rocksdb::DBCompressionType::None);

        let db = rocksdb::DB::open(&options, path.as_ref().join("urls"))?;
        let approx_size_bytes = db
            .property_int_value(rocksdb::properties::TOTAL_SST_FILES_SIZE)?
            .unwrap_or_default()
            .into();

        Ok(Self {
            db,
            approx_size_bytes,
            _cache: cache,
        })
    }

    pub fn put_batch(&mut self, batch: &[(Domain, Vec<(UrlString, UrlState)>)]) -> Result<()> {
        let mut rocksdb_batch = rocksdb::WriteBatch::default();

        for (domain, urls) in batch {
            let domain_bytes = bincode::serialize(domain)?;

            for (url, state) in urls {
                let url_bytes = bincode::serialize(url)?;
                if url_bytes.len() > MAX_URL_LEN_BYTES {
                    continue;
                }

                let key_bytes = [domain_bytes.as_slice(), url_bytes.as_slice()].concat();

                let state_bytes = bincode::serialize(state)?;

                rocksdb_batch.put(key_bytes, state_bytes);
            }
        }

        let mut write_options = rocksdb::WriteOptions::default();
        write_options.disable_wal(true);

        self.db.write_opt(rocksdb_batch, &write_options)?;

        Ok(())
    }

    pub fn get_all_urls(&self, domain: &Domain) -> Result<Vec<(UrlString, UrlState)>> {
        let domain_bytes = bincode::serialize(domain)?;

        let start = [domain_bytes.as_slice(), &[0].repeat(MAX_URL_LEN_BYTES)].concat();

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &start,
            rocksdb::Direction::Forward,
        ));

        Ok(iter
            .take_while(|r| {
                if let Ok((key, _)) = r.as_ref() {
                    if domain_bytes.len() >= key.len() {
                        return false;
                    }

                    key[..domain_bytes.len()] == domain_bytes
                } else {
                    false
                }
            })
            .filter_map(|r| {
                let (key, value) = r.ok()?;

                if domain_bytes.len() >= key.len() {
                    return None;
                }

                let url = bincode::deserialize(&key[domain_bytes.len()..]).ok()?;

                let state = bincode::deserialize(&value).ok()?;

                Some((url, state))
            })
            .collect())
    }

    pub fn approximate_size_bytes(&mut self) -> Result<u64> {
        if self.approx_size_bytes.last_updated.elapsed().as_secs() > 10 {
            self.approx_size_bytes = self
                .db
                .property_int_value(rocksdb::properties::TOTAL_SST_FILES_SIZE)?
                .unwrap_or_default()
                .into();
        }

        Ok(self.approx_size_bytes.value)
    }

    fn multi_get(&self, urls: &[(Domain, UrlString)]) -> Result<HashMap<UrlString, UrlState>> {
        let mut res = HashMap::new();

        let mut keys = Vec::with_capacity(urls.len());

        for (domain, url) in urls {
            let domain_bytes = bincode::serialize(domain)?;
            let url_bytes = bincode::serialize(url)?;

            let key_bytes = [domain_bytes.as_slice(), url_bytes.as_slice()].concat();

            keys.push(key_bytes);
        }

        for (val, (_, url)) in self
            .db
            .multi_get(keys.into_iter())
            .into_iter()
            .zip(urls.iter())
        {
            if let Some(val) = val? {
                let state: UrlState = bincode::deserialize(&val)?;
                res.insert(url.clone(), state);
            }
        }

        Ok(res)
    }
}

struct UrlStateDb {
    shards: Vec<UrlStateDbShard>,
    path: PathBuf,
}

impl UrlStateDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        if path.as_ref().exists() {
            let mut shard_names = Vec::new();
            for entry in std::fs::read_dir(path.as_ref())? {
                let entry = entry?;
                let path = entry.path();

                if path.is_dir() {
                    shard_names.push(path.to_str().unwrap().to_string());
                }
            }

            shard_names.sort();

            let mut shards = Vec::new();

            for shard_name in shard_names {
                shards.push(UrlStateDbShard::open(shard_name)?);
            }

            Ok(Self {
                shards,
                path: path.as_ref().to_path_buf(),
            })
        } else {
            let shard_id =
                chrono::Utc::now().to_rfc3339() + "_" + uuid::Uuid::new_v4().to_string().as_str();
            let shard_path = path.as_ref().join(shard_id);

            std::fs::create_dir_all(&shard_path)?;

            let shard = UrlStateDbShard::open(&shard_path)?;

            Ok(Self {
                shards: vec![shard],
                path: path.as_ref().to_path_buf(),
            })
        }
    }

    fn new_shard(&mut self) -> Result<()> {
        let shard_id =
            chrono::Utc::now().to_rfc3339() + "_" + uuid::Uuid::new_v4().to_string().as_str();
        let shard_path = self.path.as_path().join(shard_id);

        std::fs::create_dir_all(&shard_path)?;

        let shard = UrlStateDbShard::open(&shard_path)?;

        self.shards.push(shard);

        Ok(())
    }

    pub fn put_batch(&mut self, batch: &[(Domain, Vec<(UrlString, UrlState)>)]) -> Result<()> {
        if self.shards.is_empty() {
            self.new_shard()?;
        }
        let last_shard = self.shards.last_mut().unwrap();

        if last_shard.approximate_size_bytes()? > MAX_URL_DB_SIZE_BYTES {
            self.new_shard()?;
        }

        self.shards.last_mut().unwrap().put_batch(batch)?;

        Ok(())
    }

    pub fn get_all_urls(&self, domain: &Domain) -> Result<HashMap<UrlString, UrlState>> {
        let mut res = HashMap::new();

        for shard in &self.shards {
            for (url, state) in shard.get_all_urls(domain)? {
                res.insert(url, state);
            }
        }

        Ok(res)
    }

    fn multi_get(&self, urls: &[(Domain, UrlString)]) -> Result<HashMap<UrlString, UrlState>> {
        let mut res = HashMap::new();

        for shard in &self.shards {
            res.extend(shard.multi_get(urls)?.into_iter());
        }

        Ok(res)
    }
}

struct DomainStateDb {
    db: rocksdb::DB,
}

impl DomainStateDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut options = rocksdb::Options::default();

        options.create_if_missing(true);

        let mut block_options = rocksdb::BlockBasedOptions::default();
        block_options.set_ribbon_filter(10.0);
        block_options.set_format_version(5);

        options.set_block_based_table_factory(&block_options);
        options.set_optimize_filters_for_hits(true);
        options.set_max_background_jobs(8);
        options.increase_parallelism(8);
        options.set_write_buffer_size(512 * 1024 * 1024);
        options.set_allow_mmap_reads(true);
        options.set_allow_mmap_writes(true);
        options.set_max_subcompactions(8);

        options.set_level_zero_slowdown_writes_trigger(-1);
        options.set_level_zero_stop_writes_trigger(-1);

        let db = rocksdb::DB::open(&options, path.as_ref())?;

        Ok(Self { db })
    }

    fn multi_get(&self, domains: &[Domain]) -> Result<HashMap<Domain, DomainState>> {
        let mut res = HashMap::new();
        let mut domain_bytes = Vec::with_capacity(domains.len());

        for domain in domains {
            domain_bytes.push(bincode::serialize(domain)?);
        }

        for (val, domain) in self
            .db
            .multi_get(domain_bytes.into_iter())
            .into_iter()
            .zip(domains.iter())
        {
            if let Some(val) = val? {
                let domain_state: DomainState = bincode::deserialize(&val)?;
                res.insert(domain.clone(), domain_state);
            }
        }

        Ok(res)
    }

    fn put_batch(&self, batch: &[(Domain, DomainState)]) -> Result<()> {
        let mut rocksdb_batch = rocksdb::WriteBatch::default();

        for (domain, state) in batch {
            let domain_bytes = bincode::serialize(domain)?;
            let state_bytes = bincode::serialize(state)?;

            rocksdb_batch.put(domain_bytes, state_bytes);
        }

        let mut write_options = rocksdb::WriteOptions::default();
        write_options.disable_wal(true);

        self.db.write_opt(rocksdb_batch, &write_options)?;

        Ok(())
    }

    fn iter(&self) -> impl Iterator<Item = (Domain, DomainState)> + '_ {
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);

        iter.filter_map(|r| {
            let (key, value) = r.ok()?;
            let domain = bincode::deserialize(&key).ok()?;
            let state = bincode::deserialize(&value).ok()?;

            Some((domain, state))
        })
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
struct UrlString(String);

impl From<&Url> for UrlString {
    fn from(url: &Url) -> Self {
        Self(url.as_str().to_string())
    }
}

impl From<Url> for UrlString {
    fn from(url: Url) -> Self {
        Self(url.as_str().to_string())
    }
}

impl TryFrom<&UrlString> for Url {
    type Error = anyhow::Error;
    fn try_from(url: &UrlString) -> Result<Self, Self::Error> {
        Ok(Url::parse(&url.0)?)
    }
}

pub struct CrawlDb {
    domain_state: DomainStateDb,
    urls: UrlStateDb,
}

impl CrawlDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            domain_state: DomainStateDb::open(path.as_ref().join("domains"))?,
            urls: UrlStateDb::open(path.as_ref().join("urls"))?,
        })
    }

    pub fn insert_urls(&mut self, domain_urls: HashMap<Domain, Vec<UrlToInsert>>) -> Result<()> {
        let mut url_batches = Vec::new();
        let mut domain_batches = Vec::new();

        let domains = domain_urls.keys().cloned().collect::<Vec<_>>();
        let domain_states = self.domain_state.multi_get(&domains)?;
        let mut url_states = self.urls.multi_get(
            domain_urls
                .iter()
                .flat_map(|(domain, urls)| {
                    urls.iter()
                        .map(|url| (domain.clone(), UrlString::from(&url.url)))
                })
                .collect::<Vec<_>>()
                .as_slice(),
        )?;

        for (domain, urls) in domain_urls.into_iter() {
            let mut domain_state = domain_states.get(&domain).cloned().unwrap_or_default();

            let mut updated_url_states = Vec::new();

            for url in urls {
                if url.url.as_str().len() > MAX_URL_LEN_BYTES {
                    continue;
                }

                match url_states.get_mut(&UrlString::from(&url.url)) {
                    Some(state) => {
                        state.weight += url.weight;
                        domain_state.weight += url.weight;
                        if url.weight > 0.0 {
                            updated_url_states.push((UrlString::from(&url.url), state.clone()));
                        }
                    }
                    None => {
                        let mut state = UrlState::default();
                        state.weight += url.weight;
                        domain_state.weight += url.weight;

                        updated_url_states.push((UrlString::from(&url.url), state));
                    }
                };
            }

            url_batches.push((domain.clone(), updated_url_states));
            domain_batches.push((domain, domain_state));
        }

        self.urls.put_batch(&url_batches)?;
        self.domain_state.put_batch(&domain_batches)?;

        Ok(())
    }

    fn set_domain_status(&mut self, domains: &[Domain], status: DomainStatus) -> Result<()> {
        let domain_states = self.domain_state.multi_get(domains)?;

        let mut batches = Vec::new();

        for domain in domains {
            let mut domain_state = domain_states.get(domain).cloned().unwrap_or_default();
            domain_state.status = status;

            batches.push((domain.clone(), domain_state));
        }

        self.domain_state.put_batch(&batches)?;

        Ok(())
    }

    pub fn mark_jobs_complete(&mut self, domains: &[DomainCrawled]) -> Result<()> {
        let domain_states = self.domain_state.multi_get(
            domains
                .iter()
                .map(|d| d.domain.clone())
                .collect::<Vec<_>>()
                .as_slice(),
        )?;

        let mut batches = Vec::new();

        for domain in domains {
            let mut domain_state = domain_states
                .get(&domain.domain)
                .cloned()
                .unwrap_or_default();
            domain_state.status = DomainStatus::Pending;
            domain_state.weight -= domain.budget_used;

            batches.push((domain.domain.clone(), domain_state));
        }

        self.domain_state.put_batch(&batches)?;

        Ok(())
    }

    pub fn sample_domains(&mut self, num_jobs: usize) -> Result<Vec<Domain>> {
        let sampled = weighted_sample(
            self.domain_state.iter().filter_map(|(domain, state)| {
                if state.status == DomainStatus::Pending {
                    Some((domain, state.weight))
                } else {
                    None
                }
            }),
            num_jobs,
        );

        let domains = sampled.into_iter().map(|(d, _)| d).collect::<Vec<_>>();

        self.set_domain_status(&domains, DomainStatus::CrawlInProgress)?;

        Ok(domains)
    }

    pub fn prepare_jobs(&mut self, domains: &[Domain], urls_per_job: usize) -> Result<Vec<Job>> {
        let mut jobs = Vec::with_capacity(domains.len());
        let mut url_batches = Vec::new();

        let domain_states = self.domain_state.multi_get(domains)?;

        for domain in domains {
            let urls = self.urls.get_all_urls(domain)?;

            let available_urls: Vec<_> = urls
                .iter()
                .filter_map(|(url, state)| {
                    if state.status == UrlStatus::Pending {
                        Some((url.clone(), state.weight))
                    } else {
                        None
                    }
                })
                .collect();

            let sampled: Vec<_> = weighted_sample(
                available_urls.iter().map(|(url, weight)| (url, *weight)),
                urls_per_job,
            );

            let mut new_url_states = Vec::new();

            for (url, _) in &sampled {
                let mut state = urls.get(*url).cloned().unwrap_or_default();
                state.status = UrlStatus::Crawling;

                new_url_states.push(((*url).clone(), state));
            }

            url_batches.push((domain.clone(), new_url_states));

            let domain_state = domain_states.get(domain).cloned().unwrap_or_default();

            let job = Job {
                domain: domain.clone(),
                fetch_sitemap: false, // todo: fetch for new sites
                urls: sampled
                    .iter()
                    .filter_map(|(url, _)| Url::try_from(*url).ok())
                    .collect(),
                weight_budget: domain_state.weight,
            };

            jobs.push(job);
        }

        self.urls.put_batch(&url_batches)?;

        Ok(jobs)
    }
}

#[cfg(test)]
mod tests {
    use crate::gen_temp_path;

    use super::*;

    fn insert_seed_urls(urls: &[Url], db: &mut CrawlDb) {
        let mut domain_urls = HashMap::new();

        for url in urls {
            let domain = Domain::from(url);
            let url_to_insert = UrlToInsert {
                url: url.clone(),
                weight: 0.0,
            };

            domain_urls
                .entry(domain)
                .or_insert_with(Vec::new)
                .push(url_to_insert);
        }

        db.insert_urls(domain_urls).unwrap();
    }

    #[test]
    fn sampling() {
        let items: Vec<(usize, f64)> = vec![(0, 1.0), (1, 2.0), (2, 3.0), (3, 4.0)];
        let sampled = weighted_sample(items.iter().map(|(i, w)| (i, *w)), 10);
        assert_eq!(sampled.len(), items.len());

        let items: Vec<(usize, f64)> = vec![(0, 1.0), (1, 2.0), (2, 3.0), (3, 4.0)];
        let sampled = weighted_sample(items.iter().map(|(i, w)| (i, *w)), 1);
        assert_eq!(sampled.len(), 1);

        let items: Vec<(usize, f64)> = vec![(0, 1.0), (1, 2.0), (2, 3.0), (3, 4.0)];
        let sampled = weighted_sample(items.iter().map(|(i, w)| (i, *w)), 0);
        assert_eq!(sampled.len(), 0);

        let items: Vec<(usize, f64)> = vec![(0, 1000000000.0), (1, 2.0)];
        let sampled = weighted_sample(items.iter().map(|(i, w)| (i, *w)), 1);
        assert_eq!(sampled.len(), 1);
        assert_eq!(*sampled[0].0, 0);
    }

    #[test]
    fn simple_politeness() {
        let mut db = CrawlDb::open(gen_temp_path()).unwrap();

        let urls = vec![Url::parse("https://example.com").unwrap()];
        insert_seed_urls(&urls, &mut db);

        let domain = Domain::from(&Url::parse("https://example.com").unwrap());

        let sample = db.sample_domains(128).unwrap();

        assert_eq!(sample.len(), 1);
        assert_eq!(&sample[0], &domain);
        assert_eq!(
            db.domain_state.multi_get(&[domain.clone()]).unwrap()[&domain].status,
            DomainStatus::CrawlInProgress
        );

        let new_sample = db.sample_domains(128).unwrap();
        assert_eq!(new_sample.len(), 0);
    }

    #[test]
    fn get_all_urls() {
        let mut db = CrawlDb::open(gen_temp_path()).unwrap();

        let urls = vec![
            Url::parse("https://a.com").unwrap(),
            Url::parse("https://b.com").unwrap(),
        ];
        insert_seed_urls(&urls, &mut db);

        let domain = Domain::from(&Url::parse("https://a.com").unwrap());

        let urls: Vec<_> = db.urls.get_all_urls(&domain).unwrap().into_iter().collect();

        assert_eq!(urls.len(), 1);
        assert_eq!(
            urls[0].0,
            UrlString::from(&Url::parse("https://a.com").unwrap())
        );
    }
}
