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

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, VecDeque},
    hash::Hash,
    num::NonZeroUsize,
    path::Path,
};

use lru::LruCache;
use rand::Rng;
use rkyv::Deserialize;
use rocksdb::BlockBasedOptions;

use crate::webpage::Url;

use super::{Domain, Job, Result, UrlResponse};

#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Clone, PartialEq, Eq)]
#[archive(check_bytes)]
pub enum UrlStatus {
    Pending,
    Crawling,
    Failed { status_code: Option<u16> },
    Done,
}

#[derive(Clone, PartialEq, Eq)]
pub enum DomainStatus {
    Pending,
    CrawlInProgress,
}

struct IdTable<T> {
    t2id: rocksdb::DB,
    id2t: rocksdb::DB,

    next_id: u64,

    t2id_cache: LruCache<T, u64>,
    id2t_cache: LruCache<u64, T>,

    _marker: std::marker::PhantomData<T>,
}

impl<T> IdTable<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Hash + Eq + Clone,
{
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.increase_parallelism(8);
        options.set_write_buffer_size(256 * 1024 * 1024); // 256 MB memtable
        options.set_max_write_buffer_number(8);

        let mut block_options = BlockBasedOptions::default();
        block_options.set_bloom_filter(64.0, true);

        options.set_block_based_table_factory(&block_options);

        // create dir if not exists
        std::fs::create_dir_all(path.as_ref())?;

        let _ = rocksdb::DB::destroy(&options, path.as_ref().join("t2id"));
        let _ = rocksdb::DB::destroy(&options, path.as_ref().join("id2t"));

        let t2id = rocksdb::DB::open(&options, path.as_ref().join("t2id"))?;
        let id2t = rocksdb::DB::open(&options, path.as_ref().join("id2t"))?;

        Ok(Self {
            t2id,
            id2t,
            next_id: 0,

            t2id_cache: LruCache::new(NonZeroUsize::new(1_000_000).unwrap()),
            id2t_cache: LruCache::new(NonZeroUsize::new(1_000_000).unwrap()),

            _marker: std::marker::PhantomData,
        })
    }

    pub fn bulk_ids(&mut self, items: impl Iterator<Item = T>) -> Result<Vec<u64>> {
        let mut ids = Vec::new();

        let mut batch_id2t = rocksdb::WriteBatch::default();
        let mut batch_t2id = rocksdb::WriteBatch::default();

        for item in items {
            // check cache
            if let Some(id) = self.t2id_cache.get(&item) {
                ids.push(*id);
                continue;
            }

            // check if item exists
            let item_bytes = bincode::serialize(&item)?;
            let id = self.t2id.get(&item_bytes)?;
            if let Some(id) = id {
                let id = bincode::deserialize(&id)?;

                // update cache
                self.t2id_cache.put(item, id);

                ids.push(id);
                continue;
            }

            // insert item
            let id = self.next_id;
            self.next_id += 1;
            let id_bytes = bincode::serialize(&id)?;
            batch_t2id.put(&item_bytes, &id_bytes);
            batch_id2t.put(&id_bytes, &item_bytes);

            // update cache
            self.t2id_cache.put(item, id);

            ids.push(id);
        }

        let mut write_options = rocksdb::WriteOptions::default();
        write_options.set_sync(false);
        write_options.disable_wal(true);

        self.id2t.write_opt(batch_id2t, &write_options)?;
        self.t2id.write_opt(batch_t2id, &write_options)?;

        Ok(ids)
    }

    pub fn id(&mut self, item: T) -> Result<u64> {
        // check cache
        if let Some(id) = self.t2id_cache.get(&item) {
            return Ok(*id);
        }

        // check if item exists
        let item_bytes = bincode::serialize(&item)?;
        let id = self.t2id.get(&item_bytes)?;
        if let Some(id) = id {
            let id = bincode::deserialize(&id)?;

            // update cache
            self.t2id_cache.put(item.clone(), id);
            self.id2t_cache.put(id, item);

            return Ok(id);
        }

        // insert item
        let id = self.next_id;
        self.next_id += 1;
        let id_bytes = bincode::serialize(&id)?;

        let mut write_options = rocksdb::WriteOptions::default();
        write_options.set_sync(false);
        write_options.disable_wal(true);

        self.t2id.put_opt(&item_bytes, &id_bytes, &write_options)?;
        self.id2t.put_opt(&id_bytes, &item_bytes, &write_options)?;

        // update cache
        self.t2id_cache.put(item.clone(), id);
        self.id2t_cache.put(id, item);

        Ok(id)
    }

    pub fn value(&mut self, id: u64) -> Result<Option<T>> {
        // check cache
        if let Some(value) = self.id2t_cache.get(&id) {
            return Ok(Some(value.clone()));
        }

        let id_bytes = bincode::serialize(&id)?;
        let value_bytes = self.id2t.get(id_bytes)?;
        if let Some(value_bytes) = value_bytes {
            let value: T = bincode::deserialize(&value_bytes)?;

            // update cache
            self.t2id_cache.put(value.clone(), id);
            self.id2t_cache.put(id, value.clone());

            return Ok(Some(value));
        }

        Ok(None)
    }
}

struct SampledItem<'a, T> {
    item: &'a T,
    priority: f64,
}

impl<'a, T> PartialEq for SampledItem<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl<'a, T> Eq for SampledItem<'a, T> {}

impl<'a, T> PartialOrd for SampledItem<'a, T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.priority.partial_cmp(&other.priority)
    }
}

impl<'a, T> Ord for SampledItem<'a, T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .partial_cmp(&other.priority)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

fn weighted_sample<'a, T: 'a>(
    items: impl Iterator<Item = (&'a T, f64)>,
    num_items: usize,
) -> Vec<&'a T> {
    let mut sampled_items: BinaryHeap<Reverse<SampledItem<T>>> =
        BinaryHeap::with_capacity(num_items);

    let mut rng = rand::thread_rng();

    for (item, weight) in items {
        // see https://www.kaggle.com/code/kotamori/random-sample-with-weights-on-sql/notebook for details on math
        // let priority =  -log((abs(random()) % 1000000 + 0.5) / 1000000.0) / (max_incoming_links + 1)
        let priority = -(rng.gen::<f64>().abs() + 0.5).ln() / (weight + 1.0);
        if sampled_items.len() < num_items {
            sampled_items.push(Reverse(SampledItem { item, priority }));
        } else if let Some(mut min) = sampled_items.peek_mut() {
            if min.0.priority < priority {
                min.0.item = item;
                min.0.priority = priority;
            }
        }
    }

    sampled_items.into_iter().map(|s| s.0.item).collect()
}

#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
#[archive(check_bytes)]
struct UrlState {
    weight: f64,
    status: UrlStatus,
}
struct DomainState {
    weight: f64,
    status: DomainStatus,
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
)]
#[archive(check_bytes)]
pub struct DomainId(u64);

impl From<u64> for DomainId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

#[derive(
    rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Clone, Copy, Debug, PartialEq, Eq, Hash,
)]
#[archive(check_bytes)]
#[archive_attr(derive(PartialEq, Eq, Hash))]
struct UrlId(u64);

impl From<u64> for UrlId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

struct RedirectDb {
    inner: rocksdb::DB,
}

impl RedirectDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        rocksdb::DB::destroy(&rocksdb::Options::default(), path.as_ref())?;

        let inner = rocksdb::DB::open_default(path.as_ref())?;

        Ok(Self { inner })
    }

    pub fn put(&self, from: &Url, to: &Url) -> Result<()> {
        let url_bytes = bincode::serialize(from)?;
        let redirect_bytes = bincode::serialize(to)?;

        let mut write_options = rocksdb::WriteOptions::default();
        write_options.set_sync(false);
        write_options.disable_wal(true);
        self.inner
            .put_opt(url_bytes, redirect_bytes, &write_options)?;

        Ok(())
    }
}

struct UrlMap {
    inner: rocksdb::DB, // DomainId -> HashMap<UrlId, UrlState>
}

impl UrlMap {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        rocksdb::DB::destroy(&rocksdb::Options::default(), path.as_ref())?;

        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.increase_parallelism(8);
        options.set_write_buffer_size(256 * 1024 * 1024); // 256 MB memtable
        options.set_max_write_buffer_number(8);

        let mut block_options = BlockBasedOptions::default();
        block_options.set_bloom_filter(64.0, true);

        options.set_block_based_table_factory(&block_options);

        let inner = rocksdb::DB::open(&options, path.as_ref())?;

        Ok(Self { inner })
    }

    pub fn put(&self, domain_id: DomainId, map: HashMap<UrlId, UrlState>) -> Result<()> {
        let domain_bytes = rkyv::to_bytes::<_, 256>(&domain_id).expect("failed to serialize");
        let map_bytes = rkyv::to_bytes::<_, 256>(&map).expect("failed to serialize");

        let mut write_options = rocksdb::WriteOptions::default();
        write_options.set_sync(false);
        write_options.disable_wal(true);

        self.inner
            .put_opt(domain_bytes, map_bytes, &write_options)?;

        Ok(())
    }

    pub fn get(&self, domain_id: DomainId) -> Result<Option<HashMap<UrlId, UrlState>>> {
        let domain_bytes = rkyv::to_bytes::<_, 256>(&domain_id).expect("failed to serialize");

        let map_bytes = self.inner.get(domain_bytes)?;

        match map_bytes {
            Some(bytes) => {
                let map = rkyv::check_archived_root::<HashMap<UrlId, UrlState>>(&bytes)
                    .expect("failed to deserialize")
                    .deserialize(&mut rkyv::Infallible)
                    .expect("failed to deserialize");
                Ok(Some(map))
            }
            None => Ok(None),
        }
    }
}

pub struct CrawlDb {
    url_ids: IdTable<Url>,
    domain_ids: IdTable<Domain>,

    redirects: RedirectDb,

    domain_state: HashMap<DomainId, DomainState>,

    urls: UrlMap,
}

impl CrawlDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let url_ids = IdTable::open(path.as_ref().join("url_ids"))?;
        let domain_ids = IdTable::open(path.as_ref().join("domain_ids"))?;
        let url_map = UrlMap::open(path.as_ref().join("url_map"))?;
        let redirects = RedirectDb::open(path.as_ref().join("redirects"))?;

        Ok(Self {
            url_ids,
            domain_ids,
            redirects,
            domain_state: HashMap::new(),
            urls: url_map,
        })
    }

    pub fn insert_seed_urls(&mut self, urls: &[Url]) -> Result<()> {
        let mut bundled: HashMap<Domain, Vec<Url>> = HashMap::new();

        for url in urls {
            bundled
                .entry(url.domain().to_string().into())
                .or_default()
                .push(url.clone());
        }

        for (domain, urls) in bundled {
            let domain_id = self.domain_ids.id(domain)?.into();
            let mut map = self.urls.get(domain_id)?.unwrap_or_default();
            for url in urls {
                let domain_id = self.domain_ids.id(url.clone().into())?.into();
                let url_id = self.url_ids.id(url.clone())?.into();

                self.domain_state
                    .entry(domain_id)
                    .or_insert_with(|| DomainState {
                        weight: 0.0,
                        status: DomainStatus::Pending,
                    });

                map.insert(
                    url_id,
                    UrlState {
                        weight: 0.0,
                        status: UrlStatus::Pending,
                    },
                );
            }

            self.urls.put(domain_id, map)?;
        }

        Ok(())
    }

    pub fn insert_urls(&mut self, crawled_domain: &Domain, urls: &[Url]) -> Result<()> {
        let mut domains: HashMap<Domain, Vec<Url>> = HashMap::new();

        for url in urls {
            let domain: Domain = url.domain().to_string().into();

            domains.entry(domain).or_default().push(url.clone());
        }

        let domain_ids: Vec<DomainId> = self
            .domain_ids
            .bulk_ids(domains.keys().cloned())?
            .into_iter()
            .map(DomainId::from)
            .collect();

        self.url_ids.bulk_ids(domains.values().flatten().cloned())?;

        for (domain_id, (domain, urls)) in domain_ids.into_iter().zip(domains.into_iter()) {
            let mut map = self.urls.get(domain_id)?.unwrap_or_default();

            for url in urls {
                let url_id: UrlId = self.url_ids.id(url.clone())?.into();

                let domain_state =
                    self.domain_state
                        .entry(domain_id)
                        .or_insert_with(|| DomainState {
                            weight: 0.0,
                            status: DomainStatus::Pending,
                        });

                domain_state.status = DomainStatus::Pending;

                let url_state = map.entry(url_id).or_insert_with(|| UrlState {
                    weight: 0.0,
                    status: UrlStatus::Pending,
                });

                if &domain != crawled_domain {
                    url_state.weight += 1.0;
                }

                if url_state.weight > domain_state.weight {
                    domain_state.weight = url_state.weight;
                }
            }

            self.urls.put(domain_id, map)?;
        }

        Ok(())
    }

    pub fn update_url_status(&mut self, url_responses: &[UrlResponse]) -> Result<()> {
        let mut bundled: HashMap<Domain, Vec<UrlResponse>> = HashMap::new();

        for res in url_responses {
            match res {
                UrlResponse::Success { url } => {
                    let domain: Domain = url.domain().to_string().into();
                    bundled.entry(domain).or_default().push(res.clone());
                }
                UrlResponse::Failed {
                    url,
                    status_code: _,
                } => {
                    let domain: Domain = url.domain().to_string().into();
                    bundled.entry(domain).or_default().push(res.clone());
                }
                UrlResponse::Redirected { url, new_url: _ } => {
                    let domain: Domain = url.domain().to_string().into();
                    bundled.entry(domain).or_default().push(res.clone());
                }
            }
        }

        // bulk register domains
        self.domain_ids.bulk_ids(bundled.keys().cloned())?;

        // bulk register urls
        self.url_ids.bulk_ids(
            url_responses
                .iter()
                .flat_map(|res| match res {
                    UrlResponse::Success { url } => vec![url].into_iter(),
                    UrlResponse::Failed {
                        url,
                        status_code: _,
                    } => vec![url].into_iter(),
                    UrlResponse::Redirected { url, new_url } => vec![url, new_url].into_iter(),
                })
                .cloned(),
        )?;

        for (domain, url_responses) in bundled {
            let domain_id: DomainId = self.domain_ids.id(domain.clone())?.into();
            let mut map = self.urls.get(domain_id)?.unwrap_or_default();

            for response in url_responses {
                match response {
                    UrlResponse::Success { url } => {
                        let domain: Domain = url.domain().to_string().into();
                        let domain_id: DomainId = self.domain_ids.id(domain.clone())?.into();
                        let url_id: UrlId = self.url_ids.id(url.clone())?.into();

                        let domain_state =
                            self.domain_state
                                .entry(domain_id)
                                .or_insert_with(|| DomainState {
                                    weight: 0.0,
                                    status: DomainStatus::Pending,
                                });
                        domain_state.status = DomainStatus::Pending;

                        let url_state = map.entry(url_id).or_insert_with(|| UrlState {
                            weight: 0.0,
                            status: UrlStatus::Pending,
                        });

                        url_state.status = UrlStatus::Done;
                    }
                    UrlResponse::Failed { url, status_code } => {
                        let domain: Domain = url.domain().to_string().into();
                        let domain_id: DomainId = self.domain_ids.id(domain.clone())?.into();
                        let url_id: UrlId = self.url_ids.id(url.clone())?.into();

                        let domain_state =
                            self.domain_state
                                .entry(domain_id)
                                .or_insert_with(|| DomainState {
                                    weight: 0.0,
                                    status: DomainStatus::Pending,
                                });
                        domain_state.status = DomainStatus::Pending;

                        let url_state = map.entry(url_id).or_insert_with(|| UrlState {
                            weight: 0.0,
                            status: UrlStatus::Pending,
                        });

                        url_state.status = UrlStatus::Failed { status_code };
                    }
                    UrlResponse::Redirected { url, new_url } => {
                        self.redirects.put(&url, &new_url)?;
                    }
                }
            }

            self.urls.put(domain_id, map)?;
        }
        Ok(())
    }

    pub fn set_domain_status(&mut self, domain: &Domain, status: DomainStatus) -> Result<()> {
        let domain_id: DomainId = self.domain_ids.id(domain.clone())?.into();

        let domain_state = self
            .domain_state
            .entry(domain_id)
            .or_insert_with(|| DomainState {
                weight: 0.0,
                status: DomainStatus::Pending,
            });

        domain_state.status = status;

        Ok(())
    }

    pub fn sample_domains(&mut self, num_jobs: usize) -> Result<Vec<DomainId>> {
        let sampled = weighted_sample(
            self.domain_state.iter().filter_map(|(id, state)| {
                if state.status == DomainStatus::Pending {
                    Some((id, state.weight))
                } else {
                    None
                }
            }),
            num_jobs,
        )
        .into_iter()
        .copied()
        .collect();

        for id in &sampled {
            let state = self.domain_state.get_mut(id).unwrap();
            state.status = DomainStatus::CrawlInProgress;
        }

        Ok(sampled)
    }

    pub fn prepare_jobs(&mut self, domains: &[DomainId], urls_per_job: usize) -> Result<Vec<Job>> {
        let mut jobs = Vec::with_capacity(domains.len());
        for domain_id in domains {
            let mut urls = self.urls.get(*domain_id)?.unwrap_or_default();

            let sampled: Vec<_> = weighted_sample(
                urls.iter_mut().filter_map(|(id, state)| {
                    if state.status == UrlStatus::Pending {
                        Some((id, state.weight))
                    } else {
                        None
                    }
                }),
                urls_per_job,
            )
            .into_iter()
            .copied()
            .collect();

            for id in &sampled {
                let state = urls.get_mut(id).unwrap();
                state.status = UrlStatus::Crawling;
            }

            let mut job = Job {
                domain: self.domain_ids.value(domain_id.0)?.unwrap(),
                fetch_sitemap: false, // todo: fetch for new sites
                urls: VecDeque::with_capacity(urls_per_job),
            };

            for url_id in sampled {
                let url = self.url_ids.value(url_id.0)?.unwrap();
                job.urls.push_back(url);
            }

            self.urls.put(*domain_id, urls)?;

            jobs.push(job);
        }

        Ok(jobs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }
}
