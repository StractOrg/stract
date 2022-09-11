// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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
use futures::StreamExt;
use std::{collections::HashSet, hash::Hash, time::Duration};

use serde::Serialize;

use crate::{
    exponential_backoff::ExponentialBackoff,
    image_store::{Image, ImageStore},
    webpage::Url,
};

#[derive(Clone, Debug)]
pub struct ImageDownloadJob<K>
where
    K: std::fmt::Debug + Serialize + Hash + PartialEq + Eq + Clone,
{
    pub key: K,
    pub urls: Vec<Url>,
    pub timeout: Option<Duration>,
}

impl<K> ImageDownloadJob<K>
where
    K: std::fmt::Debug + Serialize + Hash + PartialEq + Eq + Clone,
{
    async fn download(self) -> Option<DownloadedImage<K>> {
        for url in &self.urls {
            if !url.is_valid_uri() {
                continue;
            }

            for duration in ExponentialBackoff::from_millis(10).take(5) {
                if let Some(image) = url
                    .download_bytes(self.timeout.unwrap_or_else(|| Duration::from_secs(20)))
                    .await
                    .and_then(|bytes| Image::from_bytes(bytes).ok())
                    .map(|image| DownloadedImage {
                        image,
                        key: self.key.clone(),
                    })
                {
                    return Some(image);
                }

                tokio::time::sleep(duration).await;
            }
        }

        None
    }
}

impl<K> Hash for ImageDownloadJob<K>
where
    K: std::fmt::Debug + Serialize + Hash + PartialEq + Eq + Clone,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

impl<K> PartialEq for ImageDownloadJob<K>
where
    K: std::fmt::Debug + Serialize + Hash + PartialEq + Eq + Clone,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K> Eq for ImageDownloadJob<K> where
    K: std::fmt::Debug + Serialize + Hash + PartialEq + Eq + Clone
{
}

#[derive(Debug)]
struct DownloadedImage<K>
where
    K: std::fmt::Debug + Serialize,
{
    image: Image,
    key: K,
}
pub struct ImageDownloader<K>
where
    K: std::fmt::Debug + Serialize + Hash + PartialEq + Eq + Clone,
{
    image_download_jobs: HashSet<ImageDownloadJob<K>>,
}

impl<K> ImageDownloader<K>
where
    K: std::fmt::Debug + Serialize + Hash + PartialEq + Eq + Clone,
{
    pub fn new() -> Self {
        Self {
            image_download_jobs: HashSet::new(),
        }
    }
    pub fn download(&mut self, store: &mut impl ImageStore<K>) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async { self.download_pending_images_async(store).await });

        self.image_download_jobs.clear();
    }

    async fn download_pending_images_async(&mut self, store: &mut impl ImageStore<K>) {
        let results = futures::stream::iter(
            self.image_download_jobs
                .drain()
                .map(|job| async move { job.download().await }),
        )
        .buffer_unordered(20)
        .collect::<Vec<Option<DownloadedImage<K>>>>()
        .await;

        for result in results.into_iter().flatten() {
            store.insert(result.key, result.image);
        }

        store.flush();
    }

    pub fn schedule(&mut self, image_download_job: ImageDownloadJob<K>) {
        self.image_download_jobs.insert(image_download_job);
    }
}
