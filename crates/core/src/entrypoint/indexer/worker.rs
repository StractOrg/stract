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

use chrono::Utc;
use std::path::Path;

use tracing::debug;

pub use super::indexable_webpage::IndexableWebpage;
pub use super::job::{Job, JobSettings};

use crate::human_website_annotations;
use crate::index::Index;
use crate::kv::rocksdb_store::RocksDbStore;
use crate::kv::Kv;
use crate::rake::RakeModel;
use crate::ranking::SignalAggregator;
use crate::webgraph::{Node, NodeID, Webgraph, WebgraphBuilder};
use crate::webpage::{safety_classifier, Html, Webpage};

pub struct IndexingWorker {
    host_centrality_store: RocksDbStore<NodeID, f64>,
    host_centrality_rank_store: RocksDbStore<NodeID, f64>,
    page_centrality_store: Option<RocksDbStore<NodeID, f64>>,
    page_centrality_rank_store: Option<RocksDbStore<NodeID, f64>>,
    page_webgraph: Option<Webgraph>,
    topics: Option<human_website_annotations::Mapper>,
    safety_classifier: Option<safety_classifier::Model>,
    job_settings: Option<JobSettings>,
    rake: RakeModel,
}

impl IndexingWorker {
    pub fn new(
        host_centrality_store_path: String,
        page_centrality_store_path: Option<String>,
        page_webgraph_path: Option<String>,
        topics_path: Option<String>,
        safety_classifier_path: Option<String>,
    ) -> Self {
        Self {
            host_centrality_store: RocksDbStore::open(
                Path::new(&host_centrality_store_path).join("harmonic"),
            ),
            host_centrality_rank_store: RocksDbStore::open(
                Path::new(&host_centrality_store_path).join("harmonic_rank"),
            ),
            page_centrality_store: page_centrality_store_path
                .as_ref()
                .map(|p| RocksDbStore::open(Path::new(&p).join("approx_harmonic"))),
            page_centrality_rank_store: page_centrality_store_path
                .as_ref()
                .map(|p| RocksDbStore::open(Path::new(&p).join("approx_harmonic_rank"))),
            page_webgraph: page_webgraph_path
                .map(|path| WebgraphBuilder::new(path).single_threaded().open()),
            topics: topics_path.map(|path| human_website_annotations::Mapper::open(path).unwrap()),
            safety_classifier: safety_classifier_path
                .map(|path| safety_classifier::Model::open(path).unwrap()),
            job_settings: None,
            rake: RakeModel::default(),
        }
    }

    pub(super) fn page_centrality_store(&self) -> Option<&RocksDbStore<NodeID, f64>> {
        self.page_centrality_store.as_ref()
    }

    pub(super) fn page_webgraph(&self) -> Option<&Webgraph> {
        self.page_webgraph.as_ref()
    }

    pub fn process(&self, job: &Job) -> Index {
        job.process(self)
    }

    pub fn set_job_settings(&mut self, job_settings: JobSettings) {
        self.job_settings = Some(job_settings);
    }

    pub fn prepare_webpage(&self, batch: &[IndexableWebpage], out: &mut Vec<Webpage>) {
        out.clear();
        let mut signal_aggregator = SignalAggregator::new(None);

        for page in batch {
            let IndexableWebpage {
                url,
                body,
                fetch_time_ms,
            } = page;

            let mut html = match Html::parse_without_text(body, url) {
                Ok(html) => html,
                Err(err) => {
                    debug!("error parsing html: {:?}", err);
                    continue;
                }
            };

            if html.is_no_index() {
                debug!("noindex");
                continue;
            }

            let title = html.title().unwrap_or_default();
            if title.is_empty() || title.chars().all(|c| c.is_whitespace()) {
                debug!("empty title");
                continue;
            }

            let node = Node::from(html.url());
            let host_node_id = node.clone().into_host().id();

            let mut host_centrality = self
                .host_centrality_store
                .get(&host_node_id)
                .unwrap_or_default();

            let mut host_centrality_rank = self
                .host_centrality_rank_store
                .get(&host_node_id)
                .unwrap_or(u64::MAX as f64);

            if let Some(host_centrality_threshold) =
                self.job_settings.and_then(|s| s.host_centrality_threshold)
            {
                if host_centrality < host_centrality_threshold {
                    debug!("skipping due to low host_centrality value");
                    continue;
                }
            }

            html.parse_text();

            if html.empty_all_text() {
                debug!("empty all text");
                continue;
            }

            if let Some(minimum_clean_words) = self.job_settings.and_then(|s| s.minimum_clean_words)
            {
                match html.clean_text() {
                    Some(clean_text) => {
                        if clean_text.split_whitespace().count() < minimum_clean_words {
                            debug!("too few clean words");
                            continue;
                        }
                    }
                    None => {
                        debug!("no clean text");
                        continue;
                    }
                }
            }

            let backlink_labels: Vec<String> = self
                .page_webgraph
                .as_ref()
                .map(|webgraph| {
                    webgraph
                        .raw_ingoing_edges_with_labels(&Node::from(html.url()).id())
                        .into_iter()
                        .map(|edge| edge.label)
                        .filter(|label| !label.is_empty())
                        .filter(|label| {
                            let label = label.to_lowercase();
                            let stopwords = [
                                "click",
                                "click here",
                                "here",
                                "link",
                                "website",
                                "webpage",
                                "page",
                                "site",
                                "url",
                                "web",
                                "visit",
                                "more",
                                "info",
                                "information",
                                "read",
                                "read more",
                            ];

                            !stopwords.contains(&label.as_str())
                        })
                        .collect()
                })
                .unwrap_or_default();

            let mut page_centrality = 0.0;

            if let Some(store) = self.page_centrality_store.as_ref() {
                let node_id = node.id();

                page_centrality = store.get(&node_id).unwrap_or_default();
            }

            let mut page_centrality_rank = u64::MAX as f64;

            if let Some(store) = self.page_centrality_rank_store.as_ref() {
                let node_id = node.id();

                page_centrality_rank = store.get(&node_id).unwrap_or(u64::MAX as f64);
            }

            if !page_centrality.is_finite() {
                page_centrality = 0.0;
            }

            if !host_centrality.is_finite() {
                host_centrality = 0.0;
            }

            if !page_centrality_rank.is_finite() {
                page_centrality_rank = u64::MAX as f64;
            }

            if !host_centrality_rank.is_finite() {
                host_centrality_rank = u64::MAX as f64;
            }

            let mut dmoz_description = None;

            if let Some(mapper) = self.topics.as_ref() {
                if let Some(info) =
                    mapper.get(&html.url().host_str().unwrap_or_default().to_string())
                {
                    dmoz_description = Some(info.description.clone())
                }
            }

            let keywords = html.keywords(&self.rake);

            let mut webpage = Webpage {
                html,
                backlink_labels,
                page_centrality,
                page_centrality_rank,
                host_centrality,
                host_centrality_rank,
                fetch_time_ms: *fetch_time_ms,
                pre_computed_score: 0.0,
                node_id: Some(host_node_id),
                dmoz_description,
                safety_classification: None,
                inserted_at: Utc::now(),
                keywords,
            };

            if let Some(model) = self.safety_classifier.as_ref() {
                webpage.safety_classification = Some(model.predict(&webpage).label);
            }

            signal_aggregator.set_current_timestamp(Utc::now().timestamp().max(0) as usize);

            webpage.pre_computed_score = signal_aggregator.precompute_score(&webpage);

            out.push(webpage);
        }
    }
}
