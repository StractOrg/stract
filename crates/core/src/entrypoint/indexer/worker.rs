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
use url::Url;

use tracing::debug;

pub use super::indexable_webpage::IndexableWebpage;
pub use super::job::{Job, JobSettings};
use crate::config::{IndexingDualEncoderConfig, IndexingLocalConfig, LiveIndexConfig};
use crate::models::dual_encoder::DualEncoder as DualEncoderModel;
use crate::Result;

use crate::human_website_annotations;
use crate::index::Index;
use crate::rake::RakeModel;
use crate::ranking::SignalComputer;
use crate::webgraph::{Node, NodeID, Webgraph, WebgraphBuilder};
use crate::webpage::{safety_classifier, Html, Webpage};

pub struct Config {
    pub host_centrality_store_path: String,
    pub page_centrality_store_path: Option<String>,
    pub page_webgraph_path: Option<String>,
    pub topics_path: Option<String>,
    pub safety_classifier_path: Option<String>,
    pub dual_encoder: Option<IndexingDualEncoderConfig>,
}

impl From<IndexingLocalConfig> for Config {
    fn from(config: IndexingLocalConfig) -> Self {
        Self {
            host_centrality_store_path: config.host_centrality_store_path,
            page_centrality_store_path: config.page_centrality_store_path,
            page_webgraph_path: config.page_webgraph_path,
            topics_path: config.topics_path,
            safety_classifier_path: config.safety_classifier_path,
            dual_encoder: config.dual_encoder,
        }
    }
}

impl From<LiveIndexConfig> for Config {
    fn from(config: LiveIndexConfig) -> Self {
        Self {
            host_centrality_store_path: config.host_centrality_store_path,
            page_centrality_store_path: config.page_centrality_store_path,
            page_webgraph_path: config.page_webgraph_path,
            topics_path: None,
            safety_classifier_path: config.safety_classifier_path,
            dual_encoder: None,
        }
    }
}

struct DualEncoder {
    model: DualEncoderModel,
    page_centrality_rank_threshold: Option<u64>,
}

pub struct IndexingWorker {
    host_centrality_store: speedy_kv::Db<NodeID, f64>,
    host_centrality_rank_store: speedy_kv::Db<NodeID, u64>,
    page_centrality_store: Option<speedy_kv::Db<NodeID, f64>>,
    page_centrality_rank_store: Option<speedy_kv::Db<NodeID, u64>>,
    page_webgraph: Option<Webgraph>,
    topics: Option<human_website_annotations::Mapper>,
    safety_classifier: Option<safety_classifier::Model>,
    job_settings: Option<JobSettings>,
    rake: RakeModel,
    dual_encoder: Option<DualEncoder>,
}

impl IndexingWorker {
    pub fn new<C>(config: C) -> Self
    where
        Config: From<C>,
    {
        let config = Config::from(config);

        Self {
            host_centrality_store: speedy_kv::Db::open_or_create(
                Path::new(&config.host_centrality_store_path).join("harmonic"),
            )
            .unwrap(),
            host_centrality_rank_store: speedy_kv::Db::open_or_create(
                Path::new(&config.host_centrality_store_path).join("harmonic_rank"),
            )
            .unwrap(),
            page_centrality_store: config.page_centrality_store_path.as_ref().map(|p| {
                speedy_kv::Db::open_or_create(Path::new(&p).join("approx_harmonic")).unwrap()
            }),
            page_centrality_rank_store: config.page_centrality_store_path.as_ref().map(|p| {
                speedy_kv::Db::open_or_create(Path::new(&p).join("approx_harmonic_rank")).unwrap()
            }),
            page_webgraph: config
                .page_webgraph_path
                .as_ref()
                .map(|path| WebgraphBuilder::new(path).single_threaded().open()),
            topics: config
                .topics_path
                .as_ref()
                .map(|path| human_website_annotations::Mapper::open(path).unwrap()),
            safety_classifier: config
                .safety_classifier_path
                .as_ref()
                .map(|path| safety_classifier::Model::open(path).unwrap()),
            job_settings: None,
            rake: RakeModel::default(),
            dual_encoder: config.dual_encoder.as_ref().map(|dual_encoder| {
                let model =
                    DualEncoderModel::open(&dual_encoder.model_path).unwrap_or_else(|err| {
                        panic!("failed to open dual encoder model: {}", err);
                    });

                DualEncoder {
                    model,
                    page_centrality_rank_threshold: dual_encoder.page_centrality_rank_threshold,
                }
            }),
        }
    }

    pub(super) fn page_centrality_store(&self) -> Option<&speedy_kv::Db<NodeID, f64>> {
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

    fn prepare(&self, page: &IndexableWebpage) -> Result<Webpage> {
        let html = match Html::parse_without_text(&page.body, &page.url) {
            Ok(html) => html,
            Err(err) => {
                return Err(anyhow::anyhow!("error parsing html: {:?}", err));
            }
        };

        if html.is_no_index() {
            return Err(anyhow::anyhow!("noindex"));
        }

        let title = html.title().unwrap_or_default();
        if title.is_empty() || title.chars().all(|c| c.is_whitespace()) {
            return Err(anyhow::anyhow!("empty title"));
        }

        Ok(Webpage::from(html))
    }

    fn set_host_centrality(&self, page: &mut Webpage) -> Result<()> {
        let node = Node::from(page.html.url());
        let host_node_id = node.clone().into_host().id();

        let host_centrality = self
            .host_centrality_store
            .get(&host_node_id)
            .unwrap()
            .unwrap_or_default();

        let host_centrality_rank = self
            .host_centrality_rank_store
            .get(&host_node_id)
            .unwrap()
            .unwrap_or(u64::MAX);

        if let Some(host_centrality_threshold) =
            self.job_settings.and_then(|s| s.host_centrality_threshold)
        {
            if host_centrality < host_centrality_threshold {
                return Err(anyhow::anyhow!("low host_centrality value"));
            }
        }

        page.node_id = Some(host_node_id);

        page.host_centrality = host_centrality;
        page.host_centrality_rank = host_centrality_rank;

        if !page.host_centrality.is_finite() {
            page.host_centrality = 0.0;
        }

        self.parse_text(page)?;

        Ok(())
    }

    fn parse_text(&self, page: &mut Webpage) -> Result<()> {
        page.html.parse_text();

        if page.html.empty_all_text() {
            return Err(anyhow::anyhow!("empty all text"));
        }

        if let Some(minimum_clean_words) = self.job_settings.and_then(|s| s.minimum_clean_words) {
            match page.html.clean_text() {
                Some(clean_text) => {
                    if clean_text.split_whitespace().count() < minimum_clean_words {
                        return Err(anyhow::anyhow!("too few clean words"));
                    }
                }
                None => {
                    return Err(anyhow::anyhow!("no clean text"));
                }
            }
        }

        Ok(())
    }

    fn backlink_labels(&self, page: &Url) -> Vec<String> {
        self.page_webgraph
            .as_ref()
            .map(|webgraph| {
                webgraph
                    .raw_ingoing_edges_with_labels(&Node::from(page).id())
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
            .unwrap_or_default()
    }

    fn set_page_centralities(&self, page: &mut Webpage) {
        let node = Node::from(page.html.url());

        if let Some(store) = self.page_centrality_store.as_ref() {
            let node_id = node.id();

            page.page_centrality = store.get(&node_id).unwrap().unwrap_or_default();
        }

        page.page_centrality_rank = u64::MAX;

        if let Some(store) = self.page_centrality_rank_store.as_ref() {
            let node_id = node.id();

            page.page_centrality_rank = store.get(&node_id).unwrap().unwrap_or(u64::MAX);
        }

        if !page.page_centrality.is_finite() {
            page.page_centrality = 0.0;
        }
    }

    fn set_dmoz_description(&self, page: &mut Webpage) {
        if let Some(mapper) = self.topics.as_ref() {
            if let Some(info) =
                mapper.get(&page.html.url().host_str().unwrap_or_default().to_string())
            {
                page.dmoz_description = Some(info.description.clone())
            }
        }
    }

    fn set_keywords(&self, page: &mut Webpage) {
        page.keywords = page.html.keywords(&self.rake);
    }

    fn set_safety_classification(&self, page: &mut Webpage) {
        if let Some(model) = self.safety_classifier.as_ref() {
            page.safety_classification = Some(model.predict(page).label);
        }
    }

    pub fn set_title_embeddings(&self, pages: &mut [Webpage]) {
        if let Some(dual_encoder) = self.dual_encoder.as_ref() {
            let (page_indexes, titles): (Vec<_>, Vec<_>) = pages
                .iter()
                .enumerate()
                .filter(|(_, w)| {
                    dual_encoder
                        .page_centrality_rank_threshold
                        .map(|thresh| w.page_centrality_rank <= thresh)
                        .unwrap_or(true)
                })
                .map(|(i, w)| (i, w.html.title().unwrap_or_default()))
                .unzip();

            let title_emb = dual_encoder
                .model
                .embed(&titles)
                .ok()
                .and_then(|t| t.to_dtype(candle_core::DType::BF16).ok());

            if let Some(title_emb) = title_emb {
                for (i, page_index) in page_indexes.into_iter().enumerate() {
                    if let Ok(emb) = title_emb.get(i) {
                        pages[page_index].title_embedding = Some(emb);
                    }
                }
            }
        }
    }

    pub fn set_keyword_embeddings(&self, pages: &mut [Webpage]) {
        if let Some(dual_encoder) = self.dual_encoder.as_ref() {
            let (page_indexes, keywords): (Vec<_>, Vec<_>) = pages
                .iter()
                .enumerate()
                .filter(|(_, w)| {
                    dual_encoder
                        .page_centrality_rank_threshold
                        .map(|thresh| w.page_centrality_rank <= thresh)
                        .unwrap_or(true)
                })
                .map(|(i, w)| (i, w.keywords.join("\n")))
                .unzip();

            let keyword_emb = dual_encoder
                .model
                .embed(&keywords)
                .ok()
                .and_then(|t| t.to_dtype(candle_core::DType::BF16).ok());

            if let Some(keyword_emb) = keyword_emb {
                for (i, page_index) in page_indexes.into_iter().enumerate() {
                    if let Ok(emb) = keyword_emb.get(i) {
                        pages[page_index].keyword_embedding = Some(emb);
                    }
                }
            }
        }
    }

    pub fn prepare_webpages(&self, batch: &[IndexableWebpage]) -> Vec<Webpage> {
        let mut res = Vec::with_capacity(batch.len());
        let mut signal_computer = SignalComputer::new(None);

        for page in batch {
            let mut prepared = match self.prepare(page) {
                Ok(html) => html,
                Err(err) => {
                    debug!("skipping webpage: {}", err);
                    continue;
                }
            };
            if let Err(e) = self.set_host_centrality(&mut prepared) {
                debug!("skipping webpage: {}", e);
                continue;
            }

            prepared.backlink_labels = self.backlink_labels(prepared.html.url());

            self.set_page_centralities(&mut prepared);
            self.set_dmoz_description(&mut prepared);
            self.set_keywords(&mut prepared);
            self.set_safety_classification(&mut prepared);

            // make sure we remember to set everything
            let mut webpage = Webpage {
                html: prepared.html,
                backlink_labels: prepared.backlink_labels,
                page_centrality: prepared.page_centrality,
                page_centrality_rank: prepared.page_centrality_rank,
                host_centrality: prepared.host_centrality,
                host_centrality_rank: prepared.host_centrality_rank,
                fetch_time_ms: page.fetch_time_ms,
                pre_computed_score: 0.0,
                node_id: prepared.node_id,
                dmoz_description: prepared.dmoz_description,
                safety_classification: prepared.safety_classification,
                inserted_at: Utc::now(),
                keywords: prepared.keywords,
                title_embedding: None,   // set later
                keyword_embedding: None, // set later
            };

            signal_computer.set_current_timestamp(Utc::now().timestamp().max(0) as usize);
            webpage.pre_computed_score = signal_computer.precompute_score(&webpage);

            res.push(webpage);
        }

        self.set_title_embeddings(&mut res);
        self.set_keyword_embeddings(&mut res);

        res
    }
}

#[cfg(test)]
mod tests {
    use crate::config::WarcSource;

    use super::*;

    fn setup_worker(data_path: &Path, threshold: Option<u64>) -> IndexingWorker {
        IndexingWorker::new(IndexingLocalConfig {
            host_centrality_store_path: crate::gen_temp_path().to_str().unwrap().to_string(),
            page_centrality_store_path: None,
            page_webgraph_path: None,
            topics_path: None,
            safety_classifier_path: None,
            dual_encoder: Some(IndexingDualEncoderConfig {
                model_path: data_path.to_str().unwrap().to_string(),
                page_centrality_rank_threshold: threshold,
            }),
            output_path: crate::gen_temp_path().to_str().unwrap().to_string(),
            limit_warc_files: None,
            skip_warc_files: None,
            warc_source: WarcSource::Local(crate::config::LocalConfig {
                folder: crate::gen_temp_path().to_str().unwrap().to_string(),
                names: vec!["".to_string()],
            }),
            host_centrality_threshold: None,
            minimum_clean_words: None,
            batch_size: 10,
        })
    }

    #[test]
    fn title_embeddings() {
        let data_path = Path::new("../../data/summarizer/dual_encoder");
        if !data_path.exists() {
            // Skip the test if the test data is not available
            return;
        }
        let worker = setup_worker(data_path, None);

        let webpages = vec![
            IndexableWebpage {
                url: "https://a.com".to_string(),
                body: "<html><head><title>Homemade Heart Brownie Recipe</title></head><body>Example</body></html>"
                    .to_string(),
                fetch_time_ms: 0,
            },
            IndexableWebpage {
                url: "https://b.com".to_string(),
                body: "<html><head><title>How To Use an iMac as a Monitor for a PC</title></head><body>Example</body></html>"
                    .to_string(),
                fetch_time_ms: 0,
            },
        ];

        let webpages = worker.prepare_webpages(&webpages);

        assert_eq!(webpages.len(), 2);

        assert_eq!(
            webpages[0].html.title(),
            Some("Homemade Heart Brownie Recipe".to_string())
        );
        assert_eq!(
            webpages[1].html.title(),
            Some("How To Use an iMac as a Monitor for a PC".to_string())
        );

        assert!(webpages.iter().all(|w| w.title_embedding.is_some()));

        let emb1 = webpages[0].title_embedding.as_ref().unwrap();
        let emb2 = webpages[1].title_embedding.as_ref().unwrap();

        let query = "best chocolate cake";
        let query_emb = worker
            .dual_encoder
            .as_ref()
            .unwrap()
            .model
            .embed(&[query.to_string()])
            .unwrap()
            .to_dtype(candle_core::DType::F16)
            .unwrap()
            .get(0)
            .unwrap();

        let sim1: f32 = query_emb
            .unsqueeze(0)
            .unwrap()
            .matmul(
                &emb1
                    .to_dtype(candle_core::DType::F16)
                    .unwrap()
                    .unsqueeze(0)
                    .unwrap()
                    .t()
                    .unwrap(),
            )
            .unwrap()
            .get(0)
            .unwrap()
            .squeeze(0)
            .unwrap()
            .to_dtype(candle_core::DType::F32)
            .unwrap()
            .to_vec0()
            .unwrap();

        let sim2: f32 = query_emb
            .unsqueeze(0)
            .unwrap()
            .matmul(
                &emb2
                    .to_dtype(candle_core::DType::F16)
                    .unwrap()
                    .unsqueeze(0)
                    .unwrap()
                    .t()
                    .unwrap(),
            )
            .unwrap()
            .get(0)
            .unwrap()
            .squeeze(0)
            .unwrap()
            .to_dtype(candle_core::DType::F32)
            .unwrap()
            .to_vec0()
            .unwrap();

        assert!(sim1 > sim2);
    }

    #[test]
    fn title_embedding_ranks() {
        let data_path = Path::new("../../data/summarizer/dual_encoder");
        if !data_path.exists() {
            // Skip the test if the test data is not available
            return;
        }
        let worker = setup_worker(data_path, Some(100_000));

        let mut a = Webpage::test_parse("<html><head><title>Homemade Heart Brownie Recipe</title></head><body>Example</body></html>", "https://a.com").unwrap();
        a.page_centrality_rank = 1;

        let mut b = Webpage::test_parse("<html><head><title>How To Use an iMac as a Monitor for a PC</title></head><body>Example</body></html>", "https://b.com").unwrap();
        b.page_centrality_rank = 1_000_000;

        let mut webpages = vec![a, b];
        worker.set_title_embeddings(&mut webpages);

        assert_eq!(webpages.len(), 2);
        assert_eq!(
            webpages[0].html.title(),
            Some("Homemade Heart Brownie Recipe".to_string())
        );

        assert!(webpages[0].title_embedding.is_some());
        assert!(webpages[1].title_embedding.is_none());

        let mut a = Webpage::test_parse("<html><head><title>Homemade Heart Brownie Recipe</title></head><body>Example</body></html>", "https://a.com").unwrap();
        a.page_centrality_rank = 1_000_000;

        let mut b = Webpage::test_parse("<html><head><title>How To Use an iMac as a Monitor for a PC</title></head><body>Example</body></html>", "https://b.com").unwrap();
        b.page_centrality_rank = 1;

        let mut webpages = vec![a, b];
        worker.set_title_embeddings(&mut webpages);

        assert_eq!(webpages.len(), 2);
        assert_eq!(
            webpages[0].html.title(),
            Some("Homemade Heart Brownie Recipe".to_string())
        );

        assert!(webpages[0].title_embedding.is_none());
        assert!(webpages[1].title_embedding.is_some());
    }
}
