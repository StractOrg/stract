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

use std::sync::Arc;

use crate::{
    api::search::ApiSearchQuery,
    search_prettifier::DisplayedWebpage,
    searcher::{SearchResult, WebsitesResult},
    summarizer::{self, ExtractiveSummarizer},
    webpage::Url,
};

pub mod local;
pub mod openai;

type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Torch error: {0}")]
    Torch(#[from] tch::TchError),

    #[error("SafeTensors error: {0}")]
    SafeTensors(#[from] safetensors::SafeTensorError),

    #[error("Tokenizers error: {0}")]
    Tokenizers(#[from] tokenizers::Error),

    #[error("Empty input")]
    EmptyInput,

    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Unexpected search result")]
    UnexpectedSearchResult,

    #[error("Summarizer: {0}")]
    Summarizer(#[from] summarizer::Error),

    #[error("Bincode: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("Base64: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error("Failed to decrypt")]
    DecryptionFailed,

    #[error("Cluster")]
    Cluster(#[from] crate::distributed::cluster::Error),

    #[error("Unexpected completion")]
    UnexpectedCompletion,

    #[error("Event source cannot clone")]
    EventSourceCannotClone(#[from] reqwest_eventsource::CannotCloneRequestError),

    #[error("Event source error")]
    EventSource(#[from] reqwest_eventsource::Error),

    #[error("Serde JSON error")]
    SerdeJson(#[from] serde_json::Error),

    #[error("Last message should be from user")]
    LastMessageNotUser,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SimplifiedWebsite {
    pub title: String,
    pub text: String,
    pub url: String,
    pub domain: String,
}

impl SimplifiedWebsite {
    fn new(webpage: DisplayedWebpage, query: &str, summarizer: &ExtractiveSummarizer) -> Self {
        let text = summarizer.summarize(query, &webpage.body);
        let url = Url::from(webpage.url.to_string());

        Self {
            title: webpage.title,
            text,
            domain: url.domain().to_string(),
            url: url.full(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "@type", rename_all = "camelCase")]
pub enum ExecutionState {
    BeginSearch {
        query: String,
    },
    SearchResult {
        query: String,
        result: Vec<SimplifiedWebsite>,
    },
    Speaking {
        text: String,
    },
    Done {
        state: local::EncodedEncryptedState,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ModelWebsite {
    pub title: String,
    pub text: String,
    pub domain: String,
}

impl From<SimplifiedWebsite> for ModelWebsite {
    fn from(value: SimplifiedWebsite) -> Self {
        Self {
            title: value.title,
            text: value.text,
            domain: value.domain,
        }
    }
}

pub struct Searcher {
    url: String,
    optic_url: Option<String>,
    summarizer: Arc<ExtractiveSummarizer>,
}

impl Searcher {
    fn raw_search(&self, query: &str) -> Result<WebsitesResult> {
        let optic = self
            .optic_url
            .as_ref()
            .and_then(|url| reqwest::blocking::get(url).ok().and_then(|r| r.text().ok()));

        let client = reqwest::blocking::Client::new();
        let query = ApiSearchQuery {
            query: query.trim().to_string(),
            num_results: Some(3),
            optic,
            page: None,
            selected_region: None,
            site_rankings: None,
            return_ranking_signals: None,
            flatten_response: Some(false),
        };
        tracing::debug!("searching at {:?}: {:#?}", self.url, query);

        let res: SearchResult = client.post(&self.url).json(&query).send()?.json()?;

        match res {
            SearchResult::Websites(res) => Ok(res),
            SearchResult::Bang(_) => Err(Error::UnexpectedSearchResult),
        }
    }

    fn search(&self, query: &str) -> Result<Vec<SimplifiedWebsite>> {
        let res = self.raw_search(query)?;

        let mut websites = Vec::new();

        for website in res.webpages {
            websites.push(SimplifiedWebsite::new(website, query, &self.summarizer));
        }

        tracing::debug!("search result: {:#?}", websites);

        Ok(websites)
    }

    async fn raw_search_async(&self, query: &str) -> Result<WebsitesResult> {
        let mut optic = None;

        if let Some(url) = &self.optic_url {
            if let Ok(r) = reqwest::get(url).await {
                if let Ok(text) = r.text().await {
                    optic = Some(text);
                }
            }
        }

        let client = reqwest::Client::new();
        let query = ApiSearchQuery {
            query: query.trim().to_string(),
            num_results: Some(3),
            optic,
            page: None,
            selected_region: None,
            site_rankings: None,
            return_ranking_signals: None,
            flatten_response: Some(false),
        };
        tracing::debug!("searching at {:?}: {:#?}", self.url, query);

        let res: SearchResult = client
            .post(&self.url)
            .json(&query)
            .send()
            .await?
            .json()
            .await?;

        match res {
            SearchResult::Websites(res) => Ok(res),
            SearchResult::Bang(_) => Err(Error::UnexpectedSearchResult),
        }
    }

    async fn search_async(&self, query: &str) -> Result<Vec<SimplifiedWebsite>> {
        let res = self.raw_search_async(query).await?;

        let mut websites = Vec::new();

        for website in res.webpages {
            websites.push(SimplifiedWebsite::new(website, query, &self.summarizer));
        }

        tracing::debug!("search result: {:#?}", websites);

        Ok(websites)
    }
}
