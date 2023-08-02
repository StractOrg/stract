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

//! Alice is an AI assistant that can help search the web to answer
//! questions and cite sources. This module takes a trained model
//! and uses it for inference. The training code can be found in another
//! [repository](https://github.com/stractOrg/alice].
//!
//! The model is a hybrid between an RNN and a Transformer. It's therefore
//! possible to save the hidden state and continue the conversation later.
//! To make sure the state has not been tampered with, it is encrypted
//! using an AES-GCM key.

use std::{
    io::{Read, Write},
    path::Path,
    rc::Rc,
    sync::Arc,
};

use aes_gcm::{
    aead::{Aead, OsRng},
    AeadCore, Aes256Gcm, Key, KeyInit, Nonce,
};
use anyhow::anyhow;
use flate2::{bufread::GzDecoder, write::GzEncoder, Compression};
use half::bf16;
use tch::Tensor;
use url::Url;

use crate::{
    api::search::ApiSearchQuery,
    config::AcceleratorDevice,
    config::AcceleratorDtype,
    config::AliceAcceleratorConfig,
    llm_utils::ClonableTensor,
    search_prettifier::DisplayedWebpage,
    searcher::{SearchResult, WebsitesResult},
    summarizer::ExtractiveSummarizer,
};

use self::{
    generate::{ActionExecutor, ActionGenerator, AliceTokenGenerator, RawActionGenerator},
    raw_model::RawModel,
};

const PROMPT_PREFIX: &str = r#"System: Your name is Alice, and you are an AI assistant trained by Stract. Below is a conversation between you and a user. Help the user as best you can.
You can lookup information on the web in the following format:
Alice[thought]: I should look up "<keyword>" on the web.
Search: <query><|endoftext|>
Result: <search result>

This Thought/Search/Result can repeat N times.
When you are ready to answer the user, use the following format:
Alice: <answer><|endoftext|>"#;

pub mod generate;
mod raw_model;

type Result<T> = std::result::Result<T, anyhow::Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Empty input")]
    EmptyInput,

    #[error("Unexpected search result")]
    UnexpectedSearchResult,

    #[error("Failed to decrypt")]
    DecryptionFailed,

    #[error("Unexpected completion")]
    UnexpectedCompletion,

    #[error("Last message should be from user")]
    LastMessageNotUser,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SimplifiedWebsite {
    pub title: String,
    pub text: String,
    pub url: String,
    pub site: String,
}

impl SimplifiedWebsite {
    fn new(webpage: DisplayedWebpage, query: &str, summarizer: &ExtractiveSummarizer) -> Self {
        let text = summarizer.summarize(query, &webpage.body);
        let url = Url::parse(&webpage.url).unwrap();

        Self {
            title: webpage.title,
            text,
            site: url.host_str().unwrap_or_default().to_string(),
            url: url.to_string(),
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
        state: EncodedEncryptedState,
    },
}

/// A simplified website that the model sees in the prompt.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ModelWebsite {
    pub title: String,
    pub text: String,
    pub site: String,
}

impl From<SimplifiedWebsite> for ModelWebsite {
    fn from(value: SimplifiedWebsite) -> Self {
        Self {
            title: value.title,
            text: value.text,
            site: value.site,
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
            SearchResult::Bang(_) => Err(Error::UnexpectedSearchResult.into()),
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
}

pub struct Alice {
    inner: Rc<RawModel>,
    tokenizer: Arc<Tokenizer>,
    summarizer: Arc<ExtractiveSummarizer>,
    end_tokens: Vec<i64>,
    initial_state: ClonableTensor,
    encryption_key: Key<Aes256Gcm>,
}

/// SAFETY:
/// Alice is thread-safe because it never mutates it's internal state.
/// It only ever spawns executors that keeps track of the generation state. These are not threadsafe.
unsafe impl Send for Alice {}
unsafe impl Sync for Alice {}

pub struct AcceleratorConfig {
    pub layer_fraction: f64,
    pub quantize_fraction: f64,
    pub device: tch::Device,
    pub kind: tch::Kind,
}

impl From<AcceleratorDevice> for tch::Device {
    fn from(value: AcceleratorDevice) -> Self {
        match value {
            AcceleratorDevice::Cpu => tch::Device::Cpu,
            AcceleratorDevice::Cuda(d) => tch::Device::Cuda(d),
            AcceleratorDevice::Mps => tch::Device::Mps,
        }
    }
}

impl From<AcceleratorDtype> for tch::Kind {
    fn from(value: AcceleratorDtype) -> Self {
        match value {
            AcceleratorDtype::Float => tch::Kind::Float,
            AcceleratorDtype::Bf16 => tch::Kind::BFloat16,
        }
    }
}

impl From<AliceAcceleratorConfig> for AcceleratorConfig {
    fn from(value: AliceAcceleratorConfig) -> Self {
        Self {
            layer_fraction: value.layer_fraction,
            quantize_fraction: value.quantize_fraction,
            device: value.device.into(),
            kind: value.dtype.into(),
        }
    }
}

impl Alice {
    pub fn open<P: AsRef<Path>>(
        folder: P,
        summarizer_path: P,
        accelerator: Option<AcceleratorConfig>,
        encryption_key: &[u8],
    ) -> Result<Alice> {
        let encryption_key = *Key::<Aes256Gcm>::from_slice(encryption_key);
        let mut model = RawModel::open(folder.as_ref().join("model.safetensors"))?;

        if let Some(accelerator) = accelerator {
            model.load_to_device(
                accelerator.layer_fraction,
                accelerator.quantize_fraction,
                accelerator.device,
                accelerator.kind,
            );
        }

        let mut summarizer = ExtractiveSummarizer::open(summarizer_path, 1)?;
        summarizer.set_window_size(50);

        let summarizer = Arc::new(summarizer);

        let inner = Rc::new(model);
        let tokenizer = Arc::new(Tokenizer::open(folder.as_ref().join("tokenizer.json"))?);

        let end_tokens = vec![tokenizer.tokenizer.token_to_id("<|endoftext|>").unwrap() as i64];

        let initial_state = Alice::prepare_initial_state(&inner, &tokenizer)?;

        Ok(Self {
            inner,
            tokenizer,
            end_tokens,
            summarizer,
            initial_state,
            encryption_key,
        })
    }

    fn prepare_initial_state(model: &RawModel, tokenizer: &Tokenizer) -> Result<ClonableTensor> {
        let tokens = tokenizer.encode(PROMPT_PREFIX.to_string())?;

        let mut state = model.init_state();

        for token in tokens {
            let (_, new_state) = model.forward(token, Some(&state));
            state = new_state;
        }

        Ok(ClonableTensor(state))
    }

    pub fn new_executor(
        &self,
        user_question: &str,
        last_state: Option<EncryptedState>,
        search_url: String,
        optic_url: Option<String>,
    ) -> Result<ActionExecutor> {
        let mut state = None;

        if let Some(s) = last_state {
            let decrypted_state = s.decrypt(&self.encryption_key)?;
            state = Some(Tensor::from_slice2(&decrypted_state));
        }

        let (tokens, state) = match state {
            Some(state) => {
                let input = format!("User: {user_question}\n\nAlice[thought]:");
                let tokens = self.tokenizer.encode(input).unwrap();

                (tokens, state)
            }
            None => {
                let input = format!("\n\nUser: {user_question}\n\nAlice[thought]:");
                let tokens = self.tokenizer.encode(input).unwrap();

                (tokens, self.initial_state.clone().0)
            }
        };

        let token_generator = AliceTokenGenerator::new(
            self.inner.clone(),
            self.end_tokens.clone(),
            state,
            &tokens,
            None,
        )?;

        let raw_action_gen = RawActionGenerator::new(token_generator, self.tokenizer.clone());
        let action_gen = ActionGenerator::new(raw_action_gen);

        let searcher = Searcher {
            url: search_url,
            optic_url,
            summarizer: Arc::clone(&self.summarizer),
        };

        Ok(ActionExecutor::new(
            action_gen,
            searcher,
            self.encryption_key,
        ))
    }
}

#[derive(Clone)]
pub struct ModelState {
    state: ClonableTensor,
    next_token: i64,
}

pub struct Tokenizer {
    tokenizer: tokenizers::Tokenizer,
}

impl Tokenizer {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Tokenizer> {
        let tokenizer = tokenizers::Tokenizer::from_file(path).map_err(|e| anyhow!(e))?;

        Ok(Self { tokenizer })
    }

    pub fn encode(&self, input: String) -> Result<Vec<i64>> {
        let encoding = self
            .tokenizer
            .encode(input, false)
            .map_err(|e| anyhow!(e))?;

        let ids = encoding
            .get_ids()
            .iter()
            .map(|&id| id as i64)
            .collect::<Vec<_>>();

        Ok(ids)
    }

    pub fn decode(&self, tokens: &[i64]) -> Result<String> {
        let tokens = tokens.iter().map(|&id| id as u32).collect::<Vec<_>>();

        let output = self
            .tokenizer
            .decode(tokens, true)
            .map_err(|e| anyhow!(e))?;

        Ok(output)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
/// base64 encoded `EncryptedState`
pub struct EncodedEncryptedState(String);

impl EncodedEncryptedState {
    pub fn encode(state: EncryptedState) -> Self {
        let bytes = bincode::serialize(&state).unwrap();
        let encoded = base64::encode(bytes);

        Self(encoded)
    }

    pub fn decode(self) -> Result<EncryptedState> {
        let bytes = base64::decode(self.0)?;
        let state = bincode::deserialize(&bytes)?;

        Ok(state)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EncryptedState {
    nonce: Vec<u8>,
    // ciphertext is bincoded `CompressedState` encrypted with `nonce` and a key
    ciphertext: Vec<u8>,
}

impl EncryptedState {
    fn encrypt(state: Vec<Vec<f32>>, key: &Key<Aes256Gcm>) -> Self {
        let compressed = compress_state(state);

        let cipher = Aes256Gcm::new(key);
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        let bytes = bincode::serialize(&compressed).unwrap();

        let ciphertext = cipher.encrypt(&nonce, bytes.as_slice()).unwrap();

        Self {
            nonce: nonce.to_vec(),
            ciphertext,
        }
    }

    fn decrypt(&self, key: &Key<Aes256Gcm>) -> Result<Vec<Vec<f32>>> {
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&self.nonce);

        let decrypted = cipher
            .decrypt(nonce, self.ciphertext.as_slice())
            .map_err(|_| Error::DecryptionFailed)?;

        let compressed: CompressedState = bincode::deserialize(&decrypted)?;

        decompress_state(compressed)
    }
}

/// base64 encoded gzipped state
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CompressedState(String);

pub fn compress_state(state: Vec<Vec<f32>>) -> CompressedState {
    let state: Vec<Vec<bf16>> = state
        .into_iter()
        .map(|v| v.into_iter().map(bf16::from_f32).collect())
        .collect();

    let mut encoder = GzEncoder::new(Vec::new(), Compression::best());

    encoder
        .write_all(&bincode::serialize(&state).unwrap())
        .unwrap();

    let compressed = encoder.finish().unwrap();

    // base64 encode the compressed state
    CompressedState(base64::encode(compressed))
}

pub fn decompress_state(state: CompressedState) -> Result<Vec<Vec<f32>>> {
    let state = base64::decode(state.0)?;

    let mut decoder = GzDecoder::new(&state[..]);

    let mut state = Vec::new();
    decoder.read_to_end(&mut state)?;

    let state: Vec<Vec<bf16>> = bincode::deserialize(&state)?;

    Ok(state
        .into_iter()
        .map(|v| v.into_iter().map(f32::from).collect())
        .collect())
}

#[derive(Debug)]
struct AnyTransitionValidator {
    validators: Vec<TransitionValidator>,
}

impl AnyTransitionValidator {
    fn new(validators: Vec<TransitionValidator>) -> Self {
        Self { validators }
    }

    fn validate(&mut self, token: i64) -> bool {
        for validator in &mut self.validators {
            if validator.validate(token) {
                return true;
            }
        }

        false
    }
}

#[derive(Debug)]
struct TransitionValidator {
    tokens: Vec<i64>,
    next_match: usize,
}

impl TransitionValidator {
    fn new(tokens: Vec<i64>) -> Self {
        Self {
            tokens,
            next_match: 0,
        }
    }

    fn validate(&mut self, token: i64) -> bool {
        if self.tokens[self.next_match] == token {
            self.next_match += 1;
            if self.next_match == self.tokens.len() {
                self.next_match = 0;
                true
            } else {
                false
            }
        } else {
            self.next_match = 0;
            false
        }
    }
}
