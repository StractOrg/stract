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
    fs::OpenOptions,
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

use aes_gcm::{
    aead::{Aead, OsRng},
    AeadCore, Aes256Gcm, Key, KeyInit, Nonce,
};
use flate2::{bufread::GzDecoder, write::GzEncoder, Compression};
use half::bf16;
use safetensors::SafeTensors;
use tch::{
    nn::{embedding, layer_norm, Embedding, LayerNorm, LayerNormConfig, Linear, ModuleT, VarStore},
    IndexOp, Kind, Tensor,
};

use crate::{
    api::search::ApiSearchQuery,
    leaky_queue::LeakyQueue,
    llm_utils::{self, ClonableTensor},
    search_prettifier::DisplayedWebpage,
    searcher::{SearchResult, WebsitesResult},
    summarizer::{self, ExtractiveSummarizer},
    webpage::Url,
    AcceleratorDevice, AcceleratorDtype, AliceAcceleratorConfig,
};

const NUM_TOKENS: i64 = 50277;

const TAU: f64 = 0.8;
const TEMP: f64 = 0.4;

const PROMPT_PREFIX: &str = r#"System: Your name is Alice, and you are an AI assistant trained by Stract. Below is a conversation between you and a user. Help the user as best you can.
You can lookup information on the web in the following format:
Alice[thought]: I should look up "<keyword>" on the web.
Search: <query><|endoftext|>
Result: <search result>

This Thought/Search/Result can repeat N times.
When you are ready to answer the user, use the following format:
Alice: <answer><|endoftext|>"#;

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
}

fn load_linear(weights: &SafeTensors, prefix: &str) -> Result<Linear> {
    let ws = weights.tensor(&format!("{prefix}.weight"))?.try_into()?;

    let mut linear = Linear { ws, bs: None };

    if let Ok(bias_tensor) = weights.tensor(&format!("{prefix}.bias")) {
        linear.bs = Some(bias_tensor.try_into()?);
    }

    if let Some(bs) = &mut linear.bs {
        *bs = bs.to_kind(Kind::Float);
    }

    linear.ws = linear.ws.to_kind(Kind::Float);

    Ok(linear)
}

fn load_emb(weights: &SafeTensors, prefix: &str, emb_size: i64) -> Result<Embedding> {
    let vars = VarStore::new(tch::Device::Cpu);

    let ws = weights.tensor(&format!("{prefix}.weight"))?;
    let ws: Tensor = ws.try_into()?;

    vars.variables_
        .lock()
        .unwrap()
        .named_variables
        .insert("weight".to_string(), ws.shallow_clone());

    let config = tch::nn::EmbeddingConfig::default();

    let mut emb = embedding(vars.root(), NUM_TOKENS, emb_size, config);

    emb.ws = ws.to_kind(Kind::Float);

    Ok(emb)
}

fn load_ln(weights: &SafeTensors, prefix: &str, emb_size: i64) -> Result<LayerNorm> {
    let vars = VarStore::new(tch::Device::Cpu);

    if let Ok(ws) = weights.tensor(&format!("{prefix}.weight")) {
        vars.variables_
            .lock()
            .unwrap()
            .named_variables
            .insert("weight".to_string(), ws.try_into()?);
    }

    if let Ok(bs) = weights.tensor(&format!("{prefix}.bias")) {
        vars.variables_
            .lock()
            .unwrap()
            .named_variables
            .insert("bias".to_string(), bs.try_into()?);
    }

    let config = LayerNormConfig::default();

    let mut ln = layer_norm(vars.root(), vec![emb_size], config);

    if let Ok(ws) = weights.tensor(&format!("{prefix}.weight")) {
        let ws: Tensor = ws.try_into()?;
        ln.ws = Some(ws.to_kind(Kind::Float));
    }

    if let Ok(bs) = weights.tensor(&format!("{prefix}.bias")) {
        let bs: Tensor = bs.try_into()?;
        ln.bs = Some(bs.to_kind(Kind::Float));
    }

    ln.normalized_shape = vec![emb_size];

    Ok(ln)
}

struct TimeMix {
    time_decay: Tensor,
    time_first: Tensor,

    time_mix_k: Tensor,
    time_mix_v: Tensor,
    time_mix_r: Tensor,

    key: Linear,
    value: Linear,
    receptance: Linear,
    output: Linear,

    block_idx: i64,
}
impl TimeMix {
    fn forward(&self, x: &Tensor, state: &mut Tensor) -> Tensor {
        let x = x.squeeze();
        let xk = &x * &self.time_mix_k + state.i(5 * self.block_idx + 1) * (1 - &self.time_mix_k);
        let xv = &x * &self.time_mix_v + state.i(5 * self.block_idx + 1) * (1 - &self.time_mix_v);
        let xr = &x * &self.time_mix_r + state.i(5 * self.block_idx + 1) * (1 - &self.time_mix_r);

        state.i(5 * self.block_idx + 1).copy_(&x);

        let r = self.receptance.forward_t(&xr, false).sigmoid();
        let k = self.key.forward_t(&xk, false);
        let v = self.value.forward_t(&xv, false);

        let aa = state.i(5 * self.block_idx + 2);
        let bb = state.i(5 * self.block_idx + 3);
        let pp = state.i(5 * self.block_idx + 4);

        let ww = &self.time_first + &k;
        let qq = pp.maximum(&ww);
        let e1 = (&pp - &qq).exp();
        let e2 = (ww - &qq).exp();

        let a = &e1 * &aa + &e2 * &v;
        let b = &e1 * &bb + &e2;
        let wkv = a / b;
        let ww = pp - self.time_decay.exp();
        let qq = ww.maximum(&k);
        let e1 = (ww - &qq).exp();
        let e2 = (k - &qq).exp();

        state
            .i(5 * self.block_idx + 2)
            .copy_(&(&e1 * &aa + &e2 * &v).squeeze());
        state
            .i(5 * self.block_idx + 3)
            .copy_(&(&e1 * &bb + &e2).squeeze());
        state.i(5 * self.block_idx + 4).copy_(&qq.squeeze());

        self.output.forward_t(&(wkv * &r), false)
    }

    fn load_to_device(&mut self, device: tch::Device, kind: tch::Kind) {
        self.time_decay = self.time_decay.to_kind(kind).to(device);
        self.time_first = self.time_first.to_kind(kind).to(device);

        self.time_mix_k = self.time_mix_k.to_kind(kind).to(device);
        self.time_mix_v = self.time_mix_v.to_kind(kind).to(device);
        self.time_mix_r = self.time_mix_r.to_kind(kind).to(device);

        self.key.ws = self.key.ws.to_kind(kind).to(device);
        self.key.bs = self.key.bs.as_ref().map(|t| t.to_kind(kind).to(device));

        self.value.ws = self.value.ws.to_kind(kind).to(device);
        self.value.bs = self.value.bs.as_ref().map(|t| t.to_kind(kind).to(device));

        self.receptance.ws = self.receptance.ws.to_kind(kind).to(device);
        self.receptance.bs = self
            .receptance
            .bs
            .as_ref()
            .map(|t| t.to_kind(kind).to(device));

        self.output.ws = self.output.ws.to_kind(kind).to(device);
        self.output.bs = self.output.bs.as_ref().map(|t| t.to_kind(kind).to(device));
    }
}

struct ChannelMix {
    time_mix_k: Tensor,
    time_mix_r: Tensor,

    key: Linear,
    value: Linear,

    receptance: Linear,

    block_idx: i64,
}
impl ChannelMix {
    fn forward(&self, x: &Tensor, state: &mut Tensor) -> Tensor {
        let x = x.squeeze();
        let xk = &x * &self.time_mix_k + state.i(5 * self.block_idx) * (1 - &self.time_mix_k);
        let xr = &x * &self.time_mix_r + state.i(5 * self.block_idx) * (1 - &self.time_mix_r);

        state.i(5 * self.block_idx).copy_(&x);

        let r = self.receptance.forward_t(&xr, false).sigmoid();
        let k = self.key.forward_t(&xk, false).relu().square();

        r * self.value.forward_t(&k, false)
    }

    fn load_to_device(&mut self, device: tch::Device, kind: tch::Kind) {
        self.time_mix_k = self.time_mix_k.to_kind(kind).to(device);
        self.time_mix_r = self.time_mix_r.to_kind(kind).to(device);

        self.key.ws = self.key.ws.to_kind(kind).to(device);
        self.key.bs = self.key.bs.as_ref().map(|t| t.to_kind(kind).to(device));

        self.value.ws = self.value.ws.to_kind(kind).to(device);
        self.value.bs = self.value.bs.as_ref().map(|t| t.to_kind(kind).to(device));

        self.receptance.ws = self.receptance.ws.to_kind(kind).to(device);
        self.receptance.bs = self
            .receptance
            .bs
            .as_ref()
            .map(|t| t.to_kind(kind).to(device));
    }
}

struct Block {
    ln0: Option<LayerNorm>,
    ln1: LayerNorm,
    ln2: LayerNorm,

    att: TimeMix,
    ffn: ChannelMix,

    device: tch::Device,
}

impl Block {
    fn load(weights: &SafeTensors, prefix: &str, emb_size: i64, block_idx: i64) -> Result<Self> {
        let mut ln0 = None;

        let names = weights.names();

        if names.contains(&&format!("{prefix}.ln0.weight")) {
            ln0 = Some(load_ln(weights, &format!("{prefix}.ln0"), emb_size)?);
        }

        let ln1 = load_ln(weights, &format!("{prefix}.ln1"), emb_size)?;
        let ln2 = load_ln(weights, &format!("{prefix}.ln2"), emb_size)?;

        let att = TimeMix {
            time_decay: weights
                .tensor(&format!("{prefix}.att.time_decay"))?
                .try_into()?,
            time_first: weights
                .tensor(&format!("{prefix}.att.time_first"))?
                .try_into()?,
            time_mix_k: weights
                .tensor(&format!("{prefix}.att.time_mix_k"))?
                .try_into()?,
            time_mix_v: weights
                .tensor(&format!("{prefix}.att.time_mix_v"))?
                .try_into()?,
            time_mix_r: weights
                .tensor(&format!("{prefix}.att.time_mix_r"))?
                .try_into()?,
            key: load_linear(weights, &format!("{prefix}.att.key"))?,
            value: load_linear(weights, &format!("{prefix}.att.value"))?,
            receptance: load_linear(weights, &format!("{prefix}.att.receptance"))?,
            output: load_linear(weights, &format!("{prefix}.att.output"))?,
            block_idx,
        };

        let ffn = ChannelMix {
            time_mix_k: weights
                .tensor(&format!("{prefix}.ffn.time_mix_k"))?
                .try_into()?,
            time_mix_r: weights
                .tensor(&format!("{prefix}.ffn.time_mix_r"))?
                .try_into()?,
            key: load_linear(weights, &format!("{prefix}.ffn.key"))?,
            value: load_linear(weights, &format!("{prefix}.ffn.value"))?,
            receptance: load_linear(weights, &format!("{prefix}.ffn.receptance"))?,
            block_idx,
        };

        Ok(Self {
            ln0,
            ln1,
            ln2,
            att,
            ffn,
            device: tch::Device::Cpu,
        })
    }

    fn forward(&self, mut x: Tensor, mut state: Tensor) -> (Tensor, Tensor) {
        if x.device() != self.device {
            x = x.to(self.device);
        }

        if state.device() != self.device {
            state = state.to(self.device);
        }

        x = match &self.ln0 {
            Some(ln) => ln.forward_t(&x, false),
            None => x,
        };

        x += self
            .att
            .forward(&self.ln1.forward_t(&x, false), &mut state)
            .reshape([1, -1]);
        x += self
            .ffn
            .forward(&self.ln2.forward_t(&x, false), &mut state)
            .reshape([1, -1]);

        (x, state)
    }

    fn load_to_device(&mut self, device: tch::Device, kind: tch::Kind) {
        self.device = device;

        if let Some(ln) = &mut self.ln0 {
            ln.ws = ln.ws.as_ref().map(|t| t.to_kind(kind).to(device));
            ln.bs = ln.bs.as_ref().map(|t| t.to_kind(kind).to(device));
        }

        self.att.load_to_device(device, kind);
        self.ffn.load_to_device(device, kind);

        self.ln1.ws = self.ln1.ws.as_ref().map(|t| t.to_kind(kind).to(device));
        self.ln1.bs = self.ln1.bs.as_ref().map(|t| t.to_kind(kind).to(device));

        self.ln2.ws = self.ln2.ws.as_ref().map(|t| t.to_kind(kind).to(device));
        self.ln2.bs = self.ln2.bs.as_ref().map(|t| t.to_kind(kind).to(device));
    }
}

struct RawModel {
    emb: Embedding,
    blocks: Vec<Block>,
    ln_out: LayerNorm,
    head: Linear,
}

impl RawModel {
    fn open<P: AsRef<Path>>(path: P) -> Result<RawModel> {
        // SAFETY
        // broadcasting: please don't modify the file while we're reading it.
        let mmap = unsafe {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .map_err(Error::Io)?;

            memmap2::MmapOptions::new()
                .map_mut(&file)
                .map_err(Error::Io)?
        };

        let weights = SafeTensors::deserialize(&mmap)?;
        let names = weights.names();

        let num_layers = names
            .iter()
            .filter(|name| name.starts_with("blocks"))
            .map(|name| name.split('.').nth(1).unwrap().parse::<usize>().unwrap())
            .max()
            .unwrap()
            + 1;

        let emb = weights.tensor("emb.weight")?;
        let emb_size = emb.shape()[1] as i64;

        let emb = load_emb(&weights, "emb", emb_size)?;

        let mut blocks = Vec::with_capacity(num_layers);
        for layer in 0..num_layers {
            let prefix = format!("blocks.{}", layer);
            blocks.push(Block::load(&weights, &prefix, emb_size, layer as i64)?);
        }

        Ok(Self {
            emb,
            blocks,
            ln_out: load_ln(&weights, "ln_out", emb_size)?,
            head: load_linear(&weights, "head")?,
        })
    }

    fn load_to_device(&mut self, layer_fraction: f64, device: tch::Device, kind: tch::Kind) {
        let layer_fraction = layer_fraction.max(0.0).min(1.0);

        self.emb.ws = self.emb.ws.to_kind(kind).to(device);

        let layers_to_move = (self.blocks.len() as f64 * layer_fraction).ceil() as usize;

        for block in self.blocks.iter_mut().take(layers_to_move) {
            block.load_to_device(device, kind);
        }

        if layer_fraction == 1.0 {
            self.ln_out.ws = self.ln_out.ws.as_ref().map(|t| t.to_kind(kind).to(device));
            self.ln_out.bs = self.ln_out.bs.as_ref().map(|t| t.to_kind(kind).to(device));

            self.head.ws = self.head.ws.to_kind(kind).to(device);
            self.head.bs = self.head.bs.as_ref().map(|t| t.to_kind(kind).to(device));
        }
    }

    fn init_state(&self) -> Tensor {
        let t = Tensor::zeros(
            [self.blocks.len() as i64 * 5, self.emb.ws.size()[1]],
            (tch::Kind::Float, tch::Device::Cpu),
        );

        for i in 0..self.blocks.len() as i64 {
            t.i(5 * i + 4).copy_(&Tensor::from_slice(&[-1e30f32]))
        }

        t
    }

    fn forward(&self, token: i64, state: Option<&Tensor>) -> (Tensor, Tensor) {
        tch::no_grad(|| self.forward_grad(token, state))
    }

    fn forward_grad(&self, token: i64, state: Option<&Tensor>) -> (Tensor, Tensor) {
        let mut tokens = Tensor::from_slice(&[token]);

        if self.emb.ws.device() != tokens.device() {
            tokens = tokens.to(self.emb.ws.device());
        }

        let mut x = self.emb.forward_t(&tokens, false);

        let mut state = match state {
            Some(state) => state.shallow_clone(),
            None => self.init_state(),
        };

        for block in &self.blocks {
            (x, state) = block.forward(x, state);
        }

        x = self.ln_out.forward_t(&x, false);
        x = self.head.forward_t(&x, false);

        (x, state)
    }
}

pub struct Alice {
    inner: Arc<RawModel>,
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
                accelerator.device,
                accelerator.kind,
            );
        }

        let mut summarizer = ExtractiveSummarizer::open(summarizer_path, 1)?;
        summarizer.set_window_size(30);

        let summarizer = Arc::new(summarizer);

        let inner = Arc::new(model);
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
            summarizer: Arc::clone(&self.summarizer),
        };

        Ok(ActionExecutor::new(
            action_gen,
            searcher,
            self.encryption_key,
        ))
    }
}

pub enum TokenGeneratorState {
    InProgress,
    Finished,
}

#[derive(Clone)]
struct ModelState {
    state: ClonableTensor,
    next_token: i64,
}

pub struct AliceTokenGenerator {
    model: Arc<RawModel>,
    // state needs to be a queue of size 2 to have the ability
    // to generate a new token if the current one is banned
    states: LeakyQueue<ModelState>,
    // the state of the model after each search result has been
    // loaded. This allows us to go back to the end of the previous search
    // and force speak if the model wants us to search for the same query again.
    end_search_states: Vec<LeakyQueue<ModelState>>,
    end_tokens: Vec<i64>,
    generation_state: TokenGeneratorState,
    max_new_tokens: Option<usize>,
    num_generated_tokens: usize,
    banned_tokens: Vec<i64>,
}

impl AliceTokenGenerator {
    fn new(
        model: Arc<RawModel>,
        end_tokens: Vec<i64>,
        state: Tensor,
        tokens: &[i64],
        max_new_tokens: Option<usize>,
    ) -> Result<Self> {
        if tokens.is_empty() {
            return Err(Error::EmptyInput);
        }

        // this must be 2, otherwise `.front()` and `.back()` calls are wrong throughout the generation code
        let num_states = 2;

        let mut states = LeakyQueue::new(num_states);

        let state = ClonableTensor(state);

        for _ in 0..num_states {
            states.push(ModelState {
                state: state.clone(),
                next_token: -1,
            });
        }

        let mut generator = Self {
            model,
            states,
            end_tokens,
            generation_state: TokenGeneratorState::InProgress,
            max_new_tokens,
            num_generated_tokens: 0,
            end_search_states: Vec::new(),
            banned_tokens: Vec::new(),
        };
        generator.load_tokens(tokens);

        Ok(generator)
    }

    pub fn load_tokens(&mut self, tokens: &[i64]) {
        for token in tokens {
            let (logits, new_state) = self
                .model
                .forward(*token, Some(&self.states.back().unwrap().state.0));
            let probs = logits
                .softmax(-1, Kind::Float)
                .squeeze()
                .to(tch::Device::Cpu);
            let next_token = llm_utils::sample_typical(probs, TEMP, TAU);
            self.states.push(ModelState {
                state: ClonableTensor(new_state),
                next_token,
            });

            self.states.front_mut().unwrap().next_token = *token;
        }
    }

    fn load_search_result(&mut self, tokens: &[i64]) {
        self.load_tokens(tokens);
        self.end_search_states.push(self.states.clone());
    }

    fn go_to_last_search(&mut self) {
        if let Some(last_search) = self.end_search_states.pop() {
            self.states = last_search;
        }
    }

    pub fn set_end_tokens(&mut self, tokens: &[i64]) {
        if self.end_tokens != tokens {
            self.end_tokens = tokens.to_vec();
        }
    }

    pub fn set_max_new_tokens(&mut self, max_new_tokens: Option<usize>) {
        if self.max_new_tokens != max_new_tokens {
            self.num_generated_tokens = 0;
            self.max_new_tokens = max_new_tokens;
        }
    }

    pub fn set_banned_tokens(&mut self, tokens: &[i64]) {
        if self.banned_tokens != tokens {
            self.banned_tokens = tokens.to_vec();
        }
    }
}

impl Iterator for AliceTokenGenerator {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        match self.generation_state {
            TokenGeneratorState::InProgress => {
                let s = self.states.back().unwrap();
                let mut token = s.next_token;
                let mut state = s.state.clone();

                if self.end_tokens.contains(&token) {
                    self.generation_state = TokenGeneratorState::Finished;
                    return None;
                }

                if let Some(max_new_tokens) = self.max_new_tokens {
                    if self.num_generated_tokens >= max_new_tokens {
                        if let Some(tok) = self.end_tokens.first() {
                            token = *tok;
                        }

                        self.generation_state = TokenGeneratorState::Finished;
                    }
                }

                if self.banned_tokens.contains(&token) {
                    let (output, new_state) = {
                        let s = self.states.front().unwrap();
                        self.model.forward(s.next_token, Some(&s.state.0))
                    };

                    let output = output.squeeze();

                    let probs = output
                        .softmax(-1, Kind::Float)
                        .squeeze()
                        .to(tch::Device::Cpu);

                    for banned_token in &self.banned_tokens {
                        probs
                            .i(*banned_token)
                            .copy_(&Tensor::from_slice(&[0.0]).squeeze());
                    }

                    let next_token = llm_utils::sample_typical(probs, TEMP, TAU);
                    token = next_token;
                    state = ClonableTensor(new_state);
                }

                let (output, state) = self.model.forward(token, Some(&state.0));
                let output = output.squeeze();

                let probs = output
                    .softmax(-1, Kind::Float)
                    .squeeze()
                    .to(tch::Device::Cpu);

                for banned_token in &self.banned_tokens {
                    probs
                        .i(*banned_token)
                        .copy_(&Tensor::from_slice(&[0.0]).squeeze());
                }

                let next_token = llm_utils::sample_typical(probs, TEMP, TAU);
                self.states.push(ModelState {
                    state: ClonableTensor(state),
                    next_token,
                });

                self.num_generated_tokens += 1;

                Some(token)
            }
            TokenGeneratorState::Finished => None,
        }
    }
}

pub struct AliceStringGenerator {
    token_generator: AliceTokenGenerator,
    tokenizer: Arc<Tokenizer>,
    tokens: Vec<i64>,
}

impl Iterator for AliceStringGenerator {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.token_generator.next() {
                Some(token) => {
                    self.tokens.push(token);
                    if let Some(s) = self.tokenizer.decode(&self.tokens).ok().and_then(|s| {
                        if !s.contains('\u{fffd}') {
                            // valid utf-8 string
                            Some(s)
                        } else {
                            None
                        }
                    }) {
                        self.tokens.clear();
                        return Some(s);
                    }
                }
                None if !self.tokens.is_empty() => {
                    let s = self.tokenizer.decode(&self.tokens).ok().and_then(|s| {
                        if !s.contains('\u{fffd}') {
                            Some(s)
                        } else {
                            None
                        }
                    });

                    self.tokens.clear();

                    return s;
                }
                None => return None,
            }
        }
    }
}

struct Tokenizer {
    tokenizer: tokenizers::Tokenizer,
}

impl Tokenizer {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Tokenizer> {
        let tokenizer = tokenizers::Tokenizer::from_file(path)?;

        Ok(Self { tokenizer })
    }

    pub fn encode(&self, input: String) -> Result<Vec<i64>> {
        let encoding = self.tokenizer.encode(input, false)?;

        let ids = encoding
            .get_ids()
            .iter()
            .map(|&id| id as i64)
            .collect::<Vec<_>>();

        Ok(ids)
    }

    pub fn decode(&self, tokens: &[i64]) -> Result<String> {
        let tokens = tokens.iter().map(|&id| id as u32).collect::<Vec<_>>();

        let output = self.tokenizer.decode(tokens, true)?;

        Ok(output)
    }
}

#[derive(Debug)]
pub enum RawAction {
    Search { query: Vec<i64> },
    Speak { token: i64 },
}

enum State {
    Thought {
        search: TransitionValidator,
        speaking: TransitionValidator,
    },
    QueryBuild {
        tokens: Vec<i64>,
        end: AnyTransitionValidator,
    },
    Speaking {
        banned_tokens: TTLBannedTokens,
        end: AnyTransitionValidator,
        max_new_tokens: Option<usize>,
    },
}

struct TTLBannedTokens {
    tokens: Vec<i64>,
    orig_ttl: usize,
    ttl: usize,
}

impl TTLBannedTokens {
    fn new(tokens: Vec<i64>, ttl: usize) -> Self {
        Self {
            tokens,
            ttl,
            orig_ttl: ttl,
        }
    }

    fn reset_ttl(&mut self) {
        self.ttl = self.orig_ttl;
    }

    fn tick(&mut self) {
        if self.ttl > 0 {
            self.ttl -= 1;
        }
    }

    fn set_banned_tokens(&self, token_generator: &mut AliceTokenGenerator) {
        if self.ttl > 0 {
            token_generator.set_banned_tokens(&self.tokens);
        } else {
            token_generator.set_banned_tokens(&[]);
        }
    }
}

impl State {
    fn new_thought(tokenizer: &Tokenizer) -> Self {
        let search = TransitionValidator::new(tokenizer.encode("Search:".to_string()).unwrap());
        let speaking = TransitionValidator::new(tokenizer.encode("Alice:".to_string()).unwrap());

        Self::Thought { search, speaking }
    }

    fn new_query_build(tokenizer: &Tokenizer) -> Self {
        let new_line = TransitionValidator::new(tokenizer.encode("\n".to_string()).unwrap());
        let end_of_text =
            TransitionValidator::new(tokenizer.encode("<|endoftext|>".to_string()).unwrap());
        let end = AnyTransitionValidator::new(vec![new_line, end_of_text]);

        Self::QueryBuild {
            tokens: Vec::new(),
            end,
        }
    }

    fn new_speaking(initially_banned_tokens: Vec<i64>) -> Self {
        let end = TransitionValidator::new(vec![0]);
        let double_newline = TransitionValidator::new(vec![187, 187]);
        let double_newline_single_tok = TransitionValidator::new(vec![535]);

        let end = AnyTransitionValidator::new(vec![end, double_newline, double_newline_single_tok]);

        Self::Speaking {
            end,
            max_new_tokens: Some(1024),
            banned_tokens: TTLBannedTokens::new(initially_banned_tokens, 2), // the tokens should only be banned at the start of generation
        }
    }
}

pub struct RawActionGenerator {
    state: State,
    tokenizer: Arc<Tokenizer>,
    token_generator: AliceTokenGenerator,
    queries_performed: usize,
    max_num_queries: usize,
}

impl RawActionGenerator {
    fn new(token_generator: AliceTokenGenerator, tokenizer: Arc<Tokenizer>) -> Self {
        let state = State::new_thought(&tokenizer);

        let mut token_generator = token_generator;
        token_generator.set_end_tokens(&[]);

        Self {
            state,
            tokenizer,
            token_generator,
            queries_performed: 0,
            max_num_queries: 3,
        }
    }

    fn force_speaking(&mut self) {
        self.token_generator.load_tokens(
            &self
                .tokenizer
                .encode("Alice[thought]: I now know the final answer.\n\n".to_string())
                .unwrap(),
        );

        self.state = State::new_speaking(vec![
            self.tokenizer.encode(" [thought]:".to_string()).unwrap()[0],
            self.tokenizer.encode("[thought]:".to_string()).unwrap()[0],
        ]);

        self.token_generator
            .load_tokens(&self.tokenizer.encode("Alice:".to_string()).unwrap());
    }
}

impl Iterator for RawActionGenerator {
    type Item = RawAction;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.state {
                State::Thought { search, speaking } => {
                    let token = self.token_generator.next()?;
                    self.token_generator.set_max_new_tokens(None);

                    if self.queries_performed >= self.max_num_queries {
                        self.force_speaking();
                    } else if search.validate(token) {
                        self.state = State::new_query_build(&self.tokenizer);
                    } else if speaking.validate(token) {
                        self.state = State::new_speaking(vec![]);
                    }
                }
                State::QueryBuild { tokens, end } => {
                    let token = self.token_generator.next()?;
                    self.token_generator.set_max_new_tokens(None);
                    tokens.push(token);

                    if end.validate(token) {
                        let query = tokens.clone();
                        *tokens = Vec::new();
                        self.state = State::new_thought(&self.tokenizer);
                        self.queries_performed += 1;
                        return Some(RawAction::Search { query });
                    }
                }
                State::Speaking {
                    end,
                    max_new_tokens,
                    banned_tokens,
                } => {
                    self.token_generator.set_max_new_tokens(*max_new_tokens);
                    banned_tokens.set_banned_tokens(&mut self.token_generator);

                    let token = self.token_generator.next()?;

                    if end.validate(token) {
                        banned_tokens.reset_ttl();
                        self.state = State::new_thought(&self.tokenizer);
                        return None;
                    } else {
                        banned_tokens.tick();
                        return Some(RawAction::Speak { token });
                    }
                }
            }
        }
    }
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

struct Searcher {
    url: String,
    summarizer: Arc<ExtractiveSummarizer>,
}

impl Searcher {
    async fn raw_search(&self, query: &str) -> Result<WebsitesResult> {
        let client = reqwest::Client::new();
        let query = ApiSearchQuery {
            query: query.trim().to_string(),
            num_results: Some(3),
            optic: None, // TODO: let user specify optic
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

    async fn search(&self, query: &str) -> Result<Vec<SimplifiedWebsite>> {
        let res = self.raw_search(query).await?;

        let mut websites = Vec::new();

        for website in res.webpages {
            websites.push(SimplifiedWebsite::new(website, query, &self.summarizer));
        }

        tracing::debug!("search result: {:#?}", websites);

        Ok(websites)
    }
}

enum Action {
    Search { query: String },
    Speak { text: String },
}

struct ActionGenerator {
    raw: RawActionGenerator,
    tokens_to_speak: Vec<i64>,
}

impl ActionGenerator {
    fn new(raw_action_gen: RawActionGenerator) -> ActionGenerator {
        Self {
            raw: raw_action_gen,
            tokens_to_speak: Vec::new(),
        }
    }
}

impl Iterator for ActionGenerator {
    type Item = Action;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let action = self.raw.next();

            match action {
                Some(RawAction::Search { query }) => {
                    return Some(Action::Search {
                        query: self
                            .raw
                            .tokenizer
                            .decode(&query)
                            .unwrap()
                            .trim()
                            .to_ascii_lowercase(),
                    });
                }
                Some(RawAction::Speak { token }) => {
                    self.tokens_to_speak.push(token);

                    if let Some(s) = self
                        .raw
                        .tokenizer
                        .decode(&self.tokens_to_speak)
                        .ok()
                        .and_then(|s| {
                            if !s.contains('\u{fffd}') {
                                // valid utf-8 string
                                Some(s)
                            } else {
                                None
                            }
                        })
                    {
                        self.tokens_to_speak.clear();
                        return Some(Action::Speak { text: s });
                    }
                }
                None if !self.tokens_to_speak.is_empty() => {
                    let s = self
                        .raw
                        .tokenizer
                        .decode(&self.tokens_to_speak)
                        .ok()
                        .and_then(|s| {
                            if !s.contains('\u{fffd}') {
                                Some(s)
                            } else {
                                None
                            }
                        });

                    self.tokens_to_speak.clear();

                    return s.map(|s| Action::Speak { text: s });
                }
                None => {
                    return None;
                }
            }
        }
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

pub struct ActionExecutor {
    generator: ActionGenerator,
    searcher: Searcher,
    query_to_search: Option<String>,
    queries_performed: Vec<String>,
    has_finished: bool,
    encryption_key: Key<Aes256Gcm>,
}

unsafe impl Send for ActionExecutor {}

impl ActionExecutor {
    fn new(
        action_gen: ActionGenerator,
        searcher: Searcher,
        encryption_key: Key<Aes256Gcm>,
    ) -> Self {
        ActionExecutor {
            generator: action_gen,
            searcher,
            query_to_search: None,
            queries_performed: Vec::new(),
            has_finished: false,
            encryption_key,
        }
    }
    pub fn state(&self) -> Option<Tensor> {
        self.generator
            .raw
            .token_generator
            .states
            .back()
            .map(|s| s.state.clone().0)
    }

    fn load_search_result(&mut self, result: &[SimplifiedWebsite]) {
        let result = result
            .iter()
            .map(|r| ModelWebsite::from(r.clone()))
            .collect::<Vec<_>>();

        let json = serde_json::to_string(&result).unwrap();

        let tokens = self
            .generator
            .raw
            .tokenizer
            .encode(format!("Result: {json}<|endoftext|>\n\n"))
            .unwrap();
        self.generator
            .raw
            .token_generator
            .load_search_result(&tokens);

        let tokens = self
            .generator
            .raw
            .tokenizer
            .encode("Alice[thought]:".to_string())
            .unwrap();
        self.generator.raw.token_generator.load_tokens(&tokens);
    }

    pub async fn next(&mut self) -> Option<ExecutionState> {
        if self.has_finished {
            return None;
        }

        if let Some(query) = self.query_to_search.take() {
            if self.queries_performed.contains(&query) {
                self.generator.raw.token_generator.go_to_last_search();
                self.generator.raw.force_speaking();
            } else {
                let res = self.searcher.search(&query).await.unwrap_or_default();

                tracing::debug!("loading search results");
                self.load_search_result(&res);
                tracing::debug!("done loading search results");
                self.queries_performed.push(query.clone());

                return Some(ExecutionState::SearchResult { query, result: res });
            }
        }

        let action = self.generator.next();

        match action {
            Some(Action::Search { query }) => {
                self.query_to_search = Some(query.clone());
                Some(ExecutionState::BeginSearch { query })
            }
            Some(Action::Speak { text }) => Some(ExecutionState::Speaking { text }),
            None if !self.has_finished => {
                self.has_finished = true;

                let state: Vec<Vec<f32>> = self
                    .state()
                    .unwrap()
                    .to_device(tch::Device::Cpu)
                    .try_into()
                    .unwrap();

                let encrypted = EncryptedState::encrypt(state, &self.encryption_key).unwrap();

                Some(ExecutionState::Done {
                    state: EncodedEncryptedState::encode(encrypted),
                })
            }
            None => None,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EncryptedState {
    nonce: Vec<u8>,
    // ciphertext is bincoded `CompressedState` encrypted with `nonce` and a key
    ciphertext: Vec<u8>,
}

impl EncryptedState {
    fn encrypt(state: Vec<Vec<f32>>, key: &Key<Aes256Gcm>) -> Result<Self> {
        let compressed = compress_state(state);

        let cipher = Aes256Gcm::new(key);
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        let bytes = bincode::serialize(&compressed).unwrap();

        let ciphertext = cipher.encrypt(&nonce, bytes.as_slice()).unwrap();

        Ok(Self {
            nonce: nonce.to_vec(),
            ciphertext,
        })
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
