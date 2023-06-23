use std::{fs::OpenOptions, path::Path};

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
use crate::alice::{Error, Result};
use safetensors::SafeTensors;
use tch::{
    nn::{embedding, layer_norm, Embedding, LayerNorm, LayerNormConfig, ModuleT, VarStore},
    IndexOp, Kind, Tensor,
};

const NUM_TOKENS: i64 = 50277;

enum Linear {
    Normal(tch::nn::Linear),
    Quantized {
        ws: Tensor,
        bs: Option<Tensor>,
        scale: Tensor,
        zero_point: Tensor,
    },
}

fn quantize_tensor_per_channel(tensor: &Tensor) -> (Tensor, Tensor, Tensor) {
    let min_vals = tensor.amin(1, true);
    let max_vals = tensor.amax(1, true);
    let qmin: f64 = -127.0; // for qint8
    let qmax: f64 = 127.0; // for qint8

    // calculate scale and zero_point
    let scale = (&max_vals - &min_vals) / (qmax - qmin);
    let initial_zero_point = qmin - &min_vals / &scale;

    // clamp zero_point to qint8 range
    let zero_point = initial_zero_point
        .clamp(qmin, qmax)
        .round()
        .to_kind(Kind::Float);

    let scale = scale.to_kind(Kind::Float);

    let quantized_weights = ((tensor / &scale) + &zero_point)
        .clamp(qmin, qmax)
        .round()
        .to_kind(Kind::Int8);

    (quantized_weights, scale, zero_point)
}

fn dequantize(tensor: &Tensor, scale: &Tensor, zero_point: &Tensor) -> Tensor {
    (tensor.to_kind(Kind::Float) - zero_point) * scale
}

impl Linear {
    fn forward_t(&self, x: &Tensor, train: bool) -> Tensor {
        match self {
            Linear::Normal(linear) => linear.forward_t(x, train),
            Linear::Quantized {
                ws,
                bs,
                scale,
                zero_point,
            } => {
                let ws = dequantize(ws, scale, zero_point);
                let mut output = x.matmul(&ws.transpose(-2, -1));
                if let Some(bias) = bs {
                    output += bias;
                }
                output
            }
        }
    }

    fn ws(&self) -> &Tensor {
        match self {
            Linear::Normal(linear) => &linear.ws,
            Linear::Quantized { ws, .. } => ws,
        }
    }

    fn set_ws(&mut self, ws: Tensor) {
        match self {
            Linear::Normal(linear) => linear.ws = ws,
            Linear::Quantized { .. } => {
                unimplemented!();
            }
        }
    }

    fn bs(&self) -> Option<&Tensor> {
        match self {
            Linear::Normal(linear) => linear.bs.as_ref(),
            Linear::Quantized { bs, .. } => bs.as_ref(),
        }
    }

    fn set_bs(&mut self, bs: Option<Tensor>) {
        if let Some(bs) = bs {
            match self {
                Linear::Normal(linear) => linear.bs = Some(bs),
                Linear::Quantized { bs: bs_, .. } => *bs_ = Some(bs),
            }
        }
    }

    fn quantize(&self) -> Self {
        match self {
            Self::Quantized {
                ws,
                bs,
                scale,
                zero_point,
            } => {
                let new_ws = Tensor::empty(ws.size(), (ws.kind(), ws.device()));
                let _ = ws.clone(&new_ws);

                let mut new_bs = None;

                if let Some(bs) = bs {
                    new_bs = Some(Tensor::empty(bs.size(), (bs.kind(), bs.device())));
                    let _ = bs.clone(new_bs.as_mut().unwrap());
                }

                let new_scale = Tensor::empty(scale.size(), (scale.kind(), scale.device()));
                let _ = scale.clone(&new_scale);

                let new_zeropoint =
                    Tensor::empty(zero_point.size(), (zero_point.kind(), zero_point.device()));
                let _ = zero_point.clone(&new_zeropoint);

                Self::Quantized {
                    ws: new_ws,
                    bs: new_bs,
                    scale: new_scale,
                    zero_point: new_zeropoint,
                }
            }
            Self::Normal(linear) => {
                let (ws, scale, zero_point) = quantize_tensor_per_channel(&linear.ws);

                let mut new_bs = None;
                if let Some(bs) = &linear.bs {
                    new_bs = Some(Tensor::empty(bs.size(), (bs.kind(), bs.device())));
                    let _ = bs.clone(new_bs.as_mut().unwrap());
                }

                Self::Quantized {
                    ws,
                    bs: new_bs,
                    scale,
                    zero_point,
                }
            }
        }
    }
}

fn load_linear(weights: &SafeTensors, prefix: &str) -> Result<Linear> {
    let ws = weights.tensor(&format!("{prefix}.weight"))?.try_into()?;

    let mut linear = tch::nn::Linear { ws, bs: None };

    if let Ok(bias_tensor) = weights.tensor(&format!("{prefix}.bias")) {
        linear.bs = Some(bias_tensor.try_into()?);
    }

    if let Some(bs) = &mut linear.bs {
        *bs = bs.to_kind(Kind::Float);
    }

    linear.ws = linear.ws.to_kind(Kind::Float);

    Ok(Linear::Normal(linear))
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

    pub key: Linear,
    pub value: Linear,
    pub receptance: Linear,
    pub output: Linear,

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

        self.key.set_ws(self.key.ws().to_kind(kind).to(device));
        self.key
            .set_bs(self.key.bs().map(|t| t.to_kind(kind).to(device)));

        self.value.set_ws(self.value.ws().to_kind(kind).to(device));
        self.value
            .set_bs(self.value.bs().map(|t| t.to_kind(kind).to(device)));

        self.receptance
            .set_ws(self.receptance.ws().to_kind(kind).to(device));
        self.receptance
            .set_bs(self.receptance.bs().map(|t| t.to_kind(kind).to(device)));

        self.output
            .set_ws(self.output.ws().to_kind(kind).to(device));
        self.output
            .set_bs(self.output.bs().map(|t| t.to_kind(kind).to(device)));
    }
}

struct ChannelMix {
    time_mix_k: Tensor,
    time_mix_r: Tensor,

    pub key: Linear,
    pub value: Linear,
    pub receptance: Linear,

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

        self.key.set_ws(self.key.ws().to_kind(kind).to(device));
        self.key
            .set_bs(self.key.bs().map(|t| t.to_kind(kind).to(device)));

        self.value.set_ws(self.value.ws().to_kind(kind).to(device));
        self.value
            .set_bs(self.value.bs().map(|t| t.to_kind(kind).to(device)));

        self.receptance
            .set_ws(self.receptance.ws().to_kind(kind).to(device));
        self.receptance
            .set_bs(self.receptance.bs().map(|t| t.to_kind(kind).to(device)));
    }
}

struct Block {
    ln0: Option<LayerNorm>,
    ln1: LayerNorm,
    ln2: LayerNorm,

    pub att: TimeMix,
    pub ffn: ChannelMix,

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

    fn quantize(&mut self) {
        self.att.key = self.att.key.quantize();
        self.att.value = self.att.value.quantize();
        self.att.receptance = self.att.receptance.quantize();

        self.ffn.key = self.ffn.key.quantize();
        self.ffn.value = self.ffn.value.quantize();
        self.ffn.receptance = self.ffn.receptance.quantize();
    }
}

pub struct RawModel {
    emb: Embedding,
    blocks: Vec<Block>,
    ln_out: LayerNorm,
    head: Linear,
}

impl RawModel {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<RawModel> {
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

    pub fn load_to_device(
        &mut self,
        layer_fraction: f64,
        quantized_fraction: f64,
        device: tch::Device,
        kind: tch::Kind,
    ) {
        let layer_fraction = layer_fraction.max(0.0).min(1.0);
        let quantized_fraction = quantized_fraction.max(0.0).min(1.0);

        self.emb.ws = self.emb.ws.to_kind(kind).to(device);

        let layers_to_move = (self.blocks.len() as f64 * layer_fraction).ceil() as usize;
        let layers_to_quantize = ((layers_to_move as f64) * quantized_fraction).ceil() as usize;

        for (idx, block) in self.blocks.iter_mut().take(layers_to_move).enumerate() {
            block.load_to_device(device, kind);

            // quantize the last layers
            if idx > layers_to_move - layers_to_quantize {
                block.quantize();
            }
        }

        if layer_fraction == 1.0 {
            self.ln_out.ws = self.ln_out.ws.as_ref().map(|t| t.to_kind(kind).to(device));
            self.ln_out.bs = self.ln_out.bs.as_ref().map(|t| t.to_kind(kind).to(device));

            self.head.set_ws(self.head.ws().to_kind(kind).to(device));
            self.head
                .set_bs(self.head.bs().map(|t| t.to_kind(kind).to(device)));
        }
    }

    pub fn init_state(&self) -> Tensor {
        let t = Tensor::zeros(
            [self.blocks.len() as i64 * 5, self.emb.ws.size()[1]],
            (tch::Kind::Float, tch::Device::Cpu),
        );

        for i in 0..self.blocks.len() as i64 {
            t.i(5 * i + 4).copy_(&Tensor::from_slice(&[-1e30f32]))
        }

        t
    }

    pub fn forward(&self, token: i64, state: Option<&Tensor>) -> (Tensor, Tensor) {
        tch::no_grad(|| self.forward_grad(token, state))
    }

    pub fn forward_grad(&self, token: i64, state: Option<&Tensor>) -> (Tensor, Tensor) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantize() {
        let t = tch::Tensor::randn([10, 10], (tch::Kind::Float, tch::Device::Cpu));
        let (quantized, scale, zp) = quantize_tensor_per_channel(&t);
        let dequantized = dequantize(&quantized, &scale, &zp);

        assert!((t - &dequantized).abs().max().double_value(&[]) < 0.1);
    }
}
