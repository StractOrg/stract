#!/usr/bin/env python3

from __future__ import annotations

import argparse
import contextlib
import json
import sys
from pathlib import Path
from typing import Any, ContextManager, Iterator, cast

import numpy as np
import torch

import gguf

class Model:
    def __init__(self, dir_model: Path, ftype: int, fname_out: Path):
        self.dir_model = dir_model
        self.ftype = ftype
        self.fname_out = fname_out
        self.is_safetensors = self._is_model_safetensors()
        self.model_name = "model.safetensors" if self.is_safetensors else "pytorch_model.bin"
        self.hparams = Model.load_hparams(self.dir_model)
        self.gguf_writer = gguf.GGUFWriter(fname_out, gguf.MODEL_ARCH_NAMES[gguf.MODEL_ARCH.BERT], endianess=gguf.GGUFEndian.LITTLE, use_temp_file=False)

    def get_tensors(self) -> Iterator[tuple[str, torch.Tensor]]:
        ctx: ContextManager[Any]
        if self.is_safetensors:
            from safetensors import safe_open
            ctx = cast(ContextManager[Any], safe_open(self.dir_model / self.model_name, framework="pt", device="cpu"))
        else:
            ctx = contextlib.nullcontext(torch.load(str(self.dir_model / self.model_name), map_location="cpu", weights_only=True))

        with ctx as model_part:
            for name in model_part.keys():
                data = model_part.get_tensor(name) if self.is_safetensors else model_part[name]
                yield name, data

    def set_gguf_parameters(self):
        self.gguf_writer.add_name(self.dir_model.name)
        self.gguf_writer.add_block_count(self.hparams.get(
            "n_layers", self.hparams.get("num_hidden_layers", self.hparams.get("n_layer")),
        ))
        if (n_ctx := self.hparams.get("max_position_embeddings")) is not None:
            self.gguf_writer.add_context_length(n_ctx)
        if (n_embd := self.hparams.get("hidden_size")) is not None:
            self.gguf_writer.add_embedding_length(n_embd)
        if (n_ff := self.hparams.get("intermediate_size")) is not None:
            self.gguf_writer.add_feed_forward_length(n_ff)
        if (n_head := self.hparams.get("num_attention_heads")) is not None:
            self.gguf_writer.add_head_count(n_head)
        if (n_head_kv := self.hparams.get("num_key_value_heads")) is not None:
            self.gguf_writer.add_head_count_kv(n_head_kv)

        if (n_rms_eps := self.hparams.get("rms_norm_eps")) is not None:
            self.gguf_writer.add_layer_norm_rms_eps(n_rms_eps)
        if (n_experts := self.hparams.get("num_local_experts")) is not None:
            self.gguf_writer.add_expert_count(n_experts)
        if (n_experts_used := self.hparams.get("num_experts_per_tok")) is not None:
            self.gguf_writer.add_expert_used_count(n_experts_used)

        self.gguf_writer.add_parallel_residual(self.hparams.get("use_parallel_residual", True))

    def write_tensors(self):
        for name, data_torch in self.get_tensors():
            # we don't need these
            if name.endswith((".attention.masked_bias", ".attention.bias", ".attention.rotary_emb.inv_freq")):
                continue

            old_dtype = data_torch.dtype

            # convert any unsupported data types to float32
            if data_torch.dtype not in (torch.float16, torch.float32):
                data_torch = data_torch.to(torch.float32)

            data = data_torch.squeeze().numpy()

            n_dims = len(data.shape)
            data_dtype = data.dtype

            # if f32 desired, convert any float16 to float32
            if self.ftype == 0 and data_dtype == np.float16:
                data = data.astype(np.float32)

            # TODO: Why cant we use these float16 as-is? There should be not reason to store float16 as float32
            if self.ftype == 1 and data_dtype == np.float16 and n_dims == 1:
                data = data.astype(np.float32)

            # if f16 desired, convert any float32 2-dim weight tensors to float16
            if self.ftype == 1 and data_dtype == np.float32 and name.endswith(".weight") and n_dims == 2:
                data = data.astype(np.float16)

            print(f"{name}, n_dims = {n_dims}, {old_dtype} --> {data.dtype}")

            self.gguf_writer.add_tensor(name, data)

    def write(self):
        self.write_tensors()
        self.gguf_writer.write_header_to_file()
        self.gguf_writer.write_kv_data_to_file()
        self.gguf_writer.write_tensors_to_file()
        self.gguf_writer.close()

    @staticmethod
    def load_hparams(dir_model):
        with open(dir_model / "config.json", "r", encoding="utf-8") as f:
            return json.load(f)

    def _is_model_safetensors(self) -> bool:
        return (self.dir_model / "model.safetensors").exists()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a huggingface model to a GGML compatible file")
    parser.add_argument(
        "--outtype", type=str, choices=["f32", "f16"], default="f16",
        help="output format - use f32 for float32, f16 for float16",
    )
    parser.add_argument(
        "model", type=Path,
        help="directory containing model file",
    )
    parser.add_argument(
        "outfile", type=Path,
        help="path to write to",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    dir_model = args.model

    if not dir_model.is_dir():
        print(f'Error: {args.model} is not a directory', file=sys.stderr)
        sys.exit(1)

    ftype_map = {
        "f32": gguf.GGMLQuantizationType.F32,
        "f16": gguf.GGMLQuantizationType.F16,
    }

    if args.outfile is not None:
        fname_out = args.outfile
    else:
        # output in the same directory as the model by default
        fname_out = dir_model / f'ggml-model-{args.outtype}.gguf'

    print(f"Loading model: {dir_model.name}")

    hparams = Model.load_hparams(dir_model)

    with torch.inference_mode():
        model_instance = Model(dir_model, ftype_map[args.outtype], fname_out)

        print("Set model parameters")
        model_instance.set_gguf_parameters()

        print(f"Exporting model to '{fname_out}'")
        model_instance.write()

        print(f"Model successfully exported to '{fname_out}'")


if __name__ == '__main__':
    main()
