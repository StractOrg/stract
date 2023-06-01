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

use tch::{Kind, Tensor};

pub fn sample_typical(probs: Tensor, temp: f64, tau: f64) -> i64 {
    debug_assert!(probs.dim() == 1, "batch support not implemented");
    let mut probs = probs;

    let logits = -probs.log();
    let ent = (&logits * &probs).nansum(-1, true, Kind::Float);
    let shifted_logits = (&logits - ent).abs();
    let sorted_ids = shifted_logits.argsort(-1, true);
    let sorted_logits = shifted_logits.index_select(-1, &sorted_ids);
    let sorted_probs = probs.index_select(-1, &sorted_ids);
    let cumulative_probs = sorted_probs.cumsum(-1, Kind::Float);
    let cutoff = cumulative_probs.lt(tau).sum(Kind::Int64).int64_value(&[]);

    probs = probs.index_fill_(-1, &sorted_logits.gt(cutoff).to_kind(Kind::Int64), 0.0);

    if temp != 1.0 {
        let t = Tensor::from_slice(&[1.0 / temp]);
        probs = probs.pow(&t);
    }

    probs.multinomial(1, true).int64_value(&[0])
}

pub struct ClonableTensor(pub Tensor);

impl Clone for ClonableTensor {
    fn clone(&self) -> Self {
        let out = Tensor::empty(self.0.size(), (self.0.kind(), self.0.device()));
        ClonableTensor(self.0.clone(&out))
    }
}
