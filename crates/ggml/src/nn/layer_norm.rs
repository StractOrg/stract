use crate::{Dims, Tensor, ValidDims};

pub struct LayerNorm {
    eps: f32,
}

impl LayerNorm {
    pub fn new(eps: f32) -> Self {
        Self { eps }
    }

    pub fn forward<const DIMS: usize>(&mut self, input: &Tensor<DIMS>) -> Tensor<DIMS>
    where
        Dims<DIMS>: ValidDims,
    {
        input.norm(self.eps)
    }
}
