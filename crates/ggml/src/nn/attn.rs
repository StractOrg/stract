use super::Linear;
use crate::{Context, GgmlType, Tensor};

pub struct MultiheadAttention {
    query: Linear,
    key: Linear,
    value: Linear,
    n_embed: u64,
    n_heads: u64,
    head_size: u64,
}
impl MultiheadAttention {
    pub fn new(ctx: &mut Context, t: GgmlType, n_embed: u64, n_heads: u64) -> Self {
        let head_size = n_embed / n_heads;
        let head_emb = n_heads * head_size;

        let query = Linear::new(ctx, t, n_embed, head_emb);
        let key = Linear::new(ctx, t, n_embed, head_emb);
        let value = Linear::new(ctx, t, n_embed, head_emb);

        Self {
            query,
            key,
            value,
            n_embed,
            n_heads,
            head_size,
        }
    }

    pub fn forward(&self, input: &Tensor<3>) -> Tensor<3> {
        let toks = input.shape()[1];
        let batch_size = input.shape()[2];
        let head_shape = [self.head_size, self.n_heads, toks, batch_size];

        let query = self
            .query
            .forward(input)
            .reshape(head_shape)
            .permute([0, 2, 1, 3]);

        let key = self
            .key
            .forward(input)
            .reshape(head_shape)
            .permute([0, 2, 1, 3]);

        let value = self
            .value
            .forward(input)
            .reshape(head_shape)
            .permute([0, 2, 1, 3]);

        let kq = (key * query)
            .scale(1.0 / (self.n_embed as f32).sqrt())
            .softmax();

        let kqv = value.permute([1, 0, 2, 3]).contiguous() * kq;
        let out_dims = [self.n_heads * self.head_size, toks, batch_size];

        kqv.contiguous().reshape(out_dims)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_attention() {
        let mut ctx = Context::new(128 * 1024 * 1024, 1);
        let input = Tensor::new(&mut ctx, GgmlType::F32, [128, 3, 5]);
        let attention = MultiheadAttention::new(&mut ctx, GgmlType::F32, 128, 12);
        let output = attention.forward(&input);
        assert_eq!(output.shape(), [120, 3, 5]);
    }
}
