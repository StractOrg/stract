use crate::{Context, GgmlType, Tensor};

pub struct Embedding {
    tensor: Tensor<2>,
}

impl Embedding {
    pub fn new(ctx: &mut Context, t: GgmlType, vocab_size: u64, embedding_dim: u64) -> Self {
        let tensor = Tensor::new(ctx, t, [embedding_dim, vocab_size]);

        Self { tensor }
    }

    pub fn forward(&mut self, input: &Tensor<1>) -> Tensor<2> {
        self.tensor.get_rows(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedding() {
        let mut ctx = Context::new(128 * 1024 * 1024, 1);

        let mut embedding = Embedding::new(&mut ctx, GgmlType::F32, 10, 128);

        let input = Tensor::new(&mut ctx, GgmlType::I32, [10]);

        let output = embedding.forward(&input);

        assert_eq!(output.shape(), [128, 10]);
    }
}
