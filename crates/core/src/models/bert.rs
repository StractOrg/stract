use ggml::{nn, Context, Tensor};

struct BertEmbeddings {
    word_embeddings: nn::Embedding,
    position_embeddings: nn::Embedding,
    token_type_embeddings: nn::Embedding,
    layer_norm: nn::LayerNorm,
}

impl BertEmbeddings {
    pub fn new(ctx: &mut Context, vocab_size: u64, hidden_size: u64) -> Self {
        let word_embeddings = nn::Embedding::new(ctx, vocab_size, hidden_size);
        let position_embeddings = nn::Embedding::new(ctx, 512, hidden_size);
        let token_type_embeddings = nn::Embedding::new(ctx, 2, hidden_size);
        let layer_norm = nn::LayerNorm::new(1e-12);

        Self {
            word_embeddings,
            position_embeddings,
            token_type_embeddings,
            layer_norm,
        }
    }

    pub fn forward(
        &mut self,
        input_ids: &Tensor<1>,
        token_type_ids: &Tensor<1>,
        position_ids: &Tensor<1>,
    ) -> Tensor<2> {
        let word_embeddings = self.word_embeddings.forward(input_ids);
        let position_embeddings = self.position_embeddings.forward(position_ids);
        let token_type_embeddings = self.token_type_embeddings.forward(token_type_ids);

        let embeddings = word_embeddings + position_embeddings + token_type_embeddings;

        self.layer_norm.forward(&embeddings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
