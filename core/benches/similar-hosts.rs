use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use stract::{
    ranking::inbound_similarity::InboundSimilarity, similar_hosts::SimilarHostsFinder,
    webgraph::WebgraphBuilder,
};

const WEBGRAPH_PATH: &str = "data/webgraph";
const INBOUND_SIMILARITY_PATH: &str = "data/centrality/inbound_similarity";

pub fn criterion_benchmark(c: &mut Criterion) {
    let webgraph = Arc::new(WebgraphBuilder::new(WEBGRAPH_PATH).open());
    let inbound = InboundSimilarity::open(INBOUND_SIMILARITY_PATH).unwrap();

    let finder = SimilarHostsFinder::new(
        webgraph,
        inbound,
        stract::config::defaults::WebgraphServer::max_similar_hosts(),
    );

    for _ in 0..10 {
        c.bench_function("similar_hosts", |b| {
            b.iter(|| finder.find_similar_hosts(&["google.com".to_string()], 100))
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
