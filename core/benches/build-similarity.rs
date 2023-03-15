use criterion::{criterion_group, criterion_main, Criterion};
use stract::{
    ranking::{centrality_store::CentralityStore, inbound_similarity::InboundSimilarity},
    webgraph::WebgraphBuilder,
};

const WEBGRAPH_PATH: &str = "data/webgraph";

pub fn criterion_benchmark(c: &mut Criterion) {
    let graph = WebgraphBuilder::new(WEBGRAPH_PATH).open();
    let store = CentralityStore::build_harmonic(&graph, stract::gen_temp_path());

    c.bench_function("Inbound similarity creation", |b| {
        b.iter(|| {
            InboundSimilarity::build(&graph, &store.harmonic);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
