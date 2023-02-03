use criterion::{criterion_group, criterion_main, Criterion};
use cuely::{
    ranking::centrality_store::HarmonicCentralityStore,
    webgraph::{
        centrality::{harmonic::HarmonicCentrality, online_harmonic::OnlineHarmonicCentrality},
        WebgraphBuilder,
    },
};

const WEBGRAPH_PATH: &str = "data/webgraph";

pub fn criterion_benchmark(c: &mut Criterion) {
    let webgraph = WebgraphBuilder::new(WEBGRAPH_PATH).open();
    let centrality = HarmonicCentrality::calculate(&webgraph);

    let harmonic_centrality_store = HarmonicCentralityStore::open(cuely::gen_temp_path());
    for (node, centrality) in centrality.host {
        harmonic_centrality_store
            .host
            .insert(webgraph.node2id(&node).unwrap(), centrality);
    }
    harmonic_centrality_store.host.flush();

    c.bench_function("Online harmonic centrality calculation", |b| {
        b.iter(|| {
            for _ in 0..10 {
                OnlineHarmonicCentrality::new(&webgraph, &harmonic_centrality_store);
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
