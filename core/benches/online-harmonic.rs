use criterion::{criterion_group, criterion_main, Criterion};
use cuely::webgraph::{
    centrality::{harmonic::HarmonicCentrality, online_harmonic::OnlineHarmonicCentrality},
    WebgraphBuilder,
};

const WEBGRAPH_PATH: &str = "data/webgraph";

pub fn criterion_benchmark(c: &mut Criterion) {
    let webgraph = WebgraphBuilder::new(WEBGRAPH_PATH).open();
    let centrality = HarmonicCentrality::calculate(&webgraph);
    c.bench_function("Online harmonic centrality calculation", |b| {
        b.iter(|| {
            for _ in 0..10 {
                OnlineHarmonicCentrality::new(&webgraph, &centrality);
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
