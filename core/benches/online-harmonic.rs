use criterion::{criterion_group, criterion_main, Criterion};
use cuely::webgraph::{centrality::online_harmonic::OnlineHarmonicCentrality, WebgraphBuilder};

const WEBGRAPH_PATH: &str = "data/webgraph";

pub fn criterion_benchmark(c: &mut Criterion) {
    let webgraph = WebgraphBuilder::new(WEBGRAPH_PATH).open();
    c.bench_function("Online harmonic centrality calculation", |b| {
        b.iter(|| {
            for _ in 0..10 {
                OnlineHarmonicCentrality::new(&webgraph);
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
