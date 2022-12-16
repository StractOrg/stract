use criterion::{criterion_group, criterion_main, Criterion};
use cuely::webgraph::{
    centrality::approximate_harmonic::ApproximatedHarmonicCentrality, WebgraphBuilder,
};

const WEBGRAPH_PATH: &str = "data/webgraph";

pub fn criterion_benchmark(c: &mut Criterion) {
    let webgraph = WebgraphBuilder::new(WEBGRAPH_PATH).with_host_graph().open();
    c.bench_function("Approximated harmonic centrality calculation", |b| {
        b.iter(|| {
            for _ in 0..10 {
                ApproximatedHarmonicCentrality::new(&webgraph);
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
