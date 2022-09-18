use criterion::{criterion_group, criterion_main, Criterion};
use cuely::{index::Index, searcher::Searcher};

const INDEX_PATH: &str = "data/index";

macro_rules! bench {
    ($query:tt, $searcher:ident, $c:ident) => {
        let mut desc = "search '".to_string();
        desc.push_str($query);
        desc.push('\'');
        $c.bench_function(desc.as_str(), |b| {
            b.iter(|| $searcher.search($query, None, None, None).unwrap())
        });
    };
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let index = Index::open(INDEX_PATH).unwrap();
    let searcher = Searcher::new(index, None, None);

    // for _ in 0..100 {
    bench!("the", searcher, c);
    bench!("dtu", searcher, c);
    bench!("the best", searcher, c);
    bench!("the circle of life", searcher, c);
    // }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
