use criterion::{criterion_group, criterion_main, Criterion};
use optics::Optic;
use stract_core::{
    index::Index,
    searcher::{LocalSearcher, SearchQuery},
};

const INDEX_PATH: &str = "data/index";

macro_rules! bench {
    ($query:tt, $searcher:ident, $optic:ident, $c:ident) => {
        let mut desc = "search '".to_string();
        desc.push_str($query);
        desc.push('\'');
        desc.push_str(" with optic");
        $c.bench_function(desc.as_str(), |b| {
            b.iter(|| {
                $searcher
                    .search(&SearchQuery {
                        query: $query.to_string(),
                        optic: Some(Optic::parse($optic).unwrap()),
                        ..Default::default()
                    })
                    .unwrap()
            })
        });
    };
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let index = Index::open(INDEX_PATH.as_ref()).unwrap();
    let searcher = LocalSearcher::new(index);
    let optic = include_str!("../../optics/testcases/samples/discussions.optic");

    for _ in 0..10_000 {
        bench!("the", searcher, optic, c);
        bench!("dtu", searcher, optic, c);
        bench!("the best", searcher, optic, c);
        bench!("the circle of life", searcher, optic, c);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
