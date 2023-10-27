use criterion::{criterion_group, criterion_main, Criterion};
use spell::Spell;
use stract_core::index::Index;

const INDEX_PATH: &str = "data/index";

macro_rules! bench {
    ($query:tt, $spell:ident, $c:ident) => {
        let mut desc = "correct '".to_string();
        desc.push_str($query);
        desc.push('\'');
        $c.bench_function(desc.as_str(), |b| {
            b.iter(|| $spell.correction($query).unwrap())
        });
    };
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let index = Index::open(INDEX_PATH).unwrap();
    let spell = Spell::for_searcher(index.inverted_index.tv_searcher());

    for _ in 0..100 {
        bench!("asdf", spell, c);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
