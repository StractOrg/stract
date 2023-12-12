use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;
use stract::ranking::bitvec_similarity::BitVec;

fn random_bitvec(max_len: usize, max_id: usize) -> BitVec {
    let mut rng = rand::thread_rng();

    let mut ranks = Vec::with_capacity(max_len);

    for _ in 0..max_len {
        ranks.push(rng.gen_range(0..max_id) as u64);
    }

    ranks.sort();
    ranks.dedup();

    BitVec::new(ranks)
}

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("(100, 100) max_id=1000", |ben| {
        for _ in 0..100 {
            let a = random_bitvec(100, 1000);
            let b = random_bitvec(100, 1000);
            ben.iter(|| {
                for _ in 0..10_000 {
                    a.sim(&b);
                }
            })
        }
    });

    c.bench_function("(1_000, 1_000) max_id=100_000", |ben| {
        for _ in 0..100 {
            let a = random_bitvec(1_000, 100_000);
            let b = random_bitvec(1_000, 100_000);
            ben.iter(|| {
                for _ in 0..10_000 {
                    a.sim(&b);
                }
            })
        }
    });

    c.bench_function("(1_000, 1_000) max_id=1_000_000", |ben| {
        for _ in 0..100 {
            let a = random_bitvec(1_000, 1_000_000);
            let b = random_bitvec(1_000, 1_000_000);
            ben.iter(|| {
                for _ in 0..10_000 {
                    a.sim(&b);
                }
            })
        }
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
