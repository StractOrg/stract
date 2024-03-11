use criterion::{criterion_group, criterion_main, Criterion};
use stract::hyperloglog::HyperLogLog;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("Hyperloglog", |b| {
        b.iter(|| {
            let mut log: HyperLogLog<128> = HyperLogLog::default();
            for i in 0..10_000_000 {
                log.add(i);
                let _ = log.size();
            }

            for _ in 0..1_000_000_000 {
                let _ = log.size();
            }
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
