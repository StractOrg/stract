use criterion::{criterion_group, criterion_main, Criterion};
use rand::seq::SliceRandom;
use stract::naive_bayes;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Label {
    Spam,
    Ham,
}

impl naive_bayes::Label for Label {}

fn spam_string(length: usize) -> String {
    let mut res = String::with_capacity(length);
    for _ in 0..length {
        res.push_str("aaa");
        res.push(' ');
    }
    res
}

fn ham_string(length: usize) -> String {
    let mut res = String::with_capacity(length);
    for _ in 0..length {
        res.push_str("bbb");
        res.push(' ');
    }
    res
}

fn create_dataset() -> Vec<(String, Label)> {
    let mut res = Vec::new();

    for _ in 0..1000 {
        res.push((spam_string(100), Label::Spam));
        res.push((ham_string(100), Label::Ham));
    }
    res.shuffle(&mut rand::thread_rng());

    res
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let dataset = create_dataset();

    // c.bench_function("train naive bayes classifier", |b| {
    //     b.iter(|| {
    //         for _ in 0..1000 {
    //             let mut pipeline = naive_bayes::Pipeline::new();
    //             pipeline.fit(&dataset);
    //         }
    //     })
    // });

    let mut pipeline = naive_bayes::Pipeline::new();
    pipeline.fit(&dataset);
    let text = spam_string(10_000);

    let num_samples = 100_000;
    c.bench_function("predict", |b| {
        b.iter(|| {
            let start = std::time::Instant::now();
            for _ in 0..num_samples {
                pipeline.predict(&text);
            }

            println!(
                "{:.2} ms/pred",
                start.elapsed().as_millis() as f32 / num_samples as f32
            );
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
