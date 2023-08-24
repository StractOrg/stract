// rust-analyzer: disable
use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;
use stract::crawler::{CrawlCoordinator, Domain, JobResponse, UrlResponse};
use url::Url;

fn rand_domain() -> Domain {
    // generate random domain name by taking from a-z
    // and appending .com
    let mut rng = rand::thread_rng();
    let mut domain = String::new();
    for _ in 0..rng.gen_range(1..3) {
        domain.push(rng.gen_range(b'a'..=b'z') as char);
    }
    domain.push_str(".com");
    domain.into()
}

fn rand_url(domain: &Domain) -> Url {
    let mut path = String::new();

    let mut rng = rand::thread_rng();
    for _ in 0..rng.gen_range(1..100) {
        path.push(rng.gen_range(b'a'..=b'z') as char);
    }

    Url::parse(&format!("https://{}/{}", domain.as_str(), path)).unwrap()
}

fn random_responses(num: usize) -> Vec<JobResponse> {
    let mut responses = Vec::with_capacity(num);
    for _ in 0..num {
        let domain = rand_domain();

        let url_responses = vec![
            UrlResponse::Success {
                url: rand_url(&domain)
            };
            100
        ];

        let mut discovered_urls = Vec::new();
        for _ in 0..3 {
            let domain = rand_domain();

            for _ in 0..10 {
                discovered_urls.push(rand_url(&domain));
            }
        }

        responses.push(JobResponse {
            domain: domain.clone(),
            url_responses,
            discovered_urls,
        });
    }

    responses
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let coordinator = CrawlCoordinator::new("data/crawldb", 100, vec![]).unwrap();

    for _ in 0..1_000 {
        c.bench_function("Add response to coordinator", |b| {
            b.iter(|| {
                let responses = random_responses(1024);
                coordinator.add_responses(&responses).unwrap();
            })
        });

        c.bench_function("Sample jobs", |b| {
            b.iter(|| {
                coordinator.sample_jobs(256).unwrap();
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
