use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use rand::Rng;
use stract::crawler::{CrawlCoordinator, Domain, JobResponse, UrlResponse};
use url::Url;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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

fn main() -> Result<()> {
    let test_size = 1_000_000_000;
    if std::path::Path::new("data/crawldb").exists() {
        std::fs::remove_dir_all("data/crawldb")?;
    }

    let coordinator = CrawlCoordinator::new("data/crawldb", test_size, vec![])?;

    let pb = ProgressBar::new(test_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar}] {pos:>7}/{len:7} ({per_sec})",
            )
            .progress_chars("#>-"),
    );

    for _ in 0..test_size {
        pb.inc(1);
        let responses = random_responses(1024);
        coordinator.add_responses(&responses)?;
    }

    Ok(())
}
