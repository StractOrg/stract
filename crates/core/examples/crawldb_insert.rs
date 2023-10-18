use anyhow::Result;
use hashbrown::HashMap;
use stract::crawler::{crawl_db::CrawlDb, Domain, UrlString, UrlToInsert};

fn random_domains() -> Vec<String> {
    let mut domains = Vec::new();

    for i in 0..1024 {
        domains.push(format!("domain-{}", i));
    }

    domains
}

fn random_urls(domain: &str) -> Vec<String> {
    let mut urls = Vec::new();

    for i in 0..256 {
        urls.push(format!("https://{domain}/{i}"));
    }

    urls
}

fn main() -> Result<()> {
    let mut db = CrawlDb::open("data/crawldb")?;

    loop {
        let domains = random_domains();

        let mut urls = HashMap::new();

        for domain in &domains {
            let rand_urls = random_urls(domain);

            urls.insert(
                Domain::from(domain.clone()),
                rand_urls
                    .into_iter()
                    .map(|url| UrlToInsert {
                        url: UrlString(url),
                        weight: 1.0,
                    })
                    .collect(),
            );
        }

        let start = std::time::Instant::now();
        db.insert_urls(urls)?;
        println!("inserted in {:?}", start.elapsed());
    }
}
