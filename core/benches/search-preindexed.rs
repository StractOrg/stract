use std::path::Path;

use criterion::{criterion_group, criterion_main, Criterion};
use optics::HostRankings;
use stract::{
    index::Index,
    ranking::inbound_similarity::InboundSimilarity,
    searcher::{LocalSearcher, SearchQuery},
};

const INDEX_PATH: &str = "data/index";
const CENTRALITY_PATH: &str = "data/centrality";

macro_rules! bench {
    ($query:tt, $searcher:ident, $c:ident) => {
        let mut desc = "search '".to_string();
        desc.push_str($query);
        desc.push('\'');
        $c.bench_function(desc.as_str(), |b| {
            b.iter(|| {
                $searcher
                    .search(&SearchQuery {
                        query: $query.to_string(),
                        host_rankings: Some(HostRankings {
                            liked: vec![
                                "docs.rs".to_string(),
                                "ycombinator.com".to_string(),
                                "nih.gov".to_string(),
                            ],
                            disliked: vec!["pinterest.com".to_string()],
                            blocked: vec![],
                        }),
                        ..Default::default()
                    })
                    .unwrap()
            })
        });
    };
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let index = Index::open(INDEX_PATH).unwrap();
    let mut searcher = LocalSearcher::new(index);
    searcher.set_inbound_similarity(
        InboundSimilarity::open(Path::new(CENTRALITY_PATH).join("inbound_similarity")).unwrap(),
    );

    for _ in 0..1000 {
        bench!("the", searcher, c);
        bench!("dtu", searcher, c);
        bench!("the best", searcher, c);
        bench!("the circle of life", searcher, c);
        bench!("what", searcher, c);
        bench!("a", searcher, c);
        bench!("sun", searcher, c);
        bench!("what a sun", searcher, c);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
