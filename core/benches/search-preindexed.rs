use criterion::{criterion_group, criterion_main, Criterion};
use cuely::{
    index::Index,
    ranking::centrality_store::CentralityStore,
    searcher::{LocalSearcher, SearchQuery},
    webgraph::centrality::topic::TopicCentrality,
};

const INDEX_PATH: &str = "data/index";
const CENTRALITY_PATH: &str = "data/centrality";
const TOPIC_CENTRALITY_PATH: &str = "data/topic_centrality";

macro_rules! bench {
    ($query:tt, $searcher:ident, $c:ident) => {
        let mut desc = "search '".to_string();
        desc.push_str($query);
        desc.push('\'');
        $c.bench_function(desc.as_str(), |b| {
            b.iter(|| {
                $searcher
                    .search(&SearchQuery {
                        original: $query.to_string(),
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
    searcher.set_centrality_store(CentralityStore::open(CENTRALITY_PATH));
    searcher.set_topic_centrality(TopicCentrality::open(TOPIC_CENTRALITY_PATH).unwrap());

    for _ in 0..100 {
        bench!("the", searcher, c);
        bench!("dtu", searcher, c);
        bench!("the best", searcher, c);
        bench!("the circle of life", searcher, c);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
