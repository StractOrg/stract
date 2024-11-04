#![no_main]

use libfuzzer_sys::fuzz_target;
use stract::{index::Index, query::Query, searcher::SearchQuery};

fuzz_target!(|query: &str| {
    let index = Index::open("/tmp/stract/fuzz-index").unwrap();

    let ctx = index.inverted_index.local_search_ctx();

    let _ = Query::parse(
        &ctx,
        &SearchQuery {
            query: query.to_string(),
            ..Default::default()
        },
        &index.inverted_index,
    );
});
