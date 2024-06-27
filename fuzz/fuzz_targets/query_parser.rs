#![no_main]

use libfuzzer_sys::fuzz_target;
use stract::{
    index::Index,
    query::Query,
    searcher::{SearchGuard, SearchQuery, SearchableIndex},
};

fuzz_target!(|query: &str| {
    let index = Index::open("/tmp/stract/fuzz-index").unwrap();

    let guard = index.guard();
    let ctx = guard.inverted_index().local_search_ctx();

    let _ = Query::parse(
        &ctx,
        &SearchQuery {
            query: query.to_string(),
            ..Default::default()
        },
        &index.inverted_index,
    );
});
