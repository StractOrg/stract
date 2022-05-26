use crate::{ranking::Ranker, search_index::Index};

pub struct Searcher {
    index: Index,
    ranker: Ranker,
}
