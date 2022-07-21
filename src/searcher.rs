// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use crate::index::{Index, RetrievedWebpage};
use crate::query::Query;
use crate::ranking::Ranker;
use crate::Result;

#[derive(Debug)]
pub struct SearchResult {
    pub num_docs: usize,
    pub documents: Vec<RetrievedWebpage>,
}

pub struct Searcher {
    index: Index,
}

impl From<Index> for Searcher {
    fn from(index: Index) -> Self {
        Searcher { index }
    }
}

impl Searcher {
    pub fn search(&self, query: &str) -> Result<SearchResult> {
        let query = Query::parse(query, self.index.schema())?;
        let ranker = Ranker::new(query.clone());
        self.index.search(&query, ranker.collector())
    }
}
