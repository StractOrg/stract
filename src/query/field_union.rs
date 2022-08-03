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
use tantivy::query::Scorer;
use tantivy::{DocId, DocSet, Score, TERMINATED};

use super::term_scorer::TermScorerForField;

/// Creates a `DocSet` that iterate through the union of two or more `DocSet`s.
pub struct FieldUnion {
    pub docsets: Vec<TermScorerForField>,
}

impl From<Vec<TermScorerForField>> for FieldUnion {
    fn from(docsets: Vec<TermScorerForField>) -> FieldUnion {
        FieldUnion { docsets }
    }
}

impl DocSet for FieldUnion {
    fn advance(&mut self) -> DocId {
        let current_doc = self.doc();

        for scorer in self
            .docsets
            .iter_mut()
            .filter(|scorer| scorer.doc() == current_doc)
        {
            scorer.advance();
        }

        self.doc()
    }

    fn doc(&self) -> DocId {
        self.docsets
            .iter()
            .map(|scorer| scorer.doc())
            .min()
            .unwrap_or(TERMINATED)
    }

    fn size_hint(&self) -> u32 {
        self.docsets
            .iter()
            .map(|docset| docset.size_hint())
            .max()
            .unwrap_or(0u32)
    }
}

impl Scorer for FieldUnion {
    fn score(&mut self) -> Score {
        let current_doc = self.doc();
        self.docsets
            .iter_mut()
            .filter(|scorer| scorer.doc() == current_doc)
            .map(|scorer| scorer.score())
            .sum()
    }
}
