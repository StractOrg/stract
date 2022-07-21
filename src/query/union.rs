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

/// Creates a `DocSet` that iterate through the union of two or more `DocSet`s.
pub struct Union<TScorer> {
    docsets: Vec<TScorer>,
    score: Score,
    doc: DocId,
}

impl<TScorer> From<Vec<TScorer>> for Union<TScorer>
where
    TScorer: Scorer,
{
    fn from(docsets: Vec<TScorer>) -> Union<TScorer> {
        let non_empty_docsets: Vec<TScorer> = docsets
            .into_iter()
            .filter(|docset| docset.doc() != TERMINATED)
            .collect();
        let mut union = Union {
            docsets: non_empty_docsets,
            doc: 0,
            score: 0.0,
        };

        union.advance();
        union
    }
}

impl<TScorer> DocSet for Union<TScorer>
where
    TScorer: Scorer,
{
    fn advance(&mut self) -> DocId {
        if let Some(min_doc) = self.docsets.iter().map(|scorer| scorer.doc()).min() {
            self.doc = min_doc;

            if self.doc == TERMINATED {
                return self.doc;
            }

            self.score = self
                .docsets
                .iter_mut()
                .filter(|scorer| scorer.doc() == self.doc)
                .map(|scorer| scorer.score())
                .sum();

            for scorer in self
                .docsets
                .iter_mut()
                .filter(|scorer| scorer.doc() == min_doc)
            {
                scorer.advance();
            }
        } else {
            self.doc = TERMINATED;
        }

        self.doc
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        self.docsets
            .iter()
            .map(|docset| docset.size_hint())
            .max()
            .unwrap_or(0u32)
    }
}

impl<TScorer> Scorer for Union<TScorer>
where
    TScorer: Scorer,
{
    fn score(&mut self) -> Score {
        self.score
    }
}
