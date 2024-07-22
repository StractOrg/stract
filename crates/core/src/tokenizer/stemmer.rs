// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

use whatlang::Lang;

pub struct Stemmer(tantivy::tokenizer::Stemmer);

impl Stemmer {
    pub fn into_tantivy(self) -> tantivy::tokenizer::Stemmer {
        self.0
    }
}

impl From<Lang> for Stemmer {
    fn from(lang: Lang) -> Self {
        match lang {
            Lang::Dan => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::Danish,
            )),
            Lang::Ara => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::Arabic,
            )),
            Lang::Nld => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::Dutch,
            )),
            Lang::Fin => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::Finnish,
            )),
            Lang::Fra => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::French,
            )),
            Lang::Deu => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::German,
            )),
            Lang::Hun => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::Hungarian,
            )),
            Lang::Ita => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::Italian,
            )),
            Lang::Por => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::Portuguese,
            )),
            Lang::Ron => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::Romanian,
            )),
            Lang::Rus => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::Russian,
            )),
            Lang::Spa => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::Spanish,
            )),
            Lang::Swe => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::Swedish,
            )),
            Lang::Tam => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::Tamil,
            )),
            Lang::Tur => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::Turkish,
            )),
            _ => Stemmer(tantivy::tokenizer::Stemmer::new(
                tantivy::tokenizer::Language::English,
            )),
        }
    }
}
