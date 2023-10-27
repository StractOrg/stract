// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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

// use crate::{
//     inverted_index::InvertedIndex, query::parser::TermCompound, ranking::SignalCoefficient,
//     search_ctx::Ctx, searcher::SearchQuery, Result,
// };
// use optics::{Optic, SiteRankings};
// use schema::{Field, TextField};
// use std::collections::HashMap;
// use tantivy::query::{BooleanQuery, Occur, QueryClone, TermQuery};
// use webpage::{region::Region, safety_classifier};

pub mod bangs;
pub mod bm25;
mod const_query;
pub mod intersection;
pub mod optic;
pub mod parser;
mod pattern_query;
pub mod shortcircuit;
pub mod union;
