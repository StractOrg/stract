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

use std::sync::Arc;

use crate::{
    ranking::{self, models::cross_encoder::CrossEncoder},
    searcher::SearchQuery,
};

use crate::ranking::pipeline::{PrecisionRankingWebpage, Scorer};

pub struct ReRanker<M: CrossEncoder> {
    crossencoder: Arc<M>,
    query: Option<SearchQuery>,
}

impl<M: CrossEncoder> ReRanker<M> {
    pub fn new(crossencoder: Arc<M>) -> Self {
        Self {
            crossencoder,
            query: None,
        }
    }

    fn crossencoder_score_webpages(&self, webpage: &mut [PrecisionRankingWebpage]) {
        let mut snippets = Vec::with_capacity(webpage.len());
        let mut titles = Vec::with_capacity(webpage.len());

        for webpage in webpage.iter_mut() {
            titles.push(webpage.retrieved_webpage().title.clone());
            snippets.push(webpage.retrieved_webpage().snippet.unhighlighted_string());
        }

        let query = &self.query.as_ref().unwrap().query;
        let snippet_scores = self.crossencoder.run(query, &snippets);
        let title_scores = self.crossencoder.run(query, &titles);

        for ((webpage, snippet), title) in webpage.iter_mut().zip(snippet_scores).zip(title_scores)
        {
            webpage
                .ranking_mut()
                .signals_mut()
                .insert(ranking::signal::CrossEncoderSnippet.into(), snippet);

            webpage
                .ranking_mut()
                .signals_mut()
                .insert(ranking::signal::CrossEncoderTitle.into(), title);
        }
    }
}

impl<M: CrossEncoder> Scorer<PrecisionRankingWebpage> for ReRanker<M> {
    fn score(&self, webpages: &mut [PrecisionRankingWebpage]) {
        self.crossencoder_score_webpages(webpages);
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.query = Some(query.clone());
    }
}
