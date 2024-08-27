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

use crate::ranking::{self, models::cross_encoder::CrossEncoder, pipeline::RankableWebpage};

use crate::ranking::pipeline::{FullRankingStage, PrecisionRankingWebpage, Top};

pub struct ReRanker<M: CrossEncoder> {
    crossencoder: Arc<M>,
    query: String,
}

impl<M: CrossEncoder> ReRanker<M> {
    pub fn new(query: String, crossencoder: Arc<M>) -> Self {
        Self {
            crossencoder,
            query,
        }
    }

    fn crossencoder_score_webpages(&self, webpage: &mut [PrecisionRankingWebpage]) {
        let mut snippets = Vec::with_capacity(webpage.len());
        let mut titles = Vec::with_capacity(webpage.len());

        for webpage in webpage.iter_mut() {
            titles.push(webpage.retrieved_webpage().title.clone());
            snippets.push(webpage.retrieved_webpage().snippet.unhighlighted_string());
        }

        let query = &self.query;
        let snippet_scores = self.crossencoder.run(query, &snippets);
        let title_scores = self.crossencoder.run(query, &titles);

        for ((webpage, snippet), title) in webpage.iter_mut().zip(snippet_scores).zip(title_scores)
        {
            webpage.ranking_mut().signals_mut().insert(
                ranking::core::CrossEncoderSnippet.into(),
                ranking::SignalCalculation::new_symmetrical(snippet),
            );

            webpage.ranking_mut().signals_mut().insert(
                ranking::core::CrossEncoderTitle.into(),
                ranking::SignalCalculation::new_symmetrical(title),
            );
        }
    }
}

impl<M: CrossEncoder> FullRankingStage for ReRanker<M> {
    type Webpage = PrecisionRankingWebpage;

    fn compute(&self, webpages: &mut [Self::Webpage]) {
        self.crossencoder_score_webpages(webpages);
    }

    fn top_n(&self) -> Top {
        Top::Limit(20)
    }
}
