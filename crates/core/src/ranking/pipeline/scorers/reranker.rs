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
    ranking::{
        models::{cross_encoder::CrossEncoder, lambdamart::LambdaMART},
        Signal, SignalCoefficient, SignalScore,
    },
    searcher::SearchQuery,
};

use crate::ranking::pipeline::{PrecisionRankingWebpage, RankableWebpage, Scorer};

use super::calculate_score;

pub struct ReRanker<M: CrossEncoder> {
    crossencoder: Arc<M>,
    lambda_mart: Option<Arc<LambdaMART>>,
    query: Option<SearchQuery>,
    signal_coefficients: Option<SignalCoefficient>,
}

impl<M: CrossEncoder> ReRanker<M> {
    pub fn new(crossencoder: Arc<M>, lambda: Option<Arc<LambdaMART>>) -> Self {
        Self {
            crossencoder,
            lambda_mart: lambda,
            query: None,
            signal_coefficients: None,
        }
    }

    fn crossencoder_snippet_coeff(&self) -> f64 {
        self.signal_coefficients
            .as_ref()
            .and_then(|coeffs| coeffs.get(&Signal::CrossEncoderSnippet))
            .unwrap_or(Signal::CrossEncoderSnippet.default_coefficient())
    }

    fn crossencoder_title_coeff(&self) -> f64 {
        self.signal_coefficients
            .as_ref()
            .and_then(|coeffs| coeffs.get(&Signal::CrossEncoderSnippet))
            .unwrap_or(Signal::CrossEncoderSnippet.default_coefficient())
    }

    fn crossencoder_score_webpages(&self, webpage: &mut [PrecisionRankingWebpage]) {
        let mut snippets = Vec::with_capacity(webpage.len());
        let mut titles = Vec::with_capacity(webpage.len());

        for webpage in webpage.iter_mut() {
            titles.push(webpage.retrieved_webpage.title.clone());
            snippets.push(webpage.retrieved_webpage.snippet.unhighlighted_string());
        }

        let query = &self.query.as_ref().unwrap().query;
        let snippet_scores = self.crossencoder.run(query, &snippets);
        let title_scores = self.crossencoder.run(query, &titles);

        for ((webpage, snippet), title) in webpage.iter_mut().zip(snippet_scores).zip(title_scores)
        {
            webpage.ranking.signals.insert(
                Signal::CrossEncoderSnippet,
                SignalScore {
                    coefficient: self.crossencoder_snippet_coeff(),
                    value: snippet,
                },
            );

            webpage.ranking.signals.insert(
                Signal::CrossEncoderTitle,
                SignalScore {
                    coefficient: self.crossencoder_title_coeff(),
                    value: title,
                },
            );
        }
    }
}

impl<M: CrossEncoder> Scorer<PrecisionRankingWebpage> for ReRanker<M> {
    fn score(&self, webpages: &mut [PrecisionRankingWebpage]) {
        self.crossencoder_score_webpages(webpages);

        for webpage in webpages.iter_mut() {
            webpage.set_score(calculate_score(
                &self.lambda_mart,
                &self.signal_coefficients,
                &webpage.ranking.signals,
            ));
        }
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.query = Some(query.clone());
        self.signal_coefficients = query.optic.as_ref().map(SignalCoefficient::from_optic);
    }
}
