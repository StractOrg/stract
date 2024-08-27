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

use crate::{
    ranking::{
        self, models,
        pipeline::{PrecisionRankingWebpage, RankableWebpage, Top},
        SignalCalculation, SignalEnum,
    },
    searcher::api::ScoredWebpagePointer,
};
use std::sync::Arc;

use super::RankingStage;

impl RankingStage for Arc<models::LambdaMART> {
    type Webpage = ScoredWebpagePointer;

    fn compute(&self, webpages: &Self::Webpage) -> (SignalEnum, SignalCalculation) {
        let signals = webpages
            .signals()
            .iter()
            .map(|(signal, calc)| (signal, calc.value))
            .collect();
        (
            ranking::core::LambdaMart.into(),
            SignalCalculation::new_symmetrical(self.predict(&signals)),
        )
    }

    fn top_n(&self) -> Top {
        Top::Limit(20)
    }
}

pub struct PrecisionLambda(Arc<models::LambdaMART>);

impl From<Arc<models::LambdaMART>> for PrecisionLambda {
    fn from(model: Arc<models::LambdaMART>) -> Self {
        Self(model)
    }
}

impl RankingStage for PrecisionLambda {
    type Webpage = PrecisionRankingWebpage;

    fn compute(&self, webpage: &Self::Webpage) -> (SignalEnum, SignalCalculation) {
        (
            ranking::core::LambdaMart.into(),
            SignalCalculation::new_symmetrical(self.0.predict(webpage.ranking().signals())),
        )
    }

    fn top_n(&self) -> Top {
        Top::Limit(20)
    }
}
