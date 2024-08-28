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

use itertools::Itertools;

use crate::ranking::pipeline::RankableWebpage;
use crate::ranking::{self, SignalCalculation, SignalEnum};
use crate::searcher::api;

fn min_slop_two_positions(pos_a: &[u32], pos_b: &[u32]) -> u32 {
    let mut cur_min = u32::MAX;

    let mut cursor_a = 0;
    let mut cursor_b = 0;

    loop {
        if cursor_a >= pos_a.len() || cursor_b >= pos_b.len() {
            break;
        }

        let a = pos_a[cursor_a];
        let b = pos_b[cursor_b];

        if b > a {
            cur_min = (b - a).min(cur_min);
            cursor_a += 1;
        } else {
            cursor_b += 1;
        }
    }

    cur_min
}

fn min_slop<'a>(positions: impl Iterator<Item = &'a [u32]>) -> u32 {
    positions
        .tuple_windows()
        .map(|(a, b)| min_slop_two_positions(a, b))
        .max()
        .unwrap_or(u32::MAX)
}

fn score_slop(slop: f64) -> f64 {
    1.0 / (slop + 1.0)
}

#[derive(Debug, Default)]
pub struct TitleDistanceScorer;

impl super::RankingStage for TitleDistanceScorer {
    type Webpage = api::ScoredWebpagePointer;

    fn compute(&self, webpage: &Self::Webpage) -> (SignalEnum, SignalCalculation) {
        let min_slop = min_slop(webpage.as_local_recall().iter_title_positions()) as f64;
        let score = score_slop(min_slop);

        (
            ranking::signals::MinTitleSlop.into(),
            ranking::SignalCalculation {
                value: min_slop,
                score,
            },
        )
    }
}

#[derive(Debug, Default)]
pub struct BodyDistanceScorer;

impl super::RankingStage for BodyDistanceScorer {
    type Webpage = api::ScoredWebpagePointer;

    fn compute(&self, webpage: &Self::Webpage) -> (SignalEnum, SignalCalculation) {
        let min_slop = min_slop(webpage.as_local_recall().iter_clean_body_positions()) as f64;
        let score = score_slop(min_slop);

        (
            ranking::signals::MinCleanBodySlop.into(),
            ranking::SignalCalculation {
                value: min_slop,
                score,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_slop() {
        let positions = [vec![13, 18, 22], vec![8, 15, 30], vec![9, 16]];

        assert_eq!(min_slop(positions.iter().map(|pos| pos.as_slice())), 2);
    }
}
