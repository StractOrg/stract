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

use crate::enum_map::EnumMap;
use optics::ast::RankingTarget;
use optics::Optic;

use std::str::FromStr;

use thiserror::Error;
use utoipa::ToSchema;

mod computer;
mod non_text;
mod prelude;
mod text;

pub use computer::SignalComputer;
pub use non_text::*;
pub use prelude::*;
pub use text::*;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unknown signal: {0}")]
    UnknownSignal(#[from] serde_json::Error),
}

impl FromStr for SignalEnumDiscriminants {
    type Err = Error;

    fn from_str(name: &str) -> std::result::Result<Self, Self::Err> {
        let s = "\"".to_string() + name + "\"";
        let signal = serde_json::from_str(&s)?;
        Ok(signal)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SignalCoefficient {
    map: EnumMap<SignalEnum, f64>,
}

impl SignalCoefficient {
    pub fn get(&self, signal: &SignalEnum) -> f64 {
        self.map
            .get(*signal)
            .copied()
            .unwrap_or(signal.default_coefficient())
    }

    pub fn new(coefficients: impl Iterator<Item = (SignalEnum, f64)>) -> Self {
        let mut map = EnumMap::default();

        for (signal, coefficient) in coefficients {
            map.insert(signal, coefficient);
        }

        Self { map }
    }

    pub fn from_optic(optic: &Optic) -> Self {
        SignalCoefficient::new(optic.rankings.iter().filter_map(|coeff| {
            match &coeff.target {
                RankingTarget::Signal(signal) => SignalEnumDiscriminants::from_str(signal)
                    .ok()
                    .map(|signal| (signal.into(), coeff.value)),
            }
        }))
    }

    pub fn merge_into(&mut self, coeffs: SignalCoefficient) {
        for signal in SignalEnum::all() {
            if let Some(coeff) = coeffs.map.get(signal).copied() {
                match self.map.get_mut(signal) {
                    Some(existing_coeff) => *existing_coeff += coeff,
                    None => {
                        self.map.insert(signal, coeff);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ComputedSignal {
    pub signal: SignalEnum,
    pub score: SignalScore,
}

#[derive(
    Debug,
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct SignalScore {
    pub coefficient: f64,
    pub value: f64,
}
