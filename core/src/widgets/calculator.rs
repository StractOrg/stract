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

use crate::widgets::{Error, Result};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

static DICE_REGEX: once_cell::sync::Lazy<regex::Regex> =
    once_cell::sync::Lazy::new(|| regex::Regex::new(r"^d[0-9]+").unwrap());

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Calculation {
    pub input: String,
    pub result: String,
}

pub fn try_calculate(expr: &str) -> Result<Calculation> {
    // if expr starts with "d[0-9]+", wrap it in "roll(...)"
    let expr = if DICE_REGEX.is_match(expr) {
        format!("roll({})", expr)
    } else {
        expr.to_string()
    };

    let mut context = fend_core::Context::new();
    context.set_random_u32_fn(|| {
        let mut rng = rand::thread_rng();
        rng.gen()
    });

    let res = fend_core::evaluate(&expr, &mut context).map_err(|_| Error::CalculatorParse)?;

    if res.get_main_result() == expr {
        return Err(Error::CalculatorParse);
    }

    Ok(Calculation {
        input: expr.to_string(),
        result: res.get_main_result().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_calculates_simple_expressions() {
        assert_eq!(try_calculate("2+2").unwrap().result, 4.0.to_string());
        assert_eq!(try_calculate("2*2").unwrap().result, 4.0.to_string());
        assert_eq!(try_calculate("2*3").unwrap().result, 6.0.to_string());
        assert_eq!(try_calculate("6/2").unwrap().result, 3.0.to_string());
    }

    #[test]
    fn it_respects_paranthesis() {
        assert_eq!(try_calculate("2+2*6").unwrap().result, 14.0.to_string());
        assert_eq!(try_calculate("(2+2)*6").unwrap().result, 24.0.to_string());
    }
}
