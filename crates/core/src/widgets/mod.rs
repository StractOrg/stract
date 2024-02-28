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

use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use self::thesaurus::ThesaurusWidget;
use crate::config::WidgetsConfig;

use self::calculator::{Calculation, Calculator};
use anyhow::{anyhow, Result};

pub mod calculator;
pub mod thesaurus;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Calculator parse")]
    CalculatorParse,

    #[error("Failed to parse float")]
    FloatParse(#[from] std::num::ParseFloatError),
}

pub struct Widgets {
    calculator: Calculator,
    thesaurus: Option<thesaurus::Dictionary>,
}

impl Widgets {
    pub fn new(config: WidgetsConfig) -> Result<Self> {
        if config.thesaurus_paths.len() > 1 {
            return Err(anyhow!("Only one thesaurus path is supported for now"));
        }

        let thesaurus = if let Some(path) = config.thesaurus_paths.first() {
            Some(thesaurus::Dictionary::build(path)?)
        } else {
            None
        };

        let exchange_update = if config.calculator_fetch_currencies_exchange {
            calculator::ExchangeUpdate::AsyncTokio
        } else {
            calculator::ExchangeUpdate::None
        };

        Ok(Self {
            calculator: Calculator::new(exchange_update),
            thesaurus,
        })
    }

    pub fn widget(&self, query: &str) -> Option<Widget> {
        let query = query.to_lowercase();

        self.calculator
            .try_calculate(&query)
            .ok()
            .map(Widget::Calculator)
            .or_else(|| {
                self.thesaurus
                    .as_ref()
                    .and_then(|thesaurus| thesaurus.lookup(&query))
                    .map(Widget::Thesaurus)
            })
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", content = "value", rename_all = "camelCase")]
pub enum Widget {
    Calculator(Calculation),
    Thesaurus(ThesaurusWidget),
}
