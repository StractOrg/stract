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

use self::calculator::{try_calculate, Calculation};

pub mod calculator;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Calculator parse")]
    CalculatorParse,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub enum Widget {
    Calculator(Calculation),
}

impl Widget {
    pub fn try_new(query: &str) -> Option<Widget> {
        Self::calculator(query)
    }

    fn calculator(query: &str) -> Option<Widget> {
        try_calculate(query).ok().map(Widget::Calculator)
    }
}
