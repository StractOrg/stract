// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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
use lalrpop_util::lalrpop_mod;
use serde::Serialize;
use std::fmt::{Debug, Display, Formatter};

lalrpop_mod!(pub parser, "/widgets/calculator.rs");
pub static PARSER: once_cell::sync::Lazy<parser::ExprParser> =
    once_cell::sync::Lazy::new(parser::ExprParser::new);

#[derive(Serialize)]
pub enum Expr {
    Number(f64),
    Op(Box<Expr>, Opcode, Box<Expr>),
}
impl Expr {
    fn calculate(&self) -> f64 {
        match self {
            Expr::Number(n) => *n,
            Expr::Op(lhs, op, rhs) => {
                let lhs = lhs.calculate();
                let rhs = rhs.calculate();

                match op {
                    Opcode::Mul => lhs * rhs,
                    Opcode::Div => lhs / rhs,
                    Opcode::Add => lhs + rhs,
                    Opcode::Sub => lhs - rhs,
                }
            }
        }
    }
}

#[derive(Copy, Clone, Serialize)]
pub enum Opcode {
    Mul,
    Div,
    Add,
    Sub,
}

impl Debug for Expr {
    fn fmt(&self, fmt: &mut Formatter) -> std::result::Result<(), std::fmt::Error> {
        use self::Expr::*;
        match *self {
            Number(n) => write!(fmt, "{n:?}"),
            Op(ref l, op, ref r) => write!(fmt, "({l:?} {op:?} {r:?})"),
        }
    }
}

impl Display for Expr {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> std::fmt::Result {
        use self::Expr::*;
        match *self {
            Number(n) => write!(fmt, "{n}"),
            Op(ref l, op, ref r) => write!(fmt, "{l} {op} {r}"),
        }
    }
}

impl Debug for Opcode {
    fn fmt(&self, fmt: &mut Formatter) -> std::result::Result<(), std::fmt::Error> {
        use self::Opcode::*;
        match *self {
            Mul => write!(fmt, "*"),
            Div => write!(fmt, "/"),
            Add => write!(fmt, "+"),
            Sub => write!(fmt, "-"),
        }
    }
}

impl Display for Opcode {
    fn fmt(&self, fmt: &mut Formatter) -> std::result::Result<(), std::fmt::Error> {
        use self::Opcode::*;
        match *self {
            Mul => write!(fmt, "*"),
            Div => write!(fmt, "/"),
            Add => write!(fmt, "+"),
            Sub => write!(fmt, "-"),
        }
    }
}

fn parse(expr: &str) -> Result<Box<Expr>> {
    match PARSER.parse(expr) {
        Ok(expr) => Ok(expr),
        Err(_) => Err(Error::CalculatorParse),
    }
}

#[derive(Debug, Serialize)]
pub struct Calculation {
    pub input: String,
    pub expr: Box<Expr>,
    pub result: f64,
}

pub fn try_calculate(expr: &str) -> Result<Calculation> {
    let input = expr.to_string();
    let expr = parse(expr)?;

    Ok(Calculation {
        input,
        result: expr.calculate(),
        expr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_calculates_simple_expressions() {
        assert_eq!(try_calculate("2+2").unwrap().result, 4.0);
        assert_eq!(try_calculate("2*2").unwrap().result, 4.0);
        assert_eq!(try_calculate("2*3").unwrap().result, 6.0);
        assert!(try_calculate("2.1-3").unwrap().result + 0.9 < 0.0001);
        assert_eq!(try_calculate("6/2").unwrap().result, 3.0);
    }

    #[test]
    fn it_respects_paranthesis() {
        assert_eq!(try_calculate("2+2*6").unwrap().result, 14.0);
        assert_eq!(try_calculate("(2+2)*6").unwrap().result, 24.0);
    }
}
