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

use std::ops::{Add, AddAssign};

#[derive(Default)]
pub struct KahanSum {
    sum: f64,
    err: f64,
}

impl From<KahanSum> for f64 {
    fn from(kahan: KahanSum) -> Self {
        kahan.sum
    }
}

impl From<f64> for KahanSum {
    fn from(val: f64) -> Self {
        Self { sum: val, err: 0.0 }
    }
}

impl AddAssign<f64> for KahanSum {
    fn add_assign(&mut self, rhs: f64) {
        let y = rhs - self.err;
        let t = self.sum + y;
        self.err = (t - self.sum) - y;
        self.sum = t;
    }
}

impl Add<f64> for KahanSum {
    type Output = Self;
    fn add(self, rhs: f64) -> Self::Output {
        let mut k = self;
        k += rhs;
        k
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut sum = KahanSum::default();
        for elem in [
            10000.0f64,
            std::f64::consts::PI,
            std::f64::consts::E,
            std::f64::consts::PI,
            std::f64::consts::E,
            std::f64::consts::PI,
            std::f64::consts::E,
        ] {
            sum += elem;
        }

        assert_eq!(10017.579623446147f64, f64::from(sum));
    }
}
