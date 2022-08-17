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
use crate::spell::Dictionary;

pub struct Compounder<'a, const DICT_N: usize> {
    dict: &'a Dictionary<DICT_N>,
}

impl<'a, const DICT_N: usize> Compounder<'a, DICT_N> {
    pub fn new(dict: &'a Dictionary<DICT_N>) -> Self {
        Compounder { dict }
    }

    pub fn combine(&self, terms: Vec<&'a str>) -> Vec<String> {
        let mut res = Vec::new();

        for i in 0..terms.len() {
            let mut s = terms[i].to_string();

            for next_term in terms.iter().skip(i + 1) {
                s.push_str(next_term);
                if self.dict.contains(&s) {
                    res.push(s.clone());
                }
            }
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combine_terms() {
        let mut dictionary = Dictionary::default();

        dictionary.insert("atest");
        dictionary.commit().unwrap();

        assert_eq!(
            Compounder::new(&dictionary).combine(vec!["this", "is", "a", "test"]),
            vec!["atest".to_string(),]
        )
    }
}
