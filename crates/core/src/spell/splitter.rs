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
use crate::spell::Dictionary;

pub struct TermSplitter<'a> {
    dict: &'a Dictionary,
}

impl<'a> TermSplitter<'a> {
    pub fn new(dict: &'a Dictionary) -> Self {
        TermSplitter { dict }
    }
    pub fn split(&self, text: &'a str) -> Vec<&'a str> {
        let mut probs = vec![1.0];
        let mut lasts: Vec<usize> = vec![0];

        for i in 1..text.len() + 1 {
            let mut best_prob_k = 0.0;
            let mut best_index = 0;

            for j in 0..i {
                if !text.is_char_boundary(j) || !text.is_char_boundary(i) {
                    continue;
                }

                if let Some(prob) = &self.dict.score(&[], &text[j..i], &[]) {
                    let new_prob = probs[j] * prob;
                    if new_prob > best_prob_k {
                        best_prob_k = new_prob;
                        best_index = j;
                    }
                }
            }

            probs.push(best_prob_k);
            lasts.push(best_index);
        }

        let mut words = Vec::new();
        let mut i = text.len();

        while i > 0 {
            if i == text.len() && lasts[i] == 0 {
                break;
            }
            words.push(&text[lasts[i]..i]);
            i = lasts[i];
        }

        words.reverse();
        words
    }
}

#[cfg(test)]
mod tests {

    use crate::spell::dictionary;

    use super::*;

    #[test]
    fn test_split() {
        let dictionary = dictionary::build_from_str("wicked weather");

        assert_eq!(
            TermSplitter::new(&dictionary).split("wickedweather"),
            vec!["wicked", "weather"]
        );

        assert_eq!(TermSplitter::new(&dictionary).split("wicked").len(), 0);
        assert_eq!(TermSplitter::new(&dictionary).split("udlæg").len(), 0);
    }

    #[test]
    fn test_most_probable() {
        let dictionary = dictionary::build_from_str("wicked weather wicked weather eat");

        assert_eq!(
            TermSplitter::new(&dictionary).split("wickedweather"),
            vec!["wicked", "weather"]
        );
    }
}
