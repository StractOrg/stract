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
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct LevenshteinDistance {
    dist: usize,
}

impl LevenshteinDistance {
    pub fn new(distance: usize) -> Self {
        LevenshteinDistance { dist: distance }
    }

    pub fn distance(&self) -> usize {
        self.dist
    }

    fn remove_one(&self, string: &str, dist: usize) -> HashSet<EditDistance> {
        let mut res = HashSet::new();

        if string.is_empty() {
            return res;
        }

        let mut last_end = string.len() - 1;

        while !string.is_char_boundary(last_end) {
            last_end -= 1;
        }

        let mut end = 1;
        while !string.is_char_boundary(end) {
            end += 1;
        }

        for i in 0..string.len() {
            let edited_string;
            if i == 0 {
                let mut begin = i + 1;

                while !string.is_char_boundary(begin) {
                    begin += 1;
                    if begin > string.len() {
                        break;
                    }
                }

                if begin > string.len() {
                    break;
                }

                edited_string = string[begin..string.len()].to_string();
            } else if i == last_end {
                edited_string = string[0..last_end].to_string();
            } else {
                let mut next_begin = end + 1;

                while !string.is_char_boundary(next_begin) {
                    next_begin += 1;
                    if next_begin > string.len() {
                        break;
                    }
                }

                if next_begin > string.len() {
                    break;
                }

                edited_string = string[0..end].to_string() + &string[next_begin..string.len()];
                end = next_begin;
            }

            res.insert(EditDistance {
                dist,
                edited_string,
            });
        }

        res
    }

    pub fn removes(&self, string: &str) -> HashSet<EditDistance> {
        if string.is_empty() {
            return HashSet::new();
        }

        let mut all_edits = Vec::new();

        all_edits.push(EditDistance {
            edited_string: string.to_string(),
            dist: 0,
        });

        let mut last = 0;

        for d in 1..self.dist + 1 {
            let mut new_strings = Vec::new();

            for s in all_edits[last..].iter() {
                for new_s in self.remove_one(&s.edited_string, d) {
                    if new_s.edited_string.is_empty() {
                        continue;
                    }

                    new_strings.push(new_s);
                }
            }

            last = all_edits.len();
            all_edits.extend(new_strings.into_iter());
        }

        all_edits.into_iter().collect()
    }

    /// Function taken from https://github.com/febeling/edit-distance/blob/5597816456e7153cf69092f6ab5d0b4edb5e3797/src/lib.rs#L31
    ///
    /// The [Levenshtein edit distance][wikipedia] between two strings is
    /// the number of individual single-character changes (insert, delete,
    /// substitute) necessary to change string `a` into `b`.
    ///
    /// This can be a used to order search results, for fuzzy
    /// auto-completion, and to find candidates for spelling correction.
    ///
    /// This function returns the edit distance between strings `a` and `b`.
    ///
    /// The runtime complexity is `O(m*n)`, where `m` and `n` are the
    /// strings' lengths.
    pub fn compare(&self, a: &str, b: &str) -> usize {
        let len_a = a.chars().count();
        let len_b = b.chars().count();
        if len_a < len_b {
            return self.compare(b, a);
        }
        // handle special case of 0 length
        if len_a == 0 {
            return len_b;
        } else if len_b == 0 {
            return len_a;
        }

        let len_b = len_b + 1;

        let mut pre;
        let mut tmp;
        let mut cur = vec![0; len_b];

        // initialize string
        for (i, val) in cur.iter_mut().enumerate().take(len_b).skip(1) {
            *val = i;
        }

        // calculate edit distance
        for (i, ca) in a.chars().enumerate() {
            // get first column for this row
            pre = cur[0];
            cur[0] = i + 1;
            for (j, cb) in b.chars().enumerate() {
                tmp = cur[j + 1];
                cur[j + 1] = std::cmp::min(
                    // deletion
                    tmp + 1,
                    std::cmp::min(
                        // insertion
                        cur[j] + 1,
                        // match or substitution
                        pre + if ca == cb { 0 } else { 1 },
                    ),
                );
                pre = tmp;
            }
        }
        cur[len_b - 1]
    }
}

#[derive(Debug)]
pub struct EditDistance {
    pub dist: usize,
    pub edited_string: String,
}

impl PartialEq for EditDistance {
    fn eq(&self, other: &Self) -> bool {
        self.edited_string == other.edited_string
    }
}

impl Eq for EditDistance {}

impl Hash for EditDistance {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.edited_string.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removes_edit_0() {
        let dist = LevenshteinDistance::new(0);

        let mut res = Vec::new();

        for edit in dist.removes("test") {
            res.push(edit);
        }

        assert_eq!(
            res,
            vec![EditDistance {
                dist: 0,
                edited_string: "test".to_string()
            }]
        );
    }

    #[test]
    fn removes_edit_1() {
        let dist = LevenshteinDistance::new(1);
        let mut res = Vec::new();

        for edit in dist.removes("test") {
            res.push(edit.edited_string);
        }

        res.sort();

        assert_eq!(
            res,
            vec![
                "est".to_string(),
                "tes".to_string(),
                "test".to_string(),
                "tet".to_string(),
                "tst".to_string(),
            ]
        );

        let mut res = Vec::new();

        for edit in dist.removes("tt") {
            res.push(edit.edited_string);
        }

        res.sort();

        assert_eq!(res, vec!["t".to_string(), "tt".to_string(),]);

        let mut res = Vec::new();

        for edit in dist.removes("t") {
            res.push(edit.edited_string);
        }

        assert_eq!(res, vec!["t".to_string()]);

        assert!(dist.removes("").is_empty());

        let mut res = Vec::new();

        for edit in dist.removes("æøå") {
            res.push(edit.edited_string);
        }
        res.sort();

        assert_eq!(
            res,
            vec![
                "æå".to_string(),
                "æø".to_string(),
                "æøå".to_string(),
                "øå".to_string(),
            ]
        );

        let mut res = Vec::new();

        for edit in dist.removes("æ") {
            res.push(edit.edited_string);
        }

        assert_eq!(res, vec!["æ".to_string()]);
    }

    #[test]
    fn removes_edit_2() {
        let dist = LevenshteinDistance::new(2);

        let mut res = Vec::new();

        for edit in dist.removes("test") {
            res.push(edit.edited_string);
        }

        res.sort();

        assert_eq!(
            res,
            vec![
                "es".to_string(),
                "est".to_string(),
                "et".to_string(),
                "st".to_string(),
                "te".to_string(),
                "tes".to_string(),
                "test".to_string(),
                "tet".to_string(),
                "ts".to_string(),
                "tst".to_string(),
                "tt".to_string(),
            ]
        );
    }
}
