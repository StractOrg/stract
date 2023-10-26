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
    pub fn compare(a: &str, b: &str) -> usize {
        let len_a = a.chars().count();
        let len_b = b.chars().count();
        if len_a < len_b {
            return Self::compare(b, a);
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
                        pre + usize::from(ca != cb),
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
