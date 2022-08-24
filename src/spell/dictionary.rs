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
use crate::spell::distance::LevenshteinDistance;
use crate::tokenizer::Normal;
use crate::webpage::Webpage;
use fst::map::Union;
use fst::{Automaton, IntoStreamer, Map, MapBuilder, Streamer};
use memmap::Mmap;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::ops::AddAssign;
use std::path::Path;
use std::{cmp, io, mem};
use tantivy::tokenizer::Tokenizer;
use thiserror::Error;

pub trait EditStrategy: Send + Sync {
    fn distance_for_string(&self, string: &str) -> usize;
    fn dist(&self) -> LevenshteinDistance;
}

pub struct LogarithmicEdit {
    max_edit_distance: usize,
}

impl LogarithmicEdit {
    pub fn new(max_edit_distance: usize) -> Self {
        Self { max_edit_distance }
    }
}

impl EditStrategy for LogarithmicEdit {
    fn distance_for_string(&self, string: &str) -> usize {
        let log_value: usize = (string.len() as f32).log2() as usize;
        cmp::max(1, cmp::min(log_value, self.max_edit_distance))
    }

    fn dist(&self) -> LevenshteinDistance {
        LevenshteinDistance::new(self.max_edit_distance)
    }
}

#[cfg(test)]
pub struct MaxEdit {
    max_edit_distance: usize,
}

#[cfg(test)]
impl MaxEdit {
    pub fn new(max_edit_distance: usize) -> Self {
        Self { max_edit_distance }
    }
}

#[cfg(test)]
impl EditStrategy for MaxEdit {
    fn distance_for_string(&self, _: &str) -> usize {
        self.max_edit_distance
    }

    fn dist(&self) -> LevenshteinDistance {
        LevenshteinDistance::new(self.max_edit_distance)
    }
}

pub struct DictionaryResult {
    pub prob: f64,
    pub correction: String,
}

impl Hash for DictionaryResult {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.correction.hash(state);
    }
}

impl PartialEq for DictionaryResult {
    fn eq(&self, other: &Self) -> bool {
        self.correction == other.correction
    }
}

impl Eq for DictionaryResult {}

#[derive(Error, Debug)]
pub enum DictionaryError {
    #[error("Underlying error from FST")]
    Fst(#[from] fst::Error),

    #[error("An IO error has occured")]
    Io(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, DictionaryError>;

enum InnerMap {
    Memory(Map<Vec<u8>>),
    File(Map<Mmap>),
}

impl InnerMap {
    fn total_freq(&self) -> u64 {
        let mut stream = match self {
            InnerMap::Memory(map) => map.into_stream(),
            InnerMap::File(map) => map.into_stream(),
        };

        let mut total_freq = 0;
        while let Some((_, freq)) = stream.next() {
            total_freq += freq;
        }

        total_freq
    }
}

/// Dictionary that contains term frequency information
pub struct Dictionary<const TOP_N: usize> {
    cache: BTreeMap<String, u64>,
    map: InnerMap,
    folder_path: Option<String>,
    total_freq: u64,
}

#[cfg(test)]
impl Default for Dictionary<1_000> {
    fn default() -> Self {
        Dictionary::open::<&str>(None).unwrap()
    }
}

impl<const TOP_N: usize> Dictionary<TOP_N> {
    pub fn open<P: AsRef<Path>>(folder_path: Option<P>) -> Result<Self> {
        if let Some(path) = folder_path.as_ref() {
            fs::create_dir_all(path)?;
        }

        let folder_path = folder_path.map(|path| path.as_ref().to_str().unwrap().to_string());

        let map = match &folder_path {
            Some(path) => {
                let dictionary_path = Path::new(path).join("dictionary");

                if !dictionary_path.exists() {
                    let file = File::create(dictionary_path.clone())?;
                    let builder = MapBuilder::new(io::BufWriter::new(file))?;
                    builder.finish()?;
                }

                let dictionary_file = OpenOptions::new().read(true).open(dictionary_path)?;

                let mmap = unsafe { Mmap::map(&dictionary_file)? };
                InnerMap::File(Map::new(mmap)?)
            }
            None => InnerMap::Memory(Map::default()),
        };

        Ok(Dictionary {
            total_freq: map.total_freq(),
            map,
            folder_path,
            cache: BTreeMap::default(),
        })
    }

    fn store_union(&mut self, mut union: Union) -> Result<()> {
        match &self.folder_path {
            Some(path) => {
                let path = Path::new(path);
                let wrt = io::BufWriter::new(File::create(path.join("new_dictionary"))?);
                let mut builder = MapBuilder::new(wrt)?;

                let mut heap = BinaryHeap::with_capacity(TOP_N + 1);

                while let Some((key, values)) = union.next() {
                    let val: u64 = values.iter().map(|idx_val| idx_val.value).sum();

                    heap.push((
                        std::cmp::Reverse(val),
                        String::from_utf8_lossy(key).to_string(),
                    ));

                    if heap.len() > TOP_N {
                        heap.pop();
                    }
                }

                let top_values: BTreeMap<_, _> =
                    heap.into_iter().map(|(val, key)| (key, val.0)).collect();

                for (key, value) in top_values {
                    builder.insert(key, value)?;
                }

                let path = Path::new(path);
                builder.finish()?;
                std::fs::rename(path.join("new_dictionary"), path.join("dictionary"))?;
                let mmap = unsafe { Mmap::map(&File::open(path.join("dictionary"))?)? };
                self.map = InnerMap::File(Map::new(mmap)?);
            }
            None => {
                let mut builder = MapBuilder::memory();

                let mut heap = BinaryHeap::with_capacity(TOP_N + 1);

                while let Some((key, values)) = union.next() {
                    let val: u64 = values.iter().map(|idx_val| idx_val.value).sum();

                    heap.push((
                        std::cmp::Reverse(val),
                        String::from_utf8_lossy(key).to_string(),
                    ));

                    if heap.len() > TOP_N {
                        heap.pop();
                    }
                }

                let top_values: BTreeMap<_, _> =
                    heap.into_iter().map(|(val, key)| (key, val.0)).collect();

                for (key, value) in top_values {
                    builder.insert(key, value)?;
                }

                let bytes = builder.into_inner().unwrap();
                self.map = InnerMap::Memory(Map::new(bytes)?);
            }
        };

        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        let cache = mem::take(&mut self.cache);
        let cache_map = Map::from_iter(cache.into_iter())?;
        let map = mem::replace(&mut self.map, InnerMap::Memory(Map::default()));

        let union = match &map {
            InnerMap::File(map) => map.op().add(&cache_map).union(),
            InnerMap::Memory(map) => map.op().add(&cache_map).union(),
        };
        self.store_union(union)?;

        self.total_freq = self.map.total_freq();

        Ok(())
    }

    pub fn insert(&mut self, term: &str) {
        self.cache
            .entry(
                term.chars()
                    .into_iter()
                    .map(|c| c.to_ascii_lowercase())
                    .filter(|c| !matches!(c, ',' | '.' | '\\' | '=' | '*' | '(' | ')'))
                    .collect(),
            )
            .or_insert(0)
            .add_assign(1);
    }

    #[inline]
    pub fn all_probabilities(
        &self,
        term: &str,
        edit_distance: usize,
    ) -> Vec<HashSet<DictionaryResult>> {
        let searcher = fst::automaton::Levenshtein::new(term, edit_distance as u32);
        let mut res = Vec::with_capacity(edit_distance + 1);

        if searcher.is_err() {
            return res;
        }

        for _ in 0..edit_distance + 1 {
            res.push(HashSet::new());
        }

        let searcher = searcher.unwrap();

        let mut matches = self.perform_search(&searcher);
        let dist_metric = LevenshteinDistance::new(edit_distance);

        while let Some((correction, freq)) = matches.next() {
            let correction = std::str::from_utf8(correction).unwrap().to_owned();
            let dist = dist_metric.compare(term, &correction);

            let prob = freq as f64 / self.total_freq as f64;

            res[dist].insert(DictionaryResult { correction, prob });
        }

        res
    }

    fn perform_search<A: Automaton>(&self, searcher: A) -> fst::map::Stream<'_, A> {
        match &self.map {
            InnerMap::File(map) => map.search(searcher).into_stream(),
            InnerMap::Memory(map) => map.search(searcher).into_stream(),
        }
    }

    #[inline]
    pub fn probability(&self, term: &str) -> Option<f64> {
        let searcher = fst::automaton::Str::new(term);
        let mut matches = self.perform_search(&searcher);

        matches
            .next()
            .map(|(_key, frequency)| frequency as f64 / self.total_freq as f64)
    }

    #[cfg(test)]
    pub fn contains(&self, term: &str) -> bool {
        self.probability(term).is_some()
    }

    pub fn insert_page(&mut self, webpage: &Webpage) {
        let text = webpage.html.clean_text().unwrap_or_default();

        let mut stream = Normal::default().token_stream(text.as_str());

        while let Some(token) = stream.next() {
            self.insert(&token.text);
        }
    }

    pub fn merge(mut self, mut other: Dictionary<TOP_N>) -> Self {
        self.commit().unwrap();
        other.commit().unwrap();

        let map = mem::replace(&mut self.map, InnerMap::Memory(Map::default()));

        let union = match (&map, &other.map) {
            (InnerMap::Memory(map), InnerMap::Memory(other_map)) => map.op().add(other_map).union(),
            (InnerMap::Memory(map), InnerMap::File(other_map)) => map.op().add(other_map).union(),
            (InnerMap::File(map), InnerMap::Memory(other_map)) => map.op().add(other_map).union(),
            (InnerMap::File(map), InnerMap::File(other_map)) => map.op().add(other_map).union(),
        };

        self.store_union(union).unwrap();
        self.total_freq = self.map.total_freq();

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_probability() {
        let mut dict = Dictionary::default();

        dict.insert("this");
        dict.insert("is");
        dict.insert("a");
        dict.insert("test");
        dict.insert("test");

        dict.commit().unwrap();

        assert_eq!(dict.probability("this"), Some(0.2));
        assert_eq!(dict.probability("is"), Some(0.2));
        assert_eq!(dict.probability("a"), Some(0.2));
        assert_eq!(dict.probability("test"), Some(0.4));
    }

    #[test]
    fn test_uncontained_term() {
        let mut dict = Dictionary::default();

        dict.insert("this");
        dict.insert("is");
        dict.insert("a");
        dict.insert("test");
        dict.insert("test");

        dict.commit().unwrap();

        assert_eq!(dict.probability("the"), None);

        assert!(dict.contains("this"));
        assert!(dict.contains("is"));
        assert!(dict.contains("test"));
        assert!(!dict.contains("the"));
    }

    #[test]
    fn test_probability_edit_3() {
        let mut dict = Dictionary::default();

        dict.insert("test");
        dict.insert("twst");
        dict.insert("kage");

        dict.commit().unwrap();
        let res = dict.all_probabilities("test", 2);

        let mut num_elements = 0;

        for set in res {
            num_elements += set.len();
        }

        assert_eq!(num_elements, 2);
    }

    #[test]
    fn merge() {
        let mut dict1 = Dictionary::default();

        dict1.insert("test");
        dict1.insert("kage");

        let mut dict2 = Dictionary::default();

        dict2.insert("yay");
        dict2.insert("ay");

        let res = dict1.merge(dict2);

        assert!(res.contains("test"));
        assert!(res.contains("kage"));
        assert!(res.contains("yay"));
        assert!(res.contains("ay"));

        assert_eq!(res.probability("test"), Some(1.0 / 4.0));
        assert_eq!(res.probability("kage"), Some(1.0 / 4.0));
        assert_eq!(res.probability("yay"), Some(1.0 / 4.0));
        assert_eq!(res.probability("ay"), Some(1.0 / 4.0));
    }

    #[test]
    fn only_store_top_n() {
        let mut dict: Dictionary<2> = Dictionary::open::<&str>(None).unwrap();

        dict.insert("test");
        dict.insert("test");
        dict.insert("hej");
        dict.insert("kage");
        dict.insert("kage");

        dict.commit().unwrap();

        assert!(dict.contains("test"));
        assert!(dict.contains("kage"));
        assert!(!dict.contains("hej"));
    }

    #[test]
    fn weird_characters_ignored() {
        let mut dict = Dictionary::default();

        dict.insert("lennon,");

        dict.commit().unwrap();

        assert!(dict.contains("lennon"));
        assert!(!dict.contains("lennon,"));
    }
}
