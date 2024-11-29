// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

use super::{MergePointer, Result};
use fst::{IntoStreamer, Streamer};

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    fs::{File, OpenOptions},
    io::BufWriter,
    path::{Path, PathBuf},
};
use uuid::Uuid;

struct DictBuilder {
    map: BTreeMap<String, u64>,
}

impl Default for DictBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DictBuilder {
    fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    fn insert(&mut self, term: &str) {
        self.map
            .entry(term.to_string())
            .and_modify(|e| *e += 1)
            .or_insert(1);
    }

    fn build<P: AsRef<Path>>(self, path: P) -> Result<StoredDict> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path.as_ref())?;

        let wtr = BufWriter::new(file);

        let mut builder = fst::MapBuilder::new(wtr)?;

        for (term, freq) in self.map {
            builder.insert(term, freq)?;
        }

        builder.finish()?;

        StoredDict::open(path)
    }
}

struct StoredDict {
    map: fst::Map<memmap2::Mmap>,
    path: PathBuf,
}

impl StoredDict {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mmap = unsafe { memmap2::Mmap::map(&File::open(path.as_ref())?)? };

        Ok(Self {
            map: fst::Map::new(mmap)?,
            path: path.as_ref().to_path_buf(),
        })
    }

    fn merge<P: AsRef<Path>>(dicts: Vec<Self>, folder: P) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(folder.as_ref())?;

        let wtr = BufWriter::new(file);
        let mut builder = fst::MapBuilder::new(wtr)?;

        let mut pointers: Vec<_> = dicts
            .iter()
            .map(|d| MergePointer {
                term: String::new(),
                value: 0,
                stream: d.map.stream(),
                is_finished: false,
            })
            .collect();

        for pointer in pointers.iter_mut() {
            pointer.advance();
        }

        let mut pointers: BinaryHeap<_> = pointers.into_iter().map(Reverse).collect();

        loop {
            let (term, mut freq, is_finished) = {
                match pointers.peek_mut() {
                    Some(mut pointer) => {
                        let res = (
                            pointer.0.term.clone(),
                            pointer.0.value,
                            pointer.0.is_finished,
                        );
                        pointer.0.advance();
                        res
                    }
                    None => break,
                }
            };

            if is_finished {
                break;
            }

            while let Some(mut other) = pointers.peek_mut() {
                if other.0.term != term || other.0.is_finished {
                    break;
                }

                freq += other.0.value;
                other.0.advance();
            }

            builder.insert(term, freq)?;
        }

        builder.finish()?;

        let mmap = unsafe { memmap2::Mmap::map(&File::open(folder.as_ref())?)? };

        Ok(StoredDict {
            map: fst::Map::new(mmap)?,
            path: folder.as_ref().to_path_buf(),
        })
    }
}

#[derive(Default, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
struct Metadata {
    #[bincode(with_serde)]
    dicts: Vec<Uuid>,
}

pub struct TermDict {
    builder: DictBuilder,
    stored: Vec<StoredDict>,
    path: PathBuf,
    metadata: Metadata,
}

impl TermDict {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        if path.as_ref().exists() {
            let file = File::open(path.as_ref().join("meta.json"))?;
            let metadata: Metadata = serde_json::from_reader(file)?;

            let mut stored = Vec::new();

            for uuid in metadata.dicts.iter() {
                stored.push(StoredDict::open(
                    path.as_ref().join(format!("{}.dict", uuid)),
                )?);
            }

            Ok(Self {
                builder: DictBuilder::new(),
                stored,
                path: path.as_ref().to_path_buf(),
                metadata,
            })
        } else {
            std::fs::create_dir_all(path.as_ref())?;

            let s = Self {
                builder: DictBuilder::new(),
                stored: Vec::new(),
                path: path.as_ref().to_path_buf(),
                metadata: Metadata::default(),
            };
            s.save_meta()?;

            Ok(s)
        }
    }

    pub fn insert(&mut self, term: &str) {
        if term.len() <= 1 {
            return;
        }

        if term.len() > 100 {
            return;
        }

        if term.contains(' ') {
            return;
        }

        let num_chars = term.chars().count();

        let punctuation_percentage =
            term.chars().filter(|c| c.is_ascii_punctuation()).count() as f64 / num_chars as f64;

        if punctuation_percentage > 0.5 {
            return;
        }

        let non_alphabetic_percentage =
            term.chars().filter(|c| !c.is_alphabetic()).count() as f64 / num_chars as f64;

        if non_alphabetic_percentage > 0.25 {
            return;
        }

        self.builder.insert(term);
    }

    pub fn commit(&mut self) -> Result<()> {
        let builder = std::mem::take(&mut self.builder);

        let uuid = uuid::Uuid::new_v4();

        let stored = builder.build(self.path.join(format!("{}.dict", uuid)))?;

        self.metadata.dicts.push(uuid);
        self.save_meta()?;

        self.stored.push(stored);
        self.gc()?;

        Ok(())
    }

    fn gc(&self) -> Result<()> {
        let all_dicts = self
            .path
            .read_dir()?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().unwrap_or_default() == "dict")
            .map(|e| e.path())
            .collect::<Vec<_>>();

        for dict in all_dicts {
            if !self.metadata.dicts.contains(
                &dict
                    .file_stem()
                    .expect("dict files should have a filename")
                    .to_str()
                    .expect("dict filenames are created from uuid `.to_string()`, so they should be valid utf8")
                    .parse()
                    .expect("dict filenames are created from uuid `.to_string()`, so they should be valid uuids"),
            ) {
                std::fs::remove_file(dict)?;
            }
        }

        Ok(())
    }

    fn save_meta(&self) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.path.join("meta.json"))?;

        serde_json::to_writer_pretty(file, &self.metadata)?;

        Ok(())
    }

    pub fn merge_dicts(&mut self) -> Result<()> {
        if self.stored.len() <= 1 {
            return Ok(());
        }

        let uuid = uuid::Uuid::new_v4();

        let merged = StoredDict::merge(
            std::mem::take(&mut self.stored),
            self.path.join(format!("{}.dict", uuid)),
        )?;
        self.metadata.dicts.clear();

        self.metadata.dicts.push(uuid);
        self.save_meta()?;

        self.stored.push(merged);
        self.gc()?;

        Ok(())
    }

    pub fn freq(&self, term: &str) -> Option<u64> {
        let mut freqs = None;

        for stored in self.stored.iter() {
            if let Some(freq) = stored.map.get(term) {
                match freqs {
                    None => freqs = Some(freq),
                    Some(f) => freqs = Some(f + freq),
                }
            }
        }

        freqs
    }

    pub fn terms(&self) -> Vec<String> {
        let mut terms = Vec::new();

        for stored in self.stored.iter() {
            let mut stream = stored.map.stream();

            while let Some((term, _)) = stream.next() {
                terms.push(std::str::from_utf8(term).unwrap().to_string());
            }
        }

        terms
    }

    pub fn search(&self, term: &str, max_edit_distance: u32) -> Vec<String> {
        let mut res = Vec::new();

        for stored in self.stored.iter() {
            if let Ok(automaton) = fst::automaton::Levenshtein::new(term, max_edit_distance) {
                if let Ok(mut s) = stored.map.search(automaton).into_stream().into_str_keys() {
                    res.append(&mut s);
                }
            }
        }

        res
    }

    pub fn merge(&mut self, other: Self) -> Result<()> {
        for stored in other.stored {
            let uuid = uuid::Uuid::new_v4();
            let new_path = self.path.join(format!("{}.dict", uuid));
            std::fs::rename(stored.path, &new_path)?;

            self.metadata.dicts.push(uuid);
            self.save_meta()?;

            self.stored.push(StoredDict::open(new_path)?);
        }

        Ok(())
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_term_dict() -> Result<()> {
        let temp_dir = file_store::gen_temp_dir().unwrap();
        let path = temp_dir.as_ref().join("dicts");
        let mut dict = TermDict::open(&path)?;

        dict.insert("foo");
        dict.insert("bar");
        dict.insert("baz");
        dict.insert("foo");
        dict.insert("bar");
        dict.insert("foo");

        dict.commit()?;

        dict.insert("foo");
        dict.insert("bar");
        dict.insert("baz");
        dict.insert("foo");
        dict.insert("bar");
        dict.insert("foo");
        dict.insert("abc");

        dict.commit()?;

        dict.merge_dicts()?;

        assert_eq!(dict.stored.len(), 1);

        assert_eq!(dict.freq("abc"), Some(1));
        assert_eq!(dict.freq("bar"), Some(4));
        assert_eq!(dict.freq("baz"), Some(2));
        assert_eq!(dict.freq("foo"), Some(6));

        Ok(())
    }

    #[test]
    fn reopen() {
        let temp_dir = file_store::gen_temp_dir().unwrap();
        let path = temp_dir.as_ref().join("dicts");

        {
            let mut dict = TermDict::open(&path).unwrap();

            dict.insert("foo");
            dict.insert("bar");
            dict.insert("baz");
            dict.insert("foo");
            dict.insert("bar");
            dict.insert("foo");

            dict.commit().unwrap();
        }

        {
            let mut dict = TermDict::open(&path).unwrap();

            dict.insert("foo");
            dict.insert("bar");
            dict.insert("baz");
            dict.insert("foo");
            dict.insert("bar");
            dict.insert("foo");

            dict.commit().unwrap();
        }

        {
            let dict = TermDict::open(&path).unwrap();

            assert_eq!(dict.stored.len(), 2);

            assert_eq!(dict.freq("foo"), Some(6));
            assert_eq!(dict.freq("bar"), Some(4));
            assert_eq!(dict.freq("baz"), Some(2));
        }
    }
}
