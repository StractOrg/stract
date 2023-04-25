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

use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::BufReader,
    ops::Range,
    path::{Path, PathBuf},
};

use rkyv::{ser::serializers::AllocSerializer, AlignedVec};

type Result<T> = std::result::Result<T, Error>;

const SCRATCH_SIZE: usize = 4096;
const LOCATIONS_NAME: &str = "locations.bin";
const MMAP_NAME: &str = "data.bin";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("key already exists")]
    KeyExists,

    #[error("io error: {0}")]
    Io(std::io::Error),

    #[error("bincode error: {0}")]
    Bincode(bincode::Error),
}

pub struct Store<K, V> {
    locations: BTreeMap<K, Range<usize>>,
    folder_path: PathBuf,
    cur_end_range: usize,
    map: memmap2::MmapMut,
    has_new_inserts: bool,
    segment_name: String,
    _values: std::marker::PhantomData<V>,
}

impl<K, V> Store<K, V>
where
    K: serde::de::DeserializeOwned + serde::Serialize + Ord,
    V: rkyv::Archive + 'static,
    V::Archived: rkyv::Deserialize<V, rkyv::de::deserializers::SharedDeserializeMap> + 'static,
{
    pub fn open<P: AsRef<Path>>(folder_path: P, segment_name: &str) -> Result<Self> {
        if folder_path.as_ref().is_file() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("{} is not a directory", folder_path.as_ref().display()),
            )));
        }

        std::fs::create_dir_all(folder_path.as_ref()).map_err(Error::Io)?;

        let locations_path = folder_path
            .as_ref()
            .join(format!("{segment_name}-{LOCATIONS_NAME}"));
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(locations_path)
            .map_err(Error::Io)?;

        let mut reader = BufReader::new(f);
        let locations: BTreeMap<K, Range<usize>> =
            bincode::deserialize_from(&mut reader).unwrap_or_default();

        let cur_end_range = locations.values().map(|range| range.end).max().unwrap_or(0);

        let mmap_path = folder_path
            .as_ref()
            .join(format!("{segment_name}-{MMAP_NAME}"));
        let map = unsafe {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(mmap_path)
                .map_err(Error::Io)?;

            memmap2::MmapOptions::new()
                .map_mut(&file)
                .map_err(Error::Io)?
        };

        Ok(Self {
            locations,
            cur_end_range,
            map,
            folder_path: folder_path.as_ref().to_path_buf(),
            has_new_inserts: false,
            _values: std::marker::PhantomData,
            segment_name: segment_name.to_string(),
        })
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let range = self.locations.get(key)?;
        let bytes = &self.map[range.clone()];

        let mut aligned = AlignedVec::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);

        // SAFETY:
        //      We ensure the target `V` is `'static` and contains only owned data so it's safe to
        //      temporarily extend the lifetime so we can allocate the type entirely.
        let v = unsafe { rkyv::util::from_bytes_unchecked::<V>(&aligned).unwrap() };

        Some(v)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, V)> + '_ {
        self.locations.iter().map(move |(key, range)| {
            let bytes = &self.map[range.clone()];

            let mut aligned = AlignedVec::with_capacity(bytes.len());
            aligned.extend_from_slice(bytes);

            // SAFETY:
            //      We ensure the target `V` is `'static` and contains only owned data so it's safe to
            //      temporarily extend the lifetime so we can allocate the type entirely.
            let v = unsafe { rkyv::util::from_bytes_unchecked::<V>(&aligned).unwrap() };

            (key, v)
        })
    }

    pub fn insert(&mut self, key: K, value: &V) -> Result<()>
    where
        V: rkyv::Serialize<AllocSerializer<SCRATCH_SIZE>>,
    {
        if self.locations.contains_key(&key) {
            return Err(Error::KeyExists);
        }

        let bytes = rkyv::to_bytes::<_, SCRATCH_SIZE>(value).unwrap();
        self.insert_raw(key, &bytes[..])
    }

    fn insert_raw(&mut self, key: K, bytes: &[u8]) -> Result<()> {
        let old_end = self.cur_end_range;

        let new_end = bytes.len() + self.cur_end_range;

        if new_end > self.map.len() {
            self.resize(new_end)?;
        }

        self.map[old_end..new_end].copy_from_slice(bytes);
        self.locations.insert(key, old_end..new_end);
        self.cur_end_range = new_end;
        self.has_new_inserts = true;

        Ok(())
    }

    fn resize(&mut self, size: usize) -> Result<()> {
        if size > self.map.len() {
            self.flush_map()?;

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(
                    self.folder_path
                        .join(format!("{}-{MMAP_NAME}", self.segment_name)),
                )
                .map_err(Error::Io)?;

            file.set_len(size as u64).map_err(Error::Io)?;

            self.map = unsafe {
                memmap2::MmapOptions::new()
                    .map_mut(&file)
                    .map_err(Error::Io)?
            };
        }

        Ok(())
    }

    fn flush_map(&mut self) -> Result<()> {
        if self.has_new_inserts {
            self.map.flush().map_err(Error::Io)?;
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.has_new_inserts {
            self.flush_map()?;

            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(
                    self.folder_path
                        .join(format!("{}-{}", self.segment_name, LOCATIONS_NAME)),
                )
                .map_err(Error::Io)?;

            bincode::serialize_into(f, &self.locations).map_err(Error::Bincode)?;
        }
        self.has_new_inserts = false;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rkyv::Archive;

    use super::*;

    #[derive(Debug, PartialEq, Eq, Clone, Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[archive(compare(PartialEq))]
    #[archive_attr(derive(Debug))]
    struct TestStruct {
        a: String,
        b: i32,
    }

    #[test]
    fn test_insert() {
        let mut kv =
            Store::<String, TestStruct>::open(crate::gen_temp_path(), "test-segment").unwrap();

        assert!(kv.get(&"test".to_string()).is_none());

        let test_struct = TestStruct {
            a: "test".to_string(),
            b: 5,
        };

        kv.insert("test".to_string(), &test_struct).unwrap();
        kv.flush().unwrap();

        let archived = kv.get(&"test".to_string()).unwrap();
        assert_eq!(test_struct, archived);
    }

    #[test]
    fn test_re_open() {
        let path = crate::gen_temp_path();
        let segment_name = "test-segment";
        let test_struct = TestStruct {
            a: "test".to_string(),
            b: 5,
        };

        {
            let mut kv = Store::<String, TestStruct>::open(path.clone(), segment_name).unwrap();

            assert!(kv.get(&"test".to_string()).is_none());

            kv.insert("test".to_string(), &test_struct).unwrap();
            kv.flush().unwrap();
        }

        let kv = Store::<String, TestStruct>::open(path, segment_name).unwrap();

        let archived = kv.get(&"test".to_string()).unwrap();
        assert_eq!(test_struct, archived);
    }

    #[test]
    fn test_insert_existing_key() {
        let mut kv =
            Store::<String, TestStruct>::open(crate::gen_temp_path(), "test-segment").unwrap();

        assert!(kv.get(&"test".to_string()).is_none());

        let test_struct = TestStruct {
            a: "test".to_string(),
            b: 5,
        };

        kv.insert("test".to_string(), &test_struct).unwrap();
        kv.flush().unwrap();

        let archived = kv.get(&"test".to_string()).unwrap();
        assert_eq!(test_struct, archived);

        let test_struct = TestStruct {
            a: "test".to_string(),
            b: 6,
        };

        assert!(kv.insert("test".to_string(), &test_struct).is_err());
    }
}
