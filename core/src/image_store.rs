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

use crate::kv::{rocksdb_store::RocksDbStore, Kv};
use crate::Result;
use image::imageops::FilterType;
use image::{DynamicImage, ImageOutputFormat};
use serde::{de, ser::SerializeStruct, Serialize};
use std::io::{Cursor, Read, Seek};
use std::path::Path;

#[derive(PartialEq, Debug, Clone)]
pub struct Image(DynamicImage);

impl Serialize for Image {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = self.as_raw_bytes();

        let mut image = serializer.serialize_struct("image", 1)?;
        image.serialize_field("0", &bytes)?;
        image.end()
    }
}

impl<'de> de::Deserialize<'de> for Image {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ImageVisitor;

        impl<'de> de::Visitor<'de> for ImageVisitor {
            type Value = Image;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a serialized `Image` struct")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let raw = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;

                let image = Image(
                    image::load_from_memory(raw)
                        .expect("bytes does not seem to represent an image"),
                );

                Ok(image)
            }
        }

        deserializer.deserialize_struct("image", &["0"], ImageVisitor)
    }
}

pub trait ImageStore<K: Serialize> {
    fn insert(&mut self, key: K, image: Image);
    fn get(&self, key: &K) -> Option<Image>;
    fn merge(&mut self, other: Self);
    fn flush(&self);
}

struct BaseImageStore {
    store: RocksDbStore<String, Image>,
    filters: Vec<Box<dyn ImageFilter>>,
    path: std::path::PathBuf,
}

impl BaseImageStore {
    #[cfg(test)]
    fn open<P: AsRef<Path>>(path: P) -> Self {
        Self::open_with_filters(path, Vec::new())
    }

    fn open_with_filters<P: AsRef<Path>>(path: P, filters: Vec<Box<dyn ImageFilter>>) -> Self {
        let store = RocksDbStore::open_read_only(&path);

        Self {
            store,
            filters,
            path: path.as_ref().to_path_buf(),
        }
    }

    fn prepare_writer(&mut self) {
        self.store = RocksDbStore::open(&self.path);
    }

    fn insert(&mut self, key: String, mut image: Image) {
        for filter in &self.filters {
            image = filter.transform(image);
        }

        self.store.insert(key, image)
    }

    fn flush(&self) {
        self.store.flush();
    }

    fn get(&self, key: &String) -> Option<Image> {
        self.store.get(key)
    }

    fn merge(&mut self, other: BaseImageStore) {
        for (key, image) in other.store.iter() {
            self.insert(key, image);
        }
    }
}

trait ImageFilter: Send + Sync {
    fn transform(&self, image: Image) -> Image;
}

struct ResizeFilter {
    width: u32,
    height: u32,
}

impl ImageFilter for ResizeFilter {
    fn transform(&self, image: Image) -> Image {
        Image(
            image
                .0
                .resize(self.width, self.height, FilterType::Gaussian),
        )
    }
}

pub struct EntityImageStore {
    store: BaseImageStore,
}

impl EntityImageStore {
    pub fn prepare_writer(&mut self) {
        self.store.prepare_writer();
    }
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let store = BaseImageStore::open_with_filters(
            path,
            vec![Box::new(ResizeFilter {
                width: 200,
                height: 200,
            })],
        );

        Self { store }
    }
}

impl ImageStore<String> for EntityImageStore {
    fn insert(&mut self, entity_name: String, image: Image) {
        self.store.insert(entity_name, image);
    }

    fn get(&self, entity_name: &String) -> Option<Image> {
        self.store.get(entity_name)
    }

    fn merge(&mut self, other: Self) {
        self.store.merge(other.store)
    }

    fn flush(&self) {
        self.store.flush()
    }
}

impl Image {
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Result<Image> {
        if let Ok(img) = image::load_from_memory(&bytes) {
            Ok(Self(img))
        } else if let Ok(img) =
            image::load_from_memory_with_format(&bytes, image::ImageFormat::Jpeg)
        {
            Ok(Self(img))
        } else {
            Ok(Self(image::load_from_memory_with_format(
                &bytes,
                image::ImageFormat::Png,
            )?))
        }
    }

    pub(crate) fn as_raw_bytes(&self) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        self.0
            .write_to(&mut cursor, ImageOutputFormat::Png)
            .unwrap();
        cursor.rewind().unwrap();

        let mut bytes = Vec::new();
        cursor.read_to_end(&mut bytes).unwrap();

        bytes
    }

    pub fn resize(self, width: u32, height: u32) -> Self {
        ResizeFilter { width, height }.transform(self)
    }
}

#[cfg(test)]
mod tests {
    use image::ImageBuffer;

    use super::*;

    #[test]
    fn serialize_deserialize_image() {
        let image = Image(
            ImageBuffer::from_pixel(2, 2, image::Rgb::<u16>([u16::MAX, u16::MAX, u16::MAX])).into(),
        );

        let bytes = bincode::serialize(&image).unwrap();
        let decoded_image = bincode::deserialize(&bytes).unwrap();

        assert_eq!(image, decoded_image);
    }

    #[test]
    fn store_and_load_image() {
        let image = Image(
            ImageBuffer::from_pixel(2, 2, image::Rgb::<u16>([u16::MAX, u16::MAX, u16::MAX])).into(),
        );
        let key = "test".to_string();
        let mut store = BaseImageStore::open(crate::gen_temp_path());
        store.prepare_writer();

        assert_eq!(store.get(&key), None);
        store.insert(key.clone(), image.clone());
        assert_eq!(store.get(&key), Some(image));
    }

    #[test]
    fn resize_filter() {
        let image = Image(
            ImageBuffer::from_pixel(32, 32, image::Rgb::<u16>([u16::MAX, u16::MAX, u16::MAX]))
                .into(),
        );
        assert_eq!(image.0.width(), 32);
        assert_eq!(image.0.height(), 32);

        let transformed_image = ResizeFilter {
            width: 16,
            height: 16,
        }
        .transform(image);

        assert_eq!(transformed_image.0.width(), 16);
        assert_eq!(transformed_image.0.height(), 16);
    }
}
