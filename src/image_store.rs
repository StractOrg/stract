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

use crate::kv::{rocksdb_store::RocksDbStore, Kv};
use image::imageops::FilterType;
use image::{DynamicImage, ImageOutputFormat};
use serde::{de, ser::SerializeStruct, Serialize};
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::Path;

#[derive(PartialEq, Debug, Clone)]
pub struct Image(DynamicImage);

impl Serialize for Image {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut cursor = Cursor::new(Vec::new());
        self.0
            .write_to(&mut cursor, ImageOutputFormat::Png)
            .unwrap();
        cursor.seek(SeekFrom::Start(0)).unwrap();

        let mut bytes = Vec::new();
        cursor.read_to_end(&mut bytes).unwrap();

        let mut image = serializer.serialize_struct("image", 1)?;
        image.serialize_field("0", &bytes)?;
        image.end()
    }
}

impl<'de> de::Deserialize<'de> for Image {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ImageVisitor;

        impl<'de> de::Visitor<'de> for ImageVisitor {
            type Value = Image;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a serialized `Image` struct")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
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

struct ImageStore {
    store: Box<dyn Kv<String, Image>>,
    filters: Vec<Box<dyn ImageFilter>>,
}

impl ImageStore {
    fn open<P: AsRef<Path>>(path: P) -> Self {
        Self::open_with_filters(path, Vec::new())
    }

    fn open_with_filters<P: AsRef<Path>>(path: P, filters: Vec<Box<dyn ImageFilter>>) -> Self {
        let store = RocksDbStore::open(path);

        Self { store, filters }
    }

    fn insert(&mut self, key: &str, mut image: Image) {
        for filter in &self.filters {
            image = filter.transform(image);
        }

        self.store.insert(key.to_string(), image)
    }

    fn contains(&self, key: &str) -> bool {
        self.store.get(&key.to_string()).is_some()
    }

    fn get(&self, key: &str) -> Option<Image> {
        self.store.get(&key.to_string())
    }
}

trait ImageFilter {
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

pub struct FaviconStore(ImageStore);

impl FaviconStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let store = ImageStore::open_with_filters(
            path,
            vec![Box::new(ResizeFilter {
                width: 16,
                height: 16,
            })],
        );

        Self(store)
    }

    pub fn insert(&mut self, key: &str, image: Image) {
        self.0.insert(key, image)
    }

    pub fn contains(&self, key: &str) -> bool {
        self.0.contains(key)
    }

    pub fn get(&self, key: &str) -> Option<Image> {
        self.0.get(key)
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
        let key = "test";
        let mut store = ImageStore::open(crate::gen_temp_path());

        assert!(!store.contains(key));
        assert_eq!(store.get(key), None);
        store.insert(key, image.clone());
        assert!(store.contains(key));
        assert_eq!(store.get(key), Some(image));
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

    #[test]
    fn favicon_store() {
        let image = Image(
            ImageBuffer::from_pixel(32, 32, image::Rgb::<u16>([u16::MAX, u16::MAX, u16::MAX]))
                .into(),
        );
        assert_eq!(image.0.width(), 32);
        assert_eq!(image.0.height(), 32);

        let mut store = FaviconStore::open(crate::gen_temp_path());

        let key = "test";

        assert!(!store.contains(key));
        assert_eq!(store.get(key), None);
        store.insert(key, image);
        assert!(store.contains(key));

        let retrieved_image = store.get(key).unwrap();
        assert_eq!(retrieved_image.0.width(), 16);
        assert_eq!(retrieved_image.0.height(), 16);
    }
}
