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
// along with this program.  If not, see <https://www.gnu.org/licenses/

//! This is taken from https://docs.rs/ownedbytes/latest/ownedbytes/
//! to avoid having to pull in another dependency.

use stable_deref_trait::StableDeref;
use std::{fmt, io, ops::Deref, path::Path, sync::Arc};

pub struct OwnedBytes {
    data: &'static [u8],
    box_stable_deref: Arc<dyn Deref<Target = [u8]> + Sync + Send>,
}

impl OwnedBytes {
    pub fn mmap_from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref();
        let mmap = unsafe { memmap2::Mmap::map(&std::fs::File::open(path)?)? };

        Ok(Self::new(mmap))
    }

    pub fn empty() -> Self {
        Self::new(&[][..])
    }

    pub fn new<T: StableDeref + Deref<Target = [u8]> + 'static + Send + Sync>(
        data_holder: T,
    ) -> Self {
        let box_stable_deref = Arc::new(data_holder);
        let bytes: &[u8] = box_stable_deref.deref();
        let data = unsafe { &*(bytes as *const [u8]) };
        Self {
            data,
            box_stable_deref,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.data
    }
}

impl Deref for OwnedBytes {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl AsRef<[u8]> for OwnedBytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}

impl fmt::Debug for OwnedBytes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // We truncate the bytes in order to make sure the debug string
        // is not too long.
        let bytes_truncated: &[u8] = if self.len() > 8 {
            &self.as_slice()[..8]
        } else {
            self.as_slice()
        };

        write!(f, "OwnedBytes({bytes_truncated:?}, len={})", self.len())
    }
}

impl Clone for OwnedBytes {
    fn clone(&self) -> Self {
        OwnedBytes {
            data: self.data,
            box_stable_deref: self.box_stable_deref.clone(),
        }
    }
}

impl io::Read for OwnedBytes {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.as_slice().read(buf)
    }
}

impl From<Vec<u8>> for OwnedBytes {
    fn from(vec: Vec<u8>) -> Self {
        Self::new(vec)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    #[test]
    fn test_owned_bytes() {
        let bytes = OwnedBytes::new(vec![1, 2, 3, 4, 5]);
        assert_eq!(bytes.len(), 5);
        assert_eq!(bytes.as_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_owned_bytes_empty() {
        let bytes = OwnedBytes::empty();
        assert_eq!(bytes.len(), 0);
        assert_eq!(bytes.as_slice(), &[]);
    }

    #[test]
    fn test_read() {
        let mut bytes = OwnedBytes::new(vec![1, 2, 3, 4, 5]);
        let mut buf = [0; 3];
        assert_eq!(bytes.read(&mut buf).unwrap(), 3);
        assert_eq!(&buf, &[1, 2, 3]);
    }
}
