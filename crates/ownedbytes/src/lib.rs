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

pub use stable_deref_trait::StableDeref;
use std::{
    fmt, io,
    ops::{Deref, Range},
    path::Path,
    sync::Arc,
};

pub struct OwnedBytes {
    data: &'static [u8],
    box_stable_deref: Arc<dyn Deref<Target = [u8]> + Sync + Send>,
}

impl OwnedBytes {
    pub fn mmap_from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref();
        let mmap = unsafe { memmap2::Mmap::map(&std::fs::File::open(path)?)? };

        let box_stable_deref = Arc::new(mmap);
        let bytes: &[u8] = box_stable_deref.deref();
        let data = unsafe { &*(bytes as *const [u8]) };
        Ok(Self {
            data,
            box_stable_deref,
        })
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

    #[must_use]
    #[inline]
    pub fn slice(&self, range: Range<usize>) -> Self {
        Self {
            data: &self.data[range],
            box_stable_deref: self.box_stable_deref.clone(),
        }
    }

    #[inline]
    #[must_use]
    pub fn split(self, split_len: usize) -> (Self, Self) {
        let (left_data, right_data) = self.data.split_at(split_len);
        let right_box_stable_deref = self.box_stable_deref.clone();
        let left = Self {
            data: left_data,
            box_stable_deref: self.box_stable_deref,
        };
        let right = Self {
            data: right_data,
            box_stable_deref: right_box_stable_deref,
        };
        (left, right)
    }

    #[inline]
    #[must_use]
    pub fn rsplit(self, split_len: usize) -> (Self, Self) {
        let data_len = self.data.len();
        self.split(data_len - split_len)
    }

    pub fn split_off(&mut self, split_len: usize) -> Self {
        let (left, right) = self.data.split_at(split_len);
        let right_box_stable_deref = self.box_stable_deref.clone();
        let right_piece = Self {
            data: right,
            box_stable_deref: right_box_stable_deref,
        };
        self.data = left;
        right_piece
    }

    #[inline]
    pub fn advance(&mut self, advance_len: usize) -> &[u8] {
        let (data, rest) = self.data.split_at(advance_len);
        self.data = rest;
        data
    }

    #[inline]
    pub fn read_u8(&mut self) -> u8 {
        self.advance(1)[0]
    }

    #[inline]
    fn read_n<const N: usize>(&mut self) -> [u8; N] {
        self.advance(N).try_into().unwrap()
    }

    #[inline]
    pub fn read_u32_le(&mut self) -> u32 {
        u32::from_le_bytes(self.read_n())
    }

    #[inline]
    pub fn read_u64_le(&mut self) -> u64 {
        u64::from_le_bytes(self.read_n())
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
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let data_len = self.data.len();
        let buf_len = buf.len();
        if data_len >= buf_len {
            let data = self.advance(buf_len);
            buf.copy_from_slice(data);
            Ok(buf_len)
        } else {
            buf[..data_len].copy_from_slice(self.data);
            self.data = &[];
            Ok(data_len)
        }
    }
    #[inline]
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        buf.extend(self.data);
        let read_len = self.data.len();
        self.data = &[];
        Ok(read_len)
    }
    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let read_len = self.read(buf)?;
        if read_len != buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        Ok(())
    }
}

impl From<Vec<u8>> for OwnedBytes {
    fn from(vec: Vec<u8>) -> Self {
        Self::new(vec)
    }
}

impl PartialEq for OwnedBytes {
    fn eq(&self, other: &OwnedBytes) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for OwnedBytes {}

impl PartialEq<[u8]> for OwnedBytes {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<str> for OwnedBytes {
    fn eq(&self, other: &str) -> bool {
        self.as_slice() == other.as_bytes()
    }
}

impl<'a, T: ?Sized> PartialEq<&'a T> for OwnedBytes
where
    OwnedBytes: PartialEq<T>,
{
    fn eq(&self, other: &&'a T) -> bool {
        *self == **other
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

    #[test]
    fn test_owned_bytes_read() -> io::Result<()> {
        let mut bytes = OwnedBytes::new(b"abcdefghiklmnopqrstuvwxyz".as_ref());
        {
            let mut buf = [0u8; 5];
            bytes.read_exact(&mut buf[..]).unwrap();
            assert_eq!(&buf, b"abcde");
            assert_eq!(bytes.as_slice(), b"fghiklmnopqrstuvwxyz")
        }
        {
            let mut buf = [0u8; 2];
            bytes.read_exact(&mut buf[..]).unwrap();
            assert_eq!(&buf, b"fg");
            assert_eq!(bytes.as_slice(), b"hiklmnopqrstuvwxyz")
        }
        Ok(())
    }

    #[test]
    fn test_owned_bytes_read_right_at_the_end() -> io::Result<()> {
        let mut bytes = OwnedBytes::new(b"abcde".as_ref());
        let mut buf = [0u8; 5];
        assert_eq!(bytes.read(&mut buf[..]).unwrap(), 5);
        assert_eq!(&buf, b"abcde");
        assert_eq!(bytes.as_slice(), b"");
        assert_eq!(bytes.read(&mut buf[..]).unwrap(), 0);
        assert_eq!(&buf, b"abcde");
        Ok(())
    }
    #[test]
    fn test_owned_bytes_read_incomplete() -> io::Result<()> {
        let mut bytes = OwnedBytes::new(b"abcde".as_ref());
        let mut buf = [0u8; 7];
        assert_eq!(bytes.read(&mut buf[..]).unwrap(), 5);
        assert_eq!(&buf[..5], b"abcde");
        assert_eq!(bytes.read(&mut buf[..]).unwrap(), 0);
        Ok(())
    }

    #[test]
    fn test_owned_bytes_read_to_end() -> io::Result<()> {
        let mut bytes = OwnedBytes::new(b"abcde".as_ref());
        let mut buf = Vec::new();
        bytes.read_to_end(&mut buf)?;
        assert_eq!(buf.as_slice(), b"abcde".as_ref());
        Ok(())
    }

    #[test]
    fn test_owned_bytes_read_u8() -> io::Result<()> {
        let mut bytes = OwnedBytes::new(b"\xFF".as_ref());
        assert_eq!(bytes.read_u8(), 255);
        assert_eq!(bytes.len(), 0);
        Ok(())
    }

    #[test]
    fn test_owned_bytes_read_u64() -> io::Result<()> {
        let mut bytes = OwnedBytes::new(b"\0\xFF\xFF\xFF\xFF\xFF\xFF\xFF".as_ref());
        assert_eq!(bytes.read_u64_le(), u64::MAX - 255);
        assert_eq!(bytes.len(), 0);
        Ok(())
    }

    #[test]
    fn test_owned_bytes_split() {
        let bytes = OwnedBytes::new(b"abcdefghi".as_ref());
        let (left, right) = bytes.split(3);
        assert_eq!(left.as_slice(), b"abc");
        assert_eq!(right.as_slice(), b"defghi");
    }

    #[test]
    fn test_owned_bytes_split_boundary() {
        let bytes = OwnedBytes::new(b"abcdefghi".as_ref());
        {
            let (left, right) = bytes.clone().split(0);
            assert_eq!(left.as_slice(), b"");
            assert_eq!(right.as_slice(), b"abcdefghi");
        }
        {
            let (left, right) = bytes.split(9);
            assert_eq!(left.as_slice(), b"abcdefghi");
            assert_eq!(right.as_slice(), b"");
        }
    }

    #[test]
    fn test_split_off() {
        let mut data = OwnedBytes::new(b"abcdef".as_ref());
        assert_eq!(data, "abcdef");
        assert_eq!(data.split_off(2), "cdef");
        assert_eq!(data, "ab");
        assert_eq!(data.split_off(1), "b");
        assert_eq!(data, "a");
    }
}
