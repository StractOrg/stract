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

//! Zim file reader.
//! https://wiki.openzim.org/wiki/ZIM_file_format

pub mod wiki;

pub use wiki::{Article, ArticleIterator, Image, ImageIterator};

use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Unexpected end of bytes")]
    UnexpectedEndOfBytes,

    #[error("Invalid magic number")]
    InvalidMagicNumber,

    #[error("Invalid checksum")]
    InvalidChecksum,

    #[error("Invalid compression type")]
    InvalidCompressionType,

    #[error("LZMA error: {0}")]
    Lzma(#[from] lzma::Error),
}

fn read_zero_terminated(bytes: &[u8]) -> Result<String, Error> {
    let mut string = String::new();

    let mut i = 0;
    while i < bytes.len() && bytes[i] != 0 {
        string.push(bytes[i] as char);
        i += 1;
    }

    Ok(string)
}

#[derive(Debug)]
#[allow(unused)]
struct Header {
    magic: u32,
    major_version: u16,
    minor_version: u16,
    uuid: [u8; 16],
    entry_count: u32,
    cluster_count: u32,
    url_ptr_pos: u64,
    title_ptr_pos: u64,
    cluster_ptr_pos: u64,
    mime_list_pos: u64,
    main_page: u32,
    layout_page: u32,
    checksum_pos: u64,
}

impl Header {
    fn from_bytes(bytes: &[u8]) -> Result<Header, Error> {
        if bytes.len() < 80 {
            return Err(Error::UnexpectedEndOfBytes);
        }

        let header = Header {
            magic: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            major_version: u16::from_le_bytes([bytes[4], bytes[5]]),
            minor_version: u16::from_le_bytes([bytes[6], bytes[7]]),
            uuid: [
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15], bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21],
                bytes[22], bytes[23],
            ],
            entry_count: u32::from_le_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]),
            cluster_count: u32::from_le_bytes([bytes[28], bytes[29], bytes[30], bytes[31]]),
            url_ptr_pos: u64::from_le_bytes([
                bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38],
                bytes[39],
            ]),
            title_ptr_pos: u64::from_le_bytes([
                bytes[40], bytes[41], bytes[42], bytes[43], bytes[44], bytes[45], bytes[46],
                bytes[47],
            ]),
            cluster_ptr_pos: u64::from_le_bytes([
                bytes[48], bytes[49], bytes[50], bytes[51], bytes[52], bytes[53], bytes[54],
                bytes[55],
            ]),
            mime_list_pos: u64::from_le_bytes([
                bytes[56], bytes[57], bytes[58], bytes[59], bytes[60], bytes[61], bytes[62],
                bytes[63],
            ]),
            main_page: u32::from_le_bytes([bytes[64], bytes[65], bytes[66], bytes[67]]),
            layout_page: u32::from_le_bytes([bytes[68], bytes[69], bytes[70], bytes[71]]),
            checksum_pos: u64::from_le_bytes([
                bytes[72], bytes[73], bytes[74], bytes[75], bytes[76], bytes[77], bytes[78],
                bytes[79],
            ]),
        };

        if header.magic != 72173914 {
            return Err(Error::InvalidMagicNumber);
        }

        Ok(header)
    }
}

#[derive(Debug)]
pub struct MimeTypes(Vec<String>);
impl MimeTypes {
    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut mime_types = Vec::new();

        let mut i = 0;
        while i < bytes.len() {
            let mime_type = read_zero_terminated(&bytes[i..])?;

            if mime_type.is_empty() {
                break;
            }

            i += mime_type.len() + 1;
            mime_types.push(mime_type);
        }

        Ok(Self(mime_types))
    }
}

impl std::ops::Index<u16> for MimeTypes {
    type Output = String;

    fn index(&self, index: u16) -> &Self::Output {
        &self.0[index as usize]
    }
}

#[derive(Debug)]
pub struct UrlPointer(pub u64);

#[derive(Debug)]
pub struct UrlPointerList(Vec<UrlPointer>);

impl std::ops::Index<u32> for UrlPointerList {
    type Output = UrlPointer;

    fn index(&self, index: u32) -> &Self::Output {
        &self.0[index as usize]
    }
}

impl UrlPointerList {
    fn from_bytes(bytes: &[u8], num_urls: u32) -> Result<Self, Error> {
        let mut url_pointers = Vec::new();

        let mut i = 0;
        for _ in 0..num_urls {
            let url_pointer = UrlPointer(u64::from_le_bytes([
                bytes[i],
                bytes[i + 1],
                bytes[i + 2],
                bytes[i + 3],
                bytes[i + 4],
                bytes[i + 5],
                bytes[i + 6],
                bytes[i + 7],
            ]));

            url_pointers.push(url_pointer);
            i += 8;
        }

        Ok(Self(url_pointers))
    }
}

#[derive(Debug)]
#[allow(unused)]
pub struct TitlePointer(u32);

#[derive(Debug)]
pub struct TitlePointerList(Vec<TitlePointer>);

impl std::ops::Index<u32> for TitlePointerList {
    type Output = TitlePointer;

    fn index(&self, index: u32) -> &Self::Output {
        &self.0[index as usize]
    }
}

impl TitlePointerList {
    fn from_bytes(bytes: &[u8], num_titles: u32) -> Result<Self, Error> {
        let mut title_pointers = Vec::new();

        let mut i = 0;
        for _ in 0..num_titles {
            let title_pointer = TitlePointer(u32::from_le_bytes([
                bytes[i],
                bytes[i + 1],
                bytes[i + 2],
                bytes[i + 3],
            ]));

            title_pointers.push(title_pointer);
            i += 4;
        }

        Ok(Self(title_pointers))
    }
}

#[derive(Debug)]
struct ClusterPointer(u64);

#[derive(Debug)]
struct ClusterPointerList(Vec<ClusterPointer>);

impl std::ops::Index<u32> for ClusterPointerList {
    type Output = ClusterPointer;

    fn index(&self, index: u32) -> &Self::Output {
        &self.0[index as usize]
    }
}

impl ClusterPointerList {
    fn from_bytes(bytes: &[u8], num_clusters: u32) -> Result<Self, Error> {
        let mut cluster_pointers = Vec::new();

        let mut i = 0;

        for _ in 0..num_clusters {
            let cluster_pointer = ClusterPointer(u64::from_le_bytes([
                bytes[i],
                bytes[i + 1],
                bytes[i + 2],
                bytes[i + 3],
                bytes[i + 4],
                bytes[i + 5],
                bytes[i + 6],
                bytes[i + 7],
            ]));

            cluster_pointers.push(cluster_pointer);
            i += 8;
        }

        Ok(Self(cluster_pointers))
    }
}

#[derive(Debug)]
pub enum DirEntry {
    Content {
        mime_type: u16,
        parameter_len: u8,
        namespace: char,
        revision: u32,
        cluster_number: u32,
        blob_number: u32,
        url: String,
        title: String,
    },
    Redirect {
        mime_type: u16,
        parameter_len: u8,
        namespace: char,
        revision: u32,
        redirect_index: u32,
        url: String,
        title: String,
    },
}

impl DirEntry {
    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mime_type = u16::from_le_bytes([bytes[0], bytes[1]]);
        let parameter_len = bytes[2];
        let namespace = bytes[3] as char;
        let revision = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        if mime_type == 0xffff {
            let redirect_index = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
            let url = read_zero_terminated(&bytes[12..])?;
            let title = read_zero_terminated(&bytes[12 + url.len() + 1..])?;
            return Ok(Self::Redirect {
                mime_type,
                parameter_len,
                namespace,
                revision,
                redirect_index,
                url,
                title,
            });
        }

        let url = read_zero_terminated(&bytes[16..])?;
        let title = read_zero_terminated(&bytes[16 + url.len() + 1..])?;
        Ok(Self::Content {
            mime_type,
            parameter_len,
            namespace,
            revision,
            cluster_number: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            blob_number: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            url,
            title,
        })
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum OffsetSize {
    U32,
    U64,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum CompressionType {
    Uncompressed,
    Lzma,
    Zstd,
}

enum CompressedReader<'a> {
    Uncompressed(BufReader<std::io::Cursor<&'a [u8]>>),
    Lzma(Box<BufReader<lzma::Reader<BufReader<&'a [u8]>>>>),
    Zstd(BufReader<zstd::Decoder<'a, BufReader<&'a [u8]>>>),
}

impl<'a> std::io::Read for CompressedReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            CompressedReader::Uncompressed(reader) => reader.read(buf),
            CompressedReader::Lzma(reader) => reader.read(buf),
            CompressedReader::Zstd(reader) => reader.read(buf),
        }
    }
}

#[derive(Debug)]
struct ClusterOffset {
    offset: u64,
}

#[derive(Debug)]
pub struct Cluster {
    blob_offsets: Vec<ClusterOffset>,
    blobs: Vec<u8>,
}

impl Cluster {
    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let cluster_info = bytes[0];
        let comp_info = cluster_info & 0x0F;
        let extended = cluster_info & 0x10;

        let size = if extended == 0 {
            OffsetSize::U32
        } else {
            OffsetSize::U64
        };

        let compression_type = match comp_info {
            0 => CompressionType::Uncompressed,
            1 => CompressionType::Uncompressed,
            4 => CompressionType::Lzma,
            5 => CompressionType::Zstd,
            _ => return Err(Error::InvalidCompressionType),
        };

        let mut reader = match compression_type {
            CompressionType::Uncompressed => {
                CompressedReader::Uncompressed(BufReader::new(std::io::Cursor::new(&bytes[1..])))
            }
            CompressionType::Lzma => {
                let decoder = lzma::Reader::from(BufReader::new(&bytes[1..]))?;
                CompressedReader::Lzma(Box::new(BufReader::new(decoder)))
            }
            CompressionType::Zstd => {
                let decoder = zstd::Decoder::new(&bytes[1..])?;
                CompressedReader::Zstd(BufReader::new(decoder))
            }
        }
        .bytes();

        let mut blob_offsets = Vec::new();

        match size {
            OffsetSize::U32 => {
                blob_offsets.push(ClusterOffset {
                    offset: u32::from_le_bytes([
                        reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                        reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                        reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                        reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                    ]) as u64,
                });
            }
            OffsetSize::U64 => {
                blob_offsets.push(ClusterOffset {
                    offset: u64::from_le_bytes([
                        reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                        reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                        reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                        reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                        reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                        reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                        reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                        reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                    ]),
                });
            }
        }

        let num_offsets = match size {
            OffsetSize::U32 => blob_offsets[0].offset as u32 / 4,
            OffsetSize::U64 => blob_offsets[0].offset as u32 / 8,
        };

        for _ in 1..num_offsets {
            match size {
                OffsetSize::U32 => {
                    blob_offsets.push(ClusterOffset {
                        offset: u32::from_le_bytes([
                            reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                            reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                            reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                            reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                        ]) as u64,
                    });
                }
                OffsetSize::U64 => {
                    blob_offsets.push(ClusterOffset {
                        offset: u64::from_le_bytes([
                            reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                            reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                            reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                            reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                            reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                            reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                            reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                            reader.next().ok_or(Error::UnexpectedEndOfBytes)??,
                        ]),
                    });
                }
            }
        }

        let bytes_read = blob_offsets.len()
            * match size {
                OffsetSize::U32 => 4,
                OffsetSize::U64 => 8,
            };

        let missing_bytes = blob_offsets.last().unwrap().offset as usize - bytes_read;

        let mut blobs = Vec::new();

        for _ in 0..missing_bytes {
            blobs.push(reader.next().ok_or(Error::UnexpectedEndOfBytes)??);
        }

        Ok(Self {
            blob_offsets,
            blobs,
        })
    }

    pub fn get_blob(&self, blob_number: usize) -> Option<&[u8]> {
        if self.blob_offsets.is_empty() {
            return None;
        }

        if blob_number >= self.blob_offsets.len() - 1 {
            return None;
        }

        let offset =
            self.blob_offsets[blob_number].offset as usize - self.blob_offsets[0].offset as usize;
        let next_offset = self.blob_offsets[blob_number + 1].offset as usize
            - self.blob_offsets[0].offset as usize;

        Some(&self.blobs[offset..next_offset])
    }
}

pub struct ZimFile {
    header: Header,
    mime_types: MimeTypes,
    url_pointers: UrlPointerList,
    title_pointers: TitlePointerList,
    cluster_pointers: ClusterPointerList,
    mmap: memmap2::Mmap,
}

impl ZimFile {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<ZimFile, Error> {
        let file = File::open(path)?;
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };

        let header = Header::from_bytes(&mmap)?;

        if header.magic != 72173914 {
            return Err(Error::InvalidMagicNumber);
        }

        let mime_types = MimeTypes::from_bytes(&mmap[header.mime_list_pos as usize..])?;
        let url_pointers =
            UrlPointerList::from_bytes(&mmap[header.url_ptr_pos as usize..], header.entry_count)?;

        let title_pointers = TitlePointerList::from_bytes(
            &mmap[header.title_ptr_pos as usize..],
            header.entry_count,
        )?;

        let cluster_pointers = ClusterPointerList::from_bytes(
            &mmap[header.cluster_ptr_pos as usize..],
            header.cluster_count,
        )?;

        if cluster_pointers.0.len() != header.cluster_count as usize {
            return Err(Error::UnexpectedEndOfBytes);
        }

        Ok(Self {
            header,
            mime_types,
            url_pointers,
            title_pointers,
            cluster_pointers,
            mmap,
        })
    }

    pub fn get_dir_entry(&self, index: usize) -> Result<Option<DirEntry>, Error> {
        if index >= self.header.entry_count as usize {
            return Ok(None);
        }

        let pointer = self.url_pointers.0[index].0 as usize;
        Ok(Some(DirEntry::from_bytes(&self.mmap[pointer..])?))
    }

    pub fn get_cluster(&self, index: u32) -> Result<Option<Cluster>, Error> {
        if index >= self.header.cluster_count {
            return Ok(None);
        }

        let pointer = self.cluster_pointers[index].0 as usize;
        Ok(Some(Cluster::from_bytes(&self.mmap[pointer..])?))
    }

    pub fn mime_types(&self) -> &MimeTypes {
        &self.mime_types
    }

    pub fn url_pointers(&self) -> &UrlPointerList {
        &self.url_pointers
    }

    pub fn title_pointers(&self) -> &TitlePointerList {
        &self.title_pointers
    }

    pub fn dir_entries(&self) -> DirEntryIterator<'_> {
        DirEntryIterator::new(&self.mmap, &self.url_pointers)
    }

    pub fn articles(&self) -> Result<ArticleIterator<'_>, Error> {
        ArticleIterator::new(self)
    }

    pub fn images(&self) -> Result<ImageIterator<'_>, Error> {
        ImageIterator::new(self)
    }
}

pub struct DirEntryIterator<'a> {
    mmap: &'a memmap2::Mmap,
    url_pointers: &'a UrlPointerList,
    counter: usize,
}

impl<'a> DirEntryIterator<'a> {
    fn new(mmap: &'a memmap2::Mmap, url_pointers: &'a UrlPointerList) -> Self {
        Self {
            mmap,
            url_pointers,
            counter: 0,
        }
    }
}

impl<'a> Iterator for DirEntryIterator<'a> {
    type Item = Result<DirEntry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.counter >= self.url_pointers.0.len() {
            return None;
        }

        let pointer = self.url_pointers.0[self.counter].0 as usize;
        self.counter += 1;

        Some(DirEntry::from_bytes(&self.mmap[pointer..]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let data_path = Path::new("../../data/test.zim");
        if !data_path.exists() {
            // Skip test if data file is not present
            return;
        }

        let zim = ZimFile::open(data_path).unwrap();

        assert_eq!(zim.header.magic, 72173914);
        assert_eq!(zim.header.major_version, 5);
        assert_eq!(zim.header.minor_version, 0);

        let first_article = zim
            .dir_entries()
            .find(|x| match x {
                Ok(DirEntry::Content { namespace, .. }) => *namespace == 'A',
                _ => false,
            })
            .unwrap()
            .unwrap();

        let url = match first_article {
            DirEntry::Content { url, .. } => url,
            _ => panic!(),
        };

        assert_eq!(url, "African_Americans");
        assert_eq!(zim.dir_entries().count(), 8477);
    }
}
