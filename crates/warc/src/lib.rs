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

// use crate::{Error, Result};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read, Seek, Write};
use std::path::Path;
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;

use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
#[cfg(test)]
use proptest_derive::Arbitrary;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("Failed to parse WARC file")]
    Parse(&'static str),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct WarcFile {
    bytes: Vec<u8>,
}

fn rtrim(s: &mut String) {
    s.truncate(s.trim_end().len());
}

fn decode(raw: &[u8]) -> String {
    if let Ok(res) = String::from_utf8(raw.to_owned()) {
        res
    } else {
        let encodings = [
            encoding_rs::WINDOWS_1251,
            encoding_rs::GBK,
            encoding_rs::SHIFT_JIS,
            encoding_rs::EUC_JP,
            encoding_rs::EUC_KR,
        ];

        for enc in encodings {
            let (cow, _, had_errors) = enc.decode(raw);
            if !had_errors {
                return cow.to_string();
            }
        }

        String::from_utf8_lossy(raw).to_string()
    }
}

impl WarcFile {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;

        Ok(Self::new(bytes))
    }

    pub fn records(&self) -> RecordIterator<&[u8]> {
        RecordIterator {
            reader: BufReader::new(MultiGzDecoder::new(&self.bytes[..])),
            num_reads: 0,
        }
    }
}

#[derive(Debug)]
struct RawWarcRecord {
    header: BTreeMap<String, String>,
    content: Vec<u8>,
}

#[derive(Debug)]
#[cfg_attr(test, derive(Clone, Arbitrary, PartialEq))]
pub struct WarcRecord {
    pub request: Request,
    pub response: Response,
    pub metadata: Metadata,
}

#[derive(Debug)]
#[cfg_attr(test, derive(Clone, Arbitrary, PartialEq))]
pub struct Request {
    // WARC-Target-URI
    pub url: String,
}

impl Request {
    fn from_raw(record: RawWarcRecord) -> Result<Self> {
        Ok(Self {
            url: record
                .header
                .get("WARC-TARGET-URI")
                .ok_or(Error::Parse("No target url"))?
                .to_owned(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(test, derive(Arbitrary))]
pub enum PayloadType {
    Html,
    Pdf,
    Rss,
    Atom,
}

impl FromStr for PayloadType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "application/html" => Ok(Self::Html),
            "text/html" => Ok(Self::Html),
            "application/pdf" => Ok(Self::Pdf),
            "application/rss" => Ok(Self::Rss),
            "application/atom" => Ok(Self::Atom),
            _ => Err(Error::Parse("Unknown payload type")),
        }
    }
}

impl ToString for PayloadType {
    fn to_string(&self) -> String {
        match self {
            Self::Html => "text/html".to_string(),
            Self::Pdf => "application/pdf".to_string(),
            Self::Rss => "application/rss".to_string(),
            Self::Atom => "application/atom".to_string(),
        }
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(Clone, Arbitrary, PartialEq))]
pub struct Response {
    pub body: String,
    pub payload_type: Option<PayloadType>,
}

impl Response {
    fn from_raw(record: RawWarcRecord) -> Result<Self> {
        let content = decode(&record.content[..]);

        let (_header, content) = content
            .split_once("\r\n\r\n")
            .ok_or(Error::Parse("Invalid http body"))?;

        Ok(Self {
            body: content.to_string(),
            payload_type: record
                .header
                .get("WARC-IDENTIFIED-PAYLOAD-TYPE")
                .and_then(|p| PayloadType::from_str(p).ok()),
        })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(Clone, Arbitrary, PartialEq))]
pub struct Metadata {
    // fetchTimeMs
    pub fetch_time_ms: usize,
}

impl Metadata {
    fn from_raw(record: RawWarcRecord) -> Result<Self> {
        let r = BufReader::new(&record.content[..]);

        for line in r.lines() {
            let mut line = line?;
            if let Some(semi) = line.find(':') {
                let value = line.split_off(semi + 1).trim().to_string();
                line.pop(); // remove colon
                let key = line;
                if key == "fetchTimeMs" {
                    let fetch_time_ms = value
                        .parse::<usize>()
                        .map_err(|_| Error::Parse("failed to parse 'fetchTimeMs'"))?;
                    return Ok(Self { fetch_time_ms });
                }
            }
        }

        Err(Error::Parse("Failed to parse metadata"))
    }
}

pub struct RecordIterator<R: Read> {
    reader: BufReader<MultiGzDecoder<R>>,
    num_reads: usize,
}

impl<R: Read> RecordIterator<R> {
    fn next_raw(&mut self) -> Option<Result<RawWarcRecord>> {
        let mut version = String::new();

        if let Err(_io) = self.reader.read_line(&mut version) {
            return None;
        }

        if version.is_empty() {
            return None;
        }

        rtrim(&mut version);

        if !version.to_uppercase().starts_with("WARC/1.") {
            return Some(Err(Error::Parse("Unknown WARC version")));
        }

        let mut header = BTreeMap::<String, String>::new();

        loop {
            let mut line_buf = String::new();
            if let Err(io) = self.reader.read_line(&mut line_buf) {
                return Some(Err(io.into()));
            }

            if &line_buf == "\r\n" || line_buf.is_empty() {
                // end of header
                break;
            }
            if let Some(semi) = line_buf.find(':') {
                let mut value = line_buf.split_off(semi + 1).to_string();

                if let Some(stripped) = value.strip_suffix("\r\n") {
                    value = stripped.to_string();
                } else if let Some(stripped) = value.strip_suffix('\n') {
                    value = stripped.to_string();
                }

                if let Some(stripped) = value.strip_prefix(' ') {
                    value = stripped.to_string();
                }

                line_buf.pop(); // remove colon
                let key = line_buf;

                header.insert(key.to_ascii_uppercase(), value);
            } else {
                return Some(Err(Error::Parse("All header lines must contain a colon")));
            }
        }

        let content_len = header.get("CONTENT-LENGTH");
        if content_len.is_none() {
            return Some(Err(Error::Parse("Record has no content-length")));
        }

        let content_len = content_len.unwrap().parse::<usize>();
        if content_len.is_err() {
            return Some(Err(Error::Parse("Could not parse content length")));
        }

        let content_len = content_len.unwrap();
        let mut content = vec![0; content_len];
        if let Err(io) = self.reader.read_exact(&mut content) {
            return Some(Err(io.into()));
        }

        let mut linefeed = [0u8; 4];
        if let Err(io) = self.reader.read_exact(&mut linefeed) {
            return Some(Err(io.into()));
        }

        if linefeed != [13, 10, 13, 10] {
            return Some(Err(Error::Parse("Invalid record ending")));
        }

        let record = RawWarcRecord { header, content };

        Some(Ok(record))
    }
}

impl<R: Read> Iterator for RecordIterator<R> {
    type Item = Result<WarcRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.num_reads == 0 {
            self.next_raw()?.ok()?; // skip warc_info
        }
        self.num_reads += 1;

        let mut request = None;

        while let Some(item) = self.next_raw() {
            if item.is_err() {
                return Some(Err(item.err().unwrap()));
            }

            let item = item.unwrap();

            if let Some(warc_type) = item.header.get("WARC-TYPE") {
                if warc_type.as_str() == "request" {
                    request = Some(Request::from_raw(item));
                    break;
                }
            }
        }

        let request = request?;

        // next 2 should be response and metadata
        let response = self.next_raw()?;

        if response.is_err() {
            return Some(Err(response.err().unwrap()));
        }

        let response = response.unwrap();

        match response.header.get("WARC-TYPE") {
            Some(warc_type) => {
                if warc_type.as_str() != "response" {
                    return Some(Err(Error::Parse("Expected response, got something else")));
                }
            }
            None => {
                return Some(Err(Error::Parse("Expected response, got something else")));
            }
        }

        let response = Response::from_raw(response);

        let metadata = self.next_raw()?;

        if metadata.is_err() {
            return Some(Err(metadata.err().unwrap()));
        }

        let metadata = metadata.unwrap();

        match metadata.header.get("WARC-TYPE") {
            Some(warc_type) => {
                if warc_type.as_str() != "metadata" {
                    return Some(Err(Error::Parse("Expected metadata, got something else")));
                }
            }
            None => {
                return Some(Err(Error::Parse("Expected metadata, got something else")));
            }
        }

        let metadata = Metadata::from_raw(metadata);

        if request.is_err() || response.is_err() || metadata.is_err() {
            return Some(Err(Error::Parse("Request, response or metadata is error")));
        }

        let request = request.unwrap();
        let response = response.unwrap();
        let metadata = metadata.unwrap();

        Some(Ok(WarcRecord {
            request,
            response,
            metadata,
        }))
    }
}

pub struct WarcWriter {
    num_writes: usize,
    writer: GzEncoder<Vec<u8>>,
}

impl WarcWriter {
    pub fn new() -> Self {
        let mut writer = GzEncoder::new(Default::default(), Compression::best());

        writer.write_all("WARC/1.0\r\n".as_bytes()).unwrap();
        writer
            .write_all("WARC-Type: warcinfo\r\n".as_bytes())
            .unwrap();

        let date = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let content = format!("ISPARTOF: crawl[{}]", date);
        let content_len = content.len();

        writer
            .write_all(format!("Content-Length: {content_len}\r\n").as_bytes())
            .unwrap();
        writer.write_all("\r\n".as_bytes()).unwrap();
        writer.write_all(content.as_bytes()).unwrap();
        writer.write_all("\r\n\r\n".as_bytes()).unwrap();

        writer.flush().unwrap();

        Self {
            num_writes: 0,
            writer,
        }
    }

    pub fn write(&mut self, record: &WarcRecord) -> Result<()> {
        self.writer.write_all("WARC/1.0\r\n".as_bytes())?;

        self.writer.write_all("WARC-Type: request\r\n".as_bytes())?;
        self.writer
            .write_all(format!("WARC-Target-URI: {}\r\n", record.request.url).as_bytes())?;
        self.writer.write_all("Content-Length: 0\r\n".as_bytes())?;
        self.writer.write_all("\r\n".as_bytes())?;
        self.writer.write_all("\r\n\r\n".as_bytes())?;

        self.writer.write_all("WARC/1.0\r\n".as_bytes())?;
        self.writer
            .write_all("WARC-Type: response\r\n".as_bytes())?;

        if let Some(payload_type) = &record.response.payload_type {
            self.writer.write_all(
                format!(
                    "WARC-Identified-Payload-Type: {}\r\n",
                    payload_type.to_string()
                )
                .as_bytes(),
            )?;
        }

        let body = record.response.body.as_bytes();
        let content_len = body.len() + 4; // +4 is for the \r\n\r\n between http header and body
        self.writer
            .write_all(format!("Content-Length: {content_len}\r\n").as_bytes())?;

        self.writer.write_all("\r\n".as_bytes())?;
        // write the http-header here if we want to in the future
        self.writer.write_all("\r\n\r\n".as_bytes())?;

        self.writer.write_all(body)?;
        self.writer.write_all("\r\n\r\n".as_bytes())?;

        self.writer.write_all("WARC/1.0\r\n".as_bytes())?;
        self.writer
            .write_all("WARC-Type: metadata\r\n".as_bytes())?;

        let body = format!("fetchTimeMs: {}", record.metadata.fetch_time_ms);
        let content_len = body.len();

        self.writer
            .write_all(format!("Content-Length: {content_len}\r\n").as_bytes())?;
        self.writer.write_all("\r\n".as_bytes())?;
        self.writer.write_all(body.as_bytes())?;
        self.writer.write_all("\r\n\r\n".as_bytes())?;

        self.writer.flush().unwrap();

        self.num_writes += 1;

        Ok(())
    }

    pub fn finish(self) -> Result<Vec<u8>> {
        Ok(self.writer.finish()?)
    }

    pub fn num_bytes(&self) -> usize {
        self.writer.get_ref().len()
    }

    pub fn num_writes(&self) -> usize {
        self.num_writes
    }
}

impl Default for WarcWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use proptest::prelude::*;
    use std::io::Write;

    #[test]
    fn it_works() {
        let raw = b"\
                warc/1.0\r\n\
                warc-tYPE: WARCINFO\r\n\
                cONTENT-lENGTH: 25\r\n\
                \r\n\
                ISpARToF: cc-main-2022-05\r\n\
                \r\n\
                warc/1.0\r\n\
                WARC-Target-URI: http://0575ls.cn/news-52300.htm\r\n\
                warc-tYPE: request\r\n\
                cONTENT-lENGTH: 15\r\n\
                \r\n\
                body of request\r\n\
                \r\n\
                warc/1.0\r\n\
                warc-tYPE: response\r\n\
                cONTENT-lENGTH: 29\r\n\
                \r\n\
                http-body\r\n\
                \r\n\
                body of response\r\n\
                \r\n\
                warc/1.0\r\n\
                warc-tYPE: metadata\r\n\
                cONTENT-lENGTH: 16\r\n\
                \r\n\
                fetchTimeMs: 937\r\n\
                \r\n";
        let mut e = GzEncoder::new(Vec::new(), Compression::default());
        e.write_all(raw).unwrap();
        let compressed = e.finish().unwrap();

        let records: Vec<WarcRecord> = WarcFile::new(compressed)
            .records()
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(records.len(), 1);
        assert_eq!(&records[0].request.url, "http://0575ls.cn/news-52300.htm");
        assert_eq!(&records[0].response.body, "body of response");
        assert_eq!(records[0].metadata.fetch_time_ms, 937);
    }

    #[test]
    fn writer_reader_invariant() {
        let mut writer = WarcWriter::new();
        let record1 = WarcRecord {
            request: Request {
                url: "https://a.com".to_string(),
            },
            response: Response {
                body: "body of a".to_string(),
                payload_type: Some(PayloadType::Html),
            },
            metadata: Metadata {
                fetch_time_ms: 1337,
            },
        };
        writer.write(&record1).unwrap();

        let record2 = WarcRecord {
            request: Request {
                url: "https://b.com".to_string(),
            },
            response: Response {
                body: "body of b".to_string(),
                payload_type: None,
            },
            metadata: Metadata {
                fetch_time_ms: 4242,
            },
        };
        writer.write(&record2).unwrap();

        let compressed = writer.finish().unwrap();

        let records: Vec<WarcRecord> = WarcFile::new(compressed)
            .records()
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(records.len(), 2);
        assert_eq!(&records[0].request.url, "https://a.com");
        assert_eq!(&records[0].response.body, "body of a");
        assert_eq!(records[0].metadata.fetch_time_ms, 1337);

        assert_eq!(&records[1].request.url, "https://b.com");
        assert_eq!(&records[1].response.body, "body of b");
        assert_eq!(records[1].metadata.fetch_time_ms, 4242);
    }

    #[test]
    fn writer_utf8() {
        let utf8 = "🦀";

        let mut writer = WarcWriter::new();
        let record = WarcRecord {
            request: Request {
                url: "https://a.com".to_string(),
            },
            response: Response {
                body: utf8.to_string(),
                payload_type: Some(PayloadType::Html),
            },
            metadata: Metadata { fetch_time_ms: 0 },
        };
        writer.write(&record).unwrap();

        let compressed = writer.finish().unwrap();
        let records: Vec<WarcRecord> = WarcFile::new(compressed)
            .records()
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(records.len(), 1);
        assert_eq!(&records[0].request.url, "https://a.com");
        assert_eq!(&records[0].response.body, utf8);
        assert_eq!(records[0].metadata.fetch_time_ms, 0);
    }

    #[test]
    fn writer_tabs() {
        let body = r#"
               this
            is
            a
            test             "#;
        let mut writer = WarcWriter::new();
        let record = WarcRecord {
            request: Request {
                url: "https://a.com".to_string(),
            },
            response: Response {
                body: body.to_string(),
                payload_type: Some(PayloadType::Html),
            },
            metadata: Metadata { fetch_time_ms: 0 },
        };
        writer.write(&record).unwrap();

        let compressed = writer.finish().unwrap();
        let records: Vec<WarcRecord> = WarcFile::new(compressed)
            .records()
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(records.len(), 1);
        assert_eq!(&records[0].request.url, "https://a.com");
        assert_eq!(&records[0].response.body, body);
        assert_eq!(records[0].metadata.fetch_time_ms, 0);
    }

    proptest! {
        #[test]
        fn write_read_invariant_prop(records: Vec<WarcRecord>) {
            let mut writer = WarcWriter::new();
            for record in records.iter() {
                writer.write(record).unwrap();
            }
            let compressed = writer.finish().unwrap();

            let read_records: Vec<WarcRecord> = WarcFile::new(compressed)
                .records()
                .map(|res| res.unwrap())
                .collect();

            prop_assert_eq!(records, read_records);
        }
    }
}
