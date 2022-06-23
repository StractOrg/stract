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
use crate::{Error, Result, WarcSource};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::time::Duration;

use flate2::read::MultiGzDecoder;

pub(crate) struct WarcFile {
    bytes: Vec<u8>,
}

fn rtrim(s: &mut String) {
    s.truncate(s.trim_end().len());
}

fn decode(raw: Vec<u8>) -> String {
    match String::from_utf8(raw.clone()) {
        Ok(res) => res,
        Err(_) => {
            let encodings = [
                encoding_rs::WINDOWS_1251,
                encoding_rs::GBK,
                encoding_rs::SHIFT_JIS,
                encoding_rs::EUC_JP,
                encoding_rs::EUC_KR,
            ];

            for enc in encodings {
                let (cow, _, had_errors) = enc.decode(&raw[..]);
                if !had_errors {
                    return cow.to_string();
                }
            }

            return String::from_utf8_lossy(&raw).to_string();
        }
    }
}

impl WarcFile {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub fn records(&self) -> RecordIterator<&[u8]> {
        RecordIterator {
            reader: BufReader::new(MultiGzDecoder::new(&self.bytes[..])),
            num_reads: 0,
        }
    }

    pub(crate) fn download(source: WarcSource, path: &str) -> Result<Self> {
        match source {
            WarcSource::S3(config) => WarcFile::download_from_s3(
                path,
                config.name.clone(),
                config.endpoint.clone(),
                config.bucket,
            ),
            WarcSource::HTTP(config) => WarcFile::download_from_http(path, config.base_url),
            WarcSource::Local(config) => WarcFile::load_from_folder(path, &config.folder),
        }
    }

    fn load_from_folder(name: &str, folder: &str) -> Result<Self> {
        let mut bytes = Vec::new();
        let f = File::open(Path::new(folder).join(name))?;

        let mut reader = BufReader::new(f);
        reader.read_to_end(&mut bytes)?;

        Ok(WarcFile::new(bytes))
    }

    fn download_from_http(path: &str, base_url: String) -> Result<Self> {
        let mut url = base_url;
        if !url.ends_with('/') {
            url += "/";
        }
        url += path;

        let client = reqwest::blocking::ClientBuilder::new()
            .tcp_keepalive(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(300))
            .pool_idle_timeout(Duration::from_secs(0))
            .pool_max_idle_per_host(0)
            .connection_verbose(true)
            .build()?;
        let res = client.get(url).send()?;

        let bytes = Vec::from(&res.bytes()?[..]);

        Ok(WarcFile::new(bytes))
    }

    fn download_from_s3(
        _path: &str,
        _region_name: String,
        _region_endpoint: String,
        _bucket: String,
    ) -> Result<Self> {
        todo!();
        // let path = path.to_string();
        // let region = Region::Custom {
        //     name: region_name,
        //     endpoint: region_endpoint,
        // };

        // let client = S3Client::new(region);

        // let obj = client
        //     .get_object(GetObjectRequest {
        //         bucket,
        //         key: path,
        //         ..Default::default()
        //     })
        //     .await?;

        // let mut bytes = Vec::new();
        // obj.body
        //     .ok_or(Error::S3DownloadError)?
        //     .into_async_read()
        //     .read_to_end(&mut bytes)
        //     .await?;

        // Ok(WarcFile::new(bytes))
    }
}

#[derive(Debug)]
struct RawWarcRecord {
    header: BTreeMap<String, String>,
    content: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct WarcRecord {
    pub(crate) request: Request,
    pub(crate) response: Response,
    pub(crate) metadata: Metadata,
}

#[derive(Debug)]
pub(crate) struct Request {
    // WARC-Target-URI
    pub(crate) url: String,
}

impl Request {
    fn from_raw(record: RawWarcRecord) -> Result<Self> {
        Ok(Self {
            url: record
                .header
                .get("WARC-TARGET-URI")
                .ok_or(Error::WarcParse("No target url"))?
                .to_owned(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct Response {
    pub(crate) body: String,
}

impl Response {
    fn from_raw(record: RawWarcRecord) -> Result<Self> {
        let content = decode(record.content);

        let (_header, content) = content
            .split_once("\r\n\r\n")
            .ok_or(Error::WarcParse("Invalid http body"))?;

        Ok(Self {
            body: content.to_string(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct Metadata {
    // fetchTimeMs
    pub(crate) fetch_time_ms: usize,
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
                    let fetch_time_ms = value.parse::<usize>()?;
                    return Ok(Self { fetch_time_ms });
                }
            }
        }

        Err(Error::WarcParse("Failed to parse metadata"))
    }
}

pub(crate) struct RecordIterator<R: Read> {
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
            return Some(Err(Error::WarcParse("Unknown WARC version")));
        }

        let mut header = BTreeMap::<String, String>::new();

        loop {
            let mut line_buf = String::new();

            if let Err(io) = self.reader.read_line(&mut line_buf) {
                return Some(Err(Error::IOError(io)));
            }

            rtrim(&mut line_buf);

            if &line_buf == "\r\n" || line_buf.is_empty() {
                // end of header
                break;
            }
            if let Some(semi) = line_buf.find(':') {
                let value = line_buf.split_off(semi + 1).trim().to_string();
                line_buf.pop(); // remove colon
                let key = line_buf;

                header.insert(key.to_ascii_uppercase(), value);
            } else {
                return Some(Err(Error::WarcParse(
                    "All header lines must contain a colon",
                )));
            }
        }

        let content_len = header.get("CONTENT-LENGTH");
        if content_len.is_none() {
            return Some(Err(Error::WarcParse("Record has no content-length")));
        }

        let content_len = content_len.unwrap().parse::<usize>();
        if content_len.is_err() {
            return Some(Err(Error::WarcParse("Could not parse content length")));
        }

        let content_len = content_len.unwrap();
        let mut content = vec![0; content_len];
        if let Err(io) = self.reader.read_exact(&mut content) {
            return Some(Err(Error::IOError(io)));
        }

        let mut linefeed = [0u8; 4];
        if let Err(io) = self.reader.read_exact(&mut linefeed) {
            return Some(Err(Error::IOError(io)));
        }

        if linefeed != [13, 10, 13, 10] {
            return Some(Err(Error::WarcParse("Invalid record ending")));
        }

        let record = RawWarcRecord { header, content };

        Some(Ok(record))
    }
}

impl<R: Read> Iterator for RecordIterator<R> {
    type Item = Result<WarcRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.num_reads == 0 {
            self.next_raw().unwrap().unwrap(); // skip warc_info
        }
        self.num_reads += 1;

        let items = [self.next_raw(), self.next_raw(), self.next_raw()];

        let mut response = None;
        let mut request = None;
        let mut metadata = None;

        for item in items {
            let item = item?;

            if item.is_err() {
                return Some(Err(item.err().unwrap()));
            }

            let item = item.unwrap();

            if let Some(warc_type) = item.header.get("WARC-TYPE") {
                match warc_type.as_str() {
                    "request" => request = Some(Request::from_raw(item)),
                    "response" => response = Some(Response::from_raw(item)),
                    "metadata" => metadata = Some(Metadata::from_raw(item)),
                    _ => {
                        return Some(Err(Error::WarcParse("Unsupported WARC type")));
                    }
                }
            }
        }

        if request.is_none() || response.is_none() || metadata.is_none() {
            return Some(Err(Error::WarcParse(
                "Request, response or metadata not found",
            )));
        }

        let request = request.unwrap();
        let response = response.unwrap();
        let metadata = metadata.unwrap();

        if request.is_err() || response.is_err() || metadata.is_err() {
            return Some(Err(Error::WarcParse(
                "Request, response or metadata is error",
            )));
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

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
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
}
