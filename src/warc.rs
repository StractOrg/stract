use crate::{Error, Result};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read};

use flate2::read::MultiGzDecoder;

pub(crate) struct WarcFile<R: Read> {
    bytes: BufReader<MultiGzDecoder<R>>,
    num_reads: usize,
}

fn rtrim(s: &mut String) {
    s.truncate(s.trim_end().len());
}

impl<R: Read> WarcFile<R> {
    pub(crate) fn new(bytes: R) -> Self {
        Self {
            bytes: BufReader::new(MultiGzDecoder::new(bytes)),
            num_reads: 0,
        }
    }

    fn next_raw(&mut self) -> Option<Result<RawWarcRecord>> {
        let mut version = String::new();

        if let Err(_io) = self.bytes.read_line(&mut version) {
            return None;
        }

        if version.is_empty() {
            return None;
        }

        rtrim(&mut version);

        if !version.to_uppercase().starts_with("WARC/1.") {
            return Some(Err(Error::WarcParse));
        }

        let mut header = BTreeMap::<String, String>::new();

        loop {
            let mut line_buf = String::new();

            if let Err(io) = self.bytes.read_line(&mut line_buf) {
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
                return Some(Err(Error::WarcParse));
            }
        }

        let content_len = header.get("CONTENT-LENGTH");
        if content_len.is_none() {
            return Some(Err(Error::WarcParse));
        }

        let content_len = content_len.unwrap().parse::<usize>();
        if content_len.is_err() {
            return Some(Err(Error::WarcParse));
        }

        let content_len = content_len.unwrap();
        let mut content = vec![0; content_len];
        if let Err(io) = self.bytes.read_exact(&mut content) {
            return Some(Err(Error::IOError(io)));
        }

        let mut linefeed = [0u8; 4];
        if let Err(io) = self.bytes.read_exact(&mut linefeed) {
            return Some(Err(Error::IOError(io)));
        }

        if linefeed != [13, 10, 13, 10] {
            return Some(Err(Error::WarcParse));
        }

        let record = RawWarcRecord { header, content };

        Some(Ok(record))
    }
}

#[derive(Debug)]
struct RawWarcRecord {
    header: BTreeMap<String, String>,
    content: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct WarcRecord {
    request: Request,
    response: Response,
}

#[derive(Debug)]
struct Request {
    // WARC-Target-URI
    url: String,
}

impl Request {
    fn from_raw(record: RawWarcRecord) -> Result<Self> {
        Ok(Self {
            url: record
                .header
                .get("WARC-TARGET-URI")
                .ok_or(Error::WarcParse)?
                .to_owned(),
        })
    }
}

#[derive(Debug)]
struct Response {
    body: String,
}

impl Response {
    fn from_raw(record: RawWarcRecord) -> Result<Self> {
        Ok(Self {
            body: String::from_utf8_lossy(&record.content).to_string(),
        })
    }
}

#[derive(Debug)]
struct Metadata {
    // fetchTimeMs
    fetch_time_ms: usize,
    languages: Vec<Language>,
}

#[derive(Debug)]
struct Language {
    name: String,
    score: f64,
}

impl<R: BufRead> Iterator for WarcFile<R> {
    type Item = Result<WarcRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.num_reads == 0 {
            self.next_raw().unwrap().unwrap(); // skip warc_info
        }
        self.num_reads += 1;

        let items = [self.next_raw(), self.next_raw(), self.next_raw()];

        let mut response = None;
        let mut request = None;

        for item in items {
            if item.is_none() {
                return None;
            }
            let item = item.unwrap();

            if item.is_err() {
                return Some(Err(Error::WarcParse));
            }
            let item = item.unwrap();

            if let Some(warc_type) = item.header.get("WARC-TYPE") {
                match warc_type.as_str() {
                    "request" => request = Some(Request::from_raw(item)),
                    "response" => response = Some(Response::from_raw(item)),
                    "metadata" => {}
                    _ => {
                        return Some(Err(Error::WarcParse));
                    }
                }
            }
        }

        if request.is_none() || response.is_none() {
            return Some(Err(Error::WarcParse));
        }

        let request = request.unwrap();
        let response = response.unwrap();

        if request.is_err() || response.is_err() {
            return Some(Err(Error::WarcParse));
        }

        let request = request.unwrap();
        let response = response.unwrap();

        Some(Ok(WarcRecord { request, response }))
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
                cONTENT-lENGTH: 16\r\n\
                \r\n\
                body of response\r\n\
                \r\n\
                warc/1.0\r\n\
                warc-tYPE: metadata\r\n\
                cONTENT-lENGTH: 16\r\n\
                \r\n\
                body of metadata\r\n\
                \r\n";
        let mut e = GzEncoder::new(Vec::new(), Compression::default());
        e.write_all(raw).unwrap();
        let compressed = e.finish().unwrap();

        let records: Vec<WarcRecord> = WarcFile::new(&compressed[..])
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(records.len(), 1);
        assert_eq!(&records[0].request.url, "http://0575ls.cn/news-52300.htm");
        assert_eq!(&records[0].response.body, "body of response");
    }
}
