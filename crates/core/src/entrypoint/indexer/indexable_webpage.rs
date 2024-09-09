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

use crate::crawler::CrawlDatum;
use crate::warc::WarcRecord;

#[derive(bincode::Encode, bincode::Decode)]
pub struct IndexableWebpage {
    pub url: String,
    pub body: String,
    pub fetch_time_ms: u64,
}

impl From<CrawlDatum> for IndexableWebpage {
    fn from(datum: CrawlDatum) -> Self {
        Self {
            url: datum.url.to_string(),
            body: datum.body,
            fetch_time_ms: datum.fetch_time_ms,
        }
    }
}

impl From<WarcRecord> for IndexableWebpage {
    fn from(record: WarcRecord) -> Self {
        Self {
            url: record.request.url,
            body: record.response.body,
            fetch_time_ms: record.metadata.fetch_time_ms,
        }
    }
}
