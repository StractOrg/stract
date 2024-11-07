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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

const SITE_URL_BATCH_SIZE: usize = 100;

use url::Url;

use crate::{
    distributed::{
        sonic::replication::ReusableShardedClient, streaming_response::StreamingResponse,
    },
    entrypoint::search_server,
    generic_query::GetSiteUrlsQuery,
    searcher::{DistributedSearcher, SearchClient},
    Result,
};

pub struct SiteUrlStream {
    site: String,
    offset: usize,
    searcher: DistributedSearcher,
}

impl SiteUrlStream {
    pub fn new(site: String, conn: ReusableShardedClient<search_server::SearchService>) -> Self {
        Self {
            site,
            offset: 0,
            searcher: DistributedSearcher::from_client(conn),
        }
    }
}

impl StreamingResponse for SiteUrlStream {
    type Item = Url;

    async fn next_batch(&mut self) -> Result<Vec<Self::Item>> {
        self.searcher
            .search_generic(GetSiteUrlsQuery {
                site: self.site.clone(),
                offset: Some(self.offset as u64),
                limit: SITE_URL_BATCH_SIZE as u64,
            })
            .await
            .map(|res| res.urls)
    }
}
