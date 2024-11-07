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

use tantivy::TantivyDocument;
use url::Url;

use crate::{
    inverted_index::RetrievedWebpage,
    schema::text_field::{self, TextField},
    search_ctx,
    webpage::url_ext::UrlExt,
};

use super::{collector::TopDocsCollector, GenericQuery};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct GetSiteUrlsQuery {
    pub site: String,
    pub limit: u64,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct GetSiteUrlsResponse {
    #[bincode(with_serde)]
    pub urls: Vec<Url>,
}

impl GenericQuery for GetSiteUrlsQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = GetSiteUrlsResponse;
    type Output = GetSiteUrlsResponse;

    fn tantivy_query(&self, ctx: &search_ctx::Ctx) -> Self::TantivyQuery {
        let field = ctx
            .tv_searcher
            .schema()
            .get_field(text_field::SiteNoTokenizer.name())
            .unwrap();
        let term = tantivy::Term::from_field_text(field, &self.site);
        Box::new(tantivy::query::TermQuery::new(
            term,
            tantivy::schema::IndexRecordOption::Basic,
        ))
    }

    fn collector(&self, _: &search_ctx::Ctx) -> Self::Collector {
        Self::Collector::new()
            .with_limit(self.limit as usize)
            .with_offset(self.offset.unwrap_or(0) as usize)
            .disable_offset()
    }

    fn remote_collector(&self) -> Self::Collector {
        Self::Collector::new()
            .with_limit(self.limit as usize)
            .with_offset(self.offset.unwrap_or(0) as usize)
            .enable_offset()
    }

    fn filter_fruit_shards(
        &self,
        shard_id: crate::ampc::dht::ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, addr)| addr.shard_id == shard_id)
            .collect()
    }

    fn retrieve(
        &self,
        ctx: &search_ctx::Ctx,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> crate::Result<Self::IntermediateOutput> {
        let mut urls = Vec::new();

        for (_, addr) in fruit {
            let doc: TantivyDocument = ctx.tv_searcher.doc(addr.into())?;
            let doc = RetrievedWebpage::from(doc);
            urls.push(Url::robust_parse(&doc.url)?);
        }

        Ok(GetSiteUrlsResponse { urls })
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut urls: Vec<_> = results.into_iter().flat_map(|r| r.urls).collect();

        urls.sort_by_key(|url| url.to_string());
        urls.dedup();

        GetSiteUrlsResponse { urls }
    }
}
