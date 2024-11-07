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
    inverted_index::{RetrievedWebpage, ShardId},
    schema::text_field::{self, TextField},
    search_ctx,
    webpage::url_ext::UrlExt,
};

use super::{collector::FirstDocCollector, GenericQuery};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct GetHomepageQuery {
    pub url: String,
}

impl GetHomepageQuery {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
        }
    }

    fn query(&self, ctx: &search_ctx::Ctx) -> Option<Box<dyn tantivy::query::Query>> {
        let url = Url::robust_parse(&self.url).ok()?;
        let host = url.normalized_host().map(|s| s.to_string())?;

        let field = ctx
            .tv_searcher
            .schema()
            .get_field(text_field::SiteIfHomepageNoTokenizer.name())
            .unwrap();

        let term = tantivy::Term::from_field_text(field, &host);
        Some(Box::new(tantivy::query::TermQuery::new(
            term,
            tantivy::schema::IndexRecordOption::Basic,
        )) as Box<dyn tantivy::query::Query>)
    }
}

impl GenericQuery for GetHomepageQuery {
    type Collector = FirstDocCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Option<RetrievedWebpage>;
    type Output = Option<RetrievedWebpage>;

    fn tantivy_query(&self, ctx: &search_ctx::Ctx) -> Self::TantivyQuery {
        self.query(ctx).unwrap_or_else(|| {
            Box::new(tantivy::query::EmptyQuery) as Box<dyn tantivy::query::Query>
        })
    }

    fn collector(&self, ctx: &search_ctx::Ctx) -> Self::Collector {
        FirstDocCollector::with_shard_id(ctx.shard_id)
    }

    fn remote_collector(&self) -> Self::Collector {
        FirstDocCollector::without_shard_id()
    }

    fn filter_fruit_shards(
        &self,
        shard_id: ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        match fruit {
            Some(doc_address) if doc_address.shard_id == shard_id => Some(doc_address),
            _ => None,
        }
    }

    fn retrieve(
        &self,
        ctx: &search_ctx::Ctx,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> crate::Result<Self::IntermediateOutput> {
        match fruit {
            Some(doc_address) => {
                let doc: TantivyDocument = ctx.tv_searcher.doc(doc_address.into())?;
                Ok(Some(RetrievedWebpage::from(doc)))
            }
            None => Ok(None),
        }
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        results.into_iter().flatten().next()
    }
}
