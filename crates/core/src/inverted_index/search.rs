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

use super::{DocAddress, InitialSearchResult, InvertedIndex, RetrievedWebpage, WebpagePointer};
use itertools::Itertools;
use tantivy::collector::Count;

use tantivy::schema::Value;

use tantivy::TantivyDocument;
use url::Url;

use crate::ampc::dht::ShardId;
use crate::collector::approx_count::ApproxCount;
use crate::collector::{approx_count, MainCollector};

use crate::generic_query::{self, Collector as _, GenericQuery};
use crate::highlighted::HighlightedFragment;
use crate::numericalfield_reader::NumericalFieldReader;
use crate::query::Query;
use crate::ranking::pipeline::LocalRecallRankingWebpage;
use crate::ranking::SignalComputer;
use crate::schema::{numerical_field, text_field, Field, NumericalFieldEnum};
use crate::search_ctx::Ctx;
use crate::snippet;
use crate::snippet::TextSnippet;
use crate::webgraph::NodeID;
use tantivy::query::ShortCircuitQuery;

use crate::schema::text_field::TextField;
use crate::webpage::url_ext::UrlExt;
use crate::Result;

impl InvertedIndex {
    pub fn search_initial(
        &self,
        query: &Query,
        ctx: &Ctx,
        collector: MainCollector,
    ) -> Result<InitialSearchResult> {
        if query.count_results_exact() {
            let collector = (Count, collector);
            let (count, pointers) = ctx.tv_searcher.search(query, &collector)?;

            return Ok(InitialSearchResult {
                num_websites: approx_count::Count::Exact(count as u64),
                top_websites: pointers,
            });
        }

        let simple_terms = query.simple_terms().to_vec();
        let query: Box<dyn tantivy::query::Query> = Box::new(query.clone());

        if let Some(limit) = collector.top_docs().max_docs().cloned() {
            if limit.segments == 0 {
                return Ok(InitialSearchResult {
                    num_websites: approx_count::Count::Exact(0),
                    top_websites: vec![],
                });
            }

            let docs_per_segment = (limit.total_docs / limit.segments) as u64;
            let query: Box<dyn tantivy::query::Query> =
                Box::new(ShortCircuitQuery::new(query, docs_per_segment));

            let (count, pointers) = ctx.tv_searcher.search(
                &query,
                &(ApproxCount::new(docs_per_segment, simple_terms), collector),
            )?;

            Ok(InitialSearchResult {
                num_websites: count,
                top_websites: pointers,
            })
        } else {
            let (count, pointers) = ctx.tv_searcher.search(&query, &(Count, collector))?;

            Ok(InitialSearchResult {
                num_websites: approx_count::Count::Approximate(count as u64),
                top_websites: pointers,
            })
        }
    }

    pub fn local_search_ctx(&self) -> Ctx {
        let tv_searcher = self.tv_searcher();
        Ctx {
            shard_id: self.shard_id.unwrap_or(ShardId::new(u64::MAX)),
            columnfield_reader: self.columnfield_reader.clone(),
            tv_searcher,
        }
    }

    pub fn tv_searcher(&self) -> tantivy::Searcher {
        self.reader.searcher()
    }

    pub fn retrieve_ranking_websites(
        &self,
        ctx: &Ctx,
        pointers: Vec<WebpagePointer>,
        mut computer: SignalComputer,
        columnfield_reader: &NumericalFieldReader,
    ) -> Result<Vec<LocalRecallRankingWebpage>> {
        let mut top_websites = Vec::new();

        // the ranking webpages needs to be constructed in order
        // of ascending doc_id as they traverse the posting lists from
        // the index to calculate bm25.

        let mut pointers: Vec<_> = pointers.into_iter().enumerate().collect();
        pointers.sort_by(|a, b| {
            a.1.address
                .segment
                .cmp(&b.1.address.segment)
                .then_with(|| a.1.address.doc_id.cmp(&b.1.address.doc_id))
        });

        if pointers.is_empty() {
            return Ok(vec![]);
        }

        let mut prev_segment = None;
        let mut numeric_segment_reader = None;
        for (orig_index, pointer) in pointers {
            let update_segment = match prev_segment {
                Some(prev_segment) if prev_segment != pointer.address.segment => true,
                None => true,
                _ => false,
            };

            let segment_reader = ctx.tv_searcher.segment_reader(pointer.address.segment);
            if update_segment {
                computer.register_segment(&ctx.tv_searcher, segment_reader, columnfield_reader)?;
                numeric_segment_reader = Some(
                    columnfield_reader
                        .borrow_segment(&segment_reader.segment_id())
                        .clone(),
                );
            }

            prev_segment = Some(pointer.address.segment);

            top_websites.push((
                orig_index,
                LocalRecallRankingWebpage::new(
                    pointer,
                    numeric_segment_reader.as_mut().unwrap(),
                    &mut computer,
                ),
            ));
        }

        top_websites.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(top_websites
            .into_iter()
            .map(|(_, website)| website)
            .collect())
    }

    pub fn website_host_node(&self, website: &WebpagePointer) -> Result<Option<NodeID>> {
        let searcher = self.reader.searcher();
        let doc: TantivyDocument = searcher.doc(website.address.into())?;

        let field = self
            .schema()
            .get_field(
                Field::Numerical(NumericalFieldEnum::from(numerical_field::HostNodeID)).name(),
            )
            .unwrap();

        let id = doc.get_first(field).unwrap().as_u64().unwrap();

        if id == u64::MAX {
            Ok(None)
        } else {
            Ok(Some(id.into()))
        }
    }

    pub fn retrieve_websites(
        &self,
        websites: &[WebpagePointer],
        query: &Query,
    ) -> Result<Vec<RetrievedWebpage>> {
        let tv_searcher = self.reader.searcher();
        let mut webpages: Vec<RetrievedWebpage> = websites
            .iter()
            .filter_map(|website| self.retrieve_doc(website.address, &tv_searcher).ok())
            .collect();

        for (url, page) in webpages.iter_mut().filter_map(|page| {
            let url = Url::parse(&page.url).ok()?;
            Some((url, page))
        }) {
            if query.simple_terms().is_empty() {
                let snippet = if let Some(description) = page.description.as_deref() {
                    let snip = description
                        .split_whitespace()
                        .take(self.snippet_config.empty_query_snippet_words)
                        .join(" ");

                    if snip.split_whitespace().count() < self.snippet_config.min_description_words {
                        page.body
                            .split_whitespace()
                            .take(self.snippet_config.empty_query_snippet_words)
                            .join(" ")
                    } else {
                        snip
                    }
                } else {
                    page.body
                        .split_whitespace()
                        .take(self.snippet_config.empty_query_snippet_words)
                        .join(" ")
                };

                page.snippet = TextSnippet {
                    fragments: vec![HighlightedFragment::new_unhighlighted(snippet)],
                };
            } else {
                let min_body_len = if url.is_homepage() {
                    self.snippet_config.min_body_length_homepage
                } else {
                    self.snippet_config.min_body_length
                };

                if page.body.split_whitespace().count() < min_body_len
                    && page
                        .description
                        .as_deref()
                        .unwrap_or_default()
                        .split_whitespace()
                        .count()
                        >= self.snippet_config.min_description_words
                {
                    page.snippet = snippet::generate(
                        query,
                        page.description.as_deref().unwrap_or_default(),
                        &page.region,
                        self.snippet_config.clone(),
                    );
                } else {
                    page.snippet = snippet::generate(
                        query,
                        &page.body,
                        &page.region,
                        self.snippet_config.clone(),
                    );
                }
            }
        }

        Ok(webpages)
    }

    fn retrieve_doc(
        &self,
        doc_address: DocAddress,
        searcher: &tantivy::Searcher,
    ) -> Result<RetrievedWebpage> {
        let doc: TantivyDocument = searcher.doc(doc_address.into())?;
        Ok(RetrievedWebpage::from(doc))
    }

    pub(crate) fn get_site_urls(&self, site: &str, offset: usize, limit: usize) -> Vec<Url> {
        let ctx = self.local_search_ctx();
        let tv_searcher = ctx.tv_searcher;

        let field = tv_searcher
            .schema()
            .get_field(text_field::SiteNoTokenizer.name())
            .unwrap();

        let term = tantivy::Term::from_field_text(field, site);

        let query = tantivy::query::TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);

        match tv_searcher.search(
            &query,
            &tantivy::collector::TopDocs::with_limit(limit).and_offset(offset),
        ) {
            Ok(res) => res
                .into_iter()
                .filter_map(|(_, doc)| {
                    self.retrieve_doc(
                        DocAddress::from_tantivy(
                            doc,
                            self.shard_id.expect("Shard ID should be set for searches"),
                        ),
                        &tv_searcher,
                    )
                    .ok()
                })
                .filter_map(|page| Url::parse(&page.url).ok())
                .collect(),
            Err(_) => vec![],
        }
    }

    pub fn search_initial_generic<Q: GenericQuery>(
        &self,
        query: &Q,
    ) -> Result<<Q::Collector as generic_query::Collector>::Fruit> {
        let ctx = self.local_search_ctx();

        let res = ctx.tv_searcher.search(
            &query.tantivy_query(&ctx),
            &generic_query::collector::TantivyCollector::from(&query.collector(&ctx)),
        )?;

        Ok(res)
    }

    pub fn retrieve_generic<Q: GenericQuery>(
        &self,
        query: &Q,
        fruit: <Q::Collector as generic_query::Collector>::Fruit,
    ) -> Result<Q::IntermediateOutput> {
        let ctx = self.local_search_ctx();
        query.retrieve(&ctx, fruit)
    }

    pub fn search_generic<Q>(&self, query: &Q) -> Result<Q::Output>
    where
        Q: GenericQuery,
        <<Q::Collector as generic_query::Collector>::Child as tantivy::collector::SegmentCollector>::Fruit:
            From<<Q::Collector as generic_query::Collector>::Fruit>,
    {
        let fruit = self.search_initial_generic(query)?;
        let mut fruit = query.remote_collector().merge_fruits(vec![fruit.into()])?;

        if let Some(shard_id) = self.shard_id {
            fruit = query.filter_fruit_shards(shard_id, fruit);
        }

        let res = self.retrieve_generic(query, fruit)?;
        Ok(Q::merge_results(vec![res]))
    }
}
