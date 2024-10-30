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

use tantivy::query::BooleanQuery;

use crate::{
    ampc::dht::ShardId,
    webgraph::{
        schema::{FromUrl, ToUrl},
        searcher::Searcher,
        Edge, EdgeLimit, Node, Query,
    },
    Result,
};

use super::{collector::TopDocsCollector, fetch_edges, raw::PhraseOrTermQuery};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct FullLinksBetweenQuery {
    from: Node,
    to: Node,
    limit: EdgeLimit,
}

impl FullLinksBetweenQuery {
    pub fn new(from: Node, to: Node) -> Self {
        Self {
            from,
            to,
            limit: EdgeLimit::Unlimited,
        }
    }

    pub fn with_limit(mut self, limit: EdgeLimit) -> Self {
        self.limit = limit;
        self
    }
}

impl Query for FullLinksBetweenQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = BooleanQuery;
    type IntermediateOutput = Vec<Edge>;
    type Output = Vec<Edge>;

    fn tantivy_query(&self, _: &Searcher) -> Self::TantivyQuery {
        let from_query = PhraseOrTermQuery::new(self.from.as_str().to_string(), FromUrl);
        let to_query = PhraseOrTermQuery::new(self.to.as_str().to_string(), ToUrl);

        BooleanQuery::intersection(vec![Box::new(from_query), Box::new(to_query)])
    }

    fn collector(&self, searcher: &Searcher) -> Self::Collector {
        TopDocsCollector::from(self.limit)
            .with_shard_id(searcher.shard())
            .disable_offset()
    }

    fn remote_collector(&self) -> Self::Collector {
        TopDocsCollector::from(self.limit).enable_offset()
    }

    fn filter_fruit_shards(
        &self,
        shard_id: ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc_address)| doc_address.shard_id == shard_id)
            .collect()
    }

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let docs: Vec<_> = fruit.into_iter().map(|(_, doc)| doc).collect();
        let edges = fetch_edges(searcher, docs)?;
        Ok(edges)
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut edges: Vec<_> = results.into_iter().flatten().collect();
        edges.sort_by(|a, b| b.sort_score.total_cmp(&a.sort_score));
        edges
    }
}

#[cfg(test)]
mod tests {
    use crate::{webgraph::Webgraph, webpage::RelFlags};

    use super::*;

    #[test]
    fn test_between() {
        let from = Node::from("https://example.com");
        let to = Node::from("https://example.org");

        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut graph = Webgraph::builder(&temp_dir, 0u64.into()).open().unwrap();

        graph
            .insert(Edge {
                from: from.clone(),
                to: to.clone(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();
        graph.commit().unwrap();

        let res = graph
            .search(&FullLinksBetweenQuery::new(from.clone(), to.clone()))
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].from, from);
        assert_eq!(res[0].to, to);
    }
}
