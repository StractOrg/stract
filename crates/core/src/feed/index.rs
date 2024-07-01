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

use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{
    inverted_index::merge_tantivy_segments,
    tokenizer::{Tokenizer, UrlTokenizer},
};
use anyhow::Result;
use hashbrown::HashSet;
use tantivy::{
    merge_policy::NoMergePolicy,
    query::{PhraseQuery, TermQuery},
    schema::{IndexRecordOption, TextFieldIndexing, TextOptions, Value},
    tokenizer::Tokenizer as TantivyTokenizer,
};
use url::Url;

use super::{Feed, FeedKind};

pub struct FeedIndex {
    tantivy_index: tantivy::Index,
    writer: tantivy::IndexWriter,
    reader: tantivy::IndexReader,
    schema: tantivy::schema::Schema,
    pub path: PathBuf,
}

impl FeedIndex {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let url_tokenizer = Tokenizer::Url(UrlTokenizer);
        let kind_tokenizer = Tokenizer::default();

        let mut builder = tantivy::schema::Schema::builder();

        builder.add_text_field(
            "url",
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer(url_tokenizer.as_str())
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                )
                .set_stored(),
        );
        builder.add_text_field(
            "kind",
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer(kind_tokenizer.as_str())
                        .set_index_option(IndexRecordOption::Basic),
                )
                .set_stored(),
        );

        let schema = builder.build();

        let tv_index = if !path.as_ref().exists() {
            fs::create_dir_all(path.as_ref())?;
            tantivy::Index::create_in_dir(path.as_ref(), schema.clone())?
        } else {
            tantivy::Index::open_in_dir(path.as_ref())?
        };

        tv_index
            .tokenizers()
            .register(url_tokenizer.as_str(), url_tokenizer);

        tv_index
            .tokenizers()
            .register(kind_tokenizer.as_str(), kind_tokenizer);

        let writer = tv_index.writer(50_000_000)?;

        let merge_policy = NoMergePolicy;
        writer.set_merge_policy(Box::new(merge_policy));

        let reader = tv_index.reader()?;

        Ok(Self {
            tantivy_index: tv_index,
            writer,
            reader,
            schema,
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn insert(&mut self, feed: &Feed) -> Result<()> {
        let mut doc = tantivy::TantivyDocument::default();

        doc.add_text(self.schema.get_field("url")?, feed.url.as_str());
        doc.add_text(
            self.schema.get_field("kind")?,
            &serde_json::to_string(&feed.kind)?,
        );

        self.writer.add_document(doc)?;

        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        self.reader.reload()?;

        Ok(())
    }

    fn terms(&self, query: &str) -> Vec<tantivy::Term> {
        let mut tokenizer = UrlTokenizer;
        let mut res = Vec::new();
        let tv_field = self.schema.get_field("url").unwrap();

        let mut stream = tokenizer.token_stream(query);

        while let Some(token) = stream.next() {
            res.push(tantivy::Term::from_field_text(tv_field, &token.text));
        }

        res
    }

    pub fn search(&self, query: &str) -> Result<Vec<Feed>> {
        let searcher = self.reader.searcher();

        let terms = self.terms(query);
        if terms.is_empty() {
            return Ok(Vec::new());
        }

        let query = if terms.len() == 1 {
            let term = terms.first().unwrap().clone();
            Box::new(TermQuery::new(
                term,
                tantivy::schema::IndexRecordOption::Basic,
            )) as Box<dyn tantivy::query::Query>
        } else {
            Box::new(PhraseQuery::new(terms)) as Box<dyn tantivy::query::Query>
        };

        let docs = searcher.search(&query, &tantivy::collector::DocSetCollector)?;

        let mut res = Vec::new();

        for address in docs {
            let doc: tantivy::TantivyDocument = searcher.doc(address)?;

            let url = doc
                .get_first(self.schema.get_field("url")?)
                .unwrap()
                .as_str()
                .unwrap();

            let url = Url::parse(url)?;

            let kind = doc
                .get_first(self.schema.get_field("kind")?)
                .unwrap()
                .as_str()
                .unwrap();

            let kind: FeedKind = serde_json::from_str(kind)?;

            res.push(Feed { url, kind });
        }

        Ok(res)
    }

    pub fn merge_into_max_segments(&mut self, max_num_segments: u64) -> Result<()> {
        let segments: Vec<_> = self
            .tantivy_index
            .load_metas()?
            .segments
            .into_iter()
            .collect();

        merge_tantivy_segments(&mut self.writer, segments, &self.path, max_num_segments)?;

        Ok(())
    }

    pub fn merge(mut self, mut other: Self) -> Self {
        let path = self.path.clone();

        {
            other.commit().expect("failed to commit index");
            self.commit().expect("failed to commit index");

            let other_meta = other
                .tantivy_index
                .load_metas()
                .expect("failed to load tantivy metadata for index");

            let mut meta = self
                .tantivy_index
                .load_metas()
                .expect("failed to load tantivy metadata for index");

            let other_path = other.path.clone();
            let other_path = other_path.as_path();
            other.writer.wait_merging_threads().unwrap();

            let path = self.path.clone();
            let self_path = path.as_path();
            self.writer.wait_merging_threads().unwrap();

            let ids: HashSet<_> = meta.segments.iter().map(|segment| segment.id()).collect();

            for segment in other_meta.segments {
                if ids.contains(&segment.id()) {
                    continue;
                }

                // TODO: handle case where current index has segment with same name
                for file in segment.list_files() {
                    let p = other_path.join(&file);
                    if p.exists() {
                        fs::rename(p, self_path.join(&file)).unwrap();
                    }
                }
                meta.segments.push(segment);
            }

            meta.segments
                .sort_by_key(|a| std::cmp::Reverse(a.max_doc()));

            fs::remove_dir_all(other_path).ok();

            let self_path = Path::new(&path);

            std::fs::write(
                self_path.join("meta.json"),
                serde_json::to_string_pretty(&meta).unwrap(),
            )
            .unwrap();
        }

        Self::open(path).expect("failed to open index")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_index() {
        let mut index = FeedIndex::open(crate::gen_temp_path()).unwrap();

        let a = Feed {
            url: Url::parse("https://a.com/feed.xml").unwrap(),
            kind: FeedKind::Atom,
        };
        index.insert(&a).unwrap();

        let b = Feed {
            url: Url::parse("https://b.com/another/feed").unwrap(),
            kind: FeedKind::Rss,
        };
        index.insert(&b).unwrap();

        index.commit().unwrap();

        let res = index.search("a.com").unwrap();
        assert_eq!(res, vec![a]);

        let res = index.search("b.com").unwrap();
        assert_eq!(res, vec![b]);
    }
}
