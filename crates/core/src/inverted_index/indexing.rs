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

use tantivy::index::SegmentId;
use tantivy::merge_policy::NoMergePolicy;

use tantivy::{IndexWriter, SegmentMeta};

use crate::numericalfield_reader::NumericalFieldReader;

use crate::webpage::Webpage;
use crate::Result;
use std::collections::HashSet;
use std::fs;
use std::path::Path;

use super::InvertedIndex;

struct SegmentMergeCandidate {
    num_docs: u32,
    segments: Vec<SegmentMeta>,
}

pub fn merge_tantivy_segments<P: AsRef<Path>>(
    writer: &mut IndexWriter,
    mut segments: Vec<SegmentMeta>,
    base_path: P,
    max_num_segments: u64,
) -> Result<()> {
    assert!(max_num_segments > 0);

    if segments.len() <= max_num_segments as usize {
        return Ok(());
    }

    let num_segments = (max_num_segments + 1) / 2; // ceil(num_segments/2)

    let mut merge_segments = Vec::new();

    for _ in 0..num_segments {
        merge_segments.push(SegmentMergeCandidate {
            num_docs: 0,
            segments: Vec::new(),
        });
    }

    segments.sort_by_key(|b| std::cmp::Reverse(b.num_docs()));

    for segment in segments {
        let best_candidate = merge_segments
            .iter_mut()
            .min_by(|a, b| a.num_docs.cmp(&b.num_docs))
            .unwrap();

        best_candidate.num_docs += segment.num_docs();
        best_candidate.segments.push(segment);
    }

    for merge in merge_segments
        .into_iter()
        .filter(|merge| !merge.segments.is_empty())
    {
        let segment_ids: Vec<_> = merge.segments.iter().map(|segment| segment.id()).collect();
        writer.merge(&segment_ids[..]).wait()?;

        for segment in merge.segments {
            for file in segment.list_files() {
                std::fs::remove_file(base_path.as_ref().join(file)).ok();
            }
        }
    }

    Ok(())
}

impl InvertedIndex {
    pub fn prepare_writer(&mut self) -> Result<()> {
        if self.writer.is_some() {
            return Ok(());
        }

        let writer = self
            .tantivy_index
            .writer_with_num_threads(1, 1_000_000_000)?;

        let merge_policy = NoMergePolicy;
        writer.set_merge_policy(Box::new(merge_policy));

        self.writer = Some(writer);

        Ok(())
    }

    pub fn set_auto_merge_policy(&mut self) {
        let merge_policy = tantivy::merge_policy::LogMergePolicy::default();
        self.writer
            .as_mut()
            .expect("writer has not been prepared")
            .set_merge_policy(Box::new(merge_policy));
    }

    pub fn insert(&self, webpage: &Webpage) -> Result<()> {
        self.writer
            .as_ref()
            .expect("writer has not been prepared")
            .add_document(webpage.as_tantivy(self)?)?;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.prepare_writer()?;
        self.writer
            .as_mut()
            .expect("writer has not been prepared")
            .commit()?;
        self.reader.reload()?;
        self.columnfield_reader = NumericalFieldReader::new(&self.reader.searcher());

        Ok(())
    }

    #[allow(clippy::missing_panics_doc)] // cannot panic as writer is prepared
    pub fn merge_into_max_segments(&mut self, max_num_segments: u64) -> Result<()> {
        self.prepare_writer()?;
        let base_path = Path::new(&self.path);
        let segments: Vec<_> = self
            .tantivy_index
            .load_metas()?
            .segments
            .into_iter()
            .collect();

        merge_tantivy_segments(
            self.writer.as_mut().expect("writer has not been prepared"),
            segments,
            base_path,
            max_num_segments,
        )?;

        Ok(())
    }

    #[allow(clippy::missing_panics_doc)] // should not panic as writer is prepared
    pub fn merge_segments_by_id(&mut self, segments: &[SegmentId]) -> Result<Option<SegmentId>> {
        self.prepare_writer()?;

        if segments.is_empty() {
            return Ok(None);
        }

        let res = self
            .writer
            .as_mut()
            .expect("writer has not been prepared")
            .merge(segments)
            .wait()?;

        Ok(res.map(|seg| seg.id()))
    }

    #[must_use]
    pub fn merge(mut self, mut other: InvertedIndex) -> Self {
        self.prepare_writer().expect("failed to prepare writer");
        other.prepare_writer().expect("failed to prepare writer");

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
            let other_path = Path::new(other_path.as_str());
            other
                .writer
                .take()
                .expect("writer has not been prepared")
                .wait_merging_threads()
                .unwrap();

            let path = self.path.clone();
            let self_path = Path::new(path.as_str());
            self.writer
                .take()
                .expect("writer has not been prepared")
                .wait_merging_threads()
                .unwrap();

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

        let mut res = Self::open(path).expect("failed to open index");

        res.prepare_writer().expect("failed to prepare writer");

        res
    }

    #[allow(clippy::missing_panics_doc)] // cannot panic as writer is prepared
    pub fn delete_segments_by_id(&mut self, segment_ids: &[SegmentId]) -> Result<()> {
        if segment_ids.is_empty() {
            return Ok(());
        }

        let segments: HashSet<_> = segment_ids.iter().copied().collect();
        let to_delete: HashSet<_> = self
            .tantivy_index
            .searchable_segments()?
            .into_iter()
            .filter(|seg| segments.contains(&seg.id()))
            .flat_map(|seg| seg.meta().list_files().into_iter())
            .collect();

        let mut index_meta = self.tantivy_index.load_metas()?;

        index_meta.segments = index_meta
            .segments
            .clone()
            .into_iter()
            .filter(|seg| !segments.contains(&seg.id()))
            .collect();

        let living_files: HashSet<_> = self
            .tantivy_index
            .directory()
            .list_managed_files()
            .difference(&to_delete)
            .cloned()
            .collect();

        self.tantivy_index
            .directory_mut()
            .garbage_collect(|| living_files)?;

        self.tantivy_index.save_metas(&index_meta)?;

        self.reader.reload()?;

        Ok(())
    }

    pub fn stop(mut self) {
        self.writer
            .take()
            .expect("writer has not been prepared")
            .wait_merging_threads()
            .unwrap()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        config::CollectorConfig,
        inverted_index::tests::search,
        query::Query,
        ranking::{LocalRanker, SignalComputer},
        searcher::SearchQuery,
    };

    use super::*;

    #[test]
    fn test_delete_segments() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                TEST
                            </body>
                        </html>
                    "#
                    ),
                    "https://www.example.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                TEST
                            </body>
                        </html>
                    "#
                    ),
                    "https://www.example.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let segments = index.segment_ids();

        assert_eq!(segments.len(), 2);
        index.delete_segments_by_id(&segments).unwrap();

        let segments = index.segment_ids();
        assert!(segments.is_empty());

        let ctx = index.local_search_ctx();
        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "test".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");

        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 0);
    }
}
