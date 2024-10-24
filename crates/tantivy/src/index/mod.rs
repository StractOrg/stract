//! The `index` module in Tantivy contains core components to read and write indexes.
//!
//! It contains `Index` and `Segment`, where a `Index` consists of one or more `Segment`s.

mod index;
mod index_meta;
mod inverted_index_reader;
mod segment;
mod segment_component;
mod segment_id;
mod segment_reader;

use std::path::Path;

use crate::IndexWriter;

pub use self::index::{Index, IndexBuilder};
pub(crate) use self::index_meta::SegmentMetaInventory;
pub use self::index_meta::{IndexMeta, IndexSettings, IndexSortByField, Order, SegmentMeta};
pub use self::inverted_index_reader::InvertedIndexReader;
pub use self::segment::Segment;
pub use self::segment_component::SegmentComponent;
pub use self::segment_id::SegmentId;
pub use self::segment_reader::{FieldMetadata, SegmentReader};

struct SegmentMergeCandidate {
    num_docs: u32,
    segments: Vec<SegmentMeta>,
}

pub fn merge_segments<P: AsRef<Path>, D: crate::Document>(
    writer: &mut IndexWriter<D>,
    mut segments: Vec<SegmentMeta>,
    base_path: P,
    max_num_segments: u64,
) -> crate::Result<()> {
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

        best_candidate.num_docs = best_candidate
            .num_docs
            .checked_add(segment.num_docs())
            .expect("num docs must always be within u32::MAX");
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
