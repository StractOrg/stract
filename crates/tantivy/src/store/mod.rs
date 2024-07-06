//! Compressed/slow/row-oriented storage for documents.
//!
//! A field needs to be marked as stored in the schema in
//! order to be handled in the `Store`.
//!
//! Internally, documents (or rather their stored fields) are serialized to a buffer.
//! When the buffer exceeds `block_size` (defaults to 16K), the buffer is compressed
//! using LZ4 or Zstd and the resulting block is written to disk.
//!
//! One can then request for a specific `DocId`.
//! A skip list helps navigating to the right block,
//! decompresses it entirely and returns the document within it.
//!
//! If the last document requested was in the same block,
//! the reader is smart enough to avoid decompressing
//! the block a second time, but their is no real
//! uncompressed block* cache.
//!
//! A typical use case for the store is, once
//! the search result page has been computed, returning
//! the actual content of the 10 best document.
//!
//! # Usage
//!
//! Most users should not access the `StoreReader` directly
//! and should rely on either
//!
//! - at the segment level, the
//!     [`SegmentReader`'s `doc` method](../struct.SegmentReader.html#method.doc)
//! - at the index level, the [`Searcher::doc()`](crate::Searcher::doc) method

mod compressors;
mod decompressors;
mod footer;
mod index;
mod reader;
mod writer;
pub use self::compressors::Compressor;
pub use self::decompressors::Decompressor;
pub(crate) use self::reader::DOCSTORE_CACHE_CAPACITY;
pub use self::reader::{CacheStats, StoreReader};
pub use self::writer::StoreWriter;
mod store_compressor;

/// Doc store version in footer to handle format changes.
pub(crate) const DOC_STORE_VERSION: u32 = 1;

#[cfg(feature = "lz4-compression")]
mod compression_lz4_block;

#[cfg(test)]
pub mod tests {

    use std::path::Path;

    use super::*;
    use crate::directory::{Directory, RamDirectory, WritePtr};
    use crate::schema::{self, Schema, TantivyDocument, TextOptions, Value, STORED, TEXT};
    use crate::{Index, IndexWriter};

    const LOREM: &str = "Doc Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
                         eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad \
                         minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip \
                         ex ea commodo consequat. Duis aute irure dolor in reprehenderit in \
                         voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur \
                         sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt \
                         mollit anim id est laborum.";

    const BLOCK_SIZE: usize = 16_384;

    pub fn write_lorem_ipsum_store(
        writer: WritePtr,
        num_docs: usize,
        compressor: Compressor,
        blocksize: usize,
        separate_thread: bool,
    ) -> Schema {
        let mut schema_builder = Schema::builder();
        let field_body = schema_builder.add_text_field("body", TextOptions::default().set_stored());
        let field_title =
            schema_builder.add_text_field("title", TextOptions::default().set_stored());
        let schema = schema_builder.build();
        {
            let mut store_writer =
                StoreWriter::new(writer, compressor, blocksize, separate_thread).unwrap();
            for i in 0..num_docs {
                let mut doc = TantivyDocument::default();
                doc.add_text(field_body, LOREM);
                doc.add_text(field_title, format!("Doc {i}"));
                store_writer.store(&doc, &schema).unwrap();
            }
            store_writer.close().unwrap();
        }
        schema
    }

    const NUM_DOCS: usize = 1_000;
    #[test]
    fn test_doc_store_iter_with_delete_bug_1077() -> crate::Result<()> {
        let path = Path::new("store");
        let directory = RamDirectory::create();
        let store_wrt = directory.open_write(path)?;
        let schema =
            write_lorem_ipsum_store(store_wrt, NUM_DOCS, Compressor::Lz4, BLOCK_SIZE, true);
        let field_title = schema.get_field("title").unwrap();
        let store_file = directory.open_read(path)?;
        let store = StoreReader::open(store_file, 10)?;
        for i in 0..NUM_DOCS as u32 {
            assert_eq!(
                store
                    .get::<TantivyDocument>(i)?
                    .get_first(field_title)
                    .unwrap()
                    .as_value()
                    .as_str()
                    .unwrap(),
                format!("Doc {i}")
            );
        }

        for doc in store.iter::<TantivyDocument>() {
            let doc = doc?;
            let title_content = doc
                .get_first(field_title)
                .unwrap()
                .as_value()
                .as_str()
                .unwrap()
                .to_string();
            if !title_content.starts_with("Doc ") {
                panic!("unexpected title_content {title_content}");
            }
        }

        Ok(())
    }

    fn test_store(
        compressor: Compressor,
        blocksize: usize,
        separate_thread: bool,
    ) -> crate::Result<()> {
        let path = Path::new("store");
        let directory = RamDirectory::create();
        let store_wrt = directory.open_write(path)?;
        let schema =
            write_lorem_ipsum_store(store_wrt, NUM_DOCS, compressor, blocksize, separate_thread);
        let field_title = schema.get_field("title").unwrap();
        let store_file = directory.open_read(path)?;
        let store = StoreReader::open(store_file, 10)?;
        for i in 0..NUM_DOCS as u32 {
            assert_eq!(
                *store
                    .get::<TantivyDocument>(i)?
                    .get_first(field_title)
                    .unwrap()
                    .as_str()
                    .unwrap(),
                format!("Doc {i}")
            );
        }
        for (i, doc) in store.iter::<TantivyDocument>().enumerate() {
            assert_eq!(
                *doc?.get_first(field_title).unwrap().as_str().unwrap(),
                format!("Doc {i}")
            );
        }
        Ok(())
    }

    #[test]
    fn test_store_no_compression_same_thread() -> crate::Result<()> {
        test_store(Compressor::None, BLOCK_SIZE, false)
    }

    #[test]
    fn test_store_no_compression() -> crate::Result<()> {
        test_store(Compressor::None, BLOCK_SIZE, true)
    }

    #[cfg(feature = "lz4-compression")]
    #[test]
    fn test_store_lz4_block() -> crate::Result<()> {
        test_store(Compressor::Lz4, BLOCK_SIZE, true)
    }

    #[test]
    fn test_merge_of_small_segments() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();

        let text_field = schema_builder.add_text_field("text_field", TEXT | STORED);
        let schema = schema_builder.build();
        let index_builder = Index::builder().schema(schema);

        let index = index_builder.create_in_ram().unwrap();

        {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field=> "1"))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(text_field=> "2"))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(text_field=> "3"))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(text_field=> "4"))?;
            index_writer.commit()?;
            index_writer.add_document(doc!(text_field=> "5"))?;
            index_writer.commit()?;
        }
        // Merging the segments
        {
            let segment_ids = index.searchable_segment_ids()?;
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.merge(&segment_ids).wait()?;
            index_writer.wait_merging_threads()?;
        }

        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let reader = searcher.segment_readers().iter().last().unwrap();
        let store = reader.get_store_reader(10)?;
        assert_eq!(store.block_checkpoints().count(), 5);
        Ok(())
    }
}
