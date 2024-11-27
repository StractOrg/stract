use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::columnar::{
    ColumnType, ColumnValues, ColumnarReader, MergeRowOrder, RowAddr, ShuffleMergeOrder,
    StackMergeOrder,
};
use itertools::Itertools;
use measure_time::debug_time;

use crate::columnfield::ColumnFieldNotAvailableError;
use crate::directory::WritePtr;
use crate::docset::{DocSet, TERMINATED};
use crate::error::DataCorruption;
use crate::fieldnorm::{FieldNormReader, FieldNormReaders, FieldNormsSerializer, FieldNormsWriter};
use crate::index::{Segment, SegmentComponent, SegmentReader};
use crate::indexer::doc_id_mapping::{MappingType, SegmentDocIdMapping};
use crate::indexer::SegmentSerializer;
use crate::postings::{InvertedIndexSerializer, Postings, SegmentPostings};
use crate::schema::{value_type_to_column_type, Field, FieldType, Schema};
use crate::store::StoreWriter;
use crate::termdict::TermMerger;
use crate::{
    DocAddress, DocId, IndexSettings, IndexSortByField, InvertedIndexReader, Order, SegmentOrdinal,
};

/// Segment's max doc must be `< MAX_DOC_LIMIT`.
///
/// We do not allow segments with more than
pub const MAX_DOC_LIMIT: u32 = 1 << 31;

fn estimate_total_num_tokens_in_single_segment(
    reader: &SegmentReader,
    field: Field,
) -> crate::Result<u64> {
    Ok(reader.inverted_index(field)?.total_num_tokens())
}

fn estimate_total_num_tokens(readers: &[SegmentReader], field: Field) -> crate::Result<u64> {
    let mut total_num_tokens: u64 = 0;
    for reader in readers {
        total_num_tokens += estimate_total_num_tokens_in_single_segment(reader, field)?;
    }
    Ok(total_num_tokens)
}

pub struct IndexMerger {
    index_settings: IndexSettings,
    schema: Schema,
    pub(crate) readers: Vec<SegmentReader>,
    max_doc: u32,
}

struct DeltaComputer {
    buffer: Vec<u32>,
}

impl DeltaComputer {
    fn new() -> DeltaComputer {
        DeltaComputer {
            buffer: vec![0u32; 512],
        }
    }

    fn compute_delta(&mut self, positions: &[u32]) -> &[u32] {
        if positions.len() > self.buffer.len() {
            self.buffer.resize(positions.len(), 0u32);
        }
        let mut last_pos = 0u32;
        for (cur_pos, dest) in positions.iter().cloned().zip(self.buffer.iter_mut()) {
            *dest = cur_pos - last_pos;
            last_pos = cur_pos;
        }
        &self.buffer[..positions.len()]
    }
}

fn convert_to_merge_order(
    columnars: &[&ColumnarReader],
    doc_id_mapping: &SegmentDocIdMapping,
) -> MergeRowOrder {
    match doc_id_mapping.mapping_type() {
        MappingType::Stacked => MergeRowOrder::Stack(StackMergeOrder::stack(columnars)),
        MappingType::Shuffled => {
            // RUST/LLVM is amazing. The following conversion is actually a no-op:
            // no allocation, no copy.
            let new_row_id_to_old_row_id: Vec<RowAddr> = doc_id_mapping
                .new_doc_id_to_old_doc_addr
                .iter()
                .map(|doc_addr| RowAddr {
                    segment_ord: doc_addr.segment_ord,
                    row_id: doc_addr.doc_id,
                })
                .collect();
            MergeRowOrder::Shuffled(ShuffleMergeOrder {
                new_row_id_to_old_row_id,
            })
        }
    }
}

fn extract_column_field_required_columns(schema: &Schema) -> Vec<(String, ColumnType)> {
    schema
        .fields()
        .map(|(_, field_entry)| field_entry)
        .filter(|field_entry| field_entry.is_columnar())
        .filter_map(|field_entry| {
            let column_name = field_entry.name().to_string();
            let column_type = value_type_to_column_type(field_entry.field_type().value_type())?;
            Some((column_name, column_type))
        })
        .collect()
}

impl IndexMerger {
    pub fn open(
        schema: Schema,
        index_settings: IndexSettings,
        segments: &[Segment],
    ) -> crate::Result<IndexMerger> {
        let mut readers = vec![];
        for segment in segments {
            if segment.meta().num_docs() > 0 {
                let reader = SegmentReader::open(segment)?;
                readers.push(reader);
            }
        }

        let max_doc = readers.iter().map(|reader| reader.num_docs()).sum();
        if let Some(sort_by_field) = index_settings.sort_by_field.as_ref() {
            readers = Self::sort_readers_by_min_sort_field(readers, sort_by_field)?;
        }
        // sort segments by their natural sort setting
        if max_doc >= MAX_DOC_LIMIT {
            let err_msg = format!(
                "The segment resulting from this merge would have {max_doc} docs,which exceeds \
                 the limit {MAX_DOC_LIMIT}."
            );
            return Err(crate::TantivyError::InvalidArgument(err_msg));
        }
        Ok(IndexMerger {
            index_settings,
            schema,
            readers,
            max_doc,
        })
    }

    fn sort_readers_by_min_sort_field(
        readers: Vec<SegmentReader>,
        sort_by_field: &IndexSortByField,
    ) -> crate::Result<Vec<SegmentReader>> {
        // presort the readers by their min_values, so that when they are disjunct, we can use
        // the regular merge logic (implicitly sorted)
        let mut readers_with_min_sort_values = readers
            .into_iter()
            .map(|reader| {
                let accessor = Self::get_sort_field_accessor(&reader, sort_by_field)?;
                Ok((reader, accessor.min_value()))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        if sort_by_field.order.is_asc() {
            readers_with_min_sort_values.sort_by_key(|(_, min_val)| *min_val);
        } else {
            readers_with_min_sort_values.sort_by_key(|(_, min_val)| std::cmp::Reverse(*min_val));
        }
        Ok(readers_with_min_sort_values
            .into_iter()
            .map(|(reader, _)| reader)
            .collect())
    }

    fn write_fieldnorms(
        &self,
        mut fieldnorms_serializer: FieldNormsSerializer,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        let fields = FieldNormsWriter::fields_with_fieldnorm(&self.schema);
        let mut fieldnorms_data = Vec::with_capacity(self.max_doc as usize);
        for field in fields {
            fieldnorms_data.clear();
            let fieldnorms_readers: Vec<FieldNormReader> = self
                .readers
                .iter()
                .map(|reader| reader.get_fieldnorms_reader(field))
                .collect::<Result<_, _>>()?;
            for old_doc_addr in doc_id_mapping.iter_old_doc_addrs() {
                let fieldnorms_reader = &fieldnorms_readers[old_doc_addr.segment_ord as usize];
                let fieldnorm_id = fieldnorms_reader.fieldnorm_id(old_doc_addr.doc_id);
                fieldnorms_data.push(fieldnorm_id);
            }
            fieldnorms_serializer.serialize_field(field, &fieldnorms_data[..])?;
        }
        fieldnorms_serializer.close()?;
        Ok(())
    }

    fn write_column_fields(
        &self,
        column_field_wrt: &mut WritePtr,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        debug_time!("write-columnar-fields");
        let required_columns = extract_column_field_required_columns(&self.schema);
        let columnars: Vec<&ColumnarReader> = self
            .readers
            .iter()
            .map(|reader| reader.column_fields().columnar())
            .collect();
        let merge_row_order = convert_to_merge_order(&columnars[..], doc_id_mapping);
        crate::columnar::merge_columnar(
            &columnars[..],
            &required_columns,
            merge_row_order,
            column_field_wrt,
        )?;
        Ok(())
    }

    fn write_row_fields(
        &self,
        row_field_wrt: &mut WritePtr,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        debug_time!("write-row-fields");

        let indexes: Vec<_> = self
            .readers
            .iter()
            .map(|reader| reader.row_fields().row_index())
            .collect();

        let order = match doc_id_mapping.mapping_type() {
            MappingType::Stacked => crate::roworder::MergeRowOrder::Stack,
            MappingType::Shuffled => crate::roworder::MergeRowOrder::Shuffled {
                addrs: doc_id_mapping
                    .iter_old_doc_addrs()
                    .map(|addr| crate::roworder::MergeAddr {
                        segment_ord: addr.segment_ord as usize,
                    })
                    .collect(),
            },
        };

        crate::roworder::merge(&indexes, order, row_field_wrt)
            .map_err(|_| DataCorruption::comment_only("Failed to merge row fields"))?;

        Ok(())
    }

    /// Checks if the readers are disjunct for their sort property and in the correct order to be
    /// able to just stack them.
    pub(crate) fn is_disjunct_and_sorted_on_sort_property(
        &self,
        sort_by_field: &IndexSortByField,
    ) -> crate::Result<bool> {
        let reader_ordinal_and_field_accessors =
            self.get_reader_with_sort_field_accessor(sort_by_field)?;

        let everything_is_in_order = reader_ordinal_and_field_accessors
            .into_iter()
            .map(|(_, col)| Arc::new(col))
            .tuple_windows()
            .all(|(field_accessor1, field_accessor2)| {
                if sort_by_field.order.is_asc() {
                    field_accessor1.max_value() <= field_accessor2.min_value()
                } else {
                    field_accessor1.min_value() >= field_accessor2.max_value()
                }
            });
        Ok(everything_is_in_order)
    }

    pub(crate) fn get_sort_field_accessor(
        reader: &SegmentReader,
        sort_by_field: &IndexSortByField,
    ) -> crate::Result<Arc<dyn ColumnValues>> {
        reader.schema().get_field(&sort_by_field.field)?;
        let (value_accessor, _column_type) = reader
            .column_fields()
            .u64_lenient(&sort_by_field.field)?
            .ok_or_else(|| ColumnFieldNotAvailableError {
                field_name: sort_by_field.field.to_string(),
            })?;
        Ok(value_accessor.values)
    }
    /// Collecting value_accessors into a vec to bind the lifetime.
    pub(crate) fn get_reader_with_sort_field_accessor(
        &self,
        sort_by_field: &IndexSortByField,
    ) -> crate::Result<Vec<(SegmentOrdinal, Arc<dyn ColumnValues>)>> {
        let reader_ordinal_and_field_accessors = self
            .readers
            .iter()
            .enumerate()
            .map(|(reader_ordinal, _)| reader_ordinal as SegmentOrdinal)
            .map(|reader_ordinal: SegmentOrdinal| {
                let value_accessor = Self::get_sort_field_accessor(
                    &self.readers[reader_ordinal as usize],
                    sort_by_field,
                )?;
                Ok((reader_ordinal, value_accessor))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(reader_ordinal_and_field_accessors)
    }

    /// Generates the doc_id mapping where position in the vec=new
    /// doc_id.
    /// ReaderWithOrdinal will include the ordinal position of the
    /// reader in self.readers.
    pub(crate) fn generate_doc_id_mapping_with_sort_by_field(
        &self,
        sort_by_field: &IndexSortByField,
    ) -> crate::Result<SegmentDocIdMapping> {
        let reader_ordinal_and_field_accessors =
            self.get_reader_with_sort_field_accessor(sort_by_field)?;
        // Loading the field accessor on demand causes a 15x regression

        // create iterators over segment/sort_accessor/doc_id  tuple
        let doc_id_reader_pair =
            reader_ordinal_and_field_accessors
                .iter()
                .map(|(reader_ord, ff_reader)| {
                    let reader = &self.readers[*reader_ord as usize];
                    reader
                        .doc_ids()
                        .map(move |doc_id| (doc_id, reader_ord, ff_reader))
                });

        let total_num_new_docs = self
            .readers
            .iter()
            .map(|reader| reader.num_docs() as usize)
            .sum();

        let mut sorted_doc_ids: Vec<DocAddress> = Vec::with_capacity(total_num_new_docs);

        // create iterator tuple of (old doc_id, reader) in order of the new doc_ids
        sorted_doc_ids.extend(
            doc_id_reader_pair
                .into_iter()
                .kmerge_by(|a, b| {
                    let val1 = a.2.get_val(a.0);
                    let val2 = b.2.get_val(b.0);
                    if sort_by_field.order == Order::Asc {
                        val1 < val2
                    } else {
                        val1 > val2
                    }
                })
                .map(|(doc_id, &segment_ord, _)| DocAddress {
                    doc_id,
                    segment_ord,
                }),
        );

        Ok(SegmentDocIdMapping::new(
            sorted_doc_ids,
            MappingType::Shuffled,
        ))
    }

    /// Creates a mapping if the segments are stacked. this is helpful to merge codelines between
    /// index sorting and the others
    pub(crate) fn get_doc_id_from_concatenated_data(&self) -> crate::Result<SegmentDocIdMapping> {
        let total_num_new_docs = self
            .readers
            .iter()
            .map(|reader| reader.num_docs() as usize)
            .sum();

        let mut mapping: Vec<DocAddress> = Vec::with_capacity(total_num_new_docs);

        mapping.extend(
            self.readers
                .iter()
                .enumerate()
                .flat_map(|(segment_ord, reader)| {
                    reader.doc_ids().map(move |doc_id| DocAddress {
                        segment_ord: segment_ord as u32,
                        doc_id,
                    })
                }),
        );

        let mapping_type = MappingType::Stacked;
        Ok(SegmentDocIdMapping::new(mapping, mapping_type))
    }

    fn write_postings_for_field(
        &self,
        indexed_field: Field,
        _field_type: &FieldType,
        serializer: &mut InvertedIndexSerializer,
        fieldnorm_reader: Option<FieldNormReader>,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        debug_time!("write-postings-for-field");
        let mut positions_buffer: Vec<u32> = Vec::with_capacity(1_000);
        let mut delta_computer = DeltaComputer::new();

        let field_readers: Vec<Arc<InvertedIndexReader>> = self
            .readers
            .iter()
            .map(|reader| reader.inverted_index(indexed_field))
            .collect::<crate::Result<Vec<_>>>()?;

        let mut field_term_streams = Vec::with_capacity(field_readers.len());
        for field_reader in &field_readers {
            let terms = field_reader.terms();
            field_term_streams.push(terms.stream()?);
        }

        let mut merged_terms = TermMerger::new(field_term_streams);

        // map from segment doc ids to the resulting merged segment doc id.
        let mut merged_doc_id_map: Vec<Vec<Option<DocId>>> = self
            .readers
            .iter()
            .map(|reader| {
                let mut segment_local_map = vec![];
                segment_local_map.resize(reader.max_doc() as usize, None);
                segment_local_map
            })
            .collect();
        for (new_doc_id, old_doc_addr) in doc_id_mapping.iter_old_doc_addrs().enumerate() {
            let segment_map = &mut merged_doc_id_map[old_doc_addr.segment_ord as usize];
            segment_map[old_doc_addr.doc_id as usize] = Some(new_doc_id as DocId);
        }

        // Note that the total number of tokens is not exact.
        // It is only used as a parameter in the BM25 formula.
        let total_num_tokens: u64 = estimate_total_num_tokens(&self.readers, indexed_field)?;

        // Create the total list of doc ids
        // by stacking the doc ids from the different segment.
        //
        // In the new segments, the doc id from the different
        // segment are stacked so that :
        // - Segment 0's doc ids become doc id [0, seg.max_doc]
        // - Segment 1's doc ids become  [seg0.max_doc, seg0.max_doc + seg.max_doc]
        // - Segment 2's doc ids become  [seg0.max_doc + seg1.max_doc, seg0.max_doc + seg1.max_doc +
        //   seg2.max_doc]
        //
        // This stacking applies only when the index is not sorted, in that case the
        // doc_ids are kmerged by their sort property
        let mut field_serializer =
            serializer.new_field(indexed_field, total_num_tokens, fieldnorm_reader)?;

        let field_entry = self.schema.get_field_entry(indexed_field);

        // ... set segment postings option the new field.
        let segment_postings_option = field_entry.field_type().get_index_record_option().expect(
            "Encountered a field that is not supposed to be
                         indexed. Have you modified the schema?",
        );

        while merged_terms.advance() {
            let mut segment_postings_containing_the_term: Vec<(usize, SegmentPostings)> =
                Vec::new();

            let term_bytes: &[u8] = merged_terms.key();

            let mut total_doc_freq = 0;

            // Let's compute the list of non-empty posting lists
            for (segment_ord, term_info) in merged_terms.current_segment_ords_and_term_infos() {
                let inverted_index: &InvertedIndexReader = &field_readers[segment_ord];
                let segment_postings = inverted_index
                    .read_postings_from_terminfo(&term_info, segment_postings_option)?;
                let doc_freq = segment_postings.doc_freq();
                if doc_freq > 0u32 {
                    total_doc_freq += doc_freq;
                    segment_postings_containing_the_term.push((segment_ord, segment_postings));
                }
            }

            // At this point, `segment_postings` contains the posting list
            // of all of the segments containing the given term (and that are non-empty)
            //
            // These segments are non-empty and advance has already been called.
            if total_doc_freq == 0u32 {
                // All docs that used to contain the term have been deleted. The `term` will be
                // entirely removed.
                continue;
            }

            // This should never happen as we early exited for total_doc_freq == 0.
            assert!(!segment_postings_containing_the_term.is_empty());

            let has_term_freq = {
                let has_term_freq = !segment_postings_containing_the_term
                    .first()
                    .unwrap()
                    .1
                    .block_cursor
                    .freqs()
                    .is_empty();
                for (_, postings) in segment_postings_containing_the_term.iter().skip(1) {
                    // This may look at a strange way to test whether we have term freq or not.
                    // With JSON object, the schema is not sufficient to know whether a term
                    // has its term frequency encoded or not:
                    // strings may have term frequencies, while number terms never have one.
                    //
                    // Ideally, we should have burnt one bit of two in the `TermInfo`.
                    // However, we preferred not changing the codec too much and detect this
                    // instead by
                    // - looking at the size of the skip data for bitpacked blocks
                    // - observing the absence of remaining data after reading the docs for vint
                    // blocks.
                    //
                    // Overall the reliable way to know if we have actual frequencies loaded or not
                    // is to check whether the actual decoded array is empty or not.
                    if has_term_freq == postings.block_cursor.freqs().is_empty() {
                        return Err(DataCorruption::comment_only(
                            "Term freqs are inconsistent across segments",
                        )
                        .into());
                    }
                }
                has_term_freq
            };

            field_serializer.new_term(term_bytes, total_doc_freq, has_term_freq)?;

            // We can now serialize this postings, by pushing each document to the
            // postings serializer.

            let mut postings_merger =
                PostingsMerger::new(segment_postings_containing_the_term, &merged_doc_id_map);

            // Each segment_postings is already sorted by their new doc_id's
            // (if a new_doc_id(a) < new_doc_id(b), then old_doc_id(a) < old_doc_id(b)).
            // We can therefore just iterate over the doc_id_mapping and write the term for each
            // document.
            while let Some(mut segment) = postings_merger.next() {
                if segment.new_doc_id == TERMINATED {
                    continue;
                }

                // we make sure to only write the term if
                // there is at least one document.
                let term_freq = if has_term_freq {
                    segment.postings.positions(&mut positions_buffer);
                    segment.postings.term_freq()
                } else {
                    // The positions_buffer may contain positions from the previous term
                    // Existence of positions depend on the value type in JSON fields.
                    // https://github.com/quickwit-oss/tantivy/issues/2283
                    positions_buffer.clear();
                    0u32
                };

                let delta_positions = delta_computer.compute_delta(&positions_buffer);
                field_serializer.write_doc(segment.new_doc_id, term_freq, delta_positions);
            }

            // closing the term.
            field_serializer.close_term()?;
        }
        field_serializer.close()?;
        Ok(())
    }

    fn write_postings(
        &self,
        serializer: &mut InvertedIndexSerializer,
        fieldnorm_readers: FieldNormReaders,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        for (field, field_entry) in self.schema.fields() {
            let fieldnorm_reader = fieldnorm_readers.get_field(field)?;
            if field_entry.is_indexed() {
                self.write_postings_for_field(
                    field,
                    field_entry.field_type(),
                    serializer,
                    fieldnorm_reader,
                    doc_id_mapping,
                )?;
            }
        }
        Ok(())
    }

    fn write_storable_fields(
        &self,
        store_writer: &mut StoreWriter,
        doc_id_mapping: &SegmentDocIdMapping,
    ) -> crate::Result<()> {
        debug_time!("write-storable-fields");
        debug!("write-storable-field");

        if !doc_id_mapping.is_trivial() {
            debug!("non-trivial-doc-id-mapping");

            let store_readers: Vec<_> = self
                .readers
                .iter()
                .map(|reader| reader.get_store_reader(50))
                .collect::<Result<_, _>>()?;

            let mut document_iterators: Vec<_> =
                store_readers.iter().map(|store| store.iter_raw()).collect();

            for old_doc_addr in doc_id_mapping.iter_old_doc_addrs() {
                let doc_bytes_it = &mut document_iterators[old_doc_addr.segment_ord as usize];
                if let Some(doc_bytes_res) = doc_bytes_it.next() {
                    let doc_bytes = doc_bytes_res?;
                    store_writer.store_bytes(&doc_bytes)?;
                } else {
                    return Err(DataCorruption::comment_only(format!(
                        "unexpected missing document in docstore on merge, doc address \
                         {old_doc_addr:?}",
                    ))
                    .into());
                }
            }
        } else {
            debug!("trivial-doc-id-mapping");
            for reader in &self.readers {
                let store_reader = reader.get_store_reader(1)?;
                store_writer.stack(store_reader)?;
            }
        }
        Ok(())
    }

    /// Writes the merged segment by pushing information
    /// to the `SegmentSerializer`.
    ///
    /// # Returns
    /// The number of documents in the resulting segment.
    pub fn write(&self, mut serializer: SegmentSerializer) -> crate::Result<u32> {
        let doc_id_mapping = if let Some(sort_by_field) = self.index_settings.sort_by_field.as_ref()
        {
            // If the documents are already sorted and stackable, we ignore the mapping and execute
            // it as if there was no sorting
            if self.is_disjunct_and_sorted_on_sort_property(sort_by_field)? {
                self.get_doc_id_from_concatenated_data()?
            } else {
                self.generate_doc_id_mapping_with_sort_by_field(sort_by_field)?
            }
        } else {
            self.get_doc_id_from_concatenated_data()?
        };
        debug!("write-fieldnorms");
        if let Some(fieldnorms_serializer) = serializer.extract_fieldnorms_serializer() {
            self.write_fieldnorms(fieldnorms_serializer, &doc_id_mapping)?;
        }
        debug!("write-postings");
        let fieldnorm_data = serializer
            .segment()
            .open_read(SegmentComponent::FieldNorms)?;
        let fieldnorm_readers = FieldNormReaders::open(fieldnorm_data)?;
        self.write_postings(
            serializer.get_postings_serializer(),
            fieldnorm_readers,
            &doc_id_mapping,
        )?;

        debug!("write-storagefields");
        self.write_storable_fields(serializer.get_store_writer(), &doc_id_mapping)?;
        debug!("write-columnfields");
        self.write_column_fields(serializer.get_column_field_write(), &doc_id_mapping)?;
        debug!("write-rowfields");
        self.write_row_fields(serializer.get_row_field_write(), &doc_id_mapping)?;

        debug!("close-serializer");
        serializer.close()?;
        Ok(self.max_doc)
    }
}

struct SegmentPostingsWithNewDocId {
    postings: SegmentPostings,
    new_doc_id: DocId,
    segment_ord: usize,
}

impl PartialEq for SegmentPostingsWithNewDocId {
    fn eq(&self, other: &Self) -> bool {
        self.new_doc_id == other.new_doc_id
    }
}

impl Eq for SegmentPostingsWithNewDocId {}

impl PartialOrd for SegmentPostingsWithNewDocId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SegmentPostingsWithNewDocId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.new_doc_id.cmp(&other.new_doc_id).reverse()
    }
}

struct PeekSegmentPostingsWithNewDocId<'a> {
    segment: PeekMut<'a, SegmentPostingsWithNewDocId>,
    doc_id_mapping: &'a [Option<DocId>],
}

impl DerefMut for PeekSegmentPostingsWithNewDocId<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.segment
    }
}

impl Deref for PeekSegmentPostingsWithNewDocId<'_> {
    type Target = SegmentPostingsWithNewDocId;

    fn deref(&self) -> &Self::Target {
        &self.segment
    }
}

impl Drop for PeekSegmentPostingsWithNewDocId<'_> {
    fn drop(&mut self) {
        self.segment.postings.advance();
        self.segment.new_doc_id = if self.segment.postings.doc() == TERMINATED {
            TERMINATED
        } else {
            self.doc_id_mapping[self.segment.postings.doc() as usize].unwrap_or(TERMINATED)
        };
    }
}

struct PostingsMerger<'a> {
    postings: BinaryHeap<SegmentPostingsWithNewDocId>,
    doc_id_mapping: &'a [Vec<Option<DocId>>],
}

impl<'a> PostingsMerger<'a> {
    fn new(
        postings: Vec<(usize, SegmentPostings)>,
        doc_id_mapping: &'a [Vec<Option<DocId>>],
    ) -> Self {
        let postings: BinaryHeap<_> = postings
            .into_iter()
            .map(|(segment_ord, postings)| SegmentPostingsWithNewDocId {
                new_doc_id: doc_id_mapping[segment_ord][postings.doc() as usize]
                    .unwrap_or(TERMINATED),
                postings,
                segment_ord,
            })
            .collect();

        Self {
            postings,
            doc_id_mapping,
        }
    }

    fn next(&mut self) -> Option<PeekSegmentPostingsWithNewDocId<'_>> {
        let min_postings = self.postings.peek_mut()?;

        if min_postings.new_doc_id == TERMINATED {
            return None;
        }

        let mapping = self.doc_id_mapping[min_postings.segment_ord].as_slice();

        Some(PeekSegmentPostingsWithNewDocId {
            segment: min_postings,
            doc_id_mapping: mapping,
        })
    }
}

#[cfg(test)]
mod tests {

    use schema::COLUMN;

    use crate::collector::tests::{
        BytesColumnFieldTestCollector, ColumnFieldTestCollector, TEST_COLLECTOR_WITH_SCORE,
    };
    use crate::index::{Index, SegmentId};
    use crate::indexer::merger::PostingsMerger;
    use crate::postings::SegmentPostings;
    use crate::query::{BooleanQuery, EnableScoring, Scorer, TermQuery};
    use crate::schema::{
        IndexRecordOption, TantivyDocument, Term, TextFieldIndexing, Value, INDEXED, TEXT,
    };
    use crate::time::OffsetDateTime;
    use crate::{assert_nearly_equals, schema, DateTime, DocAddress, DocId, DocSet, IndexWriter};

    #[test]
    fn test_index_merger_no_deletes() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_fieldtype = schema::TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text", text_fieldtype);
        let date_field = schema_builder.add_date_field("date", INDEXED);
        let score_fieldtype = schema::NumericOptions::default().set_columnar();
        let score_field = schema_builder.add_u64_field("score", score_fieldtype);
        let bytes_score_field = schema_builder.add_bytes_field("score_bytes", COLUMN);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index.reader()?;
        let curr_time = OffsetDateTime::now_utc();
        {
            let mut index_writer = index.writer_for_tests()?;
            // writing the segment
            index_writer.add_document(doc!(
                text_field => "af b",
                score_field => 3u64,
                date_field => DateTime::from_utc(curr_time),
                bytes_score_field => 3u32.to_be_bytes().as_ref()
            ))?;
            index_writer.add_document(doc!(
                text_field => "a b c",
                score_field => 5u64,
                bytes_score_field => 5u32.to_be_bytes().as_ref()
            ))?;
            index_writer.add_document(doc!(
                text_field => "a b c d",
                score_field => 7u64,
                bytes_score_field => 7u32.to_be_bytes().as_ref()
            ))?;
            index_writer.commit()?;
            // writing the segment
            index_writer.add_document(doc!(
                text_field => "af b",
                date_field => DateTime::from_utc(curr_time),
                score_field => 11u64,
                bytes_score_field => 11u32.to_be_bytes().as_ref()
            ))?;
            index_writer.add_document(doc!(
                text_field => "a b c g",
                score_field => 13u64,
                bytes_score_field => 13u32.to_be_bytes().as_ref()
            ))?;
            index_writer.commit()?;
        }
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.merge(&segment_ids).wait()?;
            index_writer.wait_merging_threads()?;
        }
        {
            reader.reload()?;
            let searcher = reader.searcher();
            let get_doc_ids = |terms: Vec<Term>| {
                let query = BooleanQuery::new_multiterms_query(terms);
                searcher
                    .search(&query, &TEST_COLLECTOR_WITH_SCORE)
                    .map(|top_docs| top_docs.docs().to_vec())
            };
            {
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "a")])?,
                    vec![
                        DocAddress::new(0, 1),
                        DocAddress::new(0, 2),
                        DocAddress::new(0, 4)
                    ]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "af")])?,
                    vec![DocAddress::new(0, 0), DocAddress::new(0, 3)]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "g")])?,
                    vec![DocAddress::new(0, 4)]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_text(text_field, "b")])?,
                    vec![
                        DocAddress::new(0, 0),
                        DocAddress::new(0, 1),
                        DocAddress::new(0, 2),
                        DocAddress::new(0, 3),
                        DocAddress::new(0, 4)
                    ]
                );
                assert_eq!(
                    get_doc_ids(vec![Term::from_field_date(
                        date_field,
                        DateTime::from_utc(curr_time)
                    )])?,
                    vec![DocAddress::new(0, 0), DocAddress::new(0, 3)]
                );
            }
            {
                let doc = searcher.doc::<TantivyDocument>(DocAddress::new(0, 0))?;
                assert_eq!(
                    doc.get_first(text_field).unwrap().as_value().as_str(),
                    Some("af b")
                );
            }
            {
                let doc = searcher.doc::<TantivyDocument>(DocAddress::new(0, 1))?;
                assert_eq!(
                    doc.get_first(text_field).unwrap().as_value().as_str(),
                    Some("a b c")
                );
            }
            {
                let doc = searcher.doc::<TantivyDocument>(DocAddress::new(0, 2))?;
                assert_eq!(
                    doc.get_first(text_field).unwrap().as_value().as_str(),
                    Some("a b c d")
                );
            }
            {
                let doc = searcher.doc::<TantivyDocument>(DocAddress::new(0, 3))?;
                assert_eq!(doc.get_first(text_field).unwrap().as_str(), Some("af b"));
            }
            {
                let doc = searcher.doc::<TantivyDocument>(DocAddress::new(0, 4))?;
                assert_eq!(doc.get_first(text_field).unwrap().as_str(), Some("a b c g"));
            }

            {
                let get_columnar_vals = |terms: Vec<Term>| {
                    let query = BooleanQuery::new_multiterms_query(terms);
                    searcher.search(&query, &ColumnFieldTestCollector::for_field("score"))
                };
                let get_columnar_vals_bytes = |terms: Vec<Term>| {
                    let query = BooleanQuery::new_multiterms_query(terms);
                    searcher.search(
                        &query,
                        &BytesColumnFieldTestCollector::for_field("score_bytes"),
                    )
                };
                assert_eq!(
                    get_columnar_vals(vec![Term::from_field_text(text_field, "a")])?,
                    vec![5, 7, 13]
                );
                assert_eq!(
                    get_columnar_vals_bytes(vec![Term::from_field_text(text_field, "a")])?,
                    vec![0, 0, 0, 5, 0, 0, 0, 7, 0, 0, 0, 13]
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_postings_merger() -> crate::Result<()> {
        let a = SegmentPostings::create_from_docs(&[1, 5, 7]);
        let b = SegmentPostings::create_from_docs(&[3, 4]);
        let c = SegmentPostings::create_from_docs(&[2, 6]);
        let doc_id_mapping = vec![
            vec![None, Some(1), None, None, None, Some(5), None, Some(7)],
            vec![None, None, None, Some(3), Some(4), None, None, None],
            vec![None, None, Some(2), None, None, None, Some(6), None],
        ];

        let mut merger = PostingsMerger::new(vec![(0, a), (1, b), (2, c)], &doc_id_mapping);

        let mut res = Vec::<(DocId, usize)>::new();

        while let Some(peek) = merger.next() {
            res.push((peek.new_doc_id, peek.segment_ord));
        }

        assert_eq!(
            res,
            vec![(1, 0), (2, 2), (3, 1), (4, 1), (5, 0), (6, 2), (7, 0)]
        );

        Ok(())
    }

    // #[test]
    // fn test_index_merger_with_deletes() -> crate::Result<()> {
    //     let mut schema_builder = schema::Schema::builder();
    //     let text_fieldtype = schema::TextOptions::default()
    //         .set_indexing_options(
    //             TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
    //         )
    //         .set_stored();
    //     let text_field = schema_builder.add_text_field("text", text_fieldtype);
    //     let score_fieldtype = schema::NumericOptions::default().set_fast();
    //     let score_field = schema_builder.add_u64_field("score", score_fieldtype);
    //     let bytes_score_field = schema_builder.add_bytes_field("score_bytes", COLUMN);
    //     let index = Index::create_in_ram(schema_builder.build());
    //     let mut index_writer = index.writer_for_tests()?;
    //     let reader = index.reader().unwrap();
    //     let search_term = |searcher: &Searcher, term: Term| {
    //         let collector = ColumnFieldTestCollector::for_field("score");
    //         // let bytes_collector = BytesColumnFieldTestCollector::for_field(bytes_score_field);
    //         let term_query = TermQuery::new(term, IndexRecordOption::Basic);
    //         // searcher
    //         //     .search(&term_query, &(collector, bytes_collector))
    //         //     .map(|(scores, bytes)| {
    //         //         let mut score_bytes = &bytes[..];
    //         //         for &score in &scores {
    //         //             assert_eq!(score as u32, score_bytes.read_u32::<BigEndian>().unwrap());
    //         //         }
    //         //         scores
    //         //     })
    //         searcher.search(&term_query, &collector)
    //     };

    //     let empty_vec = Vec::<u64>::new();
    //     {
    //         // a first commit
    //         index_writer.add_document(doc!(
    //             text_field => "a b d",
    //             score_field => 1u64,
    //             bytes_score_field => vec![0u8, 0, 0, 1],
    //         ))?;
    //         index_writer.add_document(doc!(
    //             text_field => "b c",
    //             score_field => 2u64,
    //             bytes_score_field => vec![0u8, 0, 0, 2],
    //         ))?;
    //         index_writer.delete_term(Term::from_field_text(text_field, "c"));
    //         index_writer.add_document(doc!(
    //             text_field => "c d",
    //             score_field => 3u64,
    //             bytes_score_field => vec![0u8, 0, 0, 3],
    //         ))?;
    //         index_writer.commit()?;
    //         reader.reload()?;
    //         let searcher = reader.searcher();
    //         assert_eq!(searcher.num_docs(), 2);
    //         assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
    //         assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "a"))?,
    //             vec![1]
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "b"))?,
    //             vec![1]
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "c"))?,
    //             vec![3]
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "d"))?,
    //             vec![1, 3]
    //         );
    //     }
    //     {
    //         // a second commit
    //         index_writer.add_document(doc!(
    //             text_field => "a d e",
    //             score_field => 4_000u64,
    //             bytes_score_field => vec![0u8, 0, 0, 4],
    //         ))?;
    //         index_writer.add_document(doc!(
    //             text_field => "e f",
    //             score_field => 5_000u64,
    //             bytes_score_field => vec![0u8, 0, 0, 5],
    //         ))?;
    //         index_writer.delete_term(Term::from_field_text(text_field, "a"));
    //         index_writer.delete_term(Term::from_field_text(text_field, "f"));
    //         index_writer.add_document(doc!(
    //             text_field => "f g",
    //             score_field => 6_000u64,
    //             bytes_score_field => vec![0u8, 0, 23, 112],
    //         ))?;
    //         index_writer.add_document(doc!(
    //             text_field => "g h",
    //             score_field => 7_000u64,
    //             bytes_score_field => vec![0u8, 0, 27, 88],
    //         ))?;
    //         index_writer.commit()?;
    //         reader.reload()?;
    //         let searcher = reader.searcher();

    //         assert_eq!(searcher.segment_readers().len(), 2);
    //         assert_eq!(searcher.num_docs(), 3);
    //         assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
    //         assert_eq!(searcher.segment_readers()[0].max_doc(), 4);
    //         assert_eq!(searcher.segment_readers()[1].num_docs(), 1);
    //         assert_eq!(searcher.segment_readers()[1].max_doc(), 3);
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "a"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "b"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "c"))?,
    //             vec![3]
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "d"))?,
    //             vec![3]
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "e"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "f"))?,
    //             vec![6_000]
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "g"))?,
    //             vec![6_000, 7_000]
    //         );

    //         let score_field_reader = searcher
    //             .segment_reader(0)
    //             .column_fields()
    //             .u64("score")
    //             .unwrap();
    //         assert_eq!(score_field_reader.min_value(), 4000);
    //         assert_eq!(score_field_reader.max_value(), 7000);

    //         let score_field_reader = searcher
    //             .segment_reader(1)
    //             .column_fields()
    //             .u64("score")
    //             .unwrap();
    //         assert_eq!(score_field_reader.min_value(), 1);
    //         assert_eq!(score_field_reader.max_value(), 3);
    //     }
    //     {
    //         // merging the segments
    //         let segment_ids = index.searchable_segment_ids()?;
    //         index_writer.merge(&segment_ids).wait()?;
    //         reader.reload()?;
    //         let searcher = reader.searcher();
    //         assert_eq!(searcher.segment_readers().len(), 1);
    //         assert_eq!(searcher.num_docs(), 3);
    //         assert_eq!(searcher.segment_readers()[0].num_docs(), 3);
    //         assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "a"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "b"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "c"))?,
    //             vec![3]
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "d"))?,
    //             vec![3]
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "e"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "f"))?,
    //             vec![6_000]
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "g"))?,
    //             vec![6_000, 7_000]
    //         );
    //         let score_field_reader = searcher
    //             .segment_reader(0)
    //             .column_fields()
    //             .u64("score")
    //             .unwrap();
    //         assert_eq!(score_field_reader.min_value(), 3);
    //         assert_eq!(score_field_reader.max_value(), 7000);
    //     }
    //     {
    //         // test a commit with only deletes
    //         index_writer.delete_term(Term::from_field_text(text_field, "c"));
    //         index_writer.commit()?;

    //         reader.reload()?;
    //         let searcher = reader.searcher();
    //         assert_eq!(searcher.segment_readers().len(), 1);
    //         assert_eq!(searcher.num_docs(), 2);
    //         assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
    //         assert_eq!(searcher.segment_readers()[0].max_doc(), 3);
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "a"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "b"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "c"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "d"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "e"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "f"))?,
    //             vec![6_000]
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "g"))?,
    //             vec![6_000, 7_000]
    //         );
    //         let score_field_reader = searcher
    //             .segment_reader(0)
    //             .column_fields()
    //             .u64("score")
    //             .unwrap();
    //         assert_eq!(score_field_reader.min_value(), 3);
    //         assert_eq!(score_field_reader.max_value(), 7000);
    //     }
    //     {
    //         // Test merging a single segment in order to remove deletes.
    //         let segment_ids = index.searchable_segment_ids()?;
    //         index_writer.merge(&segment_ids).wait()?;
    //         reader.reload()?;

    //         let searcher = reader.searcher();
    //         assert_eq!(searcher.segment_readers().len(), 1);
    //         assert_eq!(searcher.num_docs(), 2);
    //         assert_eq!(searcher.segment_readers()[0].num_docs(), 2);
    //         assert_eq!(searcher.segment_readers()[0].max_doc(), 2);
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "a"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "b"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "c"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "d"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "e"))?,
    //             empty_vec
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "f"))?,
    //             vec![6_000]
    //         );
    //         assert_eq!(
    //             search_term(&searcher, Term::from_field_text(text_field, "g"))?,
    //             vec![6_000, 7_000]
    //         );
    //         let score_field_reader = searcher
    //             .segment_reader(0)
    //             .column_fields()
    //             .u64("score")
    //             .unwrap();
    //         assert_eq!(score_field_reader.min_value(), 6000);
    //         assert_eq!(score_field_reader.max_value(), 7000);
    //     }

    //     {
    //         // Test removing all docs
    //         index_writer.delete_term(Term::from_field_text(text_field, "g"));
    //         index_writer.commit()?;
    //         let segment_ids = index.searchable_segment_ids()?;
    //         reader.reload()?;

    //         let searcher = reader.searcher();
    //         assert!(segment_ids.is_empty());
    //         assert!(searcher.segment_readers().is_empty());
    //         assert_eq!(searcher.num_docs(), 0);
    //     }
    //     Ok(())
    // }

    // #[test]
    // fn test_bug_merge() -> crate::Result<()> {
    //     let mut schema_builder = schema::Schema::builder();
    //     let int_field = schema_builder.add_u64_field("intvals", INDEXED);
    //     let index = Index::create_in_ram(schema_builder.build());
    //     let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
    //     index_writer.add_document(doc!(int_field => 1u64))?;
    //     index_writer.commit().expect("commit failed");
    //     index_writer.add_document(doc!(int_field => 1u64))?;
    //     index_writer.commit().expect("commit failed");
    //     let reader = index.reader()?;
    //     let searcher = reader.searcher();
    //     assert_eq!(searcher.num_docs(), 2);
    //     index_writer.delete_term(Term::from_field_u64(int_field, 1));
    //     let segment_ids = index
    //         .searchable_segment_ids()
    //         .expect("Searchable segments failed.");
    //     index_writer.merge(&segment_ids).wait()?;
    //     reader.reload()?;
    //     // commit has not been called yet. The document should still be
    //     // there.
    //     assert_eq!(reader.searcher().num_docs(), 2);
    //     Ok(())
    // }

    // proptest! {
    //     #[test]
    //     fn test_merge_columnar_int_proptest(ops in proptest::collection::vec(balanced_operation_strategy(), 1..20)) {
    //         assert!(test_merge_int_fields(&ops[..]).is_ok());
    //     }
    // }

    #[test]
    fn merges_f64_column_fields_correctly() -> crate::Result<()> {
        let mut builder = schema::SchemaBuilder::new();

        let field = builder.add_f64_field("f64", schema::COLUMN);

        let index = Index::create_in_ram(builder.build());

        let mut writer = index.writer_for_tests()?;

        // Make sure we'll attempt to merge every created segment
        let mut policy = crate::indexer::LogMergePolicy::default();
        policy.set_min_num_segments(2);
        writer.set_merge_policy(Box::new(policy));

        for i in 0..100 {
            let mut doc = TantivyDocument::new();
            doc.add_f64(field, 42.0);
            writer.add_document(doc)?;
            if i % 5 == 0 {
                writer.commit()?;
            }
        }

        writer.commit()?;
        writer.wait_merging_threads()?;

        // If a merging thread fails, we should end up with more
        // than one segment here
        assert_eq!(1, index.searchable_segments()?.len());
        Ok(())
    }

    #[test]
    fn test_merged_index_has_blockwand() -> crate::Result<()> {
        let mut builder = schema::SchemaBuilder::new();
        let text = builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_for_tests()?;
        let happy_term = Term::from_field_text(text, "happy");
        let term_query = TermQuery::new(happy_term, IndexRecordOption::WithFreqs);
        for _ in 0..62 {
            writer.add_document(doc!(text=>"hello happy tax payer"))?;
        }
        writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let mut term_scorer = term_query
            .specialized_weight(EnableScoring::enabled_from_searcher(&searcher))?
            .specialized_scorer(searcher.segment_reader(0u32), 1.0)?;
        assert_eq!(term_scorer.doc(), 0);
        assert_nearly_equals!(term_scorer.block_max_score(), 0.0079681855);
        assert_nearly_equals!(term_scorer.score(), 0.0079681855);
        for _ in 0..81 {
            writer.add_document(doc!(text=>"hello happy tax payer"))?;
        }
        writer.commit()?;
        reader.reload()?;
        let searcher = reader.searcher();

        assert_eq!(searcher.segment_readers().len(), 2);
        for segment_reader in searcher.segment_readers() {
            let mut term_scorer = term_query
                .specialized_weight(EnableScoring::enabled_from_searcher(&searcher))?
                .specialized_scorer(segment_reader, 1.0)?;
            // the difference compared to before is intrinsic to the bm25 formula. no worries
            // there.
            for doc in segment_reader.doc_ids() {
                assert_eq!(term_scorer.doc(), doc);
                assert_nearly_equals!(term_scorer.block_max_score(), 0.003478312);
                assert_nearly_equals!(term_scorer.score(), 0.003478312);
                term_scorer.advance();
            }
        }

        let segment_ids: Vec<SegmentId> = searcher
            .segment_readers()
            .iter()
            .map(|reader| reader.segment_id())
            .collect();
        writer.merge(&segment_ids[..]).wait()?;

        reader.reload()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);

        let segment_reader = searcher.segment_reader(0u32);
        let mut term_scorer = term_query
            .specialized_weight(EnableScoring::enabled_from_searcher(&searcher))?
            .specialized_scorer(segment_reader, 1.0)?;
        // the difference compared to before is intrinsic to the bm25 formula. no worries there.
        for doc in segment_reader.doc_ids() {
            assert_eq!(term_scorer.doc(), doc);
            assert_nearly_equals!(term_scorer.block_max_score(), 0.003478312);
            assert_nearly_equals!(term_scorer.score(), 0.003478312);
            term_scorer.advance();
        }

        Ok(())
    }

    #[test]
    fn test_max_doc() {
        // this is the first time I write a unit test for a constant.
        assert!(((super::MAX_DOC_LIMIT - 1) as i32) >= 0);
        assert!((super::MAX_DOC_LIMIT as i32) < 0);
    }

    #[test]
    fn test_rowfields_merge() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let u64_field = schema_builder
            .add_u64_field("u64", schema::INDEXED | schema::ROW_ORDER | schema::STORED);
        let f64_field = schema_builder
            .add_f64_field("f64", schema::INDEXED | schema::ROW_ORDER | schema::STORED);

        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;

        // Make sure we'll attempt to merge every created segment
        let mut policy = crate::indexer::LogMergePolicy::default();
        policy.set_min_num_segments(2);
        writer.set_merge_policy(Box::new(policy));

        for i in 0..100 {
            let mut doc = TantivyDocument::new();
            doc.add_f64(f64_field, 42.0);
            doc.add_u64(u64_field, 42);

            writer.add_document(doc)?;
            if i % 5 == 0 {
                writer.commit()?;
            }
        }

        writer.commit()?;
        writer.wait_merging_threads()?;

        // If a merging thread fails, we should end up with more
        // than one segment here
        assert_eq!(1, index.searchable_segments()?.len());
        Ok(())
    }
}
