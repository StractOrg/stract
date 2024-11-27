use std::io::{self, Write};

use crate::common::CountingWriter;
use crate::sstable::{SSTable, Streamer, TermOrdinal, VoidSSTable};

use super::term_merger::TermMerger;
use crate::columnar::column::serialize_column_mappable_to_u64;
use crate::columnar::column_index::SerializableColumnIndex;
use crate::columnar::iterable::Iterable;
use crate::columnar::{BytesColumn, MergeRowOrder, ShuffleMergeOrder};

// Serialize [Dictionary, Column, dictionary num bytes U64::LE]
// Column: [Column Index, Column Values, column index num bytes U32::LE]
pub fn merge_bytes_or_str_column(
    column_index: SerializableColumnIndex,
    bytes_columns: &[Option<BytesColumn>],
    merge_row_order: &MergeRowOrder,
    output: &mut impl Write,
) -> io::Result<()> {
    // Serialize dict and generate mapping for values
    let mut output = CountingWriter::wrap(output);
    // TODO !!! Remove useless terms.
    let term_ord_mapping = serialize_merged_dict(bytes_columns, merge_row_order, &mut output)?;
    let dictionary_num_bytes: u64 = output.written_bytes();
    let output = output.finish();
    let remapped_term_ordinals_values = RemappedTermOrdinalsValues {
        bytes_columns,
        term_ord_mapping: &term_ord_mapping,
        merge_row_order,
    };
    serialize_column_mappable_to_u64(column_index, &remapped_term_ordinals_values, output)?;
    output.write_all(&dictionary_num_bytes.to_le_bytes())?;
    Ok(())
}

struct RemappedTermOrdinalsValues<'a> {
    bytes_columns: &'a [Option<BytesColumn>],
    term_ord_mapping: &'a TermOrdinalMapping,
    merge_row_order: &'a MergeRowOrder,
}

impl Iterable for RemappedTermOrdinalsValues<'_> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        match self.merge_row_order {
            MergeRowOrder::Stack(_) => self.boxed_iter_stacked(),
            MergeRowOrder::Shuffled(shuffle_merge_order) => {
                self.boxed_iter_shuffled(shuffle_merge_order)
            }
        }
    }
}

impl RemappedTermOrdinalsValues<'_> {
    fn boxed_iter_stacked(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        let iter = self
            .bytes_columns
            .iter()
            .enumerate()
            .flat_map(|(seg_ord, bytes_column_opt)| {
                let bytes_column = bytes_column_opt.as_ref()?;
                Some((seg_ord, bytes_column))
            })
            .flat_map(move |(seg_ord, bytes_column)| {
                let term_ord_after_merge_mapping =
                    self.term_ord_mapping.get_segment(seg_ord as u32);
                bytes_column
                    .ords()
                    .values
                    .iter()
                    .map(move |term_ord| term_ord_after_merge_mapping[term_ord as usize])
            });
        Box::new(iter)
    }

    fn boxed_iter_shuffled<'b>(
        &'b self,
        shuffle_merge_order: &'b ShuffleMergeOrder,
    ) -> Box<dyn Iterator<Item = u64> + 'b> {
        Box::new(
            shuffle_merge_order
                .iter_new_to_old_row_addrs()
                .filter_map(move |old_addr| {
                    let segment_ord = self.term_ord_mapping.get_segment(old_addr.segment_ord);
                    if !segment_ord.is_empty() {
                        Some((old_addr, segment_ord))
                    } else {
                        None
                    }
                })
                .flat_map(|(old_addr, segment_ord)| {
                    self.bytes_columns[old_addr.segment_ord as usize]
                        .as_ref()
                        .into_iter()
                        .flat_map(move |bytes_column| {
                            bytes_column
                                .term_ords(old_addr.row_id)
                                .map(|old_term_ord: u64| segment_ord[old_term_ord as usize])
                        })
                }),
        )
    }
}

fn serialize_merged_dict(
    bytes_columns: &[Option<BytesColumn>],
    merge_row_order: &MergeRowOrder,
    output: &mut impl Write,
) -> io::Result<TermOrdinalMapping> {
    let mut term_ord_mapping = TermOrdinalMapping::default();

    let mut field_term_streams = Vec::new();
    for column_opt in bytes_columns.iter() {
        if let Some(column) = column_opt {
            term_ord_mapping.add_segment(column.dictionary.num_terms());
            let terms: Streamer<VoidSSTable> = column.dictionary.stream()?;
            field_term_streams.push(terms);
        } else {
            term_ord_mapping.add_segment(0);
            field_term_streams.push(Streamer::empty());
        }
    }

    let mut merged_terms = TermMerger::new(field_term_streams);
    let mut sstable_builder = crate::sstable::VoidSSTable::writer(output);

    match merge_row_order {
        MergeRowOrder::Stack(_) => {
            let mut current_term_ord = 0;
            while merged_terms.advance() {
                let term_bytes: &[u8] = merged_terms.key();
                sstable_builder.insert(term_bytes, &())?;
                for (segment_ord, from_term_ord) in merged_terms.matching_segments() {
                    term_ord_mapping.register_from_to(segment_ord, from_term_ord, current_term_ord);
                }
                current_term_ord += 1;
            }
            sstable_builder.finish()?;
        }
        MergeRowOrder::Shuffled(_) => {
            let mut current_term_ord = 0;
            while merged_terms.advance() {
                let term_bytes: &[u8] = merged_terms.key();
                sstable_builder.insert(term_bytes, &())?;
                for (segment_ord, from_term_ord) in merged_terms.matching_segments() {
                    term_ord_mapping.register_from_to(segment_ord, from_term_ord, current_term_ord);
                }
                current_term_ord += 1;
            }
            sstable_builder.finish()?;
        }
    }
    Ok(term_ord_mapping)
}

#[derive(Default, Debug)]
struct TermOrdinalMapping {
    per_segment_new_term_ordinals: Vec<Vec<TermOrdinal>>,
}

impl TermOrdinalMapping {
    fn add_segment(&mut self, max_term_ord: usize) {
        self.per_segment_new_term_ordinals
            .push(vec![TermOrdinal::default(); max_term_ord]);
    }

    fn register_from_to(&mut self, segment_ord: usize, from_ord: TermOrdinal, to_ord: TermOrdinal) {
        self.per_segment_new_term_ordinals[segment_ord][from_ord as usize] = to_ord;
    }

    fn get_segment(&self, segment_ord: u32) -> &[TermOrdinal] {
        &(self.per_segment_new_term_ordinals[segment_ord as usize])[..]
    }
}
