#[cfg(test)]
mod tests {
    use crate::collector::TopDocs;
    use crate::index::Index;
    use crate::postings::Postings;
    use crate::query::QueryParser;
    use crate::schema::{
        self, IndexRecordOption, NumericOptions, TextFieldIndexing, TextOptions, Value,
    };
    use crate::{
        DocAddress, DocSet, IndexSettings, IndexSortByField, IndexWriter, Order, TantivyDocument,
        Term,
    };

    fn create_test_index_posting_list_issue(index_settings: Option<IndexSettings>) -> Index {
        let mut schema_builder = schema::Schema::builder();
        let int_options = NumericOptions::default().set_columnar().set_indexed();
        let int_field = schema_builder.add_u64_field("intval", int_options);

        let schema = schema_builder.build();

        let mut index_builder = Index::builder().schema(schema);
        if let Some(settings) = index_settings {
            index_builder = index_builder.settings(settings);
        }
        let index = index_builder.create_in_ram().unwrap();

        {
            let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
            index_writer.add_document(doc!(int_field=>3_u64)).unwrap();
            index_writer.add_document(doc!(int_field=>6_u64)).unwrap();
            index_writer.commit().unwrap();
            index_writer.add_document(doc!(int_field=>5_u64)).unwrap();
            index_writer.commit().unwrap();
        }

        // Merging the segments
        {
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
            assert!(index_writer.merge(&segment_ids).wait().is_ok());
            assert!(index_writer.wait_merging_threads().is_ok());
        }
        index
    }

    // force_disjunct_segment_sort_values forces the field, by which the index is sorted have
    // disjunct ranges between segments, e.g. values in segment [1-3] [10 - 20] [50 - 500]
    fn create_test_index(
        index_settings: Option<IndexSettings>,
        force_disjunct_segment_sort_values: bool,
    ) -> crate::Result<Index> {
        let mut schema_builder = schema::Schema::builder();
        let int_options = NumericOptions::default()
            .set_columnar()
            .set_stored()
            .set_indexed();
        let int_field = schema_builder.add_u64_field("intval", int_options);

        let col1 = NumericOptions::default()
            .set_row_order()
            .set_stored()
            .set_indexed();
        let col1_field = schema_builder.add_u64_field("col1", col1);

        let col2 = NumericOptions::default()
            .set_row_order()
            .set_stored()
            .set_indexed();
        let col2_field = schema_builder.add_u64_field("col2", col2);

        let text_field_options = TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_index_option(schema::IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored();
        let text_field = schema_builder.add_text_field("text_field", text_field_options);
        let schema = schema_builder.build();

        let mut index_builder = Index::builder().schema(schema);
        if let Some(settings) = index_settings {
            index_builder = index_builder.settings(settings);
        }
        let index = index_builder.create_in_ram()?;

        {
            let mut index_writer = index.writer_for_tests()?;

            // segment 1 - range 1-3
            index_writer.add_document(
                doc!(int_field=>1_u64, col1_field=>1_u64, col2_field=>2_u64, text_field => "text"),
            )?;
            index_writer.add_document(
                doc!(int_field=>3_u64, col1_field=>3_u64, col2_field=>6_u64, text_field => "some text"),
            )?;
            index_writer.add_document(
                doc!(int_field=>1_u64, col1_field=>1u64, col2_field=>2u64, text_field=> "deleteme",  text_field => "ok text more text"),
            )?;
            index_writer.add_document(
                doc!(int_field=>2_u64, col1_field=>2u64, col2_field=>4u64, text_field => "ok text more text"),
            )?;

            index_writer.commit()?;
            // segment 2 - range 1-20 , with force_disjunct_segment_sort_values 10-20
            index_writer
                .add_document(doc!(int_field=>20_u64, col1_field=>20u64, col2_field=>40u64, text_field => "ok text more text"))?;

            let in_val = if force_disjunct_segment_sort_values {
                10_u64
            } else {
                1
            };
            index_writer.add_document(doc!(int_field=>in_val, col1_field=>in_val, col2_field=>2*in_val, text_field=> "deleteme" , text_field => "ok text more text"))?;
            index_writer.commit()?;
            // segment 3 - range 5-1000, with force_disjunct_segment_sort_values 50-1000
            let int_vals = if force_disjunct_segment_sort_values {
                [100_u64, 50]
            } else {
                [10, 5]
            };
            index_writer.add_document(
                // position of this doc after delete in desc sorting = [2], in disjunct case [1]
                doc!(int_field=>int_vals[0], col1_field=>int_vals[0], col2_field=>2*int_vals[0], text_field=> "blubber"),
            )?;
            index_writer.add_document(doc!(int_field=>int_vals[1], col1_field=>int_vals[1], col2_field=>2*int_vals[1], text_field=> "deleteme"))?;
            index_writer
                .add_document(doc!(int_field=>1_000u64, col1_field=>1_000u64, col2_field=>2_000u64, text_field => "the biggest num"))?;

            index_writer.commit()?;
        }

        // Merging the segments
        {
            let segment_ids = index.searchable_segment_ids()?;
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.merge(&segment_ids).wait()?;
            index_writer.wait_merging_threads()?;
        }
        Ok(index)
    }

    #[test]
    fn test_merge_sorted_postinglist_sort_issue() {
        create_test_index_posting_list_issue(Some(IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "intval".to_string(),
                order: Order::Desc,
            }),
            ..Default::default()
        }));
    }

    #[test]
    fn test_merge_sorted_index_desc_not_disjunct() {
        test_merge_sorted_index_desc_(false);
    }

    #[test]
    fn test_merge_sorted_index_desc_disjunct() {
        test_merge_sorted_index_desc_(true);
    }

    fn test_merge_sorted_index_desc_(force_disjunct_segment_sort_values: bool) {
        let index = create_test_index(
            Some(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "intval".to_string(),
                    order: Order::Desc,
                }),
                ..Default::default()
            }),
            force_disjunct_segment_sort_values,
        )
        .unwrap();

        let int_field = index.schema().get_field("intval").unwrap();
        let reader = index.reader().unwrap();

        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_readers().last().unwrap();

        let column_fields = segment_reader.column_fields();
        let column_field = column_fields.u64("intval").unwrap();
        assert_eq!(column_field.first(7), Some(1u64));
        if force_disjunct_segment_sort_values {
            assert_eq!(column_field.first(6), Some(2u64));
            assert_eq!(column_field.first(5), Some(3u64));
            assert_eq!(column_field.first(4), Some(10u64));
            assert_eq!(column_field.first(3), Some(20u64));
        } else {
            assert_eq!(column_field.first(6), Some(1u64));
            assert_eq!(column_field.first(5), Some(2u64));
            assert_eq!(column_field.first(4), Some(3u64));
            assert_eq!(column_field.first(3), Some(5u64));
        }
        assert_eq!(column_field.first(0), Some(1_000u64));

        // test new field norm mapping
        {
            let my_text_field = index.schema().get_field("text_field").unwrap();
            let fieldnorm_reader = segment_reader.get_fieldnorms_reader(my_text_field).unwrap();
            assert_eq!(fieldnorm_reader.fieldnorm(0), 3);
            if force_disjunct_segment_sort_values {
                assert_eq!(fieldnorm_reader.fieldnorm(2), 1); // blubber
                assert_eq!(fieldnorm_reader.fieldnorm(3), 4);
                assert_eq!(fieldnorm_reader.fieldnorm(5), 2); // some text
                assert_eq!(fieldnorm_reader.fieldnorm(7), 1);
            } else {
                assert_eq!(fieldnorm_reader.fieldnorm(2), 1);
                assert_eq!(fieldnorm_reader.fieldnorm(3), 1); // blubber
                assert_eq!(fieldnorm_reader.fieldnorm(5), 4); // some text
                assert_eq!(fieldnorm_reader.fieldnorm(7), 5);
            }
        }

        let my_text_field = index.schema().get_field("text_field").unwrap();
        let searcher = index.reader().unwrap().searcher();
        {
            let my_text_field = index.schema().get_field("text_field").unwrap();

            let do_search = |term: &str| {
                let query = QueryParser::for_index(&index, vec![my_text_field])
                    .parse_query(term)
                    .unwrap();
                let top_docs: Vec<(f32, DocAddress)> =
                    searcher.search(&query, &TopDocs::with_limit(3)).unwrap();

                top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>()
            };

            if force_disjunct_segment_sort_values {
                assert_eq!(do_search("some"), vec![5]);
                assert_eq!(do_search("blubber"), vec![1]);
            } else {
                assert_eq!(do_search("some"), vec![4]);
                assert_eq!(do_search("blubber"), vec![2]);
            }
            assert_eq!(do_search("biggest"), vec![0]);
        }

        // postings file
        {
            let my_text_field = index.schema().get_field("text_field").unwrap();
            let term_a = Term::from_field_text(my_text_field, "text");
            let inverted_index = segment_reader.inverted_index(my_text_field).unwrap();
            let mut postings = inverted_index
                .read_postings(&term_a, IndexRecordOption::WithFreqsAndPositions)
                .unwrap()
                .unwrap();

            assert_eq!(postings.doc_freq(), 6);

            assert_eq!(postings.term_freq(), 2);
            let mut output = vec![];
            postings.positions(&mut output);
            assert_eq!(output, vec![1, 3]);
            postings.advance();

            if force_disjunct_segment_sort_values {
                assert_eq!(postings.term_freq(), 2);
                postings.positions(&mut output);
                assert_eq!(output, vec![3, 5]);
            } else {
                assert_eq!(postings.term_freq(), 1);
                postings.positions(&mut output);
                assert_eq!(output, vec![1]);
            }
        }

        // access doc store
        {
            let blubber_pos = if force_disjunct_segment_sort_values {
                1
            } else {
                2
            };
            let doc = searcher
                .doc::<TantivyDocument>(DocAddress::new(0, blubber_pos))
                .unwrap();
            assert_eq!(
                doc.get_first(my_text_field).unwrap().as_value().as_str(),
                Some("blubber")
            );
            let doc = searcher
                .doc::<TantivyDocument>(DocAddress::new(0, 0))
                .unwrap();
            assert_eq!(
                doc.get_first(int_field).unwrap().as_value().as_u64(),
                Some(1000)
            );
        }

        // row fields
        {
            let col1_field = index.schema().get_field("col1").unwrap();
            let col2_field = index.schema().get_field("col2").unwrap();

            let row_index = segment_reader.row_fields().row_index();

            let get_u64 = |doc_id: usize, field_id: u32| -> u64 {
                let row = row_index.get_row(doc_id).unwrap();
                row.get_by_field_id(field_id).unwrap().as_u64().unwrap()
            };

            assert_eq!(get_u64(0, col1_field.field_id()), 1_000);
            assert_eq!(get_u64(0, col2_field.field_id()), 2_000);

            if force_disjunct_segment_sort_values {
                assert_eq!(get_u64(6, col1_field.field_id()), 2u64);
                assert_eq!(get_u64(6, col2_field.field_id()), 4u64);

                assert_eq!(get_u64(5, col1_field.field_id()), 3u64);
                assert_eq!(get_u64(5, col2_field.field_id()), 6u64);

                assert_eq!(get_u64(4, col1_field.field_id()), 10u64);
                assert_eq!(get_u64(4, col2_field.field_id()), 20u64);

                assert_eq!(get_u64(3, col1_field.field_id()), 20u64);
                assert_eq!(get_u64(3, col2_field.field_id()), 40u64);
            } else {
                assert_eq!(get_u64(6, col1_field.field_id()), 1u64);
                assert_eq!(get_u64(6, col2_field.field_id()), 2u64);

                assert_eq!(get_u64(5, col1_field.field_id()), 2u64);
                assert_eq!(get_u64(5, col2_field.field_id()), 4u64);

                assert_eq!(get_u64(4, col1_field.field_id()), 3u64);
                assert_eq!(get_u64(4, col2_field.field_id()), 6u64);

                assert_eq!(get_u64(3, col1_field.field_id()), 5u64);
                assert_eq!(get_u64(3, col2_field.field_id()), 10u64);
            }
        }
    }

    #[test]
    fn test_merge_unsorted_index() {
        let index = create_test_index(
            Some(IndexSettings {
                ..Default::default()
            }),
            false,
        )
        .unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_readers().last().unwrap();

        let searcher = index.reader().unwrap().searcher();
        {
            let my_text_field = index.schema().get_field("text_field").unwrap();

            let do_search = |term: &str| {
                let query = QueryParser::for_index(&index, vec![my_text_field])
                    .parse_query(term)
                    .unwrap();
                let top_docs: Vec<(f32, DocAddress)> =
                    searcher.search(&query, &TopDocs::with_limit(3)).unwrap();

                top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>()
            };

            assert_eq!(do_search("some"), vec![1]);
            assert_eq!(do_search("blubber"), vec![4]);
            assert_eq!(do_search("biggest"), vec![6]);
        }

        // postings file
        {
            let my_text_field = index.schema().get_field("text_field").unwrap();
            let term_a = Term::from_field_text(my_text_field, "text");
            let inverted_index = segment_reader.inverted_index(my_text_field).unwrap();
            let mut postings = inverted_index
                .read_postings(&term_a, IndexRecordOption::WithFreqsAndPositions)
                .unwrap()
                .unwrap();
            assert_eq!(postings.doc_freq(), 6);

            assert_eq!(postings.term_freq(), 1);
            let mut output = vec![];
            postings.positions(&mut output);
            assert_eq!(output, vec![0]);
            postings.advance();

            assert_eq!(postings.term_freq(), 1);
            postings.positions(&mut output);
            assert_eq!(output, vec![1]);
        }

        {
            let col1_field = index.schema().get_field("col1").unwrap();
            let col2_field = index.schema().get_field("col2").unwrap();

            let row_index = segment_reader.row_fields().row_index();
            let row = row_index.get_row(0).unwrap();
            assert_eq!(
                row.get_by_field_id(col1_field.field_id())
                    .unwrap()
                    .as_u64()
                    .unwrap(),
                1
            );
            assert_eq!(
                row.get_by_field_id(col2_field.field_id())
                    .unwrap()
                    .as_u64()
                    .unwrap(),
                2
            );

            let row = row_index.get_row(1).unwrap();
            assert_eq!(
                row.get_by_field_id(col1_field.field_id())
                    .unwrap()
                    .as_u64()
                    .unwrap(),
                3
            );
            assert_eq!(
                row.get_by_field_id(col2_field.field_id())
                    .unwrap()
                    .as_u64()
                    .unwrap(),
                6
            );

            let row = row_index.get_row(2).unwrap();
            assert_eq!(
                row.get_by_field_id(col1_field.field_id())
                    .unwrap()
                    .as_u64()
                    .unwrap(),
                1
            );
            assert_eq!(
                row.get_by_field_id(col2_field.field_id())
                    .unwrap()
                    .as_u64()
                    .unwrap(),
                2
            );

            let row = row_index.get_row(3).unwrap();
            assert_eq!(
                row.get_by_field_id(col1_field.field_id())
                    .unwrap()
                    .as_u64()
                    .unwrap(),
                2
            );
            assert_eq!(
                row.get_by_field_id(col2_field.field_id())
                    .unwrap()
                    .as_u64()
                    .unwrap(),
                4
            );
        }
    }

    // #[test]
    // fn test_merge_sorted_index_asc() {
    //     let index = create_test_index(
    //         Some(IndexSettings {
    //             sort_by_field: Some(IndexSortByField {
    //                 field: "intval".to_string(),
    //                 order: Order::Asc,
    //             }),
    //             ..Default::default()
    //         }),
    //         false,
    //     )
    //     .unwrap();

    //     let int_field = index.schema().get_field("intval").unwrap();
    //     let multi_numbers = index.schema().get_field("multi_numbers").unwrap();
    //     let bytes_field = index.schema().get_field("bytes").unwrap();
    //     let reader = index.reader().unwrap();
    //     let searcher = reader.searcher();
    //     assert_eq!(searcher.segment_readers().len(), 1);
    //     let segment_reader = searcher.segment_readers().last().unwrap();

    //     let column_fields = segment_reader.column_fields();
    //     let column_field = column_fields.u64(int_field).unwrap();
    //     assert_eq!(column_field.get_val(0), 1u64);
    //     assert_eq!(column_field.get_val(1), 2u64);
    //     assert_eq!(column_field.get_val(2), 3u64);
    //     assert_eq!(column_field.get_val(3), 10u64);
    //     assert_eq!(column_field.get_val(4), 20u64);
    //     assert_eq!(column_field.get_val(5), 1_000u64);

    //     let get_vals = |column_field: &MultiValuedColumnFieldReader<u64>, doc_id: u32| -> Vec<u64> {
    //         let mut vals = vec![];
    //         column_field.get_vals(doc_id, &mut vals);
    //         vals
    //     };
    //     let column_fields = segment_reader.column_fields();
    //     let column_field = column_fields.u64s(multi_numbers).unwrap();
    //     assert_eq!(&get_vals(&column_field, 0), &[] as &[u64]);
    //     assert_eq!(&get_vals(&column_field, 1), &[2, 3]);
    //     assert_eq!(&get_vals(&column_field, 2), &[3, 4]);
    //     assert_eq!(&get_vals(&column_field, 3), &[10, 11]);
    //     assert_eq!(&get_vals(&column_field, 4), &[20]);
    //     assert_eq!(&get_vals(&column_field, 5), &[1001, 1002]);

    //     let column_field = column_fields.bytes(bytes_field).unwrap();
    //     assert_eq!(column_field.get_bytes(0), &[] as &[u8]);
    //     assert_eq!(column_field.get_bytes(2), &[1, 2, 3]);
    //     assert_eq!(column_field.get_bytes(5), &[5, 5]);

    //     // test new field norm mapping
    //     {
    //         let my_text_field = index.schema().get_field("text_field").unwrap();
    //         let fieldnorm_reader = segment_reader.get_fieldnorms_reader(my_text_field).unwrap();
    //         assert_eq!(fieldnorm_reader.fieldnorm(0), 0);
    //         assert_eq!(fieldnorm_reader.fieldnorm(1), 4);
    //         assert_eq!(fieldnorm_reader.fieldnorm(2), 2); // some text
    //         assert_eq!(fieldnorm_reader.fieldnorm(3), 1);
    //         assert_eq!(fieldnorm_reader.fieldnorm(5), 3); // the biggest num
    //     }

    //     let searcher = index.reader().unwrap().searcher();
    //     {
    //         let my_text_field = index.schema().get_field("text_field").unwrap();

    //         let do_search = |term: &str| {
    //             let query = QueryParser::for_index(&index, vec![my_text_field])
    //                 .parse_query(term)
    //                 .unwrap();
    //             let top_docs: Vec<(f32, DocAddress)> =
    //                 searcher.search(&query, &TopDocs::with_limit(3)).unwrap();

    //             top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>()
    //         };

    //         assert_eq!(do_search("some"), vec![2]);
    //         assert_eq!(do_search("blubber"), vec![3]);
    //         assert_eq!(do_search("biggest"), vec![5]);
    //     }

    //     // postings file
    //     {
    //         let my_text_field = index.schema().get_field("text_field").unwrap();
    //         let term_a = Term::from_field_text(my_text_field, "text");
    //         let inverted_index = segment_reader.inverted_index(my_text_field).unwrap();
    //         let mut postings = inverted_index
    //             .read_postings(&term_a, IndexRecordOption::WithFreqsAndPositions)
    //             .unwrap()
    //             .unwrap();

    //         assert_eq!(postings.doc_freq(), 2);
    //         let fallback_bitset = AliveBitSet::for_test_from_deleted_docs(&[0], 100);
    //         assert_eq!(
    //             postings.doc_freq_given_deletes(
    //                 segment_reader.alive_bitset().unwrap_or(&fallback_bitset)
    //             ),
    //             2
    //         );

    //         let mut output = vec![];
    //         postings.positions(&mut output);
    //         assert_eq!(output, vec![1, 3]);
    //         postings.advance();

    //         postings.positions(&mut output);
    //         assert_eq!(output, vec![1]);
    //     }

    //     // access doc store
    //     {
    //         let doc = searcher.doc(DocAddress::new(0, 0)).unwrap();
    //         assert_eq!(doc.get_first(int_field).unwrap().as_u64(), Some(1));
    //         let doc = searcher.doc(DocAddress::new(0, 1)).unwrap();
    //         assert_eq!(doc.get_first(int_field).unwrap().as_u64(), Some(2));
    //         let doc = searcher.doc(DocAddress::new(0, 2)).unwrap();
    //         assert_eq!(doc.get_first(int_field).unwrap().as_u64(), Some(3));
    //         let doc = searcher.doc(DocAddress::new(0, 3)).unwrap();
    //         assert_eq!(doc.get_first(int_field).unwrap().as_u64(), Some(10));
    //         let doc = searcher.doc(DocAddress::new(0, 4)).unwrap();
    //         assert_eq!(doc.get_first(int_field).unwrap().as_u64(), Some(20));
    //         let doc = searcher.doc(DocAddress::new(0, 5)).unwrap();
    //         assert_eq!(doc.get_first(int_field).unwrap().as_u64(), Some(1_000));
    //     }
    // }
}
