use std::ops::Range;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use smallvec::smallvec;

use super::operation::{AddOperation, UserOperation};
use super::segment_updater::SegmentUpdater;
use super::{AddBatch, AddBatchReceiver, AddBatchSender, MergeOperation, PreparedCommit};
use crate::directory::GarbageCollectionResult;
use crate::error::TantivyError;
use crate::index::{Index, Segment, SegmentId, SegmentMeta};
use crate::indexer::index_writer_status::IndexWriterStatus;
use crate::indexer::stamper::Stamper;
use crate::indexer::{MergePolicy, SegmentEntry, SegmentWriter};
use crate::schema::document::Document;
use crate::schema::TantivyDocument;
use crate::{FutureResult, Opstamp};

// Size of the margin for the `memory_arena`. A segment is closed when the remaining memory
// in the `memory_arena` goes below MARGIN_IN_BYTES.
pub const MARGIN_IN_BYTES: usize = 1_000_000;

// We impose the memory per thread to be at least 15 MB, as the baseline consumption is 12MB.
pub const MEMORY_BUDGET_NUM_BYTES_MIN: usize = ((MARGIN_IN_BYTES as u32) * 15u32) as usize;
pub const MEMORY_BUDGET_NUM_BYTES_MAX: usize = u32::MAX as usize - MARGIN_IN_BYTES;

// We impose the number of index writer threads to be at most this.
pub const MAX_NUM_THREAD: usize = 8;

// Add document will block if the number of docs waiting in the queue to be indexed
// reaches `PIPELINE_MAX_SIZE_IN_DOCS`
const PIPELINE_MAX_SIZE_IN_DOCS: usize = 10_000;

fn error_in_index_worker_thread(context: &str) -> TantivyError {
    TantivyError::ErrorInThread(format!(
        "{context}. A worker thread encountered an error (io::Error most likely) or panicked."
    ))
}

/// `IndexWriter` is the user entry-point to add document to an index.
///
/// It manages a small number of indexing thread, as well as a shared
/// indexing queue.
/// Each indexing thread builds its own independent [`Segment`], via
/// a `SegmentWriter` object.
pub struct IndexWriter<D: Document = TantivyDocument> {
    index: Index,

    // The memory budget per thread, after which a commit is triggered.
    memory_budget_in_bytes_per_thread: usize,

    workers_join_handle: Vec<JoinHandle<crate::Result<()>>>,

    index_writer_status: IndexWriterStatus<D>,
    operation_sender: AddBatchSender<D>,

    segment_updater: SegmentUpdater,

    worker_id: usize,

    num_threads: usize,

    stamper: Stamper,
    committed_opstamp: Opstamp,
}

fn index_documents<D: Document>(
    memory_budget: usize,
    segment: Segment,
    grouped_document_iterator: &mut dyn Iterator<Item = AddBatch<D>>,
    segment_updater: &SegmentUpdater,
) -> crate::Result<()> {
    let mut segment_writer = SegmentWriter::for_segment(memory_budget, segment.clone())?;
    for document_group in grouped_document_iterator {
        for doc in document_group {
            segment_writer.add_document(doc)?;
        }
        let mem_usage = segment_writer.mem_usage();
        if mem_usage >= memory_budget - MARGIN_IN_BYTES {
            info!(
                "Buffer limit reached, flushing segment with maxdoc={}.",
                segment_writer.max_doc()
            );
            break;
        }
    }

    if !segment_updater.is_alive() {
        return Ok(());
    }

    let max_doc = segment_writer.max_doc();

    // this is ensured by the call to peek before starting
    // the worker thread.
    assert!(max_doc > 0);

    let _ = segment_writer.finalize()?;

    let segment_with_max_doc = segment.with_max_doc(max_doc);

    let meta = segment_with_max_doc.meta().clone();
    meta.untrack_temp_docstore();
    // update segment_updater inventory to remove tempstore
    let segment_entry = SegmentEntry::new(meta);
    segment_updater.schedule_add_segment(segment_entry).wait()?;
    Ok(())
}

impl<D: Document> IndexWriter<D> {
    /// Create a new index writer. Attempts to acquire a lockfile.
    ///
    /// The lockfile should be deleted on drop, but it is possible
    /// that due to a panic or other error, a stale lockfile will be
    /// left in the index directory. If you are sure that no other
    /// `IndexWriter` on the system is accessing the index directory,
    /// it is safe to manually delete the lockfile.
    ///
    /// `num_threads` specifies the number of indexing workers that
    /// should work at the same time.
    /// # Errors
    /// If the lockfile already exists, returns `Error::FileAlreadyExists`.
    /// If the memory arena per thread is too small or too big, returns
    /// `TantivyError::InvalidArgument`
    pub(crate) fn new(
        index: &Index,
        num_threads: usize,
        memory_budget_in_bytes_per_thread: usize,
    ) -> crate::Result<Self> {
        if memory_budget_in_bytes_per_thread < MEMORY_BUDGET_NUM_BYTES_MIN {
            let err_msg = format!(
                "The memory arena in bytes per thread needs to be at least \
                 {MEMORY_BUDGET_NUM_BYTES_MIN}."
            );
            return Err(TantivyError::InvalidArgument(err_msg));
        }
        if memory_budget_in_bytes_per_thread >= MEMORY_BUDGET_NUM_BYTES_MAX {
            let err_msg = format!(
                "The memory arena in bytes per thread cannot exceed {MEMORY_BUDGET_NUM_BYTES_MAX}"
            );
            return Err(TantivyError::InvalidArgument(err_msg));
        }
        let (document_sender, document_receiver) =
            crossbeam_channel::bounded(PIPELINE_MAX_SIZE_IN_DOCS);

        let current_opstamp = index.load_metas()?.opstamp;

        let stamper = Stamper::new(current_opstamp);

        let segment_updater = SegmentUpdater::create(index.clone(), stamper.clone())?;

        let mut index_writer = Self {
            memory_budget_in_bytes_per_thread,
            index: index.clone(),
            index_writer_status: IndexWriterStatus::from(document_receiver),
            operation_sender: document_sender,

            segment_updater,

            workers_join_handle: vec![],
            num_threads,

            committed_opstamp: current_opstamp,
            stamper,

            worker_id: 0,
        };
        index_writer.start_workers()?;
        Ok(index_writer)
    }

    fn drop_sender(&mut self) {
        let (sender, _receiver) = crossbeam_channel::bounded(1);
        self.operation_sender = sender;
    }

    /// Accessor to the index.
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// If there are some merging threads, blocks until they all finish their work and
    /// then drop the `IndexWriter`.
    pub fn wait_merging_threads(mut self) -> crate::Result<()> {
        // this will stop the indexing thread,
        // dropping the last reference to the segment_updater.
        self.drop_sender();

        let former_workers_handles = std::mem::take(&mut self.workers_join_handle);
        for join_handle in former_workers_handles {
            join_handle
                .join()
                .map_err(|_| error_in_index_worker_thread("Worker thread panicked."))?
                .map_err(|_| error_in_index_worker_thread("Worker thread failed."))?;
        }

        let result = self
            .segment_updater
            .wait_merging_thread()
            .map_err(|_| error_in_index_worker_thread("Failed to join merging thread."));

        if let Err(ref e) = result {
            error!("Some merging thread failed {:?}", e);
        }

        result
    }

    #[doc(hidden)]
    pub fn add_segment(&self, segment_meta: SegmentMeta) -> crate::Result<()> {
        let segment_entry = SegmentEntry::new(segment_meta);
        self.segment_updater
            .schedule_add_segment(segment_entry)
            .wait()
    }

    /// Creates a new segment.
    ///
    /// This method is useful only for users trying to do complex
    /// operations, like converting an index format to another.
    ///
    /// It is safe to start writing file associated with the new `Segment`.
    /// These will not be garbage collected as long as an instance object of
    /// `SegmentMeta` object associated with the new `Segment` is "alive".
    pub fn new_segment(&self) -> Segment {
        self.index.new_segment()
    }

    fn operation_receiver(&self) -> crate::Result<AddBatchReceiver<D>> {
        self.index_writer_status
            .operation_receiver()
            .ok_or_else(|| {
                crate::TantivyError::ErrorInThread(
                    "The index writer was killed. It can happen if an indexing worker encountered \
                     an Io error for instance."
                        .to_string(),
                )
            })
    }

    /// Spawns a new worker thread for indexing.
    /// The thread consumes documents from the pipeline.
    fn add_indexing_worker(&mut self) -> crate::Result<()> {
        let document_receiver_clone = self.operation_receiver()?;
        let index_writer_bomb = self.index_writer_status.create_bomb();

        let segment_updater = self.segment_updater.clone();

        let mem_budget = self.memory_budget_in_bytes_per_thread;
        let index = self.index.clone();
        let join_handle: JoinHandle<crate::Result<()>> = thread::Builder::new()
            .name(format!("thrd-tantivy-index{}", self.worker_id))
            .spawn(move || {
                loop {
                    let mut document_iterator = document_receiver_clone
                        .clone()
                        .into_iter()
                        .filter(|batch| !batch.is_empty())
                        .peekable();

                    // The peeking here is to avoid creating a new segment's files
                    // if no document are available.
                    //
                    // This is a valid guarantee as the peeked document now belongs to
                    // our local iterator.
                    if let Some(batch) = document_iterator.peek() {
                        debug_assert!(!batch.is_empty());
                    } else {
                        // No more documents.
                        // It happens when there is a commit, or if the `IndexWriter`
                        // was dropped.
                        index_writer_bomb.defuse();
                        return Ok(());
                    }

                    index_documents(
                        mem_budget,
                        index.new_segment(),
                        &mut document_iterator,
                        &segment_updater,
                    )?;
                }
            })?;
        self.worker_id += 1;
        self.workers_join_handle.push(join_handle);
        Ok(())
    }

    /// Accessor to the merge policy.
    pub fn get_merge_policy(&self) -> Arc<dyn MergePolicy> {
        self.segment_updater.get_merge_policy()
    }

    /// Setter for the merge policy.
    pub fn set_merge_policy(&self, merge_policy: Box<dyn MergePolicy>) {
        self.segment_updater.set_merge_policy(merge_policy);
    }

    fn start_workers(&mut self) -> crate::Result<()> {
        for _ in 0..self.num_threads {
            self.add_indexing_worker()?;
        }
        Ok(())
    }

    /// Detects and removes the files that are not used by the index anymore.
    pub fn garbage_collect_files(&self) -> FutureResult<GarbageCollectionResult> {
        self.segment_updater.schedule_garbage_collect()
    }

    /// Deletes all documents from the index
    ///
    /// Requires `commit`ing
    /// Enables users to rebuild the index,
    /// by clearing and resubmitting necessary documents
    pub fn delete_all_documents(&self) -> crate::Result<Opstamp> {
        // Delete segments
        self.segment_updater.remove_all_segments();
        // Return new stamp - reverted stamp
        self.stamper.revert(self.committed_opstamp);
        Ok(self.committed_opstamp)
    }

    /// Merges a given list of segments.
    ///
    /// If all segments are empty no new segment will be created.
    ///
    /// `segment_ids` is required to be non-empty.
    pub fn merge(&mut self, segment_ids: &[SegmentId]) -> FutureResult<Option<SegmentMeta>> {
        let merge_operation = self.segment_updater.make_merge_operation(segment_ids);
        let segment_updater = self.segment_updater.clone();
        segment_updater.merge(merge_operation)
    }

    /// Start a merge operation.
    ///
    /// Returns a future that resolves to a tuple of the segment entry and the merge operation.
    /// When the future is resolved, the merged segment has been created and committed.
    ///
    /// `end_merge` must be called to complete the merge operation.
    pub fn start_merge(
        &self,
        segment_ids: &[SegmentId],
    ) -> FutureResult<(Option<SegmentEntry>, MergeOperation)> {
        let merge_operation = self.segment_updater.make_merge_operation(segment_ids);
        self.segment_updater.start_merge(merge_operation)
    }

    /// End a merge operation.
    ///
    /// This method must be called to perform the necessary cleanup after a merge operation.
    pub fn end_merge(
        &mut self,
        merge_operation: MergeOperation,
        segment_entry: Option<SegmentEntry>,
    ) -> crate::Result<Option<SegmentMeta>> {
        self.segment_updater
            .end_merge(merge_operation, segment_entry)
    }

    /// Closes the current document channel send.
    /// and replace all the channels by new ones.
    ///
    /// The current workers will keep on indexing
    /// the pending document and stop
    /// when no documents are remaining.
    ///
    /// Returns the former segment_ready channel.
    fn recreate_document_channel(&mut self) {
        let (document_sender, document_receiver) =
            crossbeam_channel::bounded(PIPELINE_MAX_SIZE_IN_DOCS);
        self.operation_sender = document_sender;
        self.index_writer_status = IndexWriterStatus::from(document_receiver);
    }

    /// Rollback to the last commit
    ///
    /// This cancels all of the updates that
    /// happened after the last commit.
    /// After calling rollback, the index is in the same
    /// state as it was after the last commit.
    ///
    /// The opstamp at the last commit is returned.
    pub fn rollback(&mut self) -> crate::Result<Opstamp> {
        info!("Rolling back to opstamp {}", self.committed_opstamp);
        // marks the segment updater as killed. From now on, all
        // segment updates will be ignored.
        self.segment_updater.kill();
        let document_receiver_res = self.operation_receiver();

        let new_index_writer = IndexWriter::new(
            &self.index,
            self.num_threads,
            self.memory_budget_in_bytes_per_thread,
        )?;

        // the current `self` is dropped right away because of this call.
        //
        // This will drop the document queue, and the thread
        // should terminate.
        *self = new_index_writer;

        // Drains the document receiver pipeline :
        // Workers don't need to index the pending documents.
        //
        // This will reach an end as the only document_sender
        // was dropped with the index_writer.
        if let Ok(document_receiver) = document_receiver_res {
            for _ in document_receiver {}
        }

        Ok(self.committed_opstamp)
    }

    /// Prepares a commit.
    ///
    /// Calling `prepare_commit()` will cut the indexing
    /// queue. All pending documents will be sent to the
    /// indexing workers. They will then terminate, regardless
    /// of the size of their current segment and flush their
    /// work on disk.
    ///
    /// Once a commit is "prepared", you can either
    /// call
    /// * `.commit()`: to accept this commit
    /// * `.abort()`: to cancel this commit.
    ///
    /// In the current implementation, [`PreparedCommit`] borrows
    /// the [`IndexWriter`] mutably so we are guaranteed that no new
    /// document can be added as long as it is committed or is
    /// dropped.
    ///
    /// It is also possible to add a payload to the `commit`
    /// using this API.
    /// See [`PreparedCommit::set_payload()`].
    pub fn prepare_commit(&mut self) -> crate::Result<PreparedCommit<D>> {
        // Here, because we join all of the worker threads,
        // all of the segment update for this commit have been
        // sent.
        //
        // No document belonging to the next commit have been
        // pushed too, because add_document can only happen
        // on this thread.
        //
        // This will move uncommitted segments to the state of
        // committed segments.
        info!("Preparing commit");

        // this will drop the current document channel
        // and recreate a new one.
        self.recreate_document_channel();

        let former_workers_join_handle = std::mem::take(&mut self.workers_join_handle);

        for worker_handle in former_workers_join_handle {
            let indexing_worker_result = worker_handle
                .join()
                .map_err(|e| TantivyError::ErrorInThread(format!("{e:?}")))?;
            indexing_worker_result?;
            self.add_indexing_worker()?;
        }

        let commit_opstamp = self.stamper.stamp();
        let prepared_commit = PreparedCommit::new(self, commit_opstamp);
        info!("Prepared commit {}", commit_opstamp);
        Ok(prepared_commit)
    }

    /// Commits all of the pending changes
    ///
    /// A call to commit blocks.
    /// After it returns, all of the document that
    /// were added since the last commit are published
    /// and persisted.
    ///
    /// In case of a crash or an hardware failure (as
    /// long as the hard disk is spared), it will be possible
    /// to resume indexing from this point.
    ///
    /// Commit returns the `opstamp` of the last document
    /// that made it in the commit.
    pub fn commit(&mut self) -> crate::Result<Opstamp> {
        self.prepare_commit()?.commit()
    }

    pub(crate) fn segment_updater(&self) -> &SegmentUpdater {
        &self.segment_updater
    }

    /// Returns the opstamp of the last successful commit.
    ///
    /// This is, for instance, the opstamp the index will
    /// rollback to if there is a failure like a power surge.
    ///
    /// This is also the opstamp of the commit that is currently
    /// available for searchers.
    pub fn commit_opstamp(&self) -> Opstamp {
        self.committed_opstamp
    }

    /// Adds a document.
    ///
    /// If the indexing pipeline is full, this call may block.
    ///
    /// The opstamp is an increasing `u64` that can
    /// be used by the client to align commits with its own
    /// document queue.
    pub fn add_document(&self, document: D) -> crate::Result<Opstamp> {
        let opstamp = self.stamper.stamp();
        self.send_add_documents_batch(smallvec![AddOperation { opstamp, document }])?;
        Ok(opstamp)
    }

    /// Gets a range of stamps from the stamper and "pops" the last stamp
    /// from the range returning a tuple of the last optstamp and the popped
    /// range.
    ///
    /// The total number of stamps generated by this method is `count + 1`;
    /// each operation gets a stamp from the `stamps` iterator and `last_opstamp`
    /// is for the batch itself.
    fn get_batch_opstamps(&self, count: Opstamp) -> (Opstamp, Range<Opstamp>) {
        let Range { start, end } = self.stamper.stamps(count + 1u64);
        let last_opstamp = end - 1;
        (last_opstamp, start..last_opstamp)
    }

    /// Runs a group of document operations ensuring that the operations are
    /// assigned contiguous u64 opstamps and that add operations of the same
    /// group are flushed into the same segment.
    ///
    /// If the indexing pipeline is full, this call may block.
    ///
    /// Each operation of the given `user_operations` will receive an in-order,
    /// contiguous u64 opstamp. The entire batch itself is also given an
    /// opstamp that is 1 greater than the last given operation. This
    /// `batch_opstamp` is the return value of `run`. An empty group of
    /// `user_operations`, an empty `Vec<UserOperation>`, still receives
    /// a valid opstamp even though no changes were _actually_ made to the index.
    ///
    /// Like adds and deletes (see `IndexWriter.add_document` and
    /// `IndexWriter.delete_term`), the changes made by calling `run` will be
    /// visible to readers only after calling `commit()`.
    pub fn run<I>(&self, user_operations: I) -> crate::Result<Opstamp>
    where
        I: IntoIterator<Item = UserOperation<D>>,
        I::IntoIter: ExactSizeIterator,
    {
        let user_operations_it = user_operations.into_iter();
        let count = user_operations_it.len() as u64;
        if count == 0 {
            return Ok(self.stamper.stamp());
        }
        let (batch_opstamp, stamps) = self.get_batch_opstamps(count);

        let mut adds = AddBatch::default();

        for (user_op, opstamp) in user_operations_it.zip(stamps) {
            match user_op {
                UserOperation::Add(document) => {
                    let add_operation = AddOperation { opstamp, document };
                    adds.push(add_operation);
                }
            }
        }
        self.send_add_documents_batch(adds)?;
        Ok(batch_opstamp)
    }

    fn send_add_documents_batch(&self, add_ops: AddBatch<D>) -> crate::Result<()> {
        if self.index_writer_status.is_alive() && self.operation_sender.send(add_ops).is_ok() {
            Ok(())
        } else {
            Err(error_in_index_worker_thread("An index writer was killed."))
        }
    }
}

impl<D: Document> Drop for IndexWriter<D> {
    fn drop(&mut self) {
        self.segment_updater.kill();
        self.drop_sender();
        for work in self.workers_join_handle.drain(..) {
            let _ = work.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use crate::columnar::Column;

    use super::super::operation::UserOperation;
    use crate::collector::{Count, TopDocs};
    use crate::indexer::index_writer::MEMORY_BUDGET_NUM_BYTES_MIN;
    use crate::indexer::NoMergePolicy;
    use crate::query::{QueryParser, TermQuery};
    use crate::schema::{
        self, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, COLUMN, INDEXED,
        STORED, STRING, TEXT,
    };
    use crate::store::DOCSTORE_CACHE_CAPACITY;
    use crate::{
        DateTime, DocAddress, Index, IndexSettings, IndexSortByField, IndexWriter, Order,
        ReloadPolicy, TantivyDocument, Term,
    };

    const LOREM: &str = "Doc Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
                         eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad \
                         minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip \
                         ex ea commodo consequat. Duis aute irure dolor in reprehenderit in \
                         voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur \
                         sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt \
                         mollit anim id est laborum.";

    #[test]
    fn test_operations_group() {
        // an operations group with 2 items should cause 3 opstamps 0, 1, and 2.
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let index_writer = index.writer_for_tests().unwrap();
        let operations = vec![
            UserOperation::Add(doc!(text_field=>"a")),
            UserOperation::Add(doc!(text_field=>"b")),
        ];
        let batch_opstamp1 = index_writer.run(operations).unwrap();
        assert_eq!(batch_opstamp1, 2u64);
    }

    #[test]
    fn test_empty_operations_group() {
        let schema_builder = schema::Schema::builder();
        let index = Index::create_in_ram(schema_builder.build());
        let index_writer: IndexWriter = index.writer_for_tests().unwrap();
        let operations1 = vec![];
        let batch_opstamp1 = index_writer.run(operations1).unwrap();
        assert_eq!(batch_opstamp1, 0u64);
        let operations2 = vec![];
        let batch_opstamp2 = index_writer.run(operations2).unwrap();
        assert_eq!(batch_opstamp2, 1u64);
    }

    #[test]
    fn test_set_merge_policy() {
        let schema_builder = schema::Schema::builder();
        let index = Index::create_in_ram(schema_builder.build());
        let index_writer: IndexWriter = index.writer_for_tests().unwrap();
        assert_eq!(
            format!("{:?}", index_writer.get_merge_policy()),
            "LogMergePolicy { min_num_segments: 8, max_docs_before_merge: 10000000, \
             min_layer_size: 10000, level_log_size: 0.75, del_docs_ratio_before_merge: 1.0 }"
        );
        let merge_policy = Box::<NoMergePolicy>::default();
        index_writer.set_merge_policy(merge_policy);
        assert_eq!(
            format!("{:?}", index_writer.get_merge_policy()),
            "NoMergePolicy"
        );
    }

    #[test]
    fn test_lockfile_released_on_drop() {
        let schema_builder = schema::Schema::builder();
        let index = Index::create_in_ram(schema_builder.build());
        {
            let _index_writer: IndexWriter = index.writer_for_tests().unwrap();
            // the lock should be released when the
            // index_writer leaves the scope.
        }
        let _index_writer_two: IndexWriter = index.writer_for_tests().unwrap();
    }

    #[test]
    fn test_commit_and_rollback() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let num_docs_containing = |s: &str| {
            let searcher = reader.searcher();
            let term = Term::from_field_text(text_field, s);
            searcher.doc_freq(&term).unwrap()
        };

        {
            // writing the segment
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field=>"a"))?;
            index_writer.rollback()?;
            assert_eq!(index_writer.commit_opstamp(), 0u64);
            assert_eq!(num_docs_containing("a"), 0);
            index_writer.add_document(doc!(text_field=>"b"))?;
            index_writer.add_document(doc!(text_field=>"c"))?;
            index_writer.commit()?;
            reader.reload()?;
            assert_eq!(num_docs_containing("a"), 0);
            assert_eq!(num_docs_containing("b"), 1);
            assert_eq!(num_docs_containing("c"), 1);
        }
        reader.reload()?;
        reader.searcher();
        Ok(())
    }

    #[test]
    fn test_merge_on_empty_segments_single_segment() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let num_docs_containing = |s: &str| {
            let term_a = Term::from_field_text(text_field, s);
            reader.searcher().doc_freq(&term_a).unwrap()
        };
        // writing the segment
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.commit()?;
        //  this should create 1 segment

        let segments = index.searchable_segment_ids().unwrap();
        assert_eq!(segments.len(), 1);

        reader.reload().unwrap();
        assert_eq!(num_docs_containing("a"), 1);

        Ok(())
    }

    #[test]
    fn test_merge_on_empty_segments() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let num_docs_containing = |s: &str| {
            let term_a = Term::from_field_text(text_field, s);
            reader.searcher().doc_freq(&term_a).unwrap()
        };
        // writing the segment
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.commit()?;
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.commit()?;
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.commit()?;
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.commit()?;
        //  this should create 4 segments

        let segments = index.searchable_segment_ids().unwrap();
        assert_eq!(segments.len(), 4);

        reader.reload().unwrap();
        assert_eq!(num_docs_containing("a"), 4);

        index_writer.merge(&segments);
        index_writer.wait_merging_threads().unwrap();

        let segments = index.searchable_segment_ids().unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(num_docs_containing("a"), 4);

        Ok(())
    }

    #[test]
    fn test_with_merges() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let num_docs_containing = |s: &str| {
            let term_a = Term::from_field_text(text_field, s);
            reader.searcher().doc_freq(&term_a).unwrap()
        };
        // writing the segment
        let mut index_writer = index.writer(MEMORY_BUDGET_NUM_BYTES_MIN).unwrap();
        // create 8 segments with 100 tiny docs
        for _doc in 0..100 {
            index_writer.add_document(doc!(text_field=>"a"))?;
        }
        index_writer.commit()?;
        for _doc in 0..100 {
            index_writer.add_document(doc!(text_field=>"a"))?;
        }
        //  this should create 8 segments and trigger a merge.
        index_writer.commit()?;
        index_writer.wait_merging_threads()?;
        reader.reload()?;
        assert_eq!(num_docs_containing("a"), 200);
        assert!(index.searchable_segments()?.len() < 8);
        Ok(())
    }

    #[test]
    fn test_prepare_with_commit_message() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        let mut index_writer = index.writer_for_tests()?;
        for _doc in 0..100 {
            index_writer.add_document(doc!(text_field => "a"))?;
        }
        {
            let mut prepared_commit = index_writer.prepare_commit()?;
            prepared_commit.set_payload("first commit");
            prepared_commit.commit()?;
        }
        {
            let metas = index.load_metas()?;
            assert_eq!(metas.payload.unwrap(), "first commit");
        }
        for _doc in 0..100 {
            index_writer.add_document(doc!(text_field => "a"))?;
        }
        index_writer.commit()?;
        {
            let metas = index.load_metas()?;
            assert!(metas.payload.is_none());
        }
        Ok(())
    }

    #[test]
    fn test_prepare_but_rollback() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        {
            // writing the segment
            let mut index_writer =
                index.writer_with_num_threads(4, MEMORY_BUDGET_NUM_BYTES_MIN * 4)?;
            // create 8 segments with 100 tiny docs
            for _doc in 0..100 {
                index_writer.add_document(doc!(text_field => "a"))?;
            }
            {
                let mut prepared_commit = index_writer.prepare_commit()?;
                prepared_commit.set_payload("first commit");
                prepared_commit.abort()?;
            }
            {
                let metas = index.load_metas()?;
                assert!(metas.payload.is_none());
            }
            for _doc in 0..100 {
                index_writer.add_document(doc!(text_field => "b"))?;
            }
            index_writer.commit()?;
        }
        let num_docs_containing = |s: &str| {
            let term_a = Term::from_field_text(text_field, s);
            index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?
                .searcher()
                .doc_freq(&term_a)
        };
        assert_eq!(num_docs_containing("a")?, 0);
        assert_eq!(num_docs_containing("b")?, 100);
        Ok(())
    }

    #[test]
    fn test_add_then_delete_all_documents() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        let num_docs_containing = |s: &str| {
            reader.reload().unwrap();
            let searcher = reader.searcher();
            let term = Term::from_field_text(text_field, s);
            searcher.doc_freq(&term).unwrap()
        };
        let mut index_writer = index
            .writer_with_num_threads(4, MEMORY_BUDGET_NUM_BYTES_MIN * 4)
            .unwrap();

        let add_tstamp = index_writer.add_document(doc!(text_field => "a")).unwrap();
        let commit_tstamp = index_writer.commit().unwrap();
        assert!(commit_tstamp > add_tstamp);
        index_writer.delete_all_documents().unwrap();
        index_writer.commit().unwrap();

        // Search for documents with the same term that we added
        assert_eq!(num_docs_containing("a"), 0);
    }

    #[test]
    fn test_delete_all_documents_rollback_correct_stamp() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index
            .writer_with_num_threads(4, MEMORY_BUDGET_NUM_BYTES_MIN * 4)
            .unwrap();

        let add_tstamp = index_writer.add_document(doc!(text_field => "a")).unwrap();

        // commit documents - they are now available
        let first_commit = index_writer.commit();
        assert!(first_commit.is_ok());
        let first_commit_tstamp = first_commit.unwrap();
        assert!(first_commit_tstamp > add_tstamp);

        // delete_all_documents the index
        let clear_tstamp = index_writer.delete_all_documents().unwrap();
        assert_eq!(clear_tstamp, add_tstamp);

        // commit the clear command - now documents aren't available
        let second_commit = index_writer.commit();
        assert!(second_commit.is_ok());
        let second_commit_tstamp = second_commit.unwrap();

        // add new documents again
        for _ in 0..100 {
            index_writer.add_document(doc!(text_field => "b")).unwrap();
        }

        // rollback to last commit, when index was empty
        let rollback = index_writer.rollback();
        assert!(rollback.is_ok());
        let rollback_tstamp = rollback.unwrap();
        assert_eq!(rollback_tstamp, second_commit_tstamp);

        // working with an empty index == no documents
        let term_b = Term::from_field_text(text_field, "b");
        assert_eq!(
            index
                .reader()
                .unwrap()
                .searcher()
                .doc_freq(&term_b)
                .unwrap(),
            0
        );
    }

    #[test]
    fn test_delete_all_documents_then_add() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        // writing the segment
        let mut index_writer = index
            .writer_with_num_threads(4, MEMORY_BUDGET_NUM_BYTES_MIN * 4)
            .unwrap();
        let res = index_writer.delete_all_documents();
        assert!(res.is_ok());

        assert!(index_writer.commit().is_ok());
        // add one simple doc
        index_writer.add_document(doc!(text_field => "a")).unwrap();
        assert!(index_writer.commit().is_ok());

        let term_a = Term::from_field_text(text_field, "a");
        // expect the document with that term to be in the index
        assert_eq!(
            index
                .reader()
                .unwrap()
                .searcher()
                .doc_freq(&term_a)
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_delete_all_documents_and_rollback() {
        let mut schema_builder = schema::Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index
            .writer_with_num_threads(4, MEMORY_BUDGET_NUM_BYTES_MIN * 4)
            .unwrap();

        // add one simple doc
        assert!(index_writer.add_document(doc!(text_field => "a")).is_ok());
        let comm = index_writer.commit();
        assert!(comm.is_ok());
        let commit_tstamp = comm.unwrap();

        // clear but don't commit!
        let clear_tstamp = index_writer.delete_all_documents().unwrap();
        // clear_tstamp should reset to before the last commit
        assert!(clear_tstamp < commit_tstamp);

        // rollback
        let _rollback_tstamp = index_writer.rollback().unwrap();
        // Find original docs in the index
        let term_a = Term::from_field_text(text_field, "a");
        // expect the document with that term to be in the index
        assert_eq!(
            index
                .reader()
                .unwrap()
                .searcher()
                .doc_freq(&term_a)
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_delete_all_documents_empty_index() {
        let schema_builder = schema::Schema::builder();
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index
            .writer_with_num_threads(4, MEMORY_BUDGET_NUM_BYTES_MIN * 4)
            .unwrap();
        let clear = index_writer.delete_all_documents();
        let commit = index_writer.commit();
        assert!(clear.is_ok());
        assert!(commit.is_ok());
    }

    #[test]
    fn test_delete_all_documents_index_twice() {
        let schema_builder = schema::Schema::builder();
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index
            .writer_with_num_threads(4, MEMORY_BUDGET_NUM_BYTES_MIN * 4)
            .unwrap();
        let clear = index_writer.delete_all_documents();
        let commit = index_writer.commit();
        assert!(clear.is_ok());
        assert!(commit.is_ok());
        let clear_again = index_writer.delete_all_documents();
        let commit_again = index_writer.commit();
        assert!(clear_again.is_ok());
        assert!(commit_again.is_ok());
    }

    #[derive(Debug, Clone)]
    enum IndexingOp {
        AddMultipleDoc {
            id: u64,
            num_docs: u64,
            value: IndexValue,
        },
        AddDoc {
            id: u64,
            value: IndexValue,
        },
        Commit,
        Merge,
    }
    impl IndexingOp {
        fn add(id: u64) -> Self {
            IndexingOp::AddDoc {
                id,
                value: IndexValue::F64(id as f64),
            }
        }
    }

    use serde::Serialize;
    #[derive(Debug, Clone, Serialize)]
    #[serde(untagged)]
    enum IndexValue {
        F64(f64),
        U64(u64),
    }
    impl Default for IndexValue {
        fn default() -> Self {
            IndexValue::F64(0.0)
        }
    }

    fn expected_ids(ops: &[IndexingOp]) -> (HashMap<u64, u64>, HashSet<u64>) {
        let mut existing_ids = HashMap::new();
        let mut deleted_ids = HashSet::new();
        for op in ops {
            match op {
                IndexingOp::AddDoc { id, value: _ } => {
                    *existing_ids.entry(*id).or_insert(0) += 1;
                    deleted_ids.remove(id);
                }
                IndexingOp::AddMultipleDoc {
                    id,
                    num_docs,
                    value: _,
                } => {
                    *existing_ids.entry(*id).or_insert(0) += num_docs;
                    deleted_ids.remove(id);
                }
                _ => {}
            }
        }
        (existing_ids, deleted_ids)
    }

    fn test_operation_strategy(
        ops: &[IndexingOp],
        sort_index: bool,
        force_end_merge: bool,
    ) -> crate::Result<Index> {
        let mut schema_builder = schema::Schema::builder();
        let json_field = schema_builder.add_json_field("json", COLUMN | TEXT | STORED);
        let i64_field = schema_builder.add_i64_field("i64", INDEXED);
        let id_field = schema_builder.add_u64_field("id", COLUMN | INDEXED | STORED);
        let f64_field = schema_builder.add_f64_field("f64", INDEXED);
        let date_field = schema_builder.add_date_field("date", INDEXED);
        let bytes_field = schema_builder.add_bytes_field("bytes", COLUMN | INDEXED | STORED);
        let bool_field = schema_builder.add_bool_field("bool", COLUMN | INDEXED | STORED);
        let text_field = schema_builder.add_text_field(
            "text_field",
            TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_index_option(schema::IndexRecordOption::WithFreqsAndPositions),
                )
                .set_stored(),
        );

        let large_text_field = schema_builder.add_text_field("large_text_field", TEXT | STORED);

        let schema = schema_builder.build();
        let settings = if sort_index {
            IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "id".to_string(),
                    order: Order::Asc,
                }),
                ..Default::default()
            }
        } else {
            IndexSettings {
                ..Default::default()
            }
        };
        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_ram()?;
        let mut index_writer = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));

        let old_reader = index.reader()?;

        // Every 3rd doc has only id field
        let id_is_full_doc = |id| id % 3 != 0;

        let add_docs = |index_writer: &mut IndexWriter,
                        id: u64,
                        value: IndexValue,
                        num: u64|
         -> crate::Result<()> {
            let doc = if !id_is_full_doc(id) {
                // every 3rd doc has no ip field
                doc!(
                    id_field=>id,
                )
            } else {
                let json = json!({"date1": format!("2022-{id}-01T00:00:01Z"), "date2": format!("{id}-05-01T00:00:01Z"), "id": id, "val": value});
                doc!(id_field=>id,
                        json_field=>json,
                        bytes_field => id.to_le_bytes().as_slice(),
                        bool_field => (id % 2u64) != 0,
                        i64_field => id as i64,
                        f64_field => id as f64,
                        date_field => DateTime::from_timestamp_secs(id as i64),
                        text_field => id.to_string(),
                        large_text_field => LOREM,
                )
            };
            for _ in 0..num {
                index_writer.add_document(doc.clone())?;
            }
            Ok(())
        };
        for op in ops {
            match op.clone() {
                IndexingOp::AddMultipleDoc {
                    id,
                    num_docs,
                    value,
                } => {
                    add_docs(&mut index_writer, id, value, num_docs)?;
                }
                IndexingOp::AddDoc { id, value } => {
                    add_docs(&mut index_writer, id, value, 1)?;
                }
                IndexingOp::Commit => {
                    index_writer.commit()?;
                }
                IndexingOp::Merge => {
                    let mut segment_ids = index
                        .searchable_segment_ids()
                        .expect("Searchable segments failed.");
                    segment_ids.sort();
                    if segment_ids.len() >= 2 {
                        index_writer.merge(&segment_ids).wait().unwrap();
                        assert!(index_writer.segment_updater().wait_merging_thread().is_ok());
                    }
                }
            }
        }
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        let num_segments_before_merge = searcher.segment_readers().len();
        if force_end_merge {
            index_writer.wait_merging_threads()?;
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            if segment_ids.len() >= 2 {
                index_writer.merge(&segment_ids).wait().unwrap();
                assert!(index_writer.wait_merging_threads().is_ok());
            }
        }
        let num_segments_after_merge = searcher.segment_readers().len();

        old_reader.reload()?;
        let old_searcher = old_reader.searcher();

        let ids_old_searcher: HashSet<u64> = old_searcher
            .segment_readers()
            .iter()
            .flat_map(|segment_reader| {
                let ff_reader = segment_reader.column_fields().u64("id").unwrap();
                segment_reader
                    .doc_ids()
                    .flat_map(move |doc| ff_reader.first(doc).into_iter())
            })
            .collect();

        let ids: HashSet<u64> = searcher
            .segment_readers()
            .iter()
            .flat_map(|segment_reader| {
                let ff_reader = segment_reader.column_fields().u64("id").unwrap();
                segment_reader
                    .doc_ids()
                    .flat_map(move |doc| ff_reader.first(doc).into_iter())
            })
            .collect();

        let (expected_ids_and_num_occurrences, deleted_ids) = expected_ids(ops);

        let num_docs_expected = expected_ids_and_num_occurrences
            .values()
            .map(|id_occurrences| *id_occurrences as usize)
            .sum::<usize>();

        assert_eq!(searcher.num_docs() as usize, num_docs_expected);
        assert_eq!(old_searcher.num_docs() as usize, num_docs_expected);
        assert_eq!(
            ids_old_searcher,
            expected_ids_and_num_occurrences
                .keys()
                .cloned()
                .collect::<HashSet<_>>()
        );
        assert_eq!(
            ids,
            expected_ids_and_num_occurrences
                .keys()
                .cloned()
                .collect::<HashSet<_>>()
        );

        if force_end_merge && num_segments_before_merge > 1 && num_segments_after_merge == 1 {
            // Test columnfield num_docs
            let num_docs: usize = searcher
                .segment_readers()
                .iter()
                .map(|segment_reader| {
                    let ff_reader = segment_reader
                        .column_fields()
                        .column_opt::<i64>("i64")
                        .unwrap()
                        .unwrap();
                    ff_reader.num_docs() as usize
                })
                .sum();
            assert_eq!(num_docs, num_docs_expected);
        }

        // doc store tests
        for segment_reader in searcher.segment_readers().iter() {
            let store_reader = segment_reader
                .get_store_reader(DOCSTORE_CACHE_CAPACITY)
                .unwrap();
            // test store iterator
            for doc in store_reader.iter::<TantivyDocument>() {
                let id = doc
                    .unwrap()
                    .get_first(id_field)
                    .unwrap()
                    .as_value()
                    .as_u64()
                    .unwrap();
                assert!(expected_ids_and_num_occurrences.contains_key(&id));
            }
            // test store random access
            for doc_id in segment_reader.doc_ids() {
                let id = store_reader
                    .get::<TantivyDocument>(doc_id)
                    .unwrap()
                    .get_first(id_field)
                    .unwrap()
                    .as_u64()
                    .unwrap();
                assert!(expected_ids_and_num_occurrences.contains_key(&id));
            }
        }
        // test search
        let count_search = |term: &str, field| {
            let query = QueryParser::for_index(&index, vec![field])
                .parse_query(term)
                .unwrap();
            searcher.search(&query, &Count).unwrap()
        };

        let count_search2 = |term: Term| {
            let query = TermQuery::new(term, IndexRecordOption::Basic);
            searcher.search(&query, &Count).unwrap()
        };

        for (id, count) in &expected_ids_and_num_occurrences {
            // skip expensive queries
            let (existing_id, count) = (*id, *count);
            let get_num_hits = |field| count_search(&existing_id.to_string(), field) as u64;
            assert_eq!(get_num_hits(id_field), count);
            if !id_is_full_doc(existing_id) {
                continue;
            }
            assert_eq!(get_num_hits(text_field), count);
            assert_eq!(get_num_hits(i64_field), count);
            assert_eq!(get_num_hits(f64_field), count);

            // Test bytes
            let term = Term::from_field_bytes(bytes_field, existing_id.to_le_bytes().as_slice());
            assert_eq!(count_search2(term) as u64, count);

            // Test date
            let term = Term::from_field_date(
                date_field,
                DateTime::from_timestamp_secs(existing_id as i64),
            );
            assert_eq!(count_search2(term) as u64, count);
        }
        for deleted_id in deleted_ids {
            let assert_field = |field| {
                assert_eq!(count_search(&deleted_id.to_string(), field) as u64, 0);
            };
            assert_field(text_field);
            assert_field(f64_field);
            assert_field(i64_field);
            assert_field(id_field);

            // Test bytes
            let term = Term::from_field_bytes(bytes_field, deleted_id.to_le_bytes().as_slice());
            assert_eq!(count_search2(term), 0);

            // Test date
            let term =
                Term::from_field_date(date_field, DateTime::from_timestamp_secs(deleted_id as i64));
            assert_eq!(count_search2(term), 0);
        }

        // Test if index property is in sort order
        if sort_index {
            // load all id in each segment and check they are in order

            for reader in searcher.segment_readers() {
                let (ff_reader, _) = reader.column_fields().u64_lenient("id").unwrap().unwrap();
                let mut ids_in_segment: Vec<u64> = Vec::new();

                for doc in 0..reader.num_docs() {
                    ids_in_segment.extend(ff_reader.first(doc));
                }

                assert!(is_sorted(&ids_in_segment));

                fn is_sorted<T>(data: &[T]) -> bool
                where
                    T: Ord,
                {
                    data.windows(2).all(|w| w[0] <= w[1])
                }
            }
        }
        Ok(index)
    }

    #[test]
    fn test_column_field_range() {
        let ops: Vec<_> = (0..1000).map(IndexingOp::add).collect();
        assert!(test_operation_strategy(&ops, false, true).is_ok());
    }

    #[test]
    fn test_sort_index_on_opt_field_regression() {
        assert!(
            test_operation_strategy(&[IndexingOp::add(81), IndexingOp::add(70),], true, false)
                .is_ok()
        );
    }

    #[test]
    fn test_simple_multiple_doc() {
        assert!(test_operation_strategy(
            &[
                IndexingOp::AddMultipleDoc {
                    id: 7,
                    num_docs: 800,
                    value: IndexValue::U64(0),
                },
                IndexingOp::AddMultipleDoc {
                    id: 92,
                    num_docs: 800,
                    value: IndexValue::U64(0),
                },
                IndexingOp::AddMultipleDoc {
                    id: 30,
                    num_docs: 800,
                    value: IndexValue::U64(0),
                },
                IndexingOp::AddMultipleDoc {
                    id: 33,
                    num_docs: 800,
                    value: IndexValue::U64(0),
                },
            ],
            true,
            false
        )
        .is_ok());
    }

    #[test]
    fn test_minimal_sort_force_end_merge() {
        assert!(test_operation_strategy(
            &[IndexingOp::add(23), IndexingOp::add(13),],
            false,
            false
        )
        .is_ok());
    }

    #[test]
    fn test_minimal_sort() {
        let mut schema_builder = Schema::builder();
        let val = schema_builder.add_u64_field("val", COLUMN);
        let id = schema_builder.add_u64_field("id", COLUMN);
        let schema = schema_builder.build();
        let settings = IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "id".to_string(),
                order: Order::Asc,
            }),
            ..Default::default()
        };
        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_ram()
            .unwrap();
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(doc!(id=> 3u64, val=>4u64, val=>4u64))
            .unwrap();
        writer
            .add_document(doc!(id=> 2u64, val=>2u64, val=>2u64))
            .unwrap();
        writer
            .add_document(doc!(id=> 1u64, val=>1u64, val=>1u64))
            .unwrap();
        writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);
        let id_col: Column = segment_reader
            .column_fields()
            .column_opt("id")
            .unwrap()
            .unwrap();
        assert_eq!(id_col.first(0u32), Some(1u64));
        assert_eq!(id_col.first(1u32), Some(2u64));
    }

    #[test]
    fn test_minimal_sort_force_end_merge_with_delete() {
        assert!(
            test_operation_strategy(&[IndexingOp::add(23), IndexingOp::add(13),], true, true)
                .is_ok()
        );
    }

    #[test]
    fn test_minimal_no_sort_no_force_end_merge() {
        assert!(test_operation_strategy(
            &[IndexingOp::add(23), IndexingOp::add(13),],
            false,
            false
        )
        .is_ok());
    }

    #[test]
    fn test_minimal_sort_merge() {
        assert!(test_operation_strategy(&[IndexingOp::add(3),], true, true).is_ok());
    }

    // proptest! {

    //     #![proptest_config(ProptestConfig::with_cases(20))]
    //     #[test]
    //     fn test_delete_with_sort_proptest_adding(ops in proptest::collection::vec(adding_operation_strategy(), 1..100)) {
    //         assert!(test_operation_strategy(&ops[..], true, false).is_ok());
    //     }

    //     #[test]
    //     fn test_delete_without_sort_proptest_adding(ops in proptest::collection::vec(adding_operation_strategy(), 1..100)) {
    //         assert!(test_operation_strategy(&ops[..], false, false).is_ok());
    //     }

    //     #[test]
    //     fn test_delete_with_sort_proptest_with_merge_adding(ops in proptest::collection::vec(adding_operation_strategy(), 1..100)) {
    //         assert!(test_operation_strategy(&ops[..], true, true).is_ok());
    //     }

    //     #[test]
    //     fn test_delete_without_sort_proptest_with_merge_adding(ops in proptest::collection::vec(adding_operation_strategy(), 1..100)) {
    //         assert!(test_operation_strategy(&ops[..], false, true).is_ok());}

    //     #[test]
    //     fn test_delete_with_sort_proptest(ops in proptest::collection::vec(balanced_operation_strategy(), 1..10)) {
    //         assert!(test_operation_strategy(&ops[..], true, false).is_ok());
    //     }

    //     #[test]
    //     fn test_delete_without_sort_proptest(ops in proptest::collection::vec(balanced_operation_strategy(), 1..10)) {
    //         assert!(test_operation_strategy(&ops[..], false, false).is_ok());
    //     }

    //     #[test]
    //     fn test_delete_with_sort_proptest_with_merge(ops in proptest::collection::vec(balanced_operation_strategy(), 1..10)) {
    //         assert!(test_operation_strategy(&ops[..], true, true).is_ok());
    //     }

    //     #[test]
    //     fn test_delete_without_sort_proptest_with_merge(ops in proptest::collection::vec(balanced_operation_strategy(), 1..100)) {
    //         assert!(test_operation_strategy(&ops[..], false, true).is_ok());
    //     }
    // }

    #[test]
    fn test_delete_with_sort_by_field_last_opstamp_is_not_max() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let sort_by_field = schema_builder.add_u64_field("sort_by", COLUMN);
        let id_field = schema_builder.add_u64_field("id", INDEXED);
        let schema = schema_builder.build();

        let settings = IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "sort_by".to_string(),
                order: Order::Asc,
            }),
            ..Default::default()
        };

        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_ram()?;
        let mut index_writer = index.writer_for_tests()?;

        // We add a doc...
        index_writer.add_document(doc!(sort_by_field => 2u64, id_field => 0u64))?;
        // We add another doc.
        index_writer.add_document(doc!(sort_by_field=>1u64, id_field => 0u64))?;

        // The expected result is a segment with
        // maxdoc = 2
        // numdoc = 2.
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);

        let segment_reader = searcher.segment_reader(0);
        assert_eq!(segment_reader.max_doc(), 2);
        assert_eq!(segment_reader.num_docs(), 2);
        Ok(())
    }

    // #[test]
    // fn test_delete_bug_reproduction_ip_addr() {
    //     use IndexingOp::*;
    //     let ops = &[
    //         IndexingOp::add(1),
    //         IndexingOp::add(2),
    //         Commit,
    //         IndexingOp::add(3),
    //         DeleteDoc { id: 1 },
    //         Commit,
    //         Merge,
    //         IndexingOp::add(4),
    //         Commit,
    //     ];
    //     test_operation_strategy(&ops[..], false, true).unwrap();
    // }

    #[test]
    fn test_merge_regression_1() {
        use IndexingOp::*;
        let ops = &[
            IndexingOp::add(15),
            Commit,
            IndexingOp::add(9),
            Commit,
            Merge,
        ];
        test_operation_strategy(&ops[..], false, true).unwrap();
    }

    #[test]
    fn test_range_query_bug_1() {
        use IndexingOp::*;
        let ops = &[
            IndexingOp::add(9),
            IndexingOp::add(0),
            IndexingOp::add(13),
            Commit,
        ];
        test_operation_strategy(&ops[..], false, true).unwrap();
    }

    #[test]
    fn test_range_query_bug_2() {
        let ops = &[
            IndexingOp::add(3),
            IndexingOp::add(6),
            IndexingOp::add(9),
            IndexingOp::add(10),
        ];
        test_operation_strategy(&ops[..], false, false).unwrap();
    }

    #[test]
    fn test_index_doc_missing_field() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let idfield = schema_builder.add_text_field("id", STRING);
        schema_builder.add_text_field("optfield", STRING);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(idfield=>"myid"))?;
        index_writer.commit()?;
        Ok(())
    }

    #[test]
    fn test_bug_1617_3() {
        assert!(test_operation_strategy(
            &[
                IndexingOp::add(6),
                IndexingOp::Commit,
                IndexingOp::Merge,
                IndexingOp::Commit,
                IndexingOp::Commit
            ],
            false,
            false
        )
        .is_ok());
    }

    #[test]
    fn test_bug_1617_2() {
        assert!(test_operation_strategy(
            &[
                IndexingOp::AddDoc {
                    id: 13,
                    value: Default::default()
                },
                IndexingOp::Commit,
                IndexingOp::add(30),
                IndexingOp::Commit,
                IndexingOp::Merge,
            ],
            false,
            true
        )
        .is_ok());
    }

    #[test]
    fn test_bug_1617() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let id_field = schema_builder.add_u64_field("id", INDEXED);

        let schema = schema_builder.build();
        let index = Index::builder().schema(schema).create_in_ram()?;
        let mut index_writer = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));

        let existing_id = 16u64;
        let deleted_id = 13u64;
        index_writer.add_document(doc!(
            id_field=>existing_id,
        ))?;
        index_writer.add_document(doc!(
            id_field=>deleted_id,
        ))?;
        index_writer.commit()?;

        // Merge
        {
            assert!(index_writer.wait_merging_threads().is_ok());
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            index_writer.merge(&segment_ids).wait().unwrap();
            assert!(index_writer.wait_merging_threads().is_ok());
        }
        let searcher = index.reader()?.searcher();

        let query = TermQuery::new(
            Term::from_field_u64(id_field, existing_id),
            IndexRecordOption::Basic,
        );
        let top_docs: Vec<(f32, DocAddress)> =
            searcher.search(&query, &TopDocs::with_limit(10)).unwrap();

        assert_eq!(top_docs.len(), 1); // Was failing

        Ok(())
    }

    #[test]
    fn test_bug_1618() -> crate::Result<()> {
        let mut schema_builder = schema::Schema::builder();
        let id_field = schema_builder.add_i64_field("id", INDEXED);

        let schema = schema_builder.build();
        let index = Index::builder().schema(schema).create_in_ram()?;
        let mut index_writer = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));

        index_writer.add_document(doc!(
            id_field=>10i64,
        ))?;
        index_writer.add_document(doc!(
            id_field=>30i64,
        ))?;
        index_writer.commit()?;

        // Merge
        {
            assert!(index_writer.wait_merging_threads().is_ok());
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            let segment_ids = index
                .searchable_segment_ids()
                .expect("Searchable segments failed.");
            index_writer.merge(&segment_ids).wait().unwrap();
            assert!(index_writer.wait_merging_threads().is_ok());
        }
        let searcher = index.reader()?.searcher();

        let query = TermQuery::new(
            Term::from_field_i64(id_field, 10i64),
            IndexRecordOption::Basic,
        );
        let top_docs: Vec<(f32, DocAddress)> =
            searcher.search(&query, &TopDocs::with_limit(10)).unwrap();

        assert_eq!(top_docs.len(), 1); // Fails

        let query = TermQuery::new(
            Term::from_field_i64(id_field, 30i64),
            IndexRecordOption::Basic,
        );
        let top_docs: Vec<(f32, DocAddress)> =
            searcher.search(&query, &TopDocs::with_limit(10)).unwrap();

        assert_eq!(top_docs.len(), 1); // Fails

        Ok(())
    }
}
