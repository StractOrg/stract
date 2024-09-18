// # Custom collector example
//
// This example shows how you can implement your own
// collector. As an example, we will compute a collector
// that computes the standard deviation of a given columnar field.
//
// Of course, you can have a look at the tantivy's built-in collectors
// such as the `CountCollector` for more examples.
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::columnar::{BytesColumn, Column, DynamicColumn, HasAssociatedColumnType};

use crate::collector::{Collector, SegmentCollector};
use crate::{DocId, Score, SegmentReader};

/// The `FilterCollector` filters docs using a columnar field value and a predicate.
///
/// Only the documents containing at least one value for which the predicate returns `true`
/// will be passed on to the next collector.
///
/// In other words,
/// - documents with no values are filtered out.
/// - documents with several values are accepted if at least one value matches the predicate.
///
/// Note that this is limited to columnar fields which implement the
/// [`FastValue`][crate::columnfield::FastValue] trait, e.g. `u64` but not `&[u8]`.
/// To filter based on a bytes columnar field, use a [`BytesFilterCollector`] instead.
pub struct FilterCollector<TCollector, TPredicate, TPredicateValue>
where
    TPredicate: 'static + Clone,
{
    field: String,
    collector: TCollector,
    predicate: TPredicate,
    t_predicate_value: PhantomData<TPredicateValue>,
}

impl<TCollector, TPredicate, TPredicateValue>
    FilterCollector<TCollector, TPredicate, TPredicateValue>
where
    TCollector: Collector + Send + Sync,
    TPredicate: Fn(TPredicateValue) -> bool + Send + Sync + Clone,
{
    /// Create a new `FilterCollector`.
    pub fn new(field: String, predicate: TPredicate, collector: TCollector) -> Self {
        Self {
            field,
            predicate,
            collector,
            t_predicate_value: PhantomData,
        }
    }
}

impl<TCollector, TPredicate, TPredicateValue> Collector
    for FilterCollector<TCollector, TPredicate, TPredicateValue>
where
    TCollector: Collector + Send + Sync,
    TPredicate: 'static + Fn(TPredicateValue) -> bool + Send + Sync + Clone,
    TPredicateValue: HasAssociatedColumnType,
    DynamicColumn: Into<Option<crate::columnar::Column<TPredicateValue>>>,
{
    type Fruit = TCollector::Fruit;

    type Child = FilterSegmentCollector<TCollector::Child, TPredicate, TPredicateValue>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        let column_opt = segment_reader.column_fields().column_opt(&self.field)?;

        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;

        Ok(FilterSegmentCollector {
            column_opt,
            segment_collector,
            predicate: self.predicate.clone(),
            t_predicate_value: PhantomData,
        })
    }

    fn requires_scoring(&self) -> bool {
        self.collector.requires_scoring()
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<TCollector::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<TCollector::Fruit> {
        self.collector.merge_fruits(segment_fruits)
    }
}

pub struct FilterSegmentCollector<TSegmentCollector, TPredicate, TPredicateValue> {
    column_opt: Option<Column<TPredicateValue>>,
    segment_collector: TSegmentCollector,
    predicate: TPredicate,
    t_predicate_value: PhantomData<TPredicateValue>,
}

impl<TSegmentCollector, TPredicate, TPredicateValue>
    FilterSegmentCollector<TSegmentCollector, TPredicate, TPredicateValue>
where
    TPredicateValue: PartialOrd + Copy + Debug + Send + Sync + 'static,
    TPredicate: 'static + Fn(TPredicateValue) -> bool + Send + Sync,
{
    #[inline]
    fn accept_document(&self, doc_id: DocId) -> bool {
        if let Some(column) = &self.column_opt {
            if let Some(val) = column.first(doc_id) {
                if (self.predicate)(val) {
                    return true;
                }
            }
        }
        false
    }
}

impl<TSegmentCollector, TPredicate, TPredicateValue> SegmentCollector
    for FilterSegmentCollector<TSegmentCollector, TPredicate, TPredicateValue>
where
    TSegmentCollector: SegmentCollector,
    TPredicateValue: HasAssociatedColumnType,
    TPredicate: 'static + Fn(TPredicateValue) -> bool + Send + Sync, /* DynamicColumn: Into<Option<columnar::Column<TPredicateValue>>> */
{
    type Fruit = TSegmentCollector::Fruit;

    fn collect(&mut self, doc: u32, score: Score) {
        if self.accept_document(doc) {
            self.segment_collector.collect(doc, score);
        }
    }

    fn harvest(self) -> TSegmentCollector::Fruit {
        self.segment_collector.harvest()
    }
}

/// A variant of the [`FilterCollector`] specialized for bytes columnar fields.
///
/// It transparently wraps an inner [`Collector`] but filters documents
/// based on the result of applying the predicate to the bytes columnar field.
///
/// A document is accepted if and only if the predicate returns `true` for at least one value.
///
/// In other words,
/// - documents with no values are filtered out.
/// - documents with several values are accepted if at least one value matches the predicate.
pub struct BytesFilterCollector<TCollector, TPredicate>
where
    TPredicate: 'static + Clone,
{
    field: String,
    collector: TCollector,
    predicate: TPredicate,
}

impl<TCollector, TPredicate> BytesFilterCollector<TCollector, TPredicate>
where
    TCollector: Collector + Send + Sync,
    TPredicate: Fn(&[u8]) -> bool + Send + Sync + Clone,
{
    /// Create a new `BytesFilterCollector`.
    pub fn new(field: String, predicate: TPredicate, collector: TCollector) -> Self {
        Self {
            field,
            predicate,
            collector,
        }
    }
}

impl<TCollector, TPredicate> Collector for BytesFilterCollector<TCollector, TPredicate>
where
    TCollector: Collector + Send + Sync,
    TPredicate: 'static + Fn(&[u8]) -> bool + Send + Sync + Clone,
{
    type Fruit = TCollector::Fruit;

    type Child = BytesFilterSegmentCollector<TCollector::Child, TPredicate>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        let column_opt = segment_reader.column_fields().bytes(&self.field)?;

        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;

        Ok(BytesFilterSegmentCollector {
            column_opt,
            segment_collector,
            predicate: self.predicate.clone(),
            buffer: Vec::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        self.collector.requires_scoring()
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<TCollector::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<TCollector::Fruit> {
        self.collector.merge_fruits(segment_fruits)
    }
}

pub struct BytesFilterSegmentCollector<TSegmentCollector, TPredicate>
where
    TPredicate: 'static,
{
    column_opt: Option<BytesColumn>,
    segment_collector: TSegmentCollector,
    predicate: TPredicate,
    buffer: Vec<u8>,
}

impl<TSegmentCollector, TPredicate> BytesFilterSegmentCollector<TSegmentCollector, TPredicate>
where
    TSegmentCollector: SegmentCollector,
    TPredicate: 'static + Fn(&[u8]) -> bool + Send + Sync,
{
    #[inline]
    fn accept_document(&mut self, doc_id: DocId) -> bool {
        if let Some(column) = &self.column_opt {
            for ord in column.term_ords(doc_id) {
                self.buffer.clear();

                let found = column.ord_to_bytes(ord, &mut self.buffer).unwrap_or(false);

                if found && (self.predicate)(&self.buffer) {
                    return true;
                }
            }
        }
        false
    }
}

impl<TSegmentCollector, TPredicate> SegmentCollector
    for BytesFilterSegmentCollector<TSegmentCollector, TPredicate>
where
    TSegmentCollector: SegmentCollector,
    TPredicate: 'static + Fn(&[u8]) -> bool + Send + Sync,
{
    type Fruit = TSegmentCollector::Fruit;

    fn collect(&mut self, doc: u32, score: Score) {
        if self.accept_document(doc) {
            self.segment_collector.collect(doc, score);
        }
    }

    fn harvest(self) -> TSegmentCollector::Fruit {
        self.segment_collector.harvest()
    }
}
