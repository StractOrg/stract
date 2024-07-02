use super::Collector;
use crate::collector::SegmentCollector;
use crate::{DocId, Score, SegmentOrdinal, SegmentReader};

/// `CountCollector` collector only counts how many
/// documents match the query.
pub struct Count;

impl Collector for Count {
    type Fruit = usize;

    type Child = SegmentCountCollector;

    fn for_segment(
        &self,
        _: SegmentOrdinal,
        _: &SegmentReader,
    ) -> crate::Result<SegmentCountCollector> {
        Ok(SegmentCountCollector::default())
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_counts: Vec<usize>) -> crate::Result<usize> {
        Ok(segment_counts.into_iter().sum())
    }
}

#[derive(Default)]
pub struct SegmentCountCollector {
    count: usize,
}

impl SegmentCollector for SegmentCountCollector {
    type Fruit = usize;

    fn collect(&mut self, _: DocId, _: Score) {
        self.count += 1;
    }

    fn harvest(self) -> usize {
        self.count
    }
}

#[cfg(test)]
mod tests {
    use super::{Count, SegmentCountCollector};
    use crate::collector::{Collector, SegmentCollector};

    #[test]
    fn test_count_collect_does_not_requires_scoring() {
        assert!(!Count.requires_scoring());
    }

    #[test]
    fn test_segment_count_collector() {
        {
            let count_collector = SegmentCountCollector::default();
            assert_eq!(count_collector.harvest(), 0);
        }
        {
            let mut count_collector = SegmentCountCollector::default();
            count_collector.collect(0u32, 1.0);
            assert_eq!(count_collector.harvest(), 1);
        }
        {
            let mut count_collector = SegmentCountCollector::default();
            count_collector.collect(0u32, 1.0);
            assert_eq!(count_collector.harvest(), 1);
        }
        {
            let mut count_collector = SegmentCountCollector::default();
            count_collector.collect(0u32, 1.0);
            count_collector.collect(1u32, 1.0);
            assert_eq!(count_collector.harvest(), 2);
        }
    }
}
