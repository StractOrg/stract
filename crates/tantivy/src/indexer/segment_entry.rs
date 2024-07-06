use std::fmt;

use crate::index::{SegmentId, SegmentMeta};

/// A segment entry describes the state of
/// a given segment, at a given instant.
#[derive(Clone)]
pub struct SegmentEntry {
    meta: SegmentMeta,
}

impl SegmentEntry {
    /// Create a new `SegmentEntry`
    pub fn new(segment_meta: SegmentMeta) -> SegmentEntry {
        SegmentEntry { meta: segment_meta }
    }

    /// Set the `SegmentMeta` for this segment.
    pub fn set_meta(&mut self, segment_meta: SegmentMeta) {
        self.meta = segment_meta;
    }

    /// Returns the segment id.
    pub fn segment_id(&self) -> SegmentId {
        self.meta.id()
    }

    /// Accessor to the `SegmentMeta`
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }
}

impl fmt::Debug for SegmentEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "SegmentEntry({:?})", self.meta)
    }
}
