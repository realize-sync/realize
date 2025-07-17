//! ByteRange and ByteRanges types for efficient range operations.
//!
//! This module provides the `ByteRange` type, representing a closed-open range of bytes,
//! and (to be implemented) the `ByteRanges` type, representing a minimal, sorted, non-overlapping set of ranges.
//!
//! # ByteRange
//!
//! - Represents a closed-open range `[start, end)` of bytes.
//! - An empty range always has `start=0` and `end=0` and does not overlap or intersect with anything.
//! - Implements all standard traits for comparison, cloning, serialization, and display.
//! - Provides methods for range operations, chunking, and formatting.
//!
//! # Examples
//!
//! ```rust
//! use realize_types::ByteRange;
//!
//! let r1 = ByteRange::new(0, 10);
//! let r2 = ByteRange::new(5, 15);
//! assert!(r1.overlaps(&r2));
//! let intersection = r1.intersection(&r2);
//! assert_eq!(intersection, ByteRange::new(5, 10));
//! ```

use std::collections::BTreeMap;
use std::fmt;

/// A closed-open range of bytes: `[start, end)`.
///
/// - An empty range always has `start=0` and `end=0`.
/// - Ranges are used for efficient file and buffer operations.
/// - All methods treat the range as closed-open.
#[derive(
    Clone, Eq, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize, PartialOrd, Ord,
)]
pub struct ByteRange {
    /// The start offset (inclusive).
    pub start: u64,
    /// The end offset (exclusive).
    pub end: u64,
}

impl ByteRange {
    /// Create a new range from `start` to `end` (exclusive).
    ///
    /// # Panics
    ///
    /// Does not panic, but callers should ensure `start <= end` for valid ranges.
    pub fn new(start: u64, end: u64) -> Self {
        ByteRange { start, end }
    }
    /// Return the empty range `[0, 0)`.
    pub fn empty() -> Self {
        ByteRange { start: 0, end: 0 }
    }
    /// Returns true if this is the empty range `[0, 0)`.
    pub fn is_empty(&self) -> bool {
        self.end <= self.start
    }
    /// Returns the number of bytes in this range.
    pub fn bytecount(&self) -> u64 {
        // TODO: guarantee that in a range
        self.end.saturating_sub(self.start)
    }
    /// Returns true if this range overlaps with `other` (i.e., they share any bytes).
    ///
    /// Overlap is defined as: `self.start < other.end && other.start < self.end`.
    pub fn overlaps(&self, other: &ByteRange) -> bool {
        self.start < other.end && other.start < self.end
    }
    /// Returns true if this range intersects with `other` (i.e., they share any bytes).
    ///
    /// For closed-open ranges, this is the same as `overlaps`.
    pub fn intersects(&self, other: &ByteRange) -> bool {
        self.start < other.end && other.start < self.end
    }
    /// Returns the intersection of this range and `other`.
    ///
    /// If there is no intersection, returns the empty range `[0, 0)`.
    pub fn intersection(&self, other: &ByteRange) -> ByteRange {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        if start >= end {
            ByteRange::empty()
        } else {
            ByteRange { start, end }
        }
    }
    /// Returns the minimal range that covers both this range and `other`.
    pub fn span(&self, other: &ByteRange) -> ByteRange {
        ByteRange {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }
    /// Splits this range into chunks of at most `chunk_size` bytes.
    ///
    /// Returns an empty vector if the range is empty or `chunk_size` is zero.
    pub fn chunked(&self, chunk_size: u64) -> ChunkIterator {
        ChunkIterator::new(self, chunk_size)
    }

    /// Check whether a value is within the range.
    pub fn contains(&self, val: u64) -> bool {
        val >= self.start && val < self.end
    }
}

impl fmt::Display for ByteRange {
    /// Formats the range as `[start, end)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {})", self.start, self.end)
    }
}

/// Split [0, total) into chunks of the given size.
#[derive(Clone)]
pub struct ChunkIterator {
    offset: u64,
    total: u64,
    chunk_size: u64,
}
impl ChunkIterator {
    fn new(range: &ByteRange, chunk_size: u64) -> Self {
        ChunkIterator {
            offset: range.start,
            total: range.end,
            chunk_size,
        }
    }
}
impl Iterator for ChunkIterator {
    type Item = ByteRange;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: return an error if chunk_size == 0
        if self.chunk_size == 0 || self.offset >= self.total {
            return None;
        }

        let mut end = self.offset + self.chunk_size;
        if end > self.total {
            end = self.total;
        }
        let range = ByteRange::new(self.offset, end);
        self.offset = end;

        Some(range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Test creation and empty range.
    #[test]
    fn test_new_and_empty() {
        assert_eq!(ByteRange::new(1, 2), ByteRange { start: 1, end: 2 });
        assert!(ByteRange::empty().is_empty());
        assert!(ByteRange::new(2, 2).is_empty());
        assert!(!ByteRange::new(1, 2).is_empty());
    }
    #[test]
    fn test_bytecount() {
        assert_eq!(ByteRange::empty().bytecount(), 0);
        assert_eq!(ByteRange::new(2, 2).bytecount(), 0);
        assert_eq!(ByteRange::new(0, 10).bytecount(), 10);
        assert_eq!(ByteRange::new(10, 20).bytecount(), 10);
    }
    /// Test overlap logic.
    #[test]
    fn test_overlaps() {
        let a = ByteRange::new(0, 10);
        let b = ByteRange::new(5, 15);
        let c = ByteRange::new(10, 20);
        assert!(a.overlaps(&b));
        assert!(!a.overlaps(&c));
    }
    /// Test intersection logic.
    #[test]
    fn test_intersects() {
        let a = ByteRange::new(0, 10);
        let b = ByteRange::new(10, 20);
        let c = ByteRange::new(5, 15);
        assert!(!a.intersects(&b));
        assert!(a.intersects(&c));
    }
    /// Test intersection range.
    #[test]
    fn test_intersection() {
        let a = ByteRange::new(0, 10);
        let b = ByteRange::new(5, 15);
        let c = ByteRange::new(10, 20);
        assert_eq!(a.intersection(&b), ByteRange::new(5, 10));
        assert_eq!(a.intersection(&c), ByteRange::empty());
    }
    /// Test span logic.
    #[test]
    fn test_span() {
        let a = ByteRange::new(0, 10);
        let b = ByteRange::new(5, 15);
        assert_eq!(a.span(&b), ByteRange::new(0, 15));
    }
    /// Test chunking logic.
    #[test]
    fn test_chunked() {
        let a = ByteRange::new(0, 10);
        let chunks: Vec<ByteRange> = a.chunked(3).collect();
        assert_eq!(
            chunks,
            vec![
                ByteRange::new(0, 3),
                ByteRange::new(3, 6),
                ByteRange::new(6, 9),
                ByteRange::new(9, 10),
            ]
        );
        assert_eq!(ByteRange::empty().chunked(3).collect::<Vec<_>>(), vec![]);
        assert_eq!(a.chunked(0).collect::<Vec<_>>(), vec![]);
    }
    /// Test Display formatting.
    #[test]
    fn test_display() {
        let a = ByteRange::new(1, 2);
        assert_eq!(a.to_string(), "[1, 2)");
    }
}

/// A minimal, sorted, non-overlapping set of ByteRange.
///
/// Maintains the invariant that all contained ranges are non-empty, sorted, and merged when possible.
/// Provides efficient set operations and iteration.
///
/// Implementation uses a segment tree based on BTreeMap for efficient range operations.
#[derive(Clone, Eq, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ByteRanges {
    // BTreeMap where key is the start of a range and value is the end
    // This maintains sorted, non-overlapping ranges automatically
    ranges: BTreeMap<u64, u64>,
}

impl ByteRanges {
    /// Creates an empty ByteRanges set.
    pub fn new() -> Self {
        ByteRanges {
            ranges: BTreeMap::new(),
        }
    }

    pub fn single(start: u64, end: u64) -> Self {
        ByteRanges::for_range(ByteRange::new(start, end))
    }

    /// Creates a ByteRanges containing a single range.
    pub fn for_range(range: ByteRange) -> Self {
        if range.is_empty() {
            ByteRanges::new()
        } else {
            let mut ranges = BTreeMap::new();
            ranges.insert(range.start, range.end);
            ByteRanges { ranges }
        }
    }

    /// Creates and fills a ByteRanges set.
    pub fn from_ranges(into_iter: impl IntoIterator<Item = ByteRange>) -> Self {
        let mut ranges = ByteRanges::new();
        for range in into_iter.into_iter() {
            ranges.add(&range);
        }
        ranges
    }

    /// Creates and fills a ByteRanges set.
    pub fn from_range_refs<'a>(into_iter: impl IntoIterator<Item = &'a ByteRange>) -> Self {
        let mut ranges = ByteRanges::new();
        for range in into_iter.into_iter() {
            ranges.add(range);
        }
        ranges
    }

    /// Split all ranges into chunks of at most `chunk_size` bytes.
    pub fn chunked(&self, chunk_size: u64) -> impl Iterator<Item = ByteRange> {
        self.ranges.iter().flat_map(move |(&start, &end)| {
            let range = ByteRange::new(start, end);
            ChunkIterator::new(&range, chunk_size)
        })
    }

    /// Returns true if there are no ranges.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Returns the number of ranges.
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    /// Returns the total number of bytes within the ranges.
    pub fn bytecount(&self) -> u64 {
        self.ranges
            .iter()
            .fold(0, |sum, (&start, &end)| sum + (end - start))
    }

    /// Add all ranges from `other` to `self`.
    pub fn extend(&mut self, other: ByteRanges) {
        for (&start, &end) in other.ranges.iter() {
            self.add(&ByteRange::new(start, end));
        }
    }

    /// Adds a new ByteRange, merging as needed to maintain minimal, sorted, non-overlapping invariants.
    pub fn add(&mut self, range: &ByteRange) {
        if range.is_empty() {
            return;
        }

        let mut new_start = range.start;
        let mut new_end = range.end;

        // Find ranges that overlap or are adjacent to the new range
        let mut to_remove = Vec::new();

        // Find ranges that start before or at the new range's end
        let overlapping_start = self.ranges.range(..=range.end);
        for (&start, &end) in overlapping_start {
            // Check if ranges overlap or are adjacent
            if end >= range.start || start == range.end || end == range.start {
                to_remove.push(start);
                new_start = new_start.min(start);
                new_end = new_end.max(end);
            }
        }

        // Remove overlapping ranges
        for start in to_remove {
            self.ranges.remove(&start);
        }

        // Insert the merged range
        self.ranges.insert(new_start, new_end);
    }

    /// Returns the union of two ByteRanges sets.
    pub fn union(&self, other: &ByteRanges) -> ByteRanges {
        let mut result = self.clone();
        for (&start, &end) in other.ranges.iter() {
            result.add(&ByteRange::new(start, end));
        }
        result
    }

    /// Returns the intersection of two ByteRanges sets.
    pub fn intersection(&self, other: &ByteRanges) -> ByteRanges {
        let mut result = ByteRanges::new();

        for (&a_start, &a_end) in self.ranges.iter() {
            for (&b_start, &b_end) in other.ranges.iter() {
                let intersection =
                    ByteRange::new(a_start, a_end).intersection(&ByteRange::new(b_start, b_end));
                if !intersection.is_empty() {
                    result.add(&intersection);
                }
            }
        }
        result
    }

    /// Returns the subtraction of other from self (i.e., the parts of self not covered by other).
    pub fn subtraction(&self, other: &ByteRanges) -> ByteRanges {
        let mut result = ByteRanges::new();

        for (&a_start, &a_end) in self.ranges.iter() {
            let mut current_ranges = vec![ByteRange::new(a_start, a_end)];

            for (&b_start, &b_end) in other.ranges.iter() {
                let mut new_ranges = Vec::new();

                for range in current_ranges {
                    let b_range = ByteRange::new(b_start, b_end);

                    if b_range.end <= range.start || b_range.start >= range.end {
                        // No overlap
                        new_ranges.push(range);
                    } else {
                        // Overlap - split the range
                        if b_range.start > range.start {
                            new_ranges.push(ByteRange::new(range.start, b_range.start));
                        }
                        if b_range.end < range.end {
                            new_ranges.push(ByteRange::new(b_range.end, range.end));
                        }
                    }
                }

                current_ranges = new_ranges;
            }

            for range in current_ranges {
                result.add(&range);
            }
        }
        result
    }

    /// Returns true if any range in self intersects with the given ByteRange.
    pub fn intersects(&self, range: &ByteRange) -> bool {
        // Find ranges that start before the query range ends
        let overlapping_start = self.ranges.range(..range.end);
        for (&_start, &end) in overlapping_start {
            if end > range.start {
                return true;
            }
        }
        false
    }

    /// Returns true if any range in self overlaps with the given ByteRange.
    pub fn overlaps(&self, range: &ByteRange) -> bool {
        self.intersects(range)
    }

    /// Returns an iterator over the contained ByteRanges.
    pub fn iter(&self) -> ByteRangesIter<'_> {
        ByteRangesIter {
            inner: self.ranges.iter(),
        }
    }

    /// Returns a range containing the given offset, if any.
    pub fn range_containing(&self, offset: u64) -> Option<ByteRange> {
        // Find the range that starts at or before the offset
        let range_before = self.ranges.range(..=offset).next_back();

        if let Some((&start, &end)) = range_before {
            if offset < end {
                Some(ByteRange::new(start, end))
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl IntoIterator for ByteRanges {
    type Item = ByteRange;
    type IntoIter = std::vec::IntoIter<ByteRange>;

    fn into_iter(self) -> Self::IntoIter {
        self.ranges
            .into_iter()
            .map(|(start, end)| ByteRange::new(start, end))
            .collect::<Vec<_>>()
            .into_iter()
    }
}

/// Iterator over ByteRanges
pub struct ByteRangesIter<'a> {
    inner: std::collections::btree_map::Iter<'a, u64, u64>,
}

impl<'a> Iterator for ByteRangesIter<'a> {
    type Item = ByteRange;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(&start, &end)| ByteRange::new(start, end))
    }
}

impl<'a> Clone for ByteRangesIter<'a> {
    fn clone(&self) -> Self {
        ByteRangesIter {
            inner: self.inner.clone(),
        }
    }
}

impl fmt::Display for ByteRanges {
    /// Formats as a comma-separated list of ranges.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        f.write_str("{")?;
        for (&start, &end) in &self.ranges {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "[{}, {})", start, end)?;
            first = false;
        }
        f.write_str("}")?;
        Ok(())
    }
}

#[cfg(test)]
mod byteranges_tests {
    use super::*;
    #[test]
    fn test_empty() {
        let b = ByteRanges::new();
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
    }
    #[test]
    fn test_add_and_merge() {
        let mut b = ByteRanges::new();
        b.add(&ByteRange::new(0, 5));
        b.add(&ByteRange::new(5, 10));
        b.add(&ByteRange::new(12, 15));
        b.add(&ByteRange::new(8, 13));
        let expected = vec![ByteRange::new(0, 15)];
        assert_eq!(b.iter().collect::<Vec<_>>(), expected);
    }
    #[test]
    fn test_union() {
        let mut a = ByteRanges::new();
        a.add(&ByteRange::new(0, 5));
        let mut b = ByteRanges::new();
        b.add(&ByteRange::new(3, 10));
        let u = a.union(&b);
        assert_eq!(u.iter().collect::<Vec<_>>(), vec![ByteRange::new(0, 10)]);
    }
    #[test]
    fn test_intersection() {
        let mut a = ByteRanges::new();
        a.add(&ByteRange::new(0, 5));
        a.add(&ByteRange::new(10, 15));
        let mut b = ByteRanges::new();
        b.add(&ByteRange::new(3, 12));
        let i = a.intersection(&b);
        assert_eq!(
            i.iter().collect::<Vec<_>>(),
            vec![ByteRange::new(3, 5), ByteRange::new(10, 12)]
        );
    }
    #[test]
    fn test_subtraction() {
        let mut a = ByteRanges::new();
        a.add(&ByteRange::new(0, 10));
        let mut b = ByteRanges::new();
        b.add(&ByteRange::new(3, 5));
        b.add(&ByteRange::new(7, 12));
        let s = a.subtraction(&b);
        assert_eq!(
            s.iter().collect::<Vec<_>>(),
            vec![ByteRange::new(0, 3), ByteRange::new(5, 7)]
        );
    }
    #[test]
    fn test_intersects_and_overlaps() {
        let mut b = ByteRanges::new();
        b.add(&ByteRange::new(0, 5));
        b.add(&ByteRange::new(10, 15));
        assert!(b.intersects(&ByteRange::new(3, 12)));
        assert!(b.overlaps(&ByteRange::new(3, 12)));
        assert!(!b.intersects(&ByteRange::new(20, 25)));
    }
    #[test]
    fn test_bytecount() {
        assert_eq!(0, ByteRanges::new().bytecount());
        assert_eq!(10, ByteRanges::single(10, 20).bytecount());
        assert_eq!(
            20,
            ByteRanges::from_ranges(vec![
                ByteRange::new(5, 12),
                ByteRange::new(10, 15),
                ByteRange::new(30, 40)
            ])
            .bytecount()
        );
    }
    #[test]
    fn test_iter() {
        let mut b = ByteRanges::new();
        b.add(&ByteRange::new(0, 5));
        b.add(&ByteRange::new(10, 15));
        let v: Vec<_> = b.iter().collect();
        assert_eq!(v, vec![ByteRange::new(0, 5), ByteRange::new(10, 15)]);
    }
    #[test]
    fn test_display() {
        let mut b = ByteRanges::new();
        b.add(&ByteRange::new(0, 5));
        b.add(&ByteRange::new(10, 15));
        assert_eq!(b.to_string(), "{[0, 5), [10, 15)}");
    }
    #[test]
    fn extend() {
        let mut a = ByteRanges::new();
        a.add(&ByteRange::new(0, 5));
        a.add(&ByteRange::new(10, 15));

        let mut b = ByteRanges::new();
        b.add(&ByteRange::new(3, 8));
        b.add(&ByteRange::new(12, 20));

        a.extend(b);

        // Should merge overlapping ranges: [0,5) + [3,8) = [0,8), [10,15) + [12,20) = [10,20)
        assert_eq!(
            a.iter().collect::<Vec<_>>(),
            vec![ByteRange::new(0, 8), ByteRange::new(10, 20)]
        );
    }
    #[test]
    fn extend_empty() {
        let mut a = ByteRanges::new();
        a.add(&ByteRange::new(0, 5));

        let empty = ByteRanges::new();
        a.extend(empty);

        // Should remain unchanged
        assert_eq!(a.iter().collect::<Vec<_>>(), vec![ByteRange::new(0, 5)]);
    }
    #[test]
    fn extend_into_empty() {
        let mut a = ByteRanges::new();

        let mut b = ByteRanges::new();
        b.add(&ByteRange::new(0, 5));
        b.add(&ByteRange::new(10, 15));

        a.extend(b);

        // Should take all ranges from b
        assert_eq!(
            a.iter().collect::<Vec<_>>(),
            vec![ByteRange::new(0, 5), ByteRange::new(10, 15)]
        );
    }
    #[test]
    fn test_chunked() {
        assert_eq!(
            ByteRanges::single(0, 7).chunked(3).collect::<Vec<_>>(),
            vec![
                ByteRange::new(0, 3),
                ByteRange::new(3, 6),
                ByteRange::new(6, 7)
            ]
        );

        assert_eq!(
            ByteRanges::from_ranges(vec![ByteRange::new(0, 10), ByteRange::new(20, 30)])
                .chunked(8)
                .collect::<Vec<_>>(),
            vec![
                ByteRange::new(0, 8),
                ByteRange::new(8, 10),
                ByteRange::new(20, 28),
                ByteRange::new(28, 30),
            ]
        );
    }

    #[test]
    fn test_range_containing() {
        // Test with empty ByteRanges
        let empty = ByteRanges::new();
        assert_eq!(empty.range_containing(0), None);
        assert_eq!(empty.range_containing(100), None);

        // Test with single range
        let single = ByteRanges::single(10, 20);
        assert_eq!(single.range_containing(5), None); // Before range
        assert_eq!(single.range_containing(10), Some(ByteRange::new(10, 20))); // At start
        assert_eq!(single.range_containing(15), Some(ByteRange::new(10, 20))); // Inside range
        assert_eq!(single.range_containing(19), Some(ByteRange::new(10, 20))); // Just before end
        assert_eq!(single.range_containing(20), None); // At end (exclusive)
        assert_eq!(single.range_containing(25), None); // After range

        // Test with multiple ranges
        let mut multiple = ByteRanges::new();
        multiple.add(&ByteRange::new(0, 10));
        multiple.add(&ByteRange::new(20, 30));
        multiple.add(&ByteRange::new(40, 50));

        // Test offsets before first range
        assert_eq!(multiple.range_containing(0), Some(ByteRange::new(0, 10)));
        assert_eq!(multiple.range_containing(5), Some(ByteRange::new(0, 10)));
        assert_eq!(multiple.range_containing(9), Some(ByteRange::new(0, 10)));

        // Test offsets in gaps between ranges
        assert_eq!(multiple.range_containing(10), None); // At end of first range
        assert_eq!(multiple.range_containing(15), None); // In gap
        assert_eq!(multiple.range_containing(19), None); // Just before second range

        // Test offsets in second range
        assert_eq!(multiple.range_containing(20), Some(ByteRange::new(20, 30)));
        assert_eq!(multiple.range_containing(25), Some(ByteRange::new(20, 30)));
        assert_eq!(multiple.range_containing(29), Some(ByteRange::new(20, 30)));

        // Test offsets in gaps and third range
        assert_eq!(multiple.range_containing(30), None); // At end of second range
        assert_eq!(multiple.range_containing(35), None); // In gap
        assert_eq!(multiple.range_containing(40), Some(ByteRange::new(40, 50)));
        assert_eq!(multiple.range_containing(45), Some(ByteRange::new(40, 50)));
        assert_eq!(multiple.range_containing(49), Some(ByteRange::new(40, 50)));

        // Test offsets after last range
        assert_eq!(multiple.range_containing(50), None); // At end of last range
        assert_eq!(multiple.range_containing(100), None); // Far after
    }

    #[test]
    fn test_range_containing_with_merged_ranges() {
        // Test with ranges that get merged during add operations
        let mut merged = ByteRanges::new();
        merged.add(&ByteRange::new(0, 5));
        merged.add(&ByteRange::new(5, 10)); // Should merge with [0,5)
        merged.add(&ByteRange::new(15, 20));
        merged.add(&ByteRange::new(18, 25)); // Should merge with [15,20)

        // Should have two merged ranges: [0,10) and [15,25)
        assert_eq!(merged.range_containing(0), Some(ByteRange::new(0, 10)));
        assert_eq!(merged.range_containing(5), Some(ByteRange::new(0, 10)));
        assert_eq!(merged.range_containing(9), Some(ByteRange::new(0, 10)));
        assert_eq!(merged.range_containing(10), None); // At end of first merged range
        assert_eq!(merged.range_containing(15), Some(ByteRange::new(15, 25)));
        assert_eq!(merged.range_containing(20), Some(ByteRange::new(15, 25)));
        assert_eq!(merged.range_containing(24), Some(ByteRange::new(15, 25)));
        assert_eq!(merged.range_containing(25), None); // At end of second merged range
    }

    #[test]
    fn test_range_containing_edge_cases() {
        // Test with empty ranges (should be filtered out)
        let mut with_empty = ByteRanges::new();
        with_empty.add(&ByteRange::new(0, 0)); // Empty range
        with_empty.add(&ByteRange::new(10, 20));
        with_empty.add(&ByteRange::new(30, 30)); // Empty range

        assert_eq!(with_empty.range_containing(0), None); // Empty range doesn't contain anything
        assert_eq!(
            with_empty.range_containing(15),
            Some(ByteRange::new(10, 20))
        );
        assert_eq!(with_empty.range_containing(30), None); // Empty range doesn't contain anything

        // Test with very large offsets
        let large = ByteRanges::single(u64::MAX - 10, u64::MAX - 5);
        assert_eq!(
            large.range_containing(u64::MAX - 10),
            Some(ByteRange::new(u64::MAX - 10, u64::MAX - 5))
        );
        assert_eq!(
            large.range_containing(u64::MAX - 7),
            Some(ByteRange::new(u64::MAX - 10, u64::MAX - 5))
        );
        assert_eq!(large.range_containing(u64::MAX - 5), None);
        assert_eq!(large.range_containing(u64::MAX), None);
    }
}

// TODO: Implement methods and traits as described in the spec.
