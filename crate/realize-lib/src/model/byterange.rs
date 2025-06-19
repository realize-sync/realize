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
//! use realize_lib::model::ByteRange;
//!
//! let r1 = ByteRange::new(0, 10);
//! let r2 = ByteRange::new(5, 15);
//! assert!(r1.overlaps(&r2));
//! let intersection = r1.intersection(&r2);
//! assert_eq!(intersection, ByteRange::new(5, 10));
//! ```

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
        if self.end > self.start {
            self.end - self.start
        } else {
            0
        }
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
        return val >= self.start && val < self.end;
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
#[derive(Clone, Eq, PartialEq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ByteRanges {
    ranges: Vec<ByteRange>,
}

impl ByteRanges {
    /// Creates an empty ByteRanges set.
    pub fn new() -> Self {
        ByteRanges { ranges: Vec::new() }
    }

    pub fn single(start: u64, end: u64) -> Self {
        ByteRanges::for_range(ByteRange::new(start, end))
    }

    /// Creates a ByteRanges containing a single range.
    pub fn for_range(range: ByteRange) -> Self {
        if range.is_empty() {
            ByteRanges::new()
        } else {
            ByteRanges {
                ranges: vec![range],
            }
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
        self.ranges
            .iter()
            .map(move |r| ChunkIterator::new(r, chunk_size))
            .flatten()
    }

    /// Returns true if there are no ranges.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
    /// Returns the number of ranges.
    pub fn len(&self) -> usize {
        self.ranges.len()
    }
    /// Returns the of all the bytes within the ranges.
    pub fn bytecount(&self) -> u64 {
        self.ranges
            .iter()
            .fold(0, |sum, range| sum + range.bytecount())
    }
    /// Adds a new ByteRange, merging as needed to maintain minimal, sorted, non-overlapping invariants.
    pub fn add(&mut self, range: &ByteRange) {
        if range.is_empty() {
            return;
        }
        let mut new = Vec::with_capacity(self.ranges.len() + 1);
        let mut inserted = false;
        let old_ranges: Vec<_> = self.ranges.drain(..).collect();
        let mut idx = 0;
        while idx < old_ranges.len() {
            let r = &old_ranges[idx];
            if !inserted && range.end < r.start {
                new.push(range.clone());
                new.push(r.clone());
                inserted = true;
            } else if !inserted && range.start > r.end {
                new.push(r.clone());
            } else if !inserted
                && (r.overlaps(range) || r.end == range.start || r.start == range.end)
            {
                let mut merged = ByteRange {
                    start: r.start.min(range.start),
                    end: r.end.max(range.end),
                };
                idx += 1;
                while idx < old_ranges.len() {
                    let rr = &old_ranges[idx];
                    if rr.start <= merged.end && rr.end > merged.end {
                        merged.end = rr.end;
                        idx += 1;
                    } else if rr.start > merged.end {
                        break;
                    } else {
                        idx += 1;
                    }
                }
                new.push(merged);
                inserted = true;
                break;
            } else {
                new.push(r.clone());
            }
            idx += 1;
        }
        if !inserted {
            new.push(range.clone());
        }
        // Add any remaining ranges after merging
        while idx < old_ranges.len() {
            new.push(old_ranges[idx].clone());
            idx += 1;
        }
        self.ranges = new;
    }
    /// Returns the union of two ByteRanges sets.
    pub fn union(&self, other: &ByteRanges) -> ByteRanges {
        let mut result = self.clone();
        for r in &other.ranges {
            result.add(r);
        }
        result
    }
    /// Returns the intersection of two ByteRanges sets.
    pub fn intersection(&self, other: &ByteRanges) -> ByteRanges {
        let mut result = ByteRanges::new();
        for a in &self.ranges {
            for b in &other.ranges {
                let i = a.intersection(b);
                if !i.is_empty() {
                    result.add(&i);
                }
            }
        }
        result
    }
    /// Returns the subtraction of other from self (i.e., the parts of self not covered by other).
    pub fn subtraction(&self, other: &ByteRanges) -> ByteRanges {
        let mut result = ByteRanges::new();
        for a in &self.ranges {
            let mut sub = vec![a.clone()];
            for b in &other.ranges {
                sub = sub
                    .into_iter()
                    .flat_map(|r| {
                        let mut out = Vec::new();
                        if b.end <= r.start || b.start >= r.end {
                            out.push(r);
                        } else {
                            if b.start > r.start {
                                out.push(ByteRange::new(r.start, b.start));
                            }
                            if b.end < r.end {
                                out.push(ByteRange::new(b.end, r.end));
                            }
                        }
                        out
                    })
                    .collect();
            }
            for r in sub {
                result.add(&r);
            }
        }
        result
    }
    /// Returns true if any range in self intersects with the given ByteRange.
    pub fn intersects(&self, range: &ByteRange) -> bool {
        self.ranges.iter().any(|r| r.intersects(range))
    }
    /// Returns true if any range in self overlaps with the given ByteRange.
    pub fn overlaps(&self, range: &ByteRange) -> bool {
        self.ranges.iter().any(|r| r.overlaps(range))
    }
    /// Returns an iterator over the contained ByteRanges.
    pub fn iter(&self) -> std::slice::Iter<'_, ByteRange> {
        self.ranges.iter()
    }
}

impl IntoIterator for ByteRanges {
    type Item = ByteRange;
    type IntoIter = std::vec::IntoIter<ByteRange>;
    fn into_iter(self) -> Self::IntoIter {
        self.ranges.into_iter()
    }
}

impl fmt::Display for ByteRanges {
    /// Formats as a comma-separated list of ranges.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        f.write_str("{")?;
        for r in &self.ranges {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{}", r)?;
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
        assert_eq!(b.ranges, expected);
    }
    #[test]
    fn test_union() {
        let mut a = ByteRanges::new();
        a.add(&ByteRange::new(0, 5));
        let mut b = ByteRanges::new();
        b.add(&ByteRange::new(3, 10));
        let u = a.union(&b);
        assert_eq!(u.ranges, vec![ByteRange::new(0, 10)]);
    }
    #[test]
    fn test_intersection() {
        let mut a = ByteRanges::new();
        a.add(&ByteRange::new(0, 5));
        a.add(&ByteRange::new(10, 15));
        let mut b = ByteRanges::new();
        b.add(&ByteRange::new(3, 12));
        let i = a.intersection(&b);
        assert_eq!(i.ranges, vec![ByteRange::new(3, 5), ByteRange::new(10, 12)]);
    }
    #[test]
    fn test_subtraction() {
        let mut a = ByteRanges::new();
        a.add(&ByteRange::new(0, 10));
        let mut b = ByteRanges::new();
        b.add(&ByteRange::new(3, 5));
        b.add(&ByteRange::new(7, 12));
        let s = a.subtraction(&b);
        assert_eq!(s.ranges, vec![ByteRange::new(0, 3), ByteRange::new(5, 7)]);
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
        let v: Vec<_> = b.iter().cloned().collect();
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
}

// TODO: Implement methods and traits as described in the spec.
