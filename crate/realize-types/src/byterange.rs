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
use std::collections::btree_map;
use std::fmt;
use std::iter::Peekable;

/// A closed-open range of bytes: `[start, end)`.
///
/// - An empty range always has `start=0` and `end=0`.
/// - Ranges are used for efficient file and buffer operations.
/// - All methods treat the range as closed-open.
#[derive(Clone, Eq, PartialEq, Default, serde::Serialize, serde::Deserialize, PartialOrd, Ord)]
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
    /// Build a zero-length range with the given position.
    pub fn zero_len(start: u64) -> Self {
        ByteRange { start, end: start }
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

impl fmt::Debug for ByteRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
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
#[derive(Clone, Eq, PartialEq, Default, Debug, serde::Serialize, serde::Deserialize)]
pub struct ByteRanges {
    btree: BTreeMap<u64, Point>,
}

#[derive(PartialEq, Eq, Copy, Clone, Debug, serde::Serialize, serde::Deserialize)]
enum Point {
    Start,
    End,
}

impl Point {
    fn reverse(&self) -> Self {
        match self {
            Point::Start => Point::End,
            Point::End => Point::Start,
        }
    }
}

impl ByteRanges {
    /// Creates an empty ByteRanges set.
    pub fn new() -> Self {
        ByteRanges {
            btree: BTreeMap::new(),
        }
    }

    pub fn single(start: u64, end: u64) -> Self {
        let mut r = ByteRanges::new();
        if end > start {
            r.btree.insert(start, Point::Start);
            r.btree.insert(end, Point::End);
        }

        r
    }

    /// Creates a ByteRanges containing a single range.
    pub fn for_range(range: ByteRange) -> Self {
        Self::single(range.start, range.end)
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
        self.iter()
            .flat_map(move |r| ChunkIterator::new(&r, chunk_size))
    }

    /// Returns true if there are no ranges.
    pub fn is_empty(&self) -> bool {
        self.btree.is_empty()
    }

    /// Returns the number of ranges.
    pub fn len(&self) -> usize {
        self.btree.len() / 2
    }

    /// Returns the total number of bytes within the ranges.
    pub fn bytecount(&self) -> u64 {
        self.iter().fold(0, |sum, r| sum + r.bytecount())
    }

    /// Add all ranges from `other` to `self`.
    pub fn extend(&mut self, mut other: ByteRanges) {
        if self.is_empty() {
            std::mem::swap(&mut self.btree, &mut other.btree);
            return;
        }
        for r in other.iter() {
            self.add(&r);
        }
    }

    /// Adds a new ByteRange, merging as needed to maintain minimal, sorted, non-overlapping invariants.
    pub fn add(&mut self, range: &ByteRange) {
        if range.is_empty() {
            return;
        }
        for val in self
            .btree
            .range(range.start..=range.end)
            .map(|(val, _)| *val)
            .collect::<Vec<_>>()
        {
            self.btree.remove(&val);
        }

        let before_start = self.btree.range(..range.start).next_back();
        if !matches!(before_start, Some((_, Point::Start))) {
            self.btree.insert(range.start, Point::Start);
        }

        let after_end = self.btree.range(range.end..).next();
        if !matches!(after_end, Some((_, Point::End))) {
            self.btree.insert(range.end, Point::End);
        }
    }

    /// Returns the union of two ByteRanges sets.
    pub fn union(&self, other: &ByteRanges) -> ByteRanges {
        if self.is_empty() {
            return other.clone();
        }
        let mut result = ByteRanges::new();
        let mut depth = 0;
        for pt in Merged::new(self.points(), other.points()) {
            match pt {
                (val, Point::Start) => {
                    if depth == 0 {
                        result.btree.insert(val, Point::Start);
                    }
                    depth += 1;
                }
                (val, Point::End) => {
                    depth -= 1;
                    if depth == 0 {
                        result.btree.insert(val, Point::End);
                    }
                }
            }
            debug_assert!(depth >= 0);
            debug_assert!(depth <= 2);
        }
        debug_assert_eq!(depth, 0);

        result
    }

    /// Returns the intersection of two ByteRanges sets.
    pub fn intersection(&self, other: &ByteRanges) -> ByteRanges {
        if self.is_empty() || other.is_empty() {
            return ByteRanges::new();
        }

        let mut result = ByteRanges::new();
        let mut depth = 0;
        for pt in Merged::new(self.points(), other.points()) {
            match pt {
                (val, Point::Start) => {
                    depth += 1;
                    if depth == 2 {
                        result.btree.insert(val, Point::Start);
                    }
                }
                (val, Point::End) => {
                    if depth == 2 {
                        result.btree.insert(val, Point::End);
                    }
                    depth -= 1;
                }
            }
            debug_assert!(depth >= 0);
            debug_assert!(depth <= 2);
        }
        debug_assert_eq!(depth, 0);

        result
    }

    /// Returns the subtraction of other from self (i.e., the parts of self not covered by other).
    pub fn subtraction(&self, other: &ByteRanges) -> ByteRanges {
        // Subtraction is implemented here as an intersection with the
        // reverse of other.
        let mut result = ByteRanges::new();

        // The reverse of other starts and ends opened.
        let mut depth = 1;
        let mut started = false;

        for pt in Merged::new(
            self.points(),
            other.btree.iter().map(|(v, p)| (*v, p.reverse())),
        ) {
            match pt {
                (val, Point::Start) => {
                    depth += 1;
                    if depth == 2 {
                        started = true;
                        result.btree.insert(val, Point::Start);
                    }
                }
                (val, Point::End) => {
                    if depth == 2 && started {
                        result.btree.insert(val, Point::End);
                    }
                    depth -= 1;
                }
            }
            debug_assert!(depth >= 0);
            debug_assert!(depth <= 2);
        }
        debug_assert_eq!(depth, 1);

        result
    }

    /// Returns true if any range in self overlaps with the given ByteRange.
    pub fn overlaps(&self, range: &ByteRange) -> bool {
        self.btree
            .range(range.start..range.end)
            .any(|(_, pt)| *pt == Point::Start)
            || matches!(
                self.btree.range(..range.start).next_back(),
                Some((_, Point::Start)),
            )
    }

    /// Returns an iterator over the contained [ByteRange]s.
    pub fn iter(&self) -> impl Iterator<Item = ByteRange> {
        ByteRangesIter::new(self.points())
    }

    /// Returns a representation of this ByteRanges as a vector of
    /// [ByteRange].
    pub fn as_vec(&self) -> Vec<ByteRange> {
        self.iter().collect()
    }

    /// Returns the range containing the given offset, if any.
    pub fn containing_range(&self, offset: u64) -> Option<ByteRange> {
        if let Some((start, Point::Start)) = self.btree.range(..=offset).next_back() {
            if let Some((end, Point::End)) = self.btree.range(offset + 1..).next() {
                return Some(ByteRange::new(*start, *end));
            }
        }

        None
    }

    fn points(&self) -> impl Iterator<Item = (u64, Point)> {
        self.btree.iter().map(|(k, v)| (*k, *v))
    }
}

impl IntoIterator for ByteRanges {
    type Item = ByteRange;
    type IntoIter = ByteRangesIntoIter;

    fn into_iter(self) -> ByteRangesIntoIter {
        ByteRangesIntoIter {
            inner: ByteRangesIter::new(self.btree.into_iter()),
        }
    }
}

pub struct ByteRangesIntoIter {
    inner: ByteRangesIter<btree_map::IntoIter<u64, Point>>,
}

impl Iterator for ByteRangesIntoIter {
    type Item = ByteRange;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// Transforms points into [ByteRange]s.
#[derive(Clone)]
struct ByteRangesIter<T>
where
    T: Iterator<Item = (u64, Point)>,
{
    inner: T,
}

impl<T> ByteRangesIter<T>
where
    T: Iterator<Item = (u64, Point)>,
{
    fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Iterator for ByteRangesIter<T>
where
    T: Iterator<Item = (u64, Point)>,
{
    type Item = ByteRange;

    fn next(&mut self) -> Option<Self::Item> {
        if let (Some((start, Point::Start)), Some((end, Point::End))) =
            (self.inner.next(), self.inner.next())
        {
            return Some(ByteRange::new(start, end));
        }

        None
    }
}

impl fmt::Display for ByteRanges {
    /// Formats as a comma-separated list of ranges.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        f.write_str("{")?;
        for range in self.iter() {
            if first {
                first = false;
            } else {
                write!(f, ", ")?;
            }
            write!(f, "{range}")?;
        }
        f.write_str("}")?;
        Ok(())
    }
}
// impl fmt::Debug for ByteRanges {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         fmt::Display::fmt(self, f)
//     }
// }

struct Merged<A, B>
where
    A: Iterator<Item = (u64, Point)>,
    B: Iterator<Item = (u64, Point)>,
{
    a: Peekable<A>,
    b: Peekable<B>,
}

impl<A, B> Merged<A, B>
where
    A: Iterator<Item = (u64, Point)>,
    B: Iterator<Item = (u64, Point)>,
{
    fn new(a: A, b: B) -> Self {
        Self {
            a: a.peekable(),
            b: b.peekable(),
        }
    }
}

impl<A, B> Iterator for Merged<A, B>
where
    A: Iterator<Item = (u64, Point)>,
    B: Iterator<Item = (u64, Point)>,
{
    type Item = (u64, Point);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.a.peek(), self.b.peek()) {
            (None, None) => None,
            (None, Some(_)) => self.b.next(),
            (Some(_), None) => self.a.next(),
            (Some((val_a, pt_a)), Some((val_b, pt_b))) => {
                if *val_a == *val_b && pt_a != pt_b {
                    // Skip any Star/End pair at the same value;
                    // that's a no-op.
                    self.a.next();
                    self.b.next();
                    return self.next();
                }
                if *val_a < *val_b {
                    self.a.next()
                } else {
                    self.b.next()
                }
            }
        }
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
        assert_eq!(
            ByteRanges::single(0, 15),
            ByteRanges::from_ranges(vec![
                ByteRange::new(0, 5),
                ByteRange::new(5, 10),
                ByteRange::new(12, 15),
                ByteRange::new(8, 13)
            ])
        );
    }
    #[test]
    fn test_union() {
        assert_eq!(
            ByteRanges::single(0, 10),
            ByteRanges::new().union(&ByteRanges::single(0, 10))
        );
        assert_eq!(
            ByteRanges::single(0, 10),
            ByteRanges::single(0, 10).union(&ByteRanges::new())
        );
        assert_eq!(
            ByteRanges::single(0, 10),
            ByteRanges::single(0, 5).union(&ByteRanges::single(5, 10))
        );
        assert_eq!(
            ByteRanges::single(0, 10),
            ByteRanges::single(0, 5).union(&ByteRanges::single(3, 10))
        );
        assert_eq!(
            ByteRanges::from_ranges(vec![ByteRange::new(0, 5), ByteRange::new(10, 15)]),
            ByteRanges::single(0, 5).union(&ByteRanges::single(10, 15))
        );
        assert_eq!(
            ByteRanges::single(0, 25),
            ByteRanges::from_ranges(vec![ByteRange::new(0, 5), ByteRange::new(10, 15)])
                .union(&ByteRanges::from_ranges(vec![ByteRange::new(2, 25)]))
        );
    }
    #[test]
    fn test_intersection() {
        assert_eq!(
            ByteRanges::new(),
            ByteRanges::new().intersection(&ByteRanges::single(0, 10))
        );
        assert_eq!(
            ByteRanges::new(),
            ByteRanges::single(0, 10).intersection(&ByteRanges::new())
        );
        assert_eq!(
            ByteRanges::new(),
            ByteRanges::single(0, 5).intersection(&ByteRanges::single(5, 10))
        );
        assert_eq!(
            ByteRanges::new(),
            ByteRanges::single(0, 5).intersection(&ByteRanges::single(10, 15))
        );
        assert_eq!(
            ByteRanges::single(3, 5),
            ByteRanges::single(0, 5).intersection(&ByteRanges::single(3, 10))
        );
        assert_eq!(
            ByteRanges::from_ranges(vec![ByteRange::new(5, 10), ByteRange::new(15, 20)]),
            ByteRanges::single(0, 100).intersection(&ByteRanges::from_ranges(vec![
                ByteRange::new(5, 10),
                ByteRange::new(15, 20)
            ]))
        );
    }
    #[test]
    fn test_subtraction() {
        assert_eq!(
            ByteRanges::new(),
            ByteRanges::new().subtraction(&ByteRanges::single(0, 10))
        );

        assert_eq!(
            ByteRanges::single(0, 10),
            ByteRanges::single(0, 10).subtraction(&ByteRanges::new())
        );

        assert_eq!(
            ByteRanges::new(),
            ByteRanges::single(0, 10).subtraction(&ByteRanges::single(0, 10))
        );

        assert_eq!(
            ByteRanges::new(),
            ByteRanges::single(10, 20).subtraction(&ByteRanges::single(0, 30))
        );

        assert_eq!(
            ByteRanges::single(15, 20),
            ByteRanges::single(10, 20).subtraction(&ByteRanges::single(0, 15))
        );

        assert_eq!(
            ByteRanges::single(10, 15),
            ByteRanges::single(10, 20).subtraction(&ByteRanges::single(15, 20))
        );

        assert_eq!(
            ByteRanges::from_ranges(vec![
                ByteRange::new(1, 2),
                ByteRange::new(5, 6),
                ByteRange::new(9, 10),
                ByteRange::new(13, 14),
            ]),
            ByteRanges::single(0, 20).subtraction(&ByteRanges::from_ranges(vec![
                ByteRange::new(0, 1),
                ByteRange::new(2, 5),
                ByteRange::new(6, 9),
                ByteRange::new(10, 13),
                ByteRange::new(14, 20),
            ])),
        );
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
        let mut r = ByteRanges::from_ranges(vec![ByteRange::new(0, 5), ByteRange::new(10, 15)]);
        r.extend(ByteRanges::from_ranges(vec![
            ByteRange::new(3, 8),
            ByteRange::new(12, 20),
        ]));
        // Should merge overlapping ranges: [0,5) + [3,8) = [0,8), [10,15) + [12,20) = [10,20)
        assert_eq!(
            ByteRanges::from_ranges(vec![ByteRange::new(0, 8), ByteRange::new(10, 20)]),
            r
        );
    }
    #[test]
    fn extend_empty() {
        let mut r = ByteRanges::single(0, 5);
        r.extend(ByteRanges::new());

        // Should remain unchanged
        assert_eq!(ByteRanges::single(0, 5), r);
    }
    #[test]
    fn extend_into_empty() {
        let mut r = ByteRanges::new();
        r.extend(ByteRanges::from_ranges(vec![
            ByteRange::new(0, 5),
            ByteRange::new(10, 15),
        ]));
        assert_eq!(
            ByteRanges::from_ranges(vec![ByteRange::new(0, 5), ByteRange::new(10, 15)]),
            r
        )
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
        assert_eq!(empty.containing_range(0), None);
        assert_eq!(empty.containing_range(100), None);

        // Test with single range
        let single = ByteRanges::single(10, 20);
        assert_eq!(single.containing_range(5), None); // Before range
        assert_eq!(single.containing_range(10), Some(ByteRange::new(10, 20))); // At start
        assert_eq!(single.containing_range(15), Some(ByteRange::new(10, 20))); // Inside range
        assert_eq!(single.containing_range(19), Some(ByteRange::new(10, 20))); // Just before end
        assert_eq!(single.containing_range(20), None); // At end (exclusive)
        assert_eq!(single.containing_range(25), None); // After range

        // Test with multiple ranges
        let mut multiple = ByteRanges::new();
        multiple.add(&ByteRange::new(0, 10));
        multiple.add(&ByteRange::new(20, 30));
        multiple.add(&ByteRange::new(40, 50));

        // Test offsets before first range
        assert_eq!(multiple.containing_range(0), Some(ByteRange::new(0, 10)));
        assert_eq!(multiple.containing_range(5), Some(ByteRange::new(0, 10)));
        assert_eq!(multiple.containing_range(9), Some(ByteRange::new(0, 10)));

        // Test offsets in gaps between ranges
        assert_eq!(multiple.containing_range(10), None); // At end of first range
        assert_eq!(multiple.containing_range(15), None); // In gap
        assert_eq!(multiple.containing_range(19), None); // Just before second range

        // Test offsets in second range
        assert_eq!(multiple.containing_range(20), Some(ByteRange::new(20, 30)));
        assert_eq!(multiple.containing_range(25), Some(ByteRange::new(20, 30)));
        assert_eq!(multiple.containing_range(29), Some(ByteRange::new(20, 30)));

        // Test offsets in gaps and third range
        assert_eq!(multiple.containing_range(30), None); // At end of second range
        assert_eq!(multiple.containing_range(35), None); // In gap
        assert_eq!(multiple.containing_range(40), Some(ByteRange::new(40, 50)));
        assert_eq!(multiple.containing_range(45), Some(ByteRange::new(40, 50)));
        assert_eq!(multiple.containing_range(49), Some(ByteRange::new(40, 50)));

        // Test offsets after last range
        assert_eq!(multiple.containing_range(50), None); // At end of last range
        assert_eq!(multiple.containing_range(100), None); // Far after
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
        assert_eq!(merged.containing_range(0), Some(ByteRange::new(0, 10)));
        assert_eq!(merged.containing_range(5), Some(ByteRange::new(0, 10)));
        assert_eq!(merged.containing_range(9), Some(ByteRange::new(0, 10)));
        assert_eq!(merged.containing_range(10), None); // At end of first merged range
        assert_eq!(merged.containing_range(15), Some(ByteRange::new(15, 25)));
        assert_eq!(merged.containing_range(20), Some(ByteRange::new(15, 25)));
        assert_eq!(merged.containing_range(24), Some(ByteRange::new(15, 25)));
        assert_eq!(merged.containing_range(25), None); // At end of second merged range
    }

    #[test]
    fn test_range_containing_edge_cases() {
        // Test with empty ranges (should be filtered out)
        let mut with_empty = ByteRanges::new();
        with_empty.add(&ByteRange::new(0, 0)); // Empty range
        with_empty.add(&ByteRange::new(10, 20));
        with_empty.add(&ByteRange::new(30, 30)); // Empty range

        assert_eq!(with_empty.containing_range(0), None); // Empty range doesn't contain anything
        assert_eq!(
            with_empty.containing_range(15),
            Some(ByteRange::new(10, 20))
        );
        assert_eq!(with_empty.containing_range(30), None); // Empty range doesn't contain anything

        // Test with very large offsets
        let large = ByteRanges::single(u64::MAX - 10, u64::MAX - 5);
        assert_eq!(
            large.containing_range(u64::MAX - 10),
            Some(ByteRange::new(u64::MAX - 10, u64::MAX - 5))
        );
        assert_eq!(
            large.containing_range(u64::MAX - 7),
            Some(ByteRange::new(u64::MAX - 10, u64::MAX - 5))
        );
        assert_eq!(large.containing_range(u64::MAX - 5), None);
        assert_eq!(large.containing_range(u64::MAX), None);
    }
}

// TODO: Implement methods and traits as described in the spec.
