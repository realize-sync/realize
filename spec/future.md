# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Ranges {#ranges}

Let's make ByteRange a proper, useful type and use it to optimize
move_all.

ByteRange should be a proper type, not an alias. It should implement
the traits Clone, Eq, PartialEq, Debug, Display. Derive as much as
possible.

A ByteRange are closed-open ranges and should be displayed as such in
debug and display output: '[{start}, {end})'

An empty ByteRange always has start=0 and end=0 and intersects or
overlaps with nothing.

ByteRange should implement the methods:

 - new(start, end) -> ByteRange to create a new range

 - empty() -> ByteRange to get the empty range

 - is_empty(&self) -> bool to test if the range is empty

 - overlaps(&self, other: &ByteRange) -> bool to return true if `self`
   and `other` overlap

 - intersects(&self, other: &ByteRange) -> bool to return true if
   `self` and `other` intersect

 - intersection(&self, other: &ByteRange) -> ByteRange to build the
   intersection of `self` and `other` which might be empty, the empty
   range always has 0,0 offsets

 - span(&self, other: &ByteRange) -> ByteRange to build a range with
 the smaller start as start and the larger end as end.

 - chunked(&self, chunk_size) -> Vec<ByteRange> to split a range into chunks

There should be another type called `ByteRanges` to keep set of `ByteRange`:

ByteRanges should keep ByteRanges sorted and non-overlapping. The
ranges in ByteRanges should be minimal, so merged when possible. That
is if two ranges are [0, 1) and [1, 3), the range stored should be [0,
3).

ByteRanges should be a proper type, not an alias. It should hide its
content and only expose it through methods. It should implement the
traits Clone, Eq, PartialEq, Debug, Display. Derive as much as
possible.

ByteRanges should implement the following methods:

 - is_empty() -> bool
 - len() -> usie
 - add(&mut self, &ByteRange) to add a new byterange
 - union(&self, &ByteRanges) -> ByteRanges to make the union of two ByteRanges
 - intersection(&self, &ByteRanges) -> ByteRanges to make the intersection of two ByteRanges, which might be empty
 - subtraction(&self, &ByteRanges) -> ByteRanges to remove other from self and return the result.
 - intersects(&self, &ByteRange) -> bool true if the two ByteRanges intersects
 - overlaps(&self, &ByteRange) -> bool true if the two ByteRanges overlap
 - iter, into_iter - to get the ByteRanges

*Document the new types, method and module thoroughly*

1. Create a new module called "byterange.rs" in
   crate/realize-lib/src/model/byterange.rs, remember to add it to
   crate/realize-lib/src/model.rs then move the type ByteRange alias
   into that module. Run "cargo check --tests" to make sure it
   compiles.

2. Transform ByteRange into a proper type as described above. Add unit
   tests for it. Fix all uses of ByteRange. Add methods as necessary
   to meet the needs of the callers. Run "cargo check --tests" to make
   sure everything compiles and "cargo test" to make sure all tests
   pass

3. Add the type ByteRanges as described above. Add unit tests for it.
   Run "cargo test --lib" to make sure the unit tests pass.

4. Have RangedHash return a ByteRanges to represent the hashes for
   which there is a range. Have RangedHash::diff return two
   byteranges. Update all callers. Run "cargo test" to make sure
   everything still works.

5. Update file_hash to use ByteRange and ByteRanges instead of
   ByteRangeIterator. Run "cargo test" to make sure everything still
   works.

6. Have check_hash_and_delete return the ByteRanges for which the two
   hashes matched and use that in move_file to skip these ranges
   entirely, using ByteRange and ByteRanges operations to get the
   ByteRanges to process, the using ByteRanges.subtract to remove the
   ranges that matched, then ByteRange.chunk to chunk the remaining
   ranges and work on that. "cargo test" to make sure everything works

##Compression {#compress}

Implement compression as shown on:
https://raw.githubusercontent.com/google/tarpc/refs/heads/master/tarpc/examples/compression.rs

See whether it improves download. Currently we're at a surprisingly
stable 1.8MB/s when limited to 2MB/s (I assume 0.2MB/s for TLS)

This might not help much as long as the data is already compressed
(audio or video).

## Fix error message output {#errormsg}

When caught by with_context, error cause are printed.

When not caught by with_context, in move_files, error causes are not
printed. Also, in move_files, remote errors don't say which end (src
or dst) threw this.

- Fix error messages so that causes are printed. Keep error type cruft
  to a minimum.

- Add with_context to errors returned by a client (give a name to a
  client? use the address?)

- Print app errors in client at debug level

## Close connections {#closeconn}

TLS connections should be closed. Do it properly. Suppress or update
error logs complaining about it; for now they just say "read|write
errored out".

## Implement retries {#retry}

See the section "Error and retries" of spec/design.md.

## Design and add useful logging to realized {#daemonlog}

Think about what should be logged at the error, warning info and debug
level. What format should be followed. What information should be
sent.

## Optimize reads {#readopt}

Since move_files works on multiple files at once:
 - there's opportunity to write/read for one file while another file computes hash
 - the write and reads are scattered, making it hard on the OS buffers

Experiment with different ways of organizing the work that maybe speed
things up.

