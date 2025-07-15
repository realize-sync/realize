use super::types::{BlobId, BlobTableEntry, FileTableEntry};
use realize_types::{ByteRanges, Hash};
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

/// Error returned by Blob when reading outside the available range.
///
/// This error is embedded into a [std::io::Error] of kind
/// [std::io::ErrorKind::InvalidData]. You can use
/// [BlobIncomplete::matches] to check an I/O error.
#[derive(thiserror::Error, Debug)]
#[error("local blob data is incomplete")]
pub struct BlobIncomplete;

impl BlobIncomplete {
    /// Returns true if the given I/O error is actually a BlobIncomplete.
    #[allow(dead_code)]
    pub fn matches(ioerr: &std::io::Error) -> bool {
        ioerr.kind() == std::io::ErrorKind::InvalidData
            && ioerr
                .get_ref()
                .map(|e| e.is::<BlobIncomplete>())
                .unwrap_or(false)
    }
}

/// A blob that provides async read and seek access to file data.
///
/// Attempts to read outside of available range will result in an I/O
/// error of kind [std::io::ErrorKind::InvalidData] with a
/// [BlobIncomplete] error.
pub struct Blob {
    blob_id: BlobId,
    file: tokio::fs::File,
    available_ranges: ByteRanges,
    size: u64,
    hash: Hash,

    /// The read/write/seek position.
    position: u64,
}

#[allow(dead_code)]
impl Blob {
    /// Create a new blob from a file and its available byte ranges.
    pub(crate) fn new(
        blob_id: BlobId,
        file_entry: FileTableEntry,
        blob_entry: BlobTableEntry,
        file: std::fs::File,
    ) -> Self {
        Self {
            blob_id,
            file: tokio::fs::File::from_std(file),
            available_ranges: blob_entry.written_areas,
            size: file_entry.metadata.size,
            hash: file_entry.content.hash,
            position: 0,
        }
    }

    /// Get the size of the corresponding file.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Get the parts of the file that are available locally.
    pub fn local_availability(&self) -> &ByteRanges {
        &self.available_ranges
    }

    /// Get the hash of the corresponding file.
    pub fn hash(&self) -> &Hash {
        &self.hash
    }

    /// Get the blob ID on the database.
    ///
    /// The blob ID is also used to construct the file path.
    pub(crate) fn id(&self) -> BlobId {
        self.blob_id
    }

    /// Adjust the len to cover only the available portion of the requested range.
    fn adjusted_len(&self, requested_len: usize) -> Option<usize> {
        let available = ByteRanges::single(self.position, requested_len as u64)
            .intersection(&self.available_ranges);
        if let Some(range) = available.iter().next()
            && range.start == self.position
        {
            return Some(range.bytecount() as usize);
        }

        // Len cannot be ajusted; the request should not go through.
        None
    }
}

impl AsyncRead for Blob {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let requested_len = buf.remaining();
        if requested_len == 0 {
            return Poll::Ready(Ok(()));
        }

        match self.adjusted_len(requested_len) {
            None => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                BlobIncomplete,
            ))),
            Some(adjusted_len) => {
                let mut shortbuf = buf.take(adjusted_len);
                match Pin::new(&mut self.file).poll_read(cx, &mut shortbuf) {
                    Poll::Ready(Ok(())) => {
                        let filled = shortbuf.filled().len();
                        let initialized = shortbuf.initialized().len();
                        self.position += filled as u64;
                        drop(shortbuf);

                        // We know at least initialized bytes of the
                        // previously unfilled portion of the buffer
                        // have been initialized through shortbuf.
                        unsafe {
                            buf.assume_init(initialized);
                        }
                        buf.set_filled(buf.filled().len() + filled);

                        Poll::Ready(Ok(()))
                    }
                    Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}

impl AsyncSeek for Blob {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.file).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        match Pin::new(&mut self.file).poll_complete(cx) {
            Poll::Ready(Ok(pos)) => {
                self.position = pos;
                Poll::Ready(Ok(pos))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}
