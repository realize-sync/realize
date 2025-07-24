/// A trait that let job implementation report internal progress.
pub(crate) trait ByteCountProgress {
    fn update(&mut self, current_bytes: u64, total_bytes: u64);
}

#[cfg(any(test))]
pub mod testing {
    use super::*;

    /// [ByteCountProgress] implementation that does nothing.
    pub(crate) struct NoOpByteCountProgress;

    impl ByteCountProgress for NoOpByteCountProgress {
        fn update(&mut self, _current_bytes: u64, _total_bytes: u64) {}
    }

    /// [ByteCountProgress] implementation that just remembers the last
    /// value.
    pub(crate) struct SimpleByteCountProgress {
        pub(crate) current_bytes: u64,
        pub(crate) total_bytes: u64,
    }

    impl SimpleByteCountProgress {
        pub(crate) fn new() -> Self {
            Self {
                current_bytes: 0,
                total_bytes: 0,
            }
        }
    }

    impl ByteCountProgress for SimpleByteCountProgress {
        fn update(&mut self, current_bytes: u64, total_bytes: u64) {
            self.current_bytes = current_bytes;
            self.total_bytes = total_bytes;
        }
    }
}
