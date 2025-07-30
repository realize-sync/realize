use super::types::JobAction;

/// A trait that let job implementation report internal progress.
pub(crate) trait ByteCountProgress {
    fn update_action(&mut self, action: JobAction);
    fn update(&mut self, current_bytes: u64, total_bytes: u64);
}

#[cfg(any(test))]
pub mod testing {
    use super::*;

    /// [ByteCountProgress] implementation that does nothing.
    pub(crate) struct NoOpByteCountProgress;

    impl ByteCountProgress for NoOpByteCountProgress {
        fn update_action(&mut self, _: JobAction) {}
        fn update(&mut self, _current_bytes: u64, _total_bytes: u64) {}
    }

    /// [ByteCountProgress] implementation that just remembers the last
    /// value.
    pub(crate) struct SimpleByteCountProgress {
        pub(crate) current_bytes: u64,
        pub(crate) total_bytes: u64,
        pub(crate) actions: Vec<JobAction>,
    }

    impl SimpleByteCountProgress {
        pub(crate) fn new() -> Self {
            Self {
                current_bytes: 0,
                total_bytes: 0,
                actions: vec![],
            }
        }
    }

    impl ByteCountProgress for SimpleByteCountProgress {
        fn update_action(&mut self, action: JobAction) {
            self.actions.push(action);
        }
        fn update(&mut self, current_bytes: u64, total_bytes: u64) {
            self.current_bytes = current_bytes;
            self.total_bytes = total_bytes;
        }
    }
}
