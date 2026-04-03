use super::types::{TransferNotification, JobAction};
use realize_storage::JobId;
use realize_types::Arena;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

/// A trait that let job implementation report internal progress.
pub(crate) trait ByteCountProgress {
    fn update_action(&mut self, action: JobAction);
    fn update(&mut self, current_bytes: u64, total_bytes: u64);
}

/// Implementation of [ByteCountProgress] that sends updates to a
/// channel.
///
/// The number of bytecount update may optionally be limited.
pub(crate) struct TxByteCountProgress {
    tx: broadcast::Sender<TransferNotification>,
    arena: Arena,
    job_id: JobId,
    resolution_bytes: u64,
    burst_limiter: Duration,
    last_bytecount_update: Option<(u64, u64, Instant)>,
}

impl TxByteCountProgress {
    /// Create a new progress that sends notification to the given channel.
    pub(crate) fn new(
        arena: Arena,
        job_id: JobId,
        tx: broadcast::Sender<TransferNotification>,
    ) -> Self {
        Self {
            arena,
            job_id,
            tx,
            resolution_bytes: 1,
            burst_limiter: Duration::ZERO,
            last_bytecount_update: None,
        }
    }

    /// Minimum current byte count difference that will be reported.
    ///
    /// Defaults to 1, that is, any change is reported.
    pub(crate) fn with_min_byte_delta(mut self, resolution_bytes: u64) -> Self {
        self.resolution_bytes = resolution_bytes;

        self
    }

    /// After sending a bytecount update, wait that long to let
    /// another through.
    pub(crate) fn with_burst_limit(mut self, limit: Duration) -> Self {
        self.burst_limiter = limit;

        self
    }

    /// Check whether the given update should be sent.
    fn should_send(&self, current_bytes: u64, total_bytes: u64) -> bool {
        if current_bytes == total_bytes {
            return true;
        }

        match self.last_bytecount_update {
            None => true,
            Some((prev_current_bytes, prev_total_bytes, last_update_time)) => {
                if prev_total_bytes == total_bytes {
                    if delta(current_bytes, prev_current_bytes) < self.resolution_bytes {
                        return false;
                    }
                    if self.burst_limiter > Duration::ZERO
                        && Instant::now().duration_since(last_update_time) < self.burst_limiter
                    {
                        return false;
                    }
                }

                true
            }
        }
    }
}

impl ByteCountProgress for TxByteCountProgress {
    fn update_action(&mut self, action: JobAction) {
        let _ = self.tx.send(TransferNotification::UpdateAction {
            arena: self.arena,
            job_id: self.job_id,
            action,
        });
    }
    fn update(&mut self, current_bytes: u64, total_bytes: u64) {
        if self.should_send(current_bytes, total_bytes) {
            self.last_bytecount_update = Some((current_bytes, total_bytes, Instant::now()));
            let _ = self.tx.send(TransferNotification::UpdateByteCount {
                arena: self.arena,
                job_id: self.job_id,
                current_bytes,
                total_bytes,
            });
        }
    }
}

/// Return the difference between two positive values
#[inline]
fn delta(a: u64, b: u64) -> u64 {
    if a > b { a - b } else { b - a }
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

#[cfg(test)]
mod tests {
    use std::cmp::min;

    use super::*;
    use tokio::task::{self, JoinHandle};

    struct Fixture {
        accumulator: Option<JoinHandle<Vec<TransferNotification>>>,
        tx: Option<broadcast::Sender<TransferNotification>>,
        weak_tx: broadcast::WeakSender<TransferNotification>,
    }

    impl Fixture {
        fn setup() -> Self {
            let _ = env_logger::try_init();

            let (tx, mut rx) = broadcast::channel(128);
            let accumulator = task::spawn(async move {
                let mut vec = vec![];
                while let Ok(n) = rx.recv().await {
                    vec.push(n);
                }

                vec
            });

            Self {
                accumulator: Some(accumulator),
                weak_tx: tx.downgrade(),
                tx: Some(tx),
            }
        }

        fn create_progress(&mut self) -> TxByteCountProgress {
            TxByteCountProgress::new(
                Arena::from("myarena"),
                JobId(1),
                self.tx.take().expect("tx already taken"),
            )
        }

        async fn take_notifications(&mut self) -> anyhow::Result<Vec<TransferNotification>> {
            assert!(
                self.weak_tx.upgrade().is_none(),
                "Drop channel before calling take_notifications"
            );
            Ok(self
                .accumulator
                .take()
                .expect("notifications already taken")
                .await?)
        }
    }

    #[tokio::test]
    async fn channel_receives_updates() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup();
        let mut progress = fixture.create_progress();
        progress.update_action(JobAction::Download);
        progress.update(0, 1024);
        drop(progress); // closes tx, so accumulator ends

        let notifications = fixture.take_notifications().await?;
        assert_eq!(
            vec![
                TransferNotification::UpdateAction {
                    arena: Arena::from("myarena"),
                    job_id: JobId(1),
                    action: JobAction::Download,
                },
                TransferNotification::UpdateByteCount {
                    arena: Arena::from("myarena"),
                    job_id: JobId(1),
                    current_bytes: 0,
                    total_bytes: 1024,
                }
            ],
            notifications
        );

        Ok(())
    }

    #[tokio::test]
    async fn channel_limits_updates_to_bytecount() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup();

        let k: u64 = 1024;

        // Send 128 notification, every kb, but we only receive one
        // every 16kb, so 8.
        let mut progress = fixture.create_progress().with_min_byte_delta(16 * k);
        let total: u64 = 128 * k;
        for i in (0..total).step_by(k as usize) {
            progress.update(i as u64, total);
        }
        drop(progress); // closes tx, so accumulator ends

        let notifications = fixture.take_notifications().await?;
        assert_eq!(8, notifications.len(), "{notifications:?}");
        assert_eq!(16 * k, min_delta(notifications));

        Ok(())
    }

    #[tokio::test]
    async fn channel_always_sends_changes_to_total() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup();
        let mut progress = fixture.create_progress().with_min_byte_delta(1024);
        progress.update(0, 20);
        progress.update(1, 21);
        progress.update(1, 22);
        drop(progress); // closes tx, so accumulator ends

        let notifications = fixture.take_notifications().await?;
        assert_eq!(3, notifications.len(), "{notifications:?}");

        Ok(())
    }

    fn min_delta(notifications: Vec<TransferNotification>) -> u64 {
        let mut last = None;
        let mut ret = u64::max_value();
        for n in notifications {
            match n {
                TransferNotification::UpdateByteCount { current_bytes, .. } => {
                    if let Some(last_current_bytes) = last {
                        ret = min(ret, delta(last_current_bytes, current_bytes));
                    }
                    last = Some(current_bytes);
                }
                _ => {}
            }
        }

        ret
    }
}
