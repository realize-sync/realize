use std::cmp::min;

use realize_storage::JobId;
use realize_types::Arena;
use tokio::sync::broadcast;

use super::types::{ChurtenNotification, JobAction};

/// A trait that let job implementation report internal progress.
pub(crate) trait ByteCountProgress {
    fn update_action(&mut self, action: JobAction);
    fn update(&mut self, current_bytes: u64, total_bytes: u64);
}

/// Implementation of [ByteCountProgress] that sends updates to a
/// channel.
///
/// The bytecount updates are limited. If
/// [TxByteCountProgress::adaptive] is called, the limits adapt to how
/// full the channel is.
pub(crate) struct TxByteCountProgress {
    tx: broadcast::Sender<ChurtenNotification>,
    arena: Arena,
    job_id: JobId,
    resolution_bytes: u64,
    last_bytecount_update: Option<(u64, u64)>,
    channel_capacity: Option<usize>,
    index: u32,
}

impl TxByteCountProgress {
    /// Create a new progress that sends notification to the given channel.
    pub(crate) fn new(
        arena: Arena,
        job_id: JobId,
        tx: broadcast::Sender<ChurtenNotification>,
    ) -> Self {
        Self {
            arena,
            job_id,
            tx,
            resolution_bytes: 1,
            last_bytecount_update: None,
            channel_capacity: None,
            // Start at 0, this way New is 0 and Start is 1.
            index: 2,
        }
    }

    /// Minimum current byte count difference that will be reported.
    ///
    /// Defaults to 1, that is, any change is reported.
    pub(crate) fn with_min_byte_delta(mut self, resolution_bytes: u64) -> Self {
        self.resolution_bytes = resolution_bytes;

        self
    }

    /// Adapt bytecount update resolution to how full the channel is.
    pub(crate) fn adaptive(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = Some(channel_capacity);
        self
    }

    /// Check whether the given update should be sent.
    fn should_send(&self, current_bytes: u64, total_bytes: u64) -> bool {
        if current_bytes == total_bytes {
            return true;
        }

        match self.last_bytecount_update {
            None => true,
            Some((prev_current_bytes, prev_total_bytes)) => {
                if prev_total_bytes == total_bytes {
                    if delta(current_bytes, prev_current_bytes) < self.resolution_bytes {
                        return false;
                    }

                    // Limit to percentage counts; the minimum
                    // percentage depends on how full the channel is
                    // if adaptive() was called.
                    let prev_p = percent(prev_current_bytes, prev_total_bytes);
                    let p = percent(current_bytes, total_bytes);
                    let limit = self.resolution_p();
                    if delta(p, prev_p) < limit {
                        return false;
                    }
                }

                true
            }
        }
    }

    /// Only send an update if the bytecount is increased by that many
    /// percent.
    fn resolution_p(&self) -> u64 {
        match self.channel_capacity {
            None => 1,
            Some(capacity) => {
                let capacity_p = percent_usize(min(self.tx.len(), capacity), capacity);
                if capacity_p < 10 {
                    1
                } else if capacity_p < 25 {
                    5
                } else if capacity_p < 50 {
                    10
                } else if capacity_p < 75 {
                    25
                } else {
                    100
                }
            }
        }
    }

    fn next_index(&mut self) -> u32 {
        let index = self.index;
        self.index += 1;

        index
    }
}

impl ByteCountProgress for TxByteCountProgress {
    fn update_action(&mut self, action: JobAction) {
        let index = self.next_index();
        let _ = self.tx.send(ChurtenNotification::UpdateAction {
            arena: self.arena,
            job_id: self.job_id,
            index,
            action,
        });
    }
    fn update(&mut self, current_bytes: u64, total_bytes: u64) {
        if self.should_send(current_bytes, total_bytes) {
            self.last_bytecount_update = Some((current_bytes, total_bytes));
            let index = self.next_index();
            let _ = self.tx.send(ChurtenNotification::UpdateByteCount {
                arena: self.arena,
                job_id: self.job_id,
                index,
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

/// Compute percentage of u64 values.
#[inline]
fn percent(current: u64, total: u64) -> u64 {
    percent_f64(current as f64, total as f64)
}

/// Compute percentage of usize values.
#[inline]
fn percent_usize(current: usize, total: usize) -> u64 {
    percent_f64(current as f64, total as f64)
}

/// Compute percentage of positive values
#[inline]
fn percent_f64(current: f64, total: f64) -> u64 {
    (current * 100.0 / total) as u64
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
    use super::*;
    use tokio::task::{self, JoinHandle};

    struct Fixture {
        accumulator: Option<JoinHandle<Vec<ChurtenNotification>>>,
        tx: Option<broadcast::Sender<ChurtenNotification>>,
        weak_tx: broadcast::WeakSender<ChurtenNotification>,
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

        async fn take_notifications(&mut self) -> anyhow::Result<Vec<ChurtenNotification>> {
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
                ChurtenNotification::UpdateAction {
                    arena: Arena::from("myarena"),
                    job_id: JobId(1),
                    action: JobAction::Download,
                    index: 2,
                },
                ChurtenNotification::UpdateByteCount {
                    arena: Arena::from("myarena"),
                    job_id: JobId(1),
                    current_bytes: 0,
                    total_bytes: 1024,
                    index: 3,
                }
            ],
            notifications
        );

        Ok(())
    }

    #[tokio::test]
    async fn channel_limits_updates_to_percent() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup();
        let k: u64 = 1024;
        let m: u64 = 1024 * k;
        let total: u64 = 100 * m;
        // send 400 notifications, but only 100 are received, because
        // of the minimum required percentage update.
        let mut progress = fixture.create_progress();
        for i in (0..total).step_by(256 * k as usize) {
            progress.update(i as u64, total);
        }
        drop(progress); // closes tx, so accumulator ends

        let notifications = fixture.take_notifications().await?;
        assert_eq!(100, notifications.len(), "{notifications:?}");
        assert_eq!(1 * m, min_delta(notifications));

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

    #[tokio::test]
    async fn adaptive_progress() -> anyhow::Result<()> {
        let _ = env_logger::try_init();

        let capacity = 16;
        let (tx, mut rx) = broadcast::channel(capacity);
        let mut progress =
            TxByteCountProgress::new(Arena::from("myarena"), JobId(1), tx).adaptive(capacity);

        progress.update(0, 100);
        assert_eq!(1, rx.len());
        let _ = rx.recv().await;

        progress.update(1, 100);
        assert_eq!(1, rx.len());
        progress.update(2, 100);
        assert_eq!(2, rx.len());

        // 10% capacity, updates are now sent every 5%
        for i in 3..7 {
            progress.update(i, 100);
            assert_eq!(2, rx.len());
        }
        progress.update(8, 100);
        assert_eq!(3, rx.len());

        for i in 9..13 {
            progress.update(i, 100);
            assert_eq!(3, rx.len());
        }
        progress.update(14, 100);
        assert_eq!(4, rx.len());

        // 25% capacity, updates are now sent every 10%
        for i in 15..24 {
            progress.update(i, 100);
            assert_eq!(4, rx.len());
        }
        progress.update(25, 100);
        assert_eq!(5, rx.len());

        for i in 26..35 {
            progress.update(i, 100);
            assert_eq!(5, rx.len());
        }
        progress.update(36, 100);
        assert_eq!(6, rx.len());

        for i in 37..46 {
            progress.update(i, 100);
            assert_eq!(6, rx.len());
        }
        progress.update(47, 100);
        assert_eq!(7, rx.len());

        for i in 48..57 {
            progress.update(i, 100);
            assert_eq!(7, rx.len());
        }
        progress.update(58, 100);
        assert_eq!(8, rx.len());

        // 50% capacity, updates are now sent every 25%
        for i in 59..83 {
            progress.update(i, 100);
            assert_eq!(8, rx.len());
        }
        progress.update(84, 100);
        assert_eq!(9, rx.len());

        Ok(())
    }

    fn min_delta(notifications: Vec<ChurtenNotification>) -> u64 {
        let mut last = None;
        let mut ret = u64::max_value();
        for n in notifications {
            match n {
                ChurtenNotification::UpdateByteCount { current_bytes, .. } => {
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
