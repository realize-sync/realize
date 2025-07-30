use realize_storage::{Job, JobId};
use realize_types::Arena;
use std::sync::Arc;

/// Notifications broadcast by [Churten].
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ChurtenNotification {
    /// Report a new job, in state [JobProgress::Pending].
    New {
        arena: Arena,
        job_id: JobId,
        job: Arc<Job>,
    },
    /// Update the general job state.
    Update {
        arena: Arena,
        job_id: JobId,
        progress: JobProgress,
    },

    /// Report a specific action taken by the job.
    ///
    /// Any byte count progress previously reported should be
    /// considered invalid after this notification and until the next
    /// byte count update.
    UpdateAction {
        arena: Arena,
        job_id: JobId,
        action: JobAction,
    },

    /// Report bytecount update, such as for a copy or a download job.
    ///
    /// Not all jobs emit such updates.
    UpdateByteCount {
        arena: Arena,
        job_id: JobId,

        /// Current number of bytes.
        ///
        /// The first such update normally, but not necessarily has
        /// current_bytes set to 0.
        ///
        /// This value normally but not necessarily increases.
        current_bytes: u64,

        /// Total (expected) number of bytes.
        ///
        /// This value is normally, but not necessarily stable.
        total_bytes: u64,
    },
}

impl ChurtenNotification {
    pub(crate) fn arena(&self) -> Arena {
        match self {
            ChurtenNotification::New { arena, .. } => *arena,
            ChurtenNotification::Update { arena, .. } => *arena,
            ChurtenNotification::UpdateByteCount { arena, .. } => *arena,
            ChurtenNotification::UpdateAction { arena, .. } => *arena,
        }
    }
    pub(crate) fn job_id(&self) -> JobId {
        match self {
            ChurtenNotification::New { job_id, .. } => *job_id,
            ChurtenNotification::Update { job_id, .. } => *job_id,
            ChurtenNotification::UpdateByteCount { job_id, .. } => *job_id,
            ChurtenNotification::UpdateAction { job_id, .. } => *job_id,
        }
    }
}

/// Job progress reported by [ChurtenNotification]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JobProgress {
    /// The job is running.
    Running,

    /// The job was completed successfully.
    Done,

    /// The job was abandoned, likely because it is outdated.
    Abandoned,

    /// The job was cancelled by a call to [Churten::shutdown].
    Cancelled,

    /// The job failed. It may be retried.
    ///
    /// The string is an error description.
    Failed(String),
}

/// An specific action taken by a job.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub(crate) enum JobAction {
    Download,
    Verify,
    Repair,
    Move,
}
