use realize_storage::{Job, JobId};
use realize_types::Arena;
use std::sync::Arc;

/// Notifications broadcast by [Churten].
#[derive(Debug, Clone, PartialEq)]
pub enum ChurtenNotification {
    /// Report a new job, in state [JobProgress::Pending].
    New {
        arena: Arena,
        job_id: JobId,
        job: Arc<Job>,
    },

    /// Start a pending job, which enters state [JobProgress::Running].
    Start { arena: Arena, job_id: JobId },

    /// Finish a job, successfully or not.
    Finish {
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

        /// Notification sequence index for this specific job, for
        /// de-duplication.
        index: u32,
        action: JobAction,
    },

    /// Report bytecount update, such as for a copy or a download job.
    ///
    /// Not all jobs emit such updates.
    UpdateByteCount {
        arena: Arena,
        job_id: JobId,

        /// Notification sequence index for this specific job, for
        /// de-duplication.
        index: u32,

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
    /// A globally-unique identifier for the job.
    pub fn global_job_id(&self) -> (Arena, JobId) {
        (self.arena(), self.job_id())
    }

    pub fn arena(&self) -> Arena {
        match self {
            ChurtenNotification::New { arena, .. } => *arena,
            ChurtenNotification::Start { arena, .. } => *arena,
            ChurtenNotification::Finish { arena, .. } => *arena,
            ChurtenNotification::UpdateByteCount { arena, .. } => *arena,
            ChurtenNotification::UpdateAction { arena, .. } => *arena,
        }
    }
    pub fn job_id(&self) -> JobId {
        match self {
            ChurtenNotification::New { job_id, .. } => *job_id,
            ChurtenNotification::Start { job_id, .. } => *job_id,
            ChurtenNotification::Finish { job_id, .. } => *job_id,
            ChurtenNotification::UpdateByteCount { job_id, .. } => *job_id,
            ChurtenNotification::UpdateAction { job_id, .. } => *job_id,
        }
    }
}

/// Job progress reported by [ChurtenNotification]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobProgress {
    /// The job has been created, but not yet started.
    Pending,

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

impl JobProgress {
    pub fn is_finished(&self) -> bool {
        match self {
            JobProgress::Pending => false,
            JobProgress::Running => false,
            JobProgress::Done => true,
            JobProgress::Abandoned => true,
            JobProgress::Cancelled => true,
            JobProgress::Failed(_) => true,
        }
    }
}

/// An specific action taken by a job.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum JobAction {
    Download,
    Verify,
    Repair,
    Move,
}
