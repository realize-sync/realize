use realize_storage::{Job, JobId};
use realize_types::Arena;
use std::sync::Arc;

/// Notifications broadcast by [Transfer].
#[derive(Debug, Clone, PartialEq)]
pub enum TransferNotification {
    /// Report a new job, in state [JobProgress::Pending].
    New {
        arena: Arena,
        job_id: JobId,
        job: Arc<Job>,
    },

    /// Start processing a pending job, which enters state
    /// [JobProgress::Running].
    Start { arena: Arena, job_id: JobId },

    /// Stop processing the job. [JobProgress] specifies the new job
    /// status, which might be a final status..
    Stop {
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

impl TransferNotification {
    /// A globally-unique identifier for the job.
    pub fn global_job_id(&self) -> (Arena, JobId) {
        (self.arena(), self.job_id())
    }

    pub fn arena(&self) -> Arena {
        match self {
            TransferNotification::New { arena, .. } => *arena,
            TransferNotification::Start { arena, .. } => *arena,
            TransferNotification::Stop { arena, .. } => *arena,
            TransferNotification::UpdateByteCount { arena, .. } => *arena,
            TransferNotification::UpdateAction { arena, .. } => *arena,
        }
    }
    pub fn job_id(&self) -> JobId {
        match self {
            TransferNotification::New { job_id, .. } => *job_id,
            TransferNotification::Start { job_id, .. } => *job_id,
            TransferNotification::Stop { job_id, .. } => *job_id,
            TransferNotification::UpdateByteCount { job_id, .. } => *job_id,
            TransferNotification::UpdateAction { job_id, .. } => *job_id,
        }
    }
}

/// Job progress reported by [TransferNotification]
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

    /// The job was cancelled by a call to [Transfer::shutdown].
    Cancelled,

    /// Peers the job needed to connect to were offline.
    NoPeers,

    /// The job failed. It may be retried.
    ///
    /// The string is an error description.
    Failed(String),
}

impl JobProgress {
    /// Return true if this is a final progress.
    pub fn is_final(&self) -> bool {
        match self {
            JobProgress::Pending => false,
            JobProgress::Running => false,
            JobProgress::Done => true,
            JobProgress::Abandoned => true,
            JobProgress::Cancelled => true,
            JobProgress::NoPeers => false,
            JobProgress::Failed(_) => false,
        }
    }
}

/// An specific action taken by a job.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum JobAction {
    Download,
    Verify,
    Repair,
}
