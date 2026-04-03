use super::types::{TransferNotification, JobAction, JobProgress};
use realize_storage::{Job, JobId};
use realize_types::Arena;
use std::collections::{HashMap, hash_map::IntoValues};
use std::sync::Arc;

/// Information about a job and its progress.
///
/// This structure is a snapshot of [TransferNotification]s for a given
/// job, built by the [JobInfoTracker]
#[derive(Debug, Clone, PartialEq)]
pub struct JobInfo {
    pub arena: Arena,
    pub id: JobId,
    pub job: Arc<Job>,
    pub progress: JobProgress,

    /// current action, if any
    pub action: Option<JobAction>,

    /// current / total
    pub byte_progress: Option<(u64, u64)>,
}

impl JobInfo {
    /// Return a global ID for the job.
    pub fn global_job_id(&self) -> (Arena, JobId) {
        (self.arena, self.id)
    }
}

/// Keep information about active jobs.
pub struct JobInfoTracker {
    /// Jobs currently being updated
    jobs: HashMap<(Arena, JobId), JobInfo>,
}

#[allow(dead_code)]
impl JobInfoTracker {
    /// Create a new tracker.
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
        }
    }

    /// Initialize or re-initialize the tracker from a set of jobs.
    pub fn init<T>(&mut self, jobs: T)
    where
        T: IntoIterator<Item = JobInfo>,
    {
        self.jobs.clear();
        for job in jobs {
            self.jobs.insert(job.global_job_id(), job);
        }
    }

    /// Check whether there are any jobs in this tracker.
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    /// Check how many active jobs there are
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    /// Remove jobs whose [JobProgress] indicate they are stopped.
    pub fn remove_finished(&mut self) {
        self.jobs.retain(|_, jobinfo| !jobinfo.progress.is_final())
    }

    /// Get a job from the tracker, if it is available.
    pub fn get(&self, global_id: &(Arena, JobId)) -> Option<&JobInfo> {
        self.jobs.get(global_id)
    }

    /// Iterate over all active [JobInfo]s, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &JobInfo> {
        self.jobs.values()
    }

    /// Update jobs inside this tracker.
    ///
    /// Return false if the notification was a duplicate or came
    /// out-of-sequence.
    pub fn update(&mut self, notification: &TransferNotification) -> bool {
        let arena = notification.arena();
        let job_id = notification.job_id();
        let global_id = notification.global_job_id();
        match notification {
            TransferNotification::New { job, .. } => {
                if !self.jobs.contains_key(&global_id) {
                    self.jobs.insert(
                        global_id,
                        JobInfo {
                            arena,
                            id: job_id,
                            job: Arc::clone(job),
                            progress: JobProgress::Pending,
                            action: None,
                            byte_progress: None,
                        },
                    );
                    return true;
                }
            }
            TransferNotification::Start { .. } => {
                if let Some(info) = self.jobs.get_mut(&global_id)
                    && (info.progress != JobProgress::Running && !info.progress.is_final())
                {
                    info.progress = JobProgress::Running;
                    return true;
                }
            }
            TransferNotification::Stop { progress, .. } => {
                if let Some(info) = self.jobs.get_mut(&global_id)
                    && (info.progress == JobProgress::Running
                        || info.progress == JobProgress::Pending)
                {
                    eprintln!("==== Update Progress {global_id:?} {progress:?}");
                    info.progress = progress.clone();
                    return true;
                }
            }
            TransferNotification::UpdateAction { action, .. } => {
                if let Some(info) = self.jobs.get_mut(&global_id)
                    && info.progress == JobProgress::Running
                {
                    info.action = Some(*action);
                    info.byte_progress = None;
                    return true;
                }
            }
            TransferNotification::UpdateByteCount {
                current_bytes,
                total_bytes,
                ..
            } => {
                if let Some(info) = self.jobs.get_mut(&global_id)
                    && info.progress == JobProgress::Running
                {
                    info.byte_progress = Some((*current_bytes, *total_bytes));
                    return true;
                }
            }
        };

        false
    }
}

impl IntoIterator for JobInfoTracker {
    type Item = JobInfo;

    type IntoIter = IntoValues<(Arena, JobId), JobInfo>;

    fn into_iter(self) -> Self::IntoIter {
        self.jobs.into_values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use realize_storage::Job;
    use realize_types::{Arena, Hash, Path};

    /// Test fixture for creating test data
    #[derive(Clone)]
    struct Fixture {
        arena: Arena,
        job_id: JobId,
        job: Arc<Job>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                arena: Arena::from("test-arena"),
                job_id: JobId::new(1),
                job: Arc::new(Job::Download(
                    Path::parse("test-path").unwrap(),
                    Hash::zero(),
                )),
            }
        }

        fn global_job_id(&self) -> (Arena, JobId) {
            (self.arena, self.job_id)
        }

        fn create_notification(&self, notification_type: &str) -> TransferNotification {
            match notification_type {
                "new" => TransferNotification::New {
                    arena: self.arena,
                    job_id: self.job_id,
                    job: Arc::clone(&self.job),
                },
                "start" => TransferNotification::Start {
                    arena: self.arena,
                    job_id: self.job_id,
                },
                "done" => TransferNotification::Stop {
                    arena: self.arena,
                    job_id: self.job_id,
                    progress: JobProgress::Done,
                },
                "failed" => TransferNotification::Stop {
                    arena: self.arena,
                    job_id: self.job_id,
                    progress: JobProgress::Failed("test".to_string()),
                },
                "abandoned" => TransferNotification::Stop {
                    arena: self.arena,
                    job_id: self.job_id,
                    progress: JobProgress::Abandoned,
                },
                "update_action(0)" => TransferNotification::UpdateAction {
                    arena: self.arena,
                    job_id: self.job_id,
                    action: JobAction::Download,
                },
                "update_action(1)" => TransferNotification::UpdateAction {
                    arena: self.arena,
                    job_id: self.job_id,
                    action: JobAction::Download,
                },
                "update_action(2)" => TransferNotification::UpdateAction {
                    arena: self.arena,
                    job_id: self.job_id,
                    action: JobAction::Download,
                },
                "update_action(3)" => TransferNotification::UpdateAction {
                    arena: self.arena,
                    job_id: self.job_id,
                    action: JobAction::Verify,
                },
                "update_byte_count(0)" => TransferNotification::UpdateByteCount {
                    arena: self.arena,
                    job_id: self.job_id,
                    current_bytes: 50,
                    total_bytes: 1000,
                },
                "update_byte_count(1)" => TransferNotification::UpdateByteCount {
                    arena: self.arena,
                    job_id: self.job_id,
                    current_bytes: 100,
                    total_bytes: 1000,
                },
                "update_byte_count(2)" => TransferNotification::UpdateByteCount {
                    arena: self.arena,
                    job_id: self.job_id,
                    current_bytes: 100,
                    total_bytes: 1000,
                },
                "update_byte_count(3)" => TransferNotification::UpdateByteCount {
                    arena: self.arena,
                    job_id: self.job_id,
                    current_bytes: 200,
                    total_bytes: 1000,
                },
                _ => panic!("Unknown notification type: {}", notification_type),
            }
        }
    }

    #[test]
    fn test_new_tracker() {
        let tracker = JobInfoTracker::new();
        assert!(tracker.is_empty());
        assert_eq!(tracker.len(), 0);
    }

    #[test]
    fn test_add_new_job() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();
        let notification = fixture.create_notification("new");

        assert!(tracker.update(&notification));

        assert!(!tracker.is_empty());
        assert_eq!(tracker.len(), 1);
        assert_eq!(tracker.iter().count(), 1);

        let job_info = tracker.iter().next().unwrap();
        assert_eq!(job_info.arena, fixture.arena);
        assert_eq!(job_info.id, fixture.job_id);
        assert_eq!(job_info.progress, JobProgress::Pending);
        assert_eq!(job_info.action, None);
        assert_eq!(job_info.byte_progress, None);
    }

    #[test]
    fn test_update_job_progress() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // Add new job
        assert!(tracker.update(&fixture.create_notification("new")));

        // Update to running
        assert!(tracker.update(&fixture.create_notification("start")));

        let job_info = tracker.iter().next().unwrap();
        assert_eq!(job_info.progress, JobProgress::Running);
        assert_eq!(job_info.action, None);
    }

    #[test]
    fn test_update_job_action() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // Add new job and start it
        assert!(tracker.update(&fixture.create_notification("new")));
        assert!(tracker.update(&fixture.create_notification("start")));

        // Update action
        assert!(tracker.update(&fixture.create_notification("update_action(2)")));

        let job_info = tracker.iter().next().unwrap();
        assert_eq!(job_info.action, Some(JobAction::Download));
    }

    #[test]
    fn test_update_job_action_resets_byte_count() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // Add new job and start it
        assert!(tracker.update(&fixture.create_notification("new")));
        assert!(tracker.update(&fixture.create_notification("start")));

        // Update byte count
        assert!(tracker.update(&fixture.create_notification("update_byte_count(2)")));

        // Update action
        assert!(tracker.update(&fixture.create_notification("update_action(3)")));

        let job_info = tracker.iter().next().unwrap();
        assert_eq!(job_info.byte_progress, None);
    }

    #[test]
    fn test_update_byte_count() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // Add new job and start it
        assert!(tracker.update(&fixture.create_notification("new")));
        assert!(tracker.update(&fixture.create_notification("start")));

        // Update byte count
        assert!(tracker.update(&fixture.create_notification("update_byte_count(2)")));

        let job_info = tracker.iter().next().unwrap();
        assert_eq!(job_info.byte_progress, Some((100, 1000)));
    }

    #[test]
    fn test_job_fails_and_is_kept() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        assert!(tracker.update(&fixture.create_notification("new")));
        assert_eq!(tracker.iter().count(), 1);

        assert!(tracker.update(&fixture.create_notification("failed")));

        assert_eq!(tracker.iter().count(), 1);
        tracker.remove_finished();
        assert_eq!(tracker.iter().count(), 1);
    }

    #[test]
    fn test_job_abandoned_and_is_removed() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        assert!(tracker.update(&fixture.create_notification("new")));
        assert_eq!(tracker.iter().count(), 1);

        assert!(tracker.update(&fixture.create_notification("abandoned")));

        assert_eq!(tracker.iter().count(), 1);
        tracker.remove_finished();
        assert_eq!(tracker.iter().count(), 0);
    }

    #[test]
    fn test_job_done_and_is_removed() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        assert!(tracker.update(&fixture.create_notification("new")));
        assert_eq!(tracker.iter().count(), 1);

        assert!(tracker.update(&fixture.create_notification("done")));

        assert_eq!(tracker.iter().count(), 1);
        tracker.remove_finished();
        assert_eq!(tracker.iter().count(), 0);
    }

    #[test]
    fn test_multiple_jobs() {
        let mut tracker = JobInfoTracker::new();
        let fixture1 = Fixture::new();
        let fixture2 = Fixture {
            job_id: JobId::new(2),
            ..fixture1.clone()
        };

        // Add two jobs
        assert!(tracker.update(&fixture1.create_notification("new")));
        assert!(tracker.update(&fixture2.create_notification("new")));

        assert_eq!(tracker.len(), 2);
        assert_eq!(tracker.iter().count(), 2);

        // Stop one job
        assert!(tracker.update(&fixture1.create_notification("done")));

        assert_eq!(tracker.len(), 2);
        assert_eq!(tracker.iter().count(), 2);

        tracker.remove_finished();

        // Only the unfinished job remains
        assert_eq!(tracker.len(), 1);
        assert_eq!(tracker.iter().count(), 1);
    }

    #[test]
    fn test_iter_returns_all_jobs() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // Add job
        assert!(tracker.update(&fixture.create_notification("new")));

        let fixture2 = Fixture {
            job_id: JobId::new(2),
            ..fixture
        };
        assert!(tracker.update(&fixture2.create_notification("new")));

        let all_jobs: Vec<_> = tracker.iter().collect();
        assert_eq!(all_jobs.len(), 2);
    }

    #[test]
    fn test_into_iter_consumes_tracker() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        assert!(tracker.update(&fixture.create_notification("new")));

        let jobs: Vec<_> = tracker.into_iter().collect();
        assert_eq!(jobs.len(), 1);
    }

    #[test]
    fn test_update_nonexistent_job_does_nothing() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // Try to update a job that doesn't exist
        assert!(!tracker.update(&fixture.create_notification("start")));

        assert!(tracker.is_empty());
        assert_eq!(tracker.len(), 0);
    }

    #[test]
    fn test_different_arenas_dont_conflict() {
        let mut tracker = JobInfoTracker::new();
        let fixture1 = Fixture::new();
        let fixture2 = Fixture {
            arena: Arena::from("different-arena"),
            ..fixture1.clone()
        };

        // Add jobs with different arenas but same job_id
        assert!(tracker.update(&fixture1.create_notification("new")));
        assert!(tracker.update(&fixture2.create_notification("new")));

        assert_eq!(tracker.len(), 2);
        assert_eq!(tracker.iter().count(), 2);
    }

    #[test]
    fn test_job_progress_transitions() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // Start with new job
        assert!(tracker.update(&fixture.create_notification("new")));
        let job_info = tracker.iter().next().unwrap();
        assert_eq!(job_info.progress, JobProgress::Pending);

        // Update to running
        assert!(tracker.update(&fixture.create_notification("start")));
        let job_info = tracker.iter().next().unwrap();
        assert_eq!(job_info.progress, JobProgress::Running);

        // Stop the job
        assert!(tracker.update(&fixture.create_notification("done")));
    }

    #[test]
    fn test_job_action_updates() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // Add job and start it
        assert!(tracker.update(&fixture.create_notification("new")));
        assert!(tracker.update(&fixture.create_notification("start")));

        // Update action multiple times
        assert!(tracker.update(&fixture.create_notification("update_action(2)")));
        let job_info = tracker.iter().next().unwrap().clone();
        assert_eq!(job_info.action, Some(JobAction::Download));

        // Update with different action
        assert!(tracker.update(&fixture.create_notification("update_action(3)")));
        let job_info = tracker.iter().next().unwrap().clone();
        assert_eq!(job_info.action, Some(JobAction::Verify));
    }

    #[test]
    fn test_byte_progress_updates() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // Add job and start it
        assert!(tracker.update(&fixture.create_notification("new")));
        assert!(tracker.update(&fixture.create_notification("start")));

        // Update byte progress
        assert!(tracker.update(&fixture.create_notification("update_byte_count(2)")));
        let job_info = tracker.iter().next().unwrap().clone();
        assert_eq!(job_info.byte_progress, Some((100, 1000)));

        // Update byte progress again (would need a different notification)
        // For now, just verify the byte progress is set correctly
        assert!(tracker.update(&fixture.create_notification("update_byte_count(3)")));
        let job_info = tracker.iter().next().unwrap().clone();
        assert_eq!(job_info.byte_progress, Some((200, 1000)));
    }

    #[test]
    fn test_get_method() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // Initially, get should return None
        assert!(tracker.get(&fixture.global_job_id()).is_none());

        // Add a job
        tracker.update(&fixture.create_notification("new"));

        // Now get should return the job
        let job_info = tracker.get(&fixture.global_job_id());
        assert!(job_info.is_some());
        let job_info = job_info.unwrap();
        assert_eq!(job_info.arena, fixture.arena);
        assert_eq!(job_info.id, fixture.job_id);
        assert_eq!(job_info.progress, JobProgress::Pending);

        // Get with different arena should return None
        let different_arena = Arena::from("different-arena");
        assert!(tracker.get(&(different_arena, fixture.job_id)).is_none());

        // Get with different job_id should return None
        let different_job_id = JobId::new(999);
        assert!(tracker.get(&(fixture.arena, different_job_id)).is_none());
    }

    #[test]
    fn test_duplicate_new_notification_rejected() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // First new notification should succeed
        assert!(tracker.update(&fixture.create_notification("new")));
        assert_eq!(tracker.len(), 1);

        // Second new notification for same job should be rejected
        assert!(!tracker.update(&fixture.create_notification("new")));
        assert_eq!(tracker.len(), 1); // Should still have only one job
    }

    #[test]
    fn test_start_notification_out_of_order_rejected() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // Try to start a job that doesn't exist
        assert!(!tracker.update(&fixture.create_notification("start")));
        assert_eq!(tracker.len(), 0);

        // Add job and start it
        tracker.update(&fixture.create_notification("new"));
        assert!(tracker.update(&fixture.create_notification("start")));

        // Try to start again - should be rejected
        assert!(!tracker.update(&fixture.create_notification("start")));

        // Job should still be in Running state
        let job_info = tracker.get(&fixture.global_job_id()).unwrap();
        assert_eq!(job_info.progress, JobProgress::Running);
    }

    #[test]
    fn test_stop_notification_out_of_order_rejected() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // Try to stop a job that doesn't exist
        assert!(!tracker.update(&fixture.create_notification("done")));
        assert_eq!(tracker.len(), 0);

        // Add job but don't start it
        assert!(tracker.update(&fixture.create_notification("new")));

        // Try to stop a pending job - should work
        assert!(tracker.update(&fixture.create_notification("done")));

        // Try to stop again - should be rejected
        assert!(!tracker.update(&fixture.create_notification("done")));
    }

    #[test]
    fn test_update_action_resets_byte_progress() {
        let mut tracker = JobInfoTracker::new();
        let fixture = Fixture::new();

        // Add, start, and set byte progress
        tracker.update(&fixture.create_notification("new"));
        tracker.update(&fixture.create_notification("start"));
        tracker.update(&fixture.create_notification("update_byte_count(1)"));

        let job_info = tracker.get(&fixture.global_job_id()).unwrap();
        assert_eq!(job_info.byte_progress, Some((100, 1000)));

        // Update action should reset byte progress
        assert!(tracker.update(&fixture.create_notification("update_action(2)")));
        let job_info = tracker.get(&fixture.global_job_id()).unwrap();
        assert_eq!(job_info.byte_progress, None);
        assert_eq!(job_info.action, Some(JobAction::Download));
    }
}
