use super::types::{ChurtenNotification, JobAction, JobProgress};
use realize_storage::{Job, JobId};
use realize_types::Arena;
use std::collections::{HashMap, VecDeque, hash_map::IntoValues};
use std::sync::Arc;

/// Information about a job and its progress.
///
/// This structure is a snapshot of [ChurtenNotification]s for a given
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

    /// Index of the last UpdateAction/UpdateByteCount notification
    /// processed for this job.
    pub notification_index: u32,
}

impl JobInfo {
    /// Return a global ID for the job.
    pub fn global_job_id(&self) -> (Arena, JobId) {
        (self.arena, self.id)
    }
}

/// Keep limited historical information about jobs.
pub struct JobInfoTracker {
    /// Maximum desired number of jobs that should be kept by this
    /// tracker.
    ///
    /// This limit covers both the active and the finished jobs,
    /// though only the finished jobs are ever removed. This means
    /// that the tracker can be above limit if there are many active
    /// jobs.
    limit: usize,

    /// Jobs currently being updated
    jobs: HashMap<(Arena, JobId), JobInfo>,

    /// Finished jobs, ordered from the oldest (front) to the newest (back).
    finished: VecDeque<(Arena, JobId)>,
}

#[allow(dead_code)]
impl JobInfoTracker {
    /// Create a new tracker with the given limit.
    ///
    /// The tracker tries to keep the total number of jobs it keeps under
    /// the limit, though there can be more if there are many active jobs.
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            finished: VecDeque::new(),
            jobs: HashMap::new(),
        }
    }

    /// Initialize or re-initialize the tracker from a set of jobs.
    pub fn init<T>(&mut self, jobs: T)
    where
        T: IntoIterator<Item = JobInfo>,
    {
        self.finished.clear();
        self.jobs.clear();
        for job in jobs {
            let key = job.global_job_id();
            if job.progress.is_finished() {
                self.finished.push_back(key.clone())
            }
            self.jobs.insert(key, job);
        }
        self.trim();
    }

    /// Check whether there are any jobs in this tracker.
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    /// Check how many jobs there are
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    /// Return the number of active jobs.
    pub fn active_len(&self) -> usize {
        self.jobs.len() - self.finished.len()
    }

    /// Return true if there are active jobs.
    pub fn has_active_jobs(&self) -> bool {
        self.jobs.len() > self.finished.len()
    }

    /// Get a job from the tracker, if it is available.
    pub fn get(&self, global_id: &(Arena, JobId)) -> Option<&JobInfo> {
        self.jobs.get(global_id)
    }

    /// Iterate over all [JobInfo]s, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &JobInfo> {
        self.jobs.values()
    }

    /// Iterate over all active [JobInfo]s.
    pub fn active(&self) -> impl Iterator<Item = &JobInfo> {
        self.jobs.values().filter(|j| !j.progress.is_finished())
    }

    /// Iterate over all finished [JobInfo]s.
    pub fn finished(&self) -> impl Iterator<Item = &JobInfo> {
        self.jobs.values().filter(|j| j.progress.is_finished())
    }

    /// Update jobs inside this tracker.
    ///
    /// Return false if the notification was a duplicate or came
    /// out-of-sequence.
    pub fn update(&mut self, notification: &ChurtenNotification) -> bool {
        let arena = notification.arena();
        let job_id = notification.job_id();
        let global_id = notification.global_job_id();
        match notification {
            ChurtenNotification::New { job, .. } => {
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
                            notification_index: 0,
                        },
                    );
                    self.trim();
                    return true;
                }
            }
            ChurtenNotification::Start { .. } => {
                if let Some(info) = self.jobs.get_mut(&global_id)
                    && info.progress == JobProgress::Pending
                {
                    info.progress = JobProgress::Running;
                    return true;
                }
            }
            ChurtenNotification::Finish { progress, .. } => {
                if let Some(info) = self.jobs.get_mut(&global_id)
                    && !info.progress.is_finished()
                {
                    info.progress = progress.clone();
                    info.action = None;
                    self.finished.push_back(global_id.clone());
                    self.trim();
                    return true;
                }
            }
            ChurtenNotification::UpdateAction { action, index, .. } => {
                if let Some(info) = self.jobs.get_mut(&global_id)
                    && info.progress == JobProgress::Running
                    && info.notification_index < *index
                {
                    info.notification_index = *index;
                    info.action = Some(*action);
                    info.byte_progress = None;
                    return true;
                }
            }
            ChurtenNotification::UpdateByteCount {
                index,
                current_bytes,
                total_bytes,
                ..
            } => {
                if let Some(info) = self.jobs.get_mut(&global_id)
                    && info.progress == JobProgress::Running
                    && info.notification_index < *index
                {
                    info.notification_index = *index;
                    info.byte_progress = Some((*current_bytes, *total_bytes));
                    return true;
                }
            }
        };

        false
    }

    /// Get rid of the oldest finished jobs as long as
    /// the total number of jobs is above limit.
    fn trim(&mut self) {
        while self.len() > self.limit && !self.finished.is_empty() {
            if let Some(key) = self.finished.pop_front() {
                self.jobs.remove(&key);
            }
        }
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

        fn create_notification(&self, notification_type: &str) -> ChurtenNotification {
            match notification_type {
                "new" => ChurtenNotification::New {
                    arena: self.arena,
                    job_id: self.job_id,
                    job: Arc::clone(&self.job),
                },
                "start" => ChurtenNotification::Start {
                    arena: self.arena,
                    job_id: self.job_id,
                },
                "finish" => ChurtenNotification::Finish {
                    arena: self.arena,
                    job_id: self.job_id,
                    progress: JobProgress::Done,
                },
                "update_action(0)" => ChurtenNotification::UpdateAction {
                    arena: self.arena,
                    job_id: self.job_id,
                    action: JobAction::Download,
                    index: 0,
                },
                "update_action(1)" => ChurtenNotification::UpdateAction {
                    arena: self.arena,
                    job_id: self.job_id,
                    action: JobAction::Download,
                    index: 1,
                },
                "update_action(2)" => ChurtenNotification::UpdateAction {
                    arena: self.arena,
                    job_id: self.job_id,
                    action: JobAction::Download,
                    index: 2,
                },
                "update_action(3)" => ChurtenNotification::UpdateAction {
                    arena: self.arena,
                    job_id: self.job_id,
                    action: JobAction::Verify,
                    index: 3,
                },
                "update_byte_count(0)" => ChurtenNotification::UpdateByteCount {
                    arena: self.arena,
                    job_id: self.job_id,
                    current_bytes: 50,
                    total_bytes: 1000,
                    index: 0,
                },
                "update_byte_count(1)" => ChurtenNotification::UpdateByteCount {
                    arena: self.arena,
                    job_id: self.job_id,
                    current_bytes: 100,
                    total_bytes: 1000,
                    index: 1,
                },
                "update_byte_count(2)" => ChurtenNotification::UpdateByteCount {
                    arena: self.arena,
                    job_id: self.job_id,
                    current_bytes: 100,
                    total_bytes: 1000,
                    index: 2,
                },
                "update_byte_count(3)" => ChurtenNotification::UpdateByteCount {
                    arena: self.arena,
                    job_id: self.job_id,
                    current_bytes: 200,
                    total_bytes: 1000,
                    index: 3,
                },
                _ => panic!("Unknown notification type: {}", notification_type),
            }
        }
    }

    #[test]
    fn test_new_tracker() {
        let tracker = JobInfoTracker::new(10);
        assert!(tracker.is_empty());
        assert_eq!(tracker.len(), 0);
    }

    #[test]
    fn test_add_new_job() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();
        let notification = fixture.create_notification("new");

        assert!(tracker.update(&notification));

        assert!(!tracker.is_empty());
        assert_eq!(tracker.len(), 1);
        assert_eq!(tracker.active().count(), 1);
        assert_eq!(tracker.finished().count(), 0);

        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.arena, fixture.arena);
        assert_eq!(job_info.id, fixture.job_id);
        assert_eq!(job_info.progress, JobProgress::Pending);
        assert_eq!(job_info.action, None);
        assert_eq!(job_info.byte_progress, None);
    }

    #[test]
    fn test_update_job_progress() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add new job
        assert!(tracker.update(&fixture.create_notification("new")));

        // Update to running
        assert!(tracker.update(&fixture.create_notification("start")));

        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.progress, JobProgress::Running);
        assert_eq!(job_info.action, None);
    }

    #[test]
    fn test_update_job_action() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add new job and start it
        assert!(tracker.update(&fixture.create_notification("new")));
        assert!(tracker.update(&fixture.create_notification("start")));

        // Update action
        assert!(tracker.update(&fixture.create_notification("update_action(2)")));

        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.action, Some(JobAction::Download));
    }

    #[test]
    fn test_update_job_action_resets_byte_count() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add new job and start it
        assert!(tracker.update(&fixture.create_notification("new")));
        assert!(tracker.update(&fixture.create_notification("start")));

        // Update byte count
        assert!(tracker.update(&fixture.create_notification("update_byte_count(2)")));

        // Update action
        assert!(tracker.update(&fixture.create_notification("update_action(3)")));

        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.byte_progress, None);
    }

    #[test]
    fn test_update_byte_count() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add new job and start it
        assert!(tracker.update(&fixture.create_notification("new")));
        assert!(tracker.update(&fixture.create_notification("start")));

        // Update byte count
        assert!(tracker.update(&fixture.create_notification("update_byte_count(2)")));

        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.byte_progress, Some((100, 1000)));
    }

    #[test]
    fn test_job_finishes_and_moves_to_finished() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add new job
        assert!(tracker.update(&fixture.create_notification("new")));
        assert_eq!(tracker.active().count(), 1);
        assert_eq!(tracker.finished().count(), 0);

        // Finish the job
        assert!(tracker.update(&fixture.create_notification("finish")));

        assert_eq!(tracker.active().count(), 0);
        assert_eq!(tracker.finished().count(), 1);

        let finished_job = tracker.finished().next().unwrap();
        assert_eq!(finished_job.progress, JobProgress::Done);
    }

    #[test]
    fn test_finish_resets_action() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add new job
        assert!(tracker.update(&fixture.create_notification("new")));
        assert_eq!(tracker.active().count(), 1);
        assert_eq!(tracker.finished().count(), 0);

        // Start it
        assert!(tracker.update(&fixture.create_notification("start")));
        assert_eq!(tracker.active().count(), 1);
        assert_eq!(tracker.finished().count(), 0);

        // Update action
        assert!(tracker.update(&fixture.create_notification("update_action(2)")));

        // Finish the job
        assert!(tracker.update(&fixture.create_notification("finish")));

        assert_eq!(tracker.active().count(), 0);
        assert_eq!(tracker.finished().count(), 1);

        let finished_job = tracker.finished().next().unwrap();
        assert_eq!(finished_job.byte_progress, None);
    }

    #[test]
    fn test_multiple_jobs() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture1 = Fixture::new();
        let fixture2 = Fixture {
            job_id: JobId::new(2),
            ..fixture1.clone()
        };

        // Add two jobs
        assert!(tracker.update(&fixture1.create_notification("new")));
        assert!(tracker.update(&fixture2.create_notification("new")));

        assert_eq!(tracker.len(), 2);
        assert_eq!(tracker.active().count(), 2);

        // Finish one job
        assert!(tracker.update(&fixture1.create_notification("finish")));

        assert_eq!(tracker.len(), 2);
        assert_eq!(tracker.active().count(), 1);
        assert_eq!(tracker.finished().count(), 1);
    }

    #[test]
    fn test_trim_removes_oldest_finished_jobs() {
        let mut tracker = JobInfoTracker::new(3);
        let fixtures = vec![
            Fixture::new(),
            Fixture {
                job_id: JobId::new(2),
                ..Fixture::new()
            },
            Fixture {
                job_id: JobId::new(3),
                ..Fixture::new()
            },
            Fixture {
                job_id: JobId::new(4),
                ..Fixture::new()
            },
        ];

        // Add and finish 4 jobs
        for fixture in &fixtures {
            assert!(tracker.update(&fixture.create_notification("new")));
            assert!(tracker.update(&fixture.create_notification("finish")));
        }

        // Should have 3 jobs total (limit), with oldest finished job removed
        assert_eq!(tracker.len(), 3);
        assert_eq!(tracker.active().count(), 0);
        assert_eq!(tracker.finished().count(), 3);

        // The first job should be removed, check that the remaining jobs are the last 3
        let finished_jobs: Vec<_> = tracker.finished().collect();
        assert_eq!(finished_jobs.len(), 3);

        // Verify the first job (fixtures[0]) is not in the finished list
        let first_job_id = fixtures[0].job_id;
        assert!(!finished_jobs.iter().any(|job| job.id == first_job_id));
    }

    #[test]
    fn test_trim_respects_active_jobs() {
        let mut tracker = JobInfoTracker::new(2);
        let fixtures = vec![
            Fixture::new(),
            Fixture {
                job_id: JobId::new(2),
                ..Fixture::new()
            },
            Fixture {
                job_id: JobId::new(3),
                ..Fixture::new()
            },
        ];

        // Add 3 jobs, finish 2, keep 1 active
        assert!(tracker.update(&fixtures[0].create_notification("new")));
        assert!(tracker.update(&fixtures[0].create_notification("finish")));

        assert!(tracker.update(&fixtures[1].create_notification("new")));
        assert!(tracker.update(&fixtures[1].create_notification("finish")));

        assert!(tracker.update(&fixtures[2].create_notification("new")));
        // Don't finish the third job

        // After the second job finishes, we have 2 finished jobs
        // When the third job is added, we have 1 active + 1 finished = 2 total (at limit)
        // No trimming occurs because we're at the limit, not above it
        assert_eq!(tracker.len(), 2);
        assert_eq!(tracker.active().count(), 1);
        assert_eq!(tracker.finished().count(), 1);
    }

    #[test]
    fn test_iter_returns_all_jobs() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add job
        assert!(tracker.update(&fixture.create_notification("new")));

        // Finish job
        assert!(tracker.update(&fixture.create_notification("finish")));

        // Add another active job
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
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        assert!(tracker.update(&fixture.create_notification("new")));
        assert!(tracker.update(&fixture.create_notification("finish")));

        let jobs: Vec<_> = tracker.into_iter().collect();
        assert_eq!(jobs.len(), 1);
    }

    #[test]
    fn test_update_nonexistent_job_does_nothing() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Try to update a job that doesn't exist
        assert!(!tracker.update(&fixture.create_notification("start")));

        assert!(tracker.is_empty());
        assert_eq!(tracker.len(), 0);
    }

    #[test]
    fn test_different_arenas_dont_conflict() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture1 = Fixture::new();
        let fixture2 = Fixture {
            arena: Arena::from("different-arena"),
            ..fixture1.clone()
        };

        // Add jobs with different arenas but same job_id
        assert!(tracker.update(&fixture1.create_notification("new")));
        assert!(tracker.update(&fixture2.create_notification("new")));

        assert_eq!(tracker.len(), 2);
        assert_eq!(tracker.active().count(), 2);
    }

    #[test]
    fn test_job_progress_transitions() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Start with new job
        assert!(tracker.update(&fixture.create_notification("new")));
        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.progress, JobProgress::Pending);

        // Update to running
        assert!(tracker.update(&fixture.create_notification("start")));
        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.progress, JobProgress::Running);

        // Finish the job
        assert!(tracker.update(&fixture.create_notification("finish")));
        let finished_job = tracker.finished().next().unwrap();
        assert_eq!(finished_job.progress, JobProgress::Done);
    }

    #[test]
    fn test_job_action_updates() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add job and start it
        assert!(tracker.update(&fixture.create_notification("new")));
        assert!(tracker.update(&fixture.create_notification("start")));

        // Update action multiple times
        assert!(tracker.update(&fixture.create_notification("update_action(2)")));
        let job_info = tracker.active().next().unwrap().clone();
        assert_eq!(job_info.action, Some(JobAction::Download));

        // Update with different action
        assert!(tracker.update(&fixture.create_notification("update_action(3)")));
        let job_info = tracker.active().next().unwrap().clone();
        assert_eq!(job_info.action, Some(JobAction::Verify));
    }

    #[test]
    fn test_byte_progress_updates() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add job and start it
        assert!(tracker.update(&fixture.create_notification("new")));
        assert!(tracker.update(&fixture.create_notification("start")));

        // Update byte progress
        assert!(tracker.update(&fixture.create_notification("update_byte_count(2)")));
        let job_info = tracker.active().next().unwrap().clone();
        assert_eq!(job_info.byte_progress, Some((100, 1000)));

        // Update byte progress again (would need a different notification)
        // For now, just verify the byte progress is set correctly
        assert!(tracker.update(&fixture.create_notification("update_byte_count(3)")));
        let job_info = tracker.active().next().unwrap().clone();
        assert_eq!(job_info.byte_progress, Some((200, 1000)));
    }

    #[test]
    fn test_get_method() {
        let mut tracker = JobInfoTracker::new(10);
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
        let mut tracker = JobInfoTracker::new(10);
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
        let mut tracker = JobInfoTracker::new(10);
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
    fn test_finish_notification_out_of_order_rejected() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Try to finish a job that doesn't exist
        assert!(!tracker.update(&fixture.create_notification("finish")));
        assert_eq!(tracker.len(), 0);

        // Add job but don't start it
        assert!(tracker.update(&fixture.create_notification("new")));

        // Try to finish a pending job - should work
        assert!(tracker.update(&fixture.create_notification("finish")));

        // Job should now be in Done state
        let job_info = tracker.get(&fixture.global_job_id()).unwrap();
        assert_eq!(job_info.progress, JobProgress::Done);

        // Try to finish again - should be rejected
        assert!(!tracker.update(&fixture.create_notification("finish")));
    }

    #[test]
    fn test_update_action_out_of_order_rejected() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Try to update action on non-existent job
        assert!(!tracker.update(&fixture.create_notification("update_action(1)")));
        assert_eq!(tracker.len(), 0);

        // Add job but don't start it
        tracker.update(&fixture.create_notification("new"));

        // Try to update action on pending job - should be rejected
        assert!(!tracker.update(&fixture.create_notification("update_action(1)")));

        // Start the job
        tracker.update(&fixture.create_notification("start"));

        // Now update action should work
        assert!(tracker.update(&fixture.create_notification("update_action(1)")));

        // Try to update with same or lower index - should be rejected
        assert!(!tracker.update(&fixture.create_notification("update_action(1)")));
        assert!(!tracker.update(&fixture.create_notification("update_action(0)")));

        // Update with higher index should work
        assert!(tracker.update(&fixture.create_notification("update_action(2)")));
    }

    #[test]
    fn test_update_byte_count_out_of_order_rejected() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Try to update byte count on non-existent job
        assert!(!tracker.update(&fixture.create_notification("update_byte_count(1)")));
        assert_eq!(tracker.len(), 0);

        // Add job but don't start it
        tracker.update(&fixture.create_notification("new"));

        // Try to update byte count on pending job - should be rejected
        assert!(!tracker.update(&fixture.create_notification("update_byte_count(1)")));

        // Start the job
        tracker.update(&fixture.create_notification("start"));

        // Now update byte count should work
        assert!(tracker.update(&fixture.create_notification("update_byte_count(1)")));

        // Try to update with same or lower index - should be rejected
        assert!(!tracker.update(&fixture.create_notification("update_byte_count(1)")));
        assert!(!tracker.update(&fixture.create_notification("update_byte_count(0)")));

        // Update with higher index should work
        assert!(tracker.update(&fixture.create_notification("update_byte_count(2)")));
    }

    #[test]
    fn test_notification_index_tracking() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add and start job
        tracker.update(&fixture.create_notification("new"));
        tracker.update(&fixture.create_notification("start"));

        // Update action with index 1
        assert!(tracker.update(&fixture.create_notification("update_action(1)")));
        let job_info = tracker.get(&fixture.global_job_id()).unwrap();
        assert_eq!(job_info.notification_index, 1);
        assert_eq!(job_info.action, Some(JobAction::Download));

        // Update byte count with index 2
        assert!(tracker.update(&fixture.create_notification("update_byte_count(2)")));
        let job_info = tracker.get(&fixture.global_job_id()).unwrap();
        assert_eq!(job_info.notification_index, 2);
        assert_eq!(job_info.byte_progress, Some((100, 1000)));

        // Update action with index 3
        assert!(tracker.update(&fixture.create_notification("update_action(3)")));
        let job_info = tracker.get(&fixture.global_job_id()).unwrap();
        assert_eq!(job_info.notification_index, 3);
        assert_eq!(job_info.action, Some(JobAction::Verify));
        assert_eq!(job_info.byte_progress, None); // Should be reset
    }

    #[test]
    fn test_finish_resets_notification_state() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add, start, and update job
        tracker.update(&fixture.create_notification("new"));
        tracker.update(&fixture.create_notification("start"));
        tracker.update(&fixture.create_notification("update_action(1)"));

        // Finish the job
        assert!(tracker.update(&fixture.create_notification("finish")));

        let job_info = tracker.get(&fixture.global_job_id()).unwrap();
        assert_eq!(job_info.progress, JobProgress::Done);
        assert_eq!(job_info.action, None); // Should be reset
        assert_eq!(job_info.byte_progress, None); // Should be None
    }

    #[test]
    fn test_update_action_resets_byte_progress() {
        let mut tracker = JobInfoTracker::new(10);
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

    #[test]
    fn test_multiple_jobs_independent_notification_indices() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture1 = Fixture::new();
        let fixture2 = Fixture {
            job_id: JobId::new(2),
            ..fixture1.clone()
        };

        // Add and start both jobs
        tracker.update(&fixture1.create_notification("new"));
        tracker.update(&fixture1.create_notification("start"));
        tracker.update(&fixture2.create_notification("new"));
        tracker.update(&fixture2.create_notification("start"));

        // Update job1 with index 1
        assert!(tracker.update(&fixture1.create_notification("update_action(1)")));

        // Update job2 with index 1 (should work independently)
        assert!(tracker.update(&fixture2.create_notification("update_action(1)")));

        // Update job1 with index 2
        assert!(tracker.update(&fixture1.create_notification("update_action(2)")));

        // Both jobs should have their own notification indices
        let job1_info = tracker.get(&fixture1.global_job_id()).unwrap();
        let job2_info = tracker.get(&fixture2.global_job_id()).unwrap();
        assert_eq!(job1_info.notification_index, 2);
        assert_eq!(job2_info.notification_index, 1);
    }
}
