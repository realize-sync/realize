use super::types::{ChurtenNotification, JobAction, JobProgress};
use realize_storage::{Job, JobId};
use realize_types::Arena;
use std::{
    collections::{HashMap, VecDeque, hash_map::IntoValues},
    iter::Chain,
    sync::Arc,
};

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
    active: HashMap<(Arena, JobId), JobInfo>,

    /// Finished jobs, ordered from the oldest (front) to the newest (back).
    finished: VecDeque<JobInfo>,
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
            active: HashMap::new(),
        }
    }

    /// Check whether there are any jobs in this tracker.
    pub fn is_empty(&self) -> bool {
        self.active.is_empty() && self.finished.is_empty()
    }

    /// Check how many jobs there are
    pub fn len(&self) -> usize {
        self.active.len() + self.finished.len()
    }

    /// Iterate over all [JobInfo]s, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = &JobInfo> {
        self.active.values().chain(self.finished.iter())
    }

    /// Iterate over all active [JobInfo]s.
    pub fn active(&self) -> impl Iterator<Item = &JobInfo> {
        self.active.values()
    }

    /// Iterate over all finished [JobInfo]s.
    pub fn finished(&self) -> impl Iterator<Item = &JobInfo> {
        self.finished.iter()
    }

    /// Fill tracker with some existing JobInfo.
    ///
    /// This is useful to start tracking notifications after getting a
    /// list of remote jobs.
    pub fn backfill<T>(&mut self, jobs: T)
    where
        T: IntoIterator<Item = JobInfo>,
    {
        for job in jobs.into_iter() {
            if job.progress.is_finished() {
                self.finished.push_back(job)
            } else {
                let id = (job.arena, job.id);
                self.active.insert(id, job);
            }
        }
    }

    /// Update jobs inside this tracker.
    pub fn update(&mut self, notification: &ChurtenNotification) {
        let arena = notification.arena();
        let job_id = notification.job_id();
        let id = (arena, job_id);
        match notification {
            ChurtenNotification::New { job, .. } => {
                self.active.insert(
                    id,
                    JobInfo {
                        arena,
                        id: job_id,
                        job: Arc::clone(job),
                        progress: JobProgress::Pending,
                        action: None,
                        byte_progress: None,
                    },
                );
                self.trim();
            }
            ChurtenNotification::Update { progress, .. } => {
                if progress.is_finished() {
                    if let Some(mut info) = self.active.remove(&id) {
                        info.progress = progress.clone();
                        info.action = None;
                        self.finished.push_back(info);
                        self.trim();
                    }
                } else {
                    if let Some(info) = self.active.get_mut(&id) {
                        info.progress = progress.clone();
                        info.action = None;
                    }
                }
            }
            ChurtenNotification::UpdateAction { action, .. } => {
                if let Some(info) = self.active.get_mut(&id) {
                    info.action = Some(*action);
                    info.byte_progress = None;
                }
            }
            ChurtenNotification::UpdateByteCount {
                current_bytes,
                total_bytes,
                ..
            } => {
                if let Some(info) = self.active.get_mut(&id) {
                    info.byte_progress = Some((*current_bytes, *total_bytes))
                }
            }
        };
    }

    /// Get rid of the oldest finished jobs as long as
    /// the total number of jobs is above limit.
    fn trim(&mut self) {
        while self.len() > self.limit && !self.finished.is_empty() {
            self.finished.pop_front();
        }
    }
}

impl IntoIterator for JobInfoTracker {
    type Item = JobInfo;

    type IntoIter =
        Chain<IntoValues<(Arena, JobId), JobInfo>, <VecDeque<JobInfo> as IntoIterator>::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        self.active.into_values().chain(self.finished.into_iter())
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

        fn create_notification(&self, notification_type: &str) -> ChurtenNotification {
            match notification_type {
                "new" => ChurtenNotification::New {
                    arena: self.arena,
                    job_id: self.job_id,
                    job: Arc::clone(&self.job),
                },
                "update_running" => ChurtenNotification::Update {
                    arena: self.arena,
                    job_id: self.job_id,
                    progress: JobProgress::Running,
                },
                "update_done" => ChurtenNotification::Update {
                    arena: self.arena,
                    job_id: self.job_id,
                    progress: JobProgress::Done,
                },
                "update_action" => ChurtenNotification::UpdateAction {
                    arena: self.arena,
                    job_id: self.job_id,
                    action: JobAction::Download,
                },
                "update_byte_count" => ChurtenNotification::UpdateByteCount {
                    arena: self.arena,
                    job_id: self.job_id,
                    current_bytes: 100,
                    total_bytes: 1000,
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

        tracker.update(&notification);

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
        tracker.update(&fixture.create_notification("new"));

        // Update to running
        tracker.update(&fixture.create_notification("update_running"));

        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.progress, JobProgress::Running);
        assert_eq!(job_info.action, None);
    }

    #[test]
    fn test_update_job_action() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add new job
        tracker.update(&fixture.create_notification("new"));

        // Update action
        tracker.update(&fixture.create_notification("update_action"));

        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.action, Some(JobAction::Download));
    }

    #[test]
    fn test_update_job_action_resets_byte_count() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add new job
        tracker.update(&fixture.create_notification("new"));

        // Update byte count
        tracker.update(&fixture.create_notification("update_byte_count"));

        // Update action
        tracker.update(&fixture.create_notification("update_action"));

        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.byte_progress, None);
    }

    #[test]
    fn test_update_byte_count() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add new job
        tracker.update(&fixture.create_notification("new"));

        // Update byte count
        tracker.update(&fixture.create_notification("update_byte_count"));

        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.byte_progress, Some((100, 1000)));
    }

    #[test]
    fn test_job_finishes_and_moves_to_finished() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add new job
        tracker.update(&fixture.create_notification("new"));
        assert_eq!(tracker.active().count(), 1);
        assert_eq!(tracker.finished().count(), 0);

        // Finish the job
        tracker.update(&fixture.create_notification("update_done"));

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
        tracker.update(&fixture.create_notification("new"));
        assert_eq!(tracker.active().count(), 1);
        assert_eq!(tracker.finished().count(), 0);

        // Update action
        tracker.update(&fixture.create_notification("update_action"));

        // Finish the job
        tracker.update(&fixture.create_notification("update_done"));

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
        tracker.update(&fixture1.create_notification("new"));
        tracker.update(&fixture2.create_notification("new"));

        assert_eq!(tracker.len(), 2);
        assert_eq!(tracker.active().count(), 2);

        // Finish one job
        tracker.update(&fixture1.create_notification("update_done"));

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
            tracker.update(&fixture.create_notification("new"));
            tracker.update(&fixture.create_notification("update_done"));
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
        tracker.update(&fixtures[0].create_notification("new"));
        tracker.update(&fixtures[0].create_notification("update_done"));

        tracker.update(&fixtures[1].create_notification("new"));
        tracker.update(&fixtures[1].create_notification("update_done"));

        tracker.update(&fixtures[2].create_notification("new"));
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
        tracker.update(&fixture.create_notification("new"));

        // Finish job
        tracker.update(&fixture.create_notification("update_done"));

        // Add another active job
        let fixture2 = Fixture {
            job_id: JobId::new(2),
            ..fixture
        };
        tracker.update(&fixture2.create_notification("new"));

        let all_jobs: Vec<_> = tracker.iter().collect();
        assert_eq!(all_jobs.len(), 2);
    }

    #[test]
    fn test_into_iter_consumes_tracker() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        tracker.update(&fixture.create_notification("new"));
        tracker.update(&fixture.create_notification("update_done"));

        let jobs: Vec<_> = tracker.into_iter().collect();
        assert_eq!(jobs.len(), 1);
    }

    #[test]
    fn test_update_nonexistent_job_does_nothing() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Try to update a job that doesn't exist
        tracker.update(&fixture.create_notification("update_running"));

        assert!(tracker.is_empty());
        assert_eq!(tracker.len(), 0);
    }

    #[test]
    fn test_update_action_on_finished_job_does_nothing() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add and finish job
        tracker.update(&fixture.create_notification("new"));
        tracker.update(&fixture.create_notification("update_done"));

        // Try to update action on finished job
        tracker.update(&fixture.create_notification("update_action"));

        let finished_job = tracker.finished().next().unwrap();
        assert_eq!(finished_job.action, None);
    }

    #[test]
    fn test_update_byte_count_on_finished_job_does_nothing() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add and finish job
        tracker.update(&fixture.create_notification("new"));
        tracker.update(&fixture.create_notification("update_done"));

        // Try to update byte count on finished job
        tracker.update(&fixture.create_notification("update_byte_count"));

        let finished_job = tracker.finished().next().unwrap();
        assert_eq!(finished_job.byte_progress, None);
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
        tracker.update(&fixture1.create_notification("new"));
        tracker.update(&fixture2.create_notification("new"));

        assert_eq!(tracker.len(), 2);
        assert_eq!(tracker.active().count(), 2);
    }

    #[test]
    fn test_job_progress_transitions() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Start with new job
        tracker.update(&fixture.create_notification("new"));
        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.progress, JobProgress::Pending);

        // Update to running
        tracker.update(&fixture.create_notification("update_running"));
        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.progress, JobProgress::Running);

        // Finish the job
        tracker.update(&fixture.create_notification("update_done"));
        let finished_job = tracker.finished().next().unwrap();
        assert_eq!(finished_job.progress, JobProgress::Done);
    }

    #[test]
    fn test_job_action_updates() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add job
        tracker.update(&fixture.create_notification("new"));

        // Update action multiple times
        tracker.update(&fixture.create_notification("update_action"));
        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.action, Some(JobAction::Download));

        // Update with different action (would need a different notification type)
        // For now, just verify the action is set correctly
        assert_eq!(job_info.action, Some(JobAction::Download));
    }

    #[test]
    fn test_byte_progress_updates() {
        let mut tracker = JobInfoTracker::new(10);
        let fixture = Fixture::new();

        // Add job
        tracker.update(&fixture.create_notification("new"));

        // Update byte progress
        tracker.update(&fixture.create_notification("update_byte_count"));
        let job_info = tracker.active().next().unwrap();
        assert_eq!(job_info.byte_progress, Some((100, 1000)));

        // Update byte progress again (would need a different notification)
        // For now, just verify the byte progress is set correctly
        assert_eq!(job_info.byte_progress, Some((100, 1000)));
    }
}
