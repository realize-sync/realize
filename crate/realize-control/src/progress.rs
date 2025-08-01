use realize_core::consensus::tracker::{JobInfo, JobInfoTracker};
use realize_core::consensus::types::{ChurtenNotification, JobProgress};
use realize_core::rpc::control::client::ChurtenUpdates;
use realize_storage::Job;

use crate::OutputMode;

pub(crate) async fn create(output_mode: OutputMode) -> anyhow::Result<impl Progress> {
    match output_mode {
        OutputMode::Quiet | OutputMode::Log => Ok(LogProgress::new()),
        _ => Err(anyhow::anyhow!("Unsupported --output={output_mode:?}")),
    }
}

pub(crate) trait Progress {
    fn update(&mut self, updates: ChurtenUpdates) -> impl Future<Output = ()> + Send;
    fn finished(&mut self) -> impl Future<Output = ()> + Send;
}

struct LogProgress {
    tracker: JobInfoTracker,
}

impl LogProgress {
    fn new() -> LogProgress {
        Self {
            tracker: JobInfoTracker::new(32),
        }
    }
}
impl Progress for LogProgress {
    async fn update(&mut self, updates: ChurtenUpdates) {
        match updates {
            ChurtenUpdates::Reset(jobs) => {
                let total = jobs.len();
                log::info!("{total} jobs{}", if total > 0 { ": " } else { "" });
                for (i, job) in jobs.iter().enumerate() {
                    log::info!(
                        "  [{i}/{total}] {:?}{} {}",
                        job.progress,
                        if let Some(a) = job.action {
                            format!("/{:?}", a)
                        } else {
                            "".to_string()
                        },
                        format_log_string(job)
                    );
                }
                self.tracker.init(jobs);
            }
            ChurtenUpdates::Notify(n) => {
                if !self.tracker.update(&n) {
                    return;
                }

                if let Some(job) = self.tracker.get(n.arena(), n.job_id()) {
                    match n {
                        ChurtenNotification::New { .. } => {}
                        ChurtenNotification::Start { .. } | ChurtenNotification::Finish { .. } => {
                            let progress = &job.progress;
                            match progress {
                                JobProgress::Pending => {}
                                JobProgress::Running => {
                                    log::info!("START: {}", format_log_string(job));
                                }
                                JobProgress::Done => {
                                    log::info!("DONE: {}", format_log_string(job));
                                }
                                JobProgress::Failed(msg) => {
                                    log::warn!("FAIL: {msg}: {}", format_log_string(job));
                                }
                                _ => {
                                    log::warn!("{progress:?}: {}", format_log_string(job));
                                }
                            };
                        }
                        ChurtenNotification::UpdateAction { action, .. } => {
                            log::info!("{action:?} {}", format_log_string(job));
                        }
                        ChurtenNotification::UpdateByteCount { .. } => {}
                    }
                }
            }
        }
    }

    async fn finished(&mut self) {}
}

fn format_log_string(job: &JobInfo) -> String {
    format!(
        "[{}]/{} {} {}",
        job.arena,
        job.job.path(),
        match *job.job {
            Job::Download(_, _) => "Download",
            Job::Realize(_, _, _) => "Realize",
            Job::Unrealize(_, _) => "Unrealize",
        },
        job.job.hash(),
    )
}
