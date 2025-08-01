use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget};
use realize_core::consensus::tracker::{JobInfo, JobInfoTracker};
use realize_core::consensus::types::{ChurtenNotification, JobAction, JobProgress};
use realize_core::rpc::control::client::ChurtenUpdates;
use realize_storage::{Job, JobId};
use realize_types::Arena;
use std::collections::HashMap;

use crate::output::wide_message_with_byte_progress;
use crate::{OutputMode, output};

pub(crate) struct ChurtenDisplay {
    output_mode: OutputMode,
    tracker: JobInfoTracker,
    multi: MultiProgress,
    overall_bar: ProgressBar,
    job_bars: HashMap<(Arena, JobId), ProgressBar>,
}

impl ChurtenDisplay {
    pub(crate) fn new(output_mode: OutputMode) -> Self {
        let tracker = JobInfoTracker::new(16);
        let multi = MultiProgress::with_draw_target(if output_mode == OutputMode::Progress {
            ProgressDrawTarget::stdout()
        } else {
            ProgressDrawTarget::hidden()
        });
        let overall_bar = multi.add(ProgressBar::no_length());
        overall_bar.set_prefix("Running");
        update_overall_bar(&overall_bar, tracker.active_len());

        Self {
            output_mode,
            tracker,
            multi,
            overall_bar,
            job_bars: HashMap::new(),
        }
    }

    pub(crate) async fn finished(&mut self) {
        self.overall_bar.finish_and_clear();
    }

    pub(crate) async fn update(&mut self, updates: ChurtenUpdates) {
        match updates {
            ChurtenUpdates::Reset(jobs) => {
                let total = jobs.len();
                log_jobs(&jobs, total);
                self.init_bars(&jobs);
                self.tracker.init(jobs);
            }
            ChurtenUpdates::Notify(n) => {
                if !self.tracker.update(&n) {
                    return;
                }

                self.log_notification(&n);
                self.update_bar_from_notification(&n);
            }
        }
    }

    fn init_bars(&mut self, jobs: &Vec<JobInfo>) {
        let mut existing = std::mem::take(&mut self.job_bars);
        let mut active_count = 0;
        for job in jobs {
            let id = job.global_job_id();
            let bar = existing.remove(&id);
            if job.progress.is_finished() {
                if let Some(bar) = bar {
                    self.finish_bar(bar, &job);
                }
            } else {
                active_count += 1;
                if let Some(mut bar) = bar {
                    update_bar_for_job(&mut bar, job);
                    self.job_bars.insert(id, bar);
                } else {
                    let mut bar = self.create_bar(&job);
                    update_bar_for_job(&mut bar, job);
                    self.job_bars.insert(id, bar);
                }
            }
        }
        existing
            .into_values()
            .for_each(|bar| bar.finish_and_clear());

        update_overall_bar(&self.overall_bar, active_count);
    }

    fn update_bar_from_notification(&mut self, n: &ChurtenNotification) {
        if let Some(job) = self.tracker.get(&n.global_job_id()) {
            match n {
                ChurtenNotification::New { .. } => {
                    let bar = self.create_bar(job);
                    self.job_bars.insert(n.global_job_id(), bar);
                    update_overall_bar(&self.overall_bar, self.tracker.active_len());
                }
                ChurtenNotification::Finish { .. } => {
                    if let Some(bar) = self.job_bars.remove(&n.global_job_id()) {
                        self.finish_bar(bar, &job);
                        update_overall_bar(&self.overall_bar, self.tracker.active_len());
                    }
                }
                _ => {
                    if let Some(bar) = self.job_bars.get_mut(&n.global_job_id()) {
                        update_bar_for_job(bar, job);
                    }
                }
            }
        }
    }

    fn create_bar(&self, job: &JobInfo) -> ProgressBar {
        let mut bar = self.multi.insert_from_back(1, ProgressBar::no_length());

        bar.set_style(output::wide_message(false));
        bar.set_message(display_path(job));
        update_bar_for_job(&mut bar, job);

        bar
    }

    fn finish_bar(&self, bar: ProgressBar, job: &JobInfo) {
        bar.finish_and_clear();
        self.multi.suspend(|| match &job.progress {
            JobProgress::Done => {
                let prefix = match *job.job {
                    Job::Download(_, _) => "Downloaded",
                    Job::Realize(_, _, _) | Job::Unrealize(_, _) => "Moved",
                };
                output::print_success_aligned(self.output_mode, prefix, display_path(job));
            }
            JobProgress::Failed(msg) => {
                output::print_error_aligned(
                    self.output_mode,
                    format!("{} {}: {msg}", job_name(job), display_path(job)),
                );
            }
            _ => {
                output::print_warning_aligned(
                    self.output_mode,
                    format!("{:?}", job.progress),
                    format!("{} {}", job_name(job), display_path(job)),
                );
            }
        });
    }

    fn log_notification(&self, n: &ChurtenNotification) {
        if let Some(job) = self.tracker.get(&n.global_job_id()) {
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

fn log_jobs(jobs: &Vec<JobInfo>, total: usize) {
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
}

fn format_log_string(job: &JobInfo) -> String {
    format!(
        "[{}]/{} {} {}",
        job.arena,
        job.job.path(),
        job_name(job),
        job.job.hash(),
    )
}

fn update_bar_for_job(bar: &mut ProgressBar, job: &JobInfo) {
    bar.set_prefix(prefix_for_job(job));
    if bar.length().is_none() && job.byte_progress.is_some() {
        bar.set_style(wide_message_with_byte_progress(false))
    }
    if bar.length().is_some() && job.byte_progress.is_none() {
        bar.set_style(output::wide_message(false))
    }
    if let Some((current, total)) = &job.byte_progress {
        bar.set_length(*total);
        bar.set_position(*current)
    }
}

fn update_overall_bar(overall_bar: &ProgressBar, job_count: usize) {
    if job_count == 0 {
        overall_bar.set_style(output::wide_message(true));
        overall_bar.set_message("no active jobs. Press Ctrl-C to stop");
    } else if job_count == 1 {
        overall_bar.set_style(output::wide_message(false));
        overall_bar.set_message("1 active job");
    } else {
        overall_bar.set_style(output::wide_message(false));
        overall_bar.set_message(format!("{job_count} active jobs"));
    }
}

fn prefix_for_job(job: &JobInfo) -> &'static str {
    match job.action {
        Some(JobAction::Download) => "Download",
        Some(JobAction::Verify) => "Verify",
        Some(JobAction::Repair) => "Repair",
        Some(JobAction::Move) => "Move",
        None => job_name(job),
    }
}

fn job_name(job: &JobInfo) -> &'static str {
    match *job.job {
        Job::Download(_, _) => "Download",
        Job::Realize(_, _, _) => "Realize",
        Job::Unrealize(_, _) => "Unrealize",
    }
}

fn display_path(job: &JobInfo) -> String {
    format!("[{}]/{}", job.arena, job.job.path())
}
