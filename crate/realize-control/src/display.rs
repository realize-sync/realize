use super::output::{self, MessageType, OutputMode};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget};
use realize_core::consensus::tracker::{JobInfo, JobInfoTracker};
use realize_core::consensus::types::{ChurtenNotification, JobAction, JobProgress};
use realize_core::rpc::control::client::ChurtenUpdates;
use realize_storage::{Job, JobId};
use realize_types::Arena;
use std::collections::HashMap;

pub(crate) struct ChurtenDisplay {
    output_mode: OutputMode,
    tracker: JobInfoTracker,
    multi: MultiProgress,
    overall_bar: ProgressBar,
    job_bars: HashMap<(Arena, JobId), ProgressBar>,

    /// Tracks whether "processing"/"no more jobs" was printed in
    /// `print_has_jobs` in plain output mode.
    had_jobs: bool,
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
        update_overall_bar(&overall_bar, tracker.active_len());

        Self {
            output_mode,
            tracker,
            multi,
            overall_bar,
            job_bars: HashMap::new(),
            had_jobs: false,
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
                if self.output_mode == OutputMode::Progress {
                    self.init_bars(&jobs);
                }
                self.tracker.init(jobs);
            }
            ChurtenUpdates::Notify(n) => {
                if !self.tracker.update(&n) {
                    return;
                }

                self.log_notification(&n); // log in all modes
                match self.output_mode {
                    OutputMode::Log => {}
                    OutputMode::Progress => self.update_bar_from_notification(&n),
                    OutputMode::Plain => {
                        self.print_success(&n);
                        self.print_errors(&n);
                        self.print_has_jobs(&n);
                    }
                    OutputMode::Quiet => self.print_errors(&n),
                }
            }
        }
    }

    /// Print failed jobs, for non-terminal and quiet mode.
    fn print_errors(&mut self, n: &ChurtenNotification) {
        match n {
            ChurtenNotification::Finish { progress, .. } => match progress {
                JobProgress::Pending | JobProgress::Done => {}
                JobProgress::Failed(msg) => {
                    if let Some(job) = self.tracker.get(&n.global_job_id()) {
                        output::print_error(
                            self.output_mode,
                            format!("{}: {}", display_path(job), msg),
                        );
                    }
                }
                _ => {
                    if let Some(job) = self.tracker.get(&n.global_job_id()) {
                        output::print_warning(
                            self.output_mode,
                            format!("{progress:?}"),
                            display_path(job),
                        );
                    }
                }
            },
            _ => {}
        }
    }

    /// Print successful job, for non-terminal mode.
    fn print_success(&mut self, n: &ChurtenNotification) {
        match n {
            ChurtenNotification::Finish { progress, .. } => match progress {
                JobProgress::Done => {
                    if let Some(job) = self.tracker.get(&n.global_job_id()) {
                        output::print_success(
                            self.output_mode,
                            finished_job_name(job),
                            display_path(job),
                        );
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }

    fn print_has_jobs(&mut self, n: &ChurtenNotification) {
        match n {
            ChurtenNotification::New { .. } => {
                if !self.had_jobs && self.tracker.has_active_jobs() {
                    self.had_jobs = true;
                    output::print_progress(self.output_mode, "Processing", "...");
                }
            }
            ChurtenNotification::Finish { .. } => {
                if self.had_jobs && !self.tracker.has_active_jobs() {
                    self.had_jobs = false;
                    output::print_progress(
                        self.output_mode,
                        "Waiting",
                        "for more jobs. Press Ctrl-C to stop",
                    );
                }
            }
            _ => {}
        };
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

        bar.set_style(output::progress_style(MessageType::PROGRESS, false));
        bar.set_message(display_path(job));
        update_bar_for_job(&mut bar, job);

        bar
    }

    fn finish_bar(&self, mut bar: ProgressBar, job: &JobInfo) {
        update_bar_for_job(&mut bar, job);
        match &job.progress {
            JobProgress::Done => {
                // If notifications were lost, progress might not have
                // reached 100% even though it's finished.
                if let Some(length) = bar.length() {
                    bar.set_position(length);
                }
                bar.set_prefix(finished_job_name(job));
                bar.set_style(output::progress_style(
                    MessageType::SUCCESS,
                    bar.length().is_some(),
                ));
            }
            JobProgress::Failed(err) => {
                bar.set_message(format!("{}: {err}", display_path(job)));
                bar.set_style(output::progress_style(MessageType::ERROR, false));
            }
            _ => {
                bar.set_style(output::progress_style(
                    MessageType::WARNING,
                    bar.length().is_some(),
                ));
            }
        }

        bar.finish();
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
        in_progress_job_name(job),
        job.job.hash(),
    )
}

fn update_bar_for_job(bar: &mut ProgressBar, job: &JobInfo) {
    bar.set_prefix(prefix_for_job(job));
    if let Some((current, total)) = &job.byte_progress {
        if bar.length().is_none() {
            bar.set_style(output::progress_style(MessageType::PROGRESS, true));
        }
        bar.set_length(*total);
        bar.set_position(*current)
    }
}

fn update_overall_bar(overall_bar: &ProgressBar, job_count: usize) {
    if job_count == 0 {
        overall_bar.set_style(output::progress_style(MessageType::WARNING, false));
        overall_bar.set_prefix("Waiting");
        overall_bar.set_message("for more jobs. Press Ctrl-C to stop");
    } else if job_count == 1 {
        overall_bar.set_prefix("Processing");
        overall_bar.set_style(output::progress_style(MessageType::PROGRESS, false));
        overall_bar.set_message("1 active job");
    } else {
        overall_bar.set_prefix("Processing");
        overall_bar.set_style(output::progress_style(MessageType::PROGRESS, false));
        overall_bar.set_message(format!("{job_count} active jobs"));
    }
}

fn prefix_for_job(job: &JobInfo) -> &'static str {
    match job.action {
        Some(JobAction::Download) => "Download",
        Some(JobAction::Verify) => "Verify",
        Some(JobAction::Repair) => "Repair",
        Some(JobAction::Move) => "Move",
        None => in_progress_job_name(job),
    }
}

fn in_progress_job_name(job: &JobInfo) -> &'static str {
    match *job.job {
        Job::Download(_, _) => "Download",
        Job::Realize(_, _, _) => "Realize",
        Job::Unrealize(_, _) => "Unrealize",
    }
}

fn finished_job_name(job: &JobInfo) -> &'static str {
    match *job.job {
        Job::Download(_, _) => "Downloaded",
        Job::Realize(_, _, _) => "Realized",
        Job::Unrealize(_, _) => "Unrealized",
    }
}

fn display_path(job: &JobInfo) -> String {
    format!("[{}]/{}", job.arena, job.job.path())
}
