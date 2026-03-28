use crate::output::Output;

use super::output::{self, MessageType, OutputMode};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget};
use realize_core::consensus::tracker::{JobInfo, JobInfoTracker};
use realize_core::consensus::types::{ChurtenNotification, JobAction, JobProgress};
use realize_core::rpc::control::client::ChurtenUpdates;
use realize_storage::{Job, JobId};
use realize_types::Arena;
use std::collections::HashMap;

pub(crate) struct ChurtenDisplay {
    output: Output,
    tracker: JobInfoTracker,
    multi: MultiProgress,
    overall_bar: ProgressBar,
    job_bars: HashMap<(Arena, JobId), ProgressBar>,

    /// Tracks whether "processing"/"no more jobs" was printed in
    /// `print_has_jobs` in plain output mode.
    had_jobs: bool,
}

impl ChurtenDisplay {
    pub(crate) fn default(output: Output) -> Self {
        let target = if output.mode() == OutputMode::Progress {
            ProgressDrawTarget::stdout()
        } else {
            ProgressDrawTarget::hidden()
        };

        Self::new(output, target)
    }

    pub(crate) fn new(output: Output, target: ProgressDrawTarget) -> Self {
        let tracker = JobInfoTracker::new(16);
        let multi = MultiProgress::with_draw_target(target);
        let overall_bar = multi.add(ProgressBar::no_length());
        update_overall_bar(&overall_bar, tracker.active_len());

        Self {
            output,
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
                self.init(&jobs);
                self.tracker.init(jobs);
            }
            ChurtenUpdates::Notify(n) => {
                if !self.tracker.update(&n) {
                    return;
                }

                self.log_notification(&n); // log in all modes
                match self.output.mode() {
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
                        self.output
                            .print_error(format!("{}: {}", display_path(job), msg));
                    }
                }
                _ => {
                    if let Some(job) = self.tracker.get(&n.global_job_id()) {
                        self.output
                            .print_warning(format!("{progress:?}"), display_path(job));
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
                        self.output
                            .print_success(finished_job_name(job), display_path(job));
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
                    self.output.print_progress("Processing", "...");
                }
            }
            ChurtenNotification::Finish { .. } => {
                if self.had_jobs && !self.tracker.has_active_jobs() {
                    self.had_jobs = false;
                    self.output
                        .print_progress("Waiting", "for more jobs. Press Ctrl-C to stop");
                }
            }
            _ => {}
        };
    }

    pub(crate) fn init(&mut self, jobs: &Vec<JobInfo>) {
        if self.output.mode() != OutputMode::Progress {
            return;
        }

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
                            log::info!("{}", format_log_string(job, "Start "));
                        }
                        JobProgress::Done => {
                            log::info!("{}", format_log_string(job, "Done "));
                        }
                        JobProgress::Failed(msg) => {
                            log::warn!("{}: {msg}", format_log_string(job, "Failed "));
                        }
                        _ => {
                            log::warn!("{}", format_log_string(job, &format!("{progress:?} ")));
                        }
                    };
                }
                ChurtenNotification::UpdateAction { action, .. } => {
                    log::info!("{}", format_log_string(job, &format!("{action:?} ")));
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
            format_log_string(job, "")
        );
    }
}

fn format_log_string(job: &JobInfo, prefix: &str) -> String {
    format!(
        "[{}] {prefix}{} {} {}",
        job.arena,
        in_progress_job_name(job),
        job.job.path(),
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
        None => in_progress_job_name(job),
    }
}

fn in_progress_job_name(job: &JobInfo) -> &'static str {
    match *job.job {
        Job::Download(_, _) => "Download",
    }
}

fn finished_job_name(job: &JobInfo) -> &'static str {
    match *job.job {
        Job::Download(_, _) => "Downloaded",
    }
}

fn display_path(job: &JobInfo) -> String {
    format!("[{}]/{}", job.arena, job.job.path())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::testing::{OutputFixture, forced_style};
    use console::set_colors_enabled;
    use indicatif::TermLike;
    use realize_types::{Hash, Path};

    struct Fixture {
        out: OutputFixture,
        display: ChurtenDisplay,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            set_colors_enabled(true);
            let out = OutputFixture::setup(OutputMode::Progress)?;
            let display = ChurtenDisplay::new(
                out.output.clone(),
                ProgressDrawTarget::term_like(Box::new(out.actual.clone())),
            );

            Ok(Self { out, display })
        }

        /// Return the actual terminal content.
        pub fn actual(&self) -> String {
            self.out.actual()
        }

        /// Return the expected terminal content
        pub fn expected(&self) -> String {
            self.out.expected()
        }
    }

    #[test]
    fn empty() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;

        let exp = &fixture.out.expected;
        exp.write_line(&format!(
            "{} for more jobs. Press Ctrl-C to stop",
            forced_style("     Waiting").yellow().bold(),
        ))?;

        assert_eq!(fixture.expected(), fixture.actual());

        Ok(())
    }

    #[test]
    fn one_job() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;
        fixture.display.init(&vec![JobInfo {
            arena: Arena::from("myarena"),
            id: JobId(1),
            job: Arc::new(Job::Download(Path::parse("foo/bar")?, Hash([1u8; 32]))),
            progress: JobProgress::Running,
            action: Some(JobAction::Download),
            byte_progress: None,
            notification_index: 0,
        }]);

        let exp = &fixture.out.expected;
        exp.write_line(&format!(
            "{} [myarena]/foo/bar",
            forced_style("    Download").cyan().bold(),
        ))?;
        exp.write_line(&format!(
            "{} 1 active job",
            forced_style("  Processing").cyan().bold(),
        ))?;

        assert_eq!(fixture.expected(), fixture.actual());

        Ok(())
    }
}
