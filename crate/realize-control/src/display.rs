use crate::output::Output;

use super::output::{self, MessageType, OutputMode};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressDrawTarget};
use realize_core::consensus::tracker::{JobInfo, JobInfoTracker};
use realize_core::consensus::types::{TransferNotification, JobAction, JobProgress};
use realize_core::rpc::control::client::TransferUpdates;
use realize_storage::{Job, JobId};
use realize_types::Arena;
use std::collections::HashMap;

pub(crate) struct TransferDisplay {
    output: Output,
    tracker: JobInfoTracker,
    multi: MultiProgress,
    overall_bar: ProgressBar,
    job_bars: HashMap<(Arena, JobId), ProgressBar>,

    /// Tracks whether "processing"/"no more jobs" was printed in
    /// `print_has_jobs` in plain output mode.
    had_jobs: bool,
}

impl TransferDisplay {
    pub(crate) fn default(output: Output) -> Self {
        let target = if output.mode() == OutputMode::Progress {
            ProgressDrawTarget::stdout()
        } else {
            ProgressDrawTarget::hidden()
        };

        Self::new(output, target)
    }

    pub(crate) fn new(output: Output, target: ProgressDrawTarget) -> Self {
        let tracker = JobInfoTracker::new();
        let multi = MultiProgress::with_draw_target(target);
        let overall_bar = multi.add(ProgressBar::no_length());
        update_overall_bar(&overall_bar, tracker.len());

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

    pub(crate) async fn update(&mut self, updates: TransferUpdates) {
        match updates {
            TransferUpdates::Reset(jobs) => {
                let total = jobs.len();
                log_jobs(&jobs, total);
                self.init(&jobs);
                self.tracker.init(jobs);
            }
            TransferUpdates::Notify(n) => {
                eprintln!("==== Notify {n:?}");
                if !self.tracker.update(&n) {
                    eprintln!("==== OUT OF ORDER");
                    return;
                }

                self.log_notification(&n); // log in all modes
                match self.output.mode() {
                    OutputMode::Log => {}
                    OutputMode::Progress => self.update_bar_from_notification(&n),
                    OutputMode::Plain => {
                        self.print_all_success(&n);
                        self.print_all_error(&n);
                        self.print_has_jobs(&n);
                    }
                    OutputMode::Quiet => self.print_all_error(&n),
                }
            }
        }
    }

    fn print_all_error(&mut self, n: &TransferNotification) {
        match n {
            TransferNotification::Stop { progress, .. } => match progress {
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

    fn print_all_success(&mut self, n: &TransferNotification) {
        match n {
            TransferNotification::Stop { progress, .. } => match progress {
                JobProgress::Done => {
                    if let Some(job) = self.tracker.get(&n.global_job_id()) {
                        print_job_done(&self.output, job);
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }

    fn print_has_jobs(&mut self, n: &TransferNotification) {
        match n {
            TransferNotification::New { .. } => {
                if !self.had_jobs && !self.tracker.is_empty() {
                    self.had_jobs = true;
                    self.output.print_progress("Processing", "...");
                }
            }
            TransferNotification::Stop { .. } => {
                if self.had_jobs && self.tracker.is_empty() {
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
            if job.progress.is_final() {
                if let Some(mut bar) = bar {
                    update_bar_for_job(&mut bar, job);
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
        self.tracker.remove_finished();
        existing
            .into_values()
            .for_each(|bar| bar.finish_and_clear());

        update_overall_bar(&self.overall_bar, active_count);
    }

    fn update_bar_from_notification(&mut self, n: &TransferNotification) {
        if let Some(job) = self.tracker.get(&n.global_job_id()) {
            match n {
                TransferNotification::New { .. } => {
                    let bar = self.create_bar(job);
                    self.job_bars.insert(n.global_job_id(), bar);
                    update_overall_bar(&self.overall_bar, self.tracker.len());
                }
                TransferNotification::Stop { progress, .. } => {
                    if let Some(bar) = self.job_bars.get_mut(&n.global_job_id()) {
                        update_bar_for_job(bar, job);
                    }
                    eprintln!("==== STOP {progress:?}");
                    if progress.is_final() {
                        if let Some(mut bar) = self.job_bars.remove(&n.global_job_id()) {
                            eprintln!("==== REMOVE {progress:?}");
                            update_bar_for_job(&mut bar, job);
                            self.finish_bar(bar, job);
                            self.tracker.remove_finished();
                            update_overall_bar(&self.overall_bar, self.tracker.len());
                            eprintln!("==== REMOVED {progress:?}");
                        }
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

    fn finish_bar(&self, bar: ProgressBar, job: &JobInfo) {
        bar.finish_and_clear();
        if job.progress == JobProgress::Done {
            let output = self.output.clone();
            self.multi.suspend(|| {
                print_job_done(&output, job);
            });
        }
    }

    fn log_notification(&self, n: &TransferNotification) {
        if let Some(job) = self.tracker.get(&n.global_job_id()) {
            match n {
                TransferNotification::New { .. } => {}
                TransferNotification::Start { .. } | TransferNotification::Stop { .. } => {
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
                TransferNotification::UpdateAction { action, .. } => {
                    log::info!("{}", format_log_string(job, &format!("{action:?} ")));
                }
                TransferNotification::UpdateByteCount { .. } => {}
            }
        }
    }
}

fn print_job_done(output: &Output, job: &JobInfo) {
    let mut msg = display_path(job);
    if let Some((_, total)) = &job.byte_progress {
        msg = format!("{} ({})", msg, HumanBytes(*total));
    }
    output.print_success(finished_job_name(job), msg);
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
    match &job.progress {
        JobProgress::Pending => {
            bar.set_prefix("Pending");
            set_bar_style(bar, MessageType::PROGRESS, None);
        }
        JobProgress::Running => {
            bar.set_prefix(prefix_for_job(job));
            set_bar_style(bar, MessageType::PROGRESS, job.byte_progress.as_ref());
        }
        JobProgress::Done => {
            bar.set_prefix(finished_job_name(job));
            set_bar_style(
                bar,
                MessageType::SUCCESS,
                job.byte_progress.map(|(_, total)| (total, total)).as_ref(),
            );
        }
        JobProgress::Abandoned => {
            bar.set_prefix("Abandoned");
            set_bar_style(bar, MessageType::WARNING, None);
        }
        JobProgress::Cancelled => {
            bar.set_prefix("Cancelled");
            set_bar_style(bar, MessageType::WARNING, None);
        }
        JobProgress::NoPeers => {
            bar.set_prefix("Connecting");
            set_bar_style(bar, MessageType::WARNING, job.byte_progress.as_ref());
        }
        JobProgress::Failed(_) => {
            bar.set_prefix("Retry");
            set_bar_style(bar, MessageType::ERROR, job.byte_progress.as_ref());
        }
    }
}

fn set_bar_style(bar: &mut ProgressBar, style: MessageType, byte_progress: Option<&(u64, u64)>) {
    if let Some((current, total)) = byte_progress {
        bar.set_style(output::progress_style(style, true));
        bar.set_length(*total);
        bar.set_position(*current);
    } else {
        bar.set_style(output::progress_style(style, false));
        bar.unset_length();
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
        display: TransferDisplay,
    }

    impl Fixture {
        fn setup() -> anyhow::Result<Self> {
            set_colors_enabled(true);
            let out = OutputFixture::setup(OutputMode::Progress)?;
            let display = TransferDisplay::new(
                out.output.clone(),
                ProgressDrawTarget::term_like(Box::new(out.actual.clone())),
            );

            Ok(Self { out, display })
        }

        /// Return the actual terminal content.
        pub fn actual(&self) -> String {
            self.out.actual()
        }

        /// Return the actual terminal content.
        pub fn actual_rows(&self, range: std::ops::Range<usize>) -> String {
            self.out.actual_rows(range)
        }

        /// Return the expected terminal content
        pub fn expected(&self) -> String {
            self.out.expected()
        }
    }

    fn test_job() -> JobInfo {
        JobInfo {
            arena: Arena::from("myarena"),
            id: JobId(1),
            job: Arc::new(Job::Download(
                Path::parse("foo/bar").unwrap(),
                Hash([1u8; 32]),
            )),
            progress: JobProgress::Running,
            action: Some(JobAction::Download),
            byte_progress: None,
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
        fixture.display.init(&vec![test_job()]);

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

    #[test]
    fn pending() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;
        fixture.display.init(&vec![JobInfo {
            progress: JobProgress::Pending,
            ..test_job()
        }]);

        let exp = &fixture.out.expected;
        exp.write_line(&format!(
            "{} [myarena]/foo/bar",
            forced_style("     Pending").cyan().bold(),
        ))?;

        assert_eq!(fixture.expected(), fixture.actual_rows(0..1));

        Ok(())
    }

    #[test]
    fn download() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;
        fixture.display.init(&vec![JobInfo {
            progress: JobProgress::Running,
            ..test_job()
        }]);

        let exp = &fixture.out.expected;
        exp.write_line(&format!(
            "{} [myarena]/foo/bar",
            forced_style("    Download").cyan().bold(),
        ))?;

        assert_eq!(fixture.expected(), fixture.actual_rows(0..1));

        Ok(())
    }

    #[test]
    fn download_with_byte_progress() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;
        fixture.display.init(&vec![JobInfo {
            progress: JobProgress::Running,
            byte_progress: Some((1024 * 1024, 4 * 1024 * 1024)), // 1M / 4M
            ..test_job()
        }]);

        let exp = &fixture.out.expected;
        exp.write_line(&format!(
            "{} [myarena]/foo/bar                           (1.00 MiB/4.00 MiB) 25%",
            forced_style("    Download").cyan().bold(),
        ))?;

        assert_eq!(fixture.expected(), fixture.actual_rows(0..1));

        Ok(())
    }

    #[test]
    fn no_peers() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;
        fixture.display.init(&vec![JobInfo {
            progress: JobProgress::NoPeers,
            ..test_job()
        }]);

        let exp = &fixture.out.expected;
        exp.write_line(&format!(
            "{} [myarena]/foo/bar",
            forced_style("  Connecting").yellow().bold(),
        ))?;

        assert_eq!(fixture.expected(), fixture.actual_rows(0..1));

        Ok(())
    }

    #[test]
    fn no_peers_with_byte_progress() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;
        fixture.display.init(&vec![JobInfo {
            progress: JobProgress::NoPeers,
            byte_progress: Some((1024 * 1024, 4 * 1024 * 1024)), // 1M / 4M
            ..test_job()
        }]);

        let exp = &fixture.out.expected;
        exp.write_line(&format!(
            "{} [myarena]/foo/bar                           (1.00 MiB/4.00 MiB) 25%",
            forced_style("  Connecting").yellow().bold(),
        ))?;

        assert_eq!(fixture.expected(), fixture.actual_rows(0..1));

        Ok(())
    }

    #[test]
    fn failed() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;
        fixture.display.init(&vec![JobInfo {
            progress: JobProgress::Failed("test".to_string()),
            ..test_job()
        }]);

        let exp = &fixture.out.expected;
        exp.write_line(&format!(
            "{} [myarena]/foo/bar",
            forced_style("       Retry").red().bold(),
        ))?;

        assert_eq!(fixture.expected(), fixture.actual_rows(0..1));

        Ok(())
    }

    #[test]
    fn failed_byte_progress() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;
        fixture.display.init(&vec![JobInfo {
            progress: JobProgress::Failed("test".to_string()),
            byte_progress: Some((1024 * 1024, 4 * 1024 * 1024)), // 1M / 4M
            ..test_job()
        }]);

        let exp = &fixture.out.expected;
        exp.write_line(&format!(
            "{} [myarena]/foo/bar                           (1.00 MiB/4.00 MiB) 25%",
            forced_style("       Retry").red().bold(),
        ))?;

        assert_eq!(fixture.expected(), fixture.actual_rows(0..1));

        Ok(())
    }

    #[tokio::test]
    async fn multiple_jobs_from_notifications() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup()?;
        let arena = Arena::from("myarena");
        let job1 = JobId(1);
        fixture
            .display
            .update(TransferUpdates::Notify(TransferNotification::New {
                arena,
                job_id: job1,
                job: Arc::new(Job::Download(Path::parse("foo").unwrap(), Hash([1u8; 32]))),
            }))
            .await;

        let exp = &fixture.out.expected;
        exp.write_line(&format!(
            "{} [myarena]/foo",
            forced_style("     Pending").cyan().bold(),
        ))?;
        exp.write_line(&format!(
            "{} 1 active job",
            forced_style("  Processing").cyan().bold(),
        ))?;
        assert_eq!(fixture.expected(), fixture.actual());

        let job2 = JobId(2);
        fixture
            .display
            .update(TransferUpdates::Notify(TransferNotification::New {
                arena,
                job_id: job2,
                job: Arc::new(Job::Download(Path::parse("bar").unwrap(), Hash([1u8; 32]))),
            }))
            .await;

        exp.reset();
        exp.write_line(&format!(
            "{} [myarena]/foo",
            forced_style("     Pending").cyan().bold(),
        ))?;
        exp.write_line(&format!(
            "{} [myarena]/bar",
            forced_style("     Pending").cyan().bold(),
        ))?;
        exp.write_line(&format!(
            "{} 2 active jobs",
            forced_style("  Processing").cyan().bold(),
        ))?;
        assert_eq!(fixture.expected(), fixture.actual());

        fixture
            .display
            .update(TransferUpdates::Notify(TransferNotification::Start {
                arena,
                job_id: job2,
            }))
            .await;

        fixture
            .display
            .update(TransferUpdates::Notify(TransferNotification::UpdateAction {
                arena,
                job_id: job2,
                action: JobAction::Download,
            }))
            .await;
        fixture
            .display
            .update(TransferUpdates::Notify(
                TransferNotification::UpdateByteCount {
                    arena,
                    job_id: job2,
                    current_bytes: 1024 * 1024,
                    total_bytes: 4 * 1024 * 1024,
                },
            ))
            .await;

        exp.reset();
        exp.write_line(&format!(
            "{} [myarena]/foo",
            forced_style("     Pending").cyan().bold(),
        ))?;
        exp.write_line(&format!(
            "{} [myarena]/bar                               (1.00 MiB/4.00 MiB) 25%",
            forced_style("    Download").cyan().bold(),
        ))?;
        exp.write_line(&format!(
            "{} 2 active jobs",
            forced_style("  Processing").cyan().bold(),
        ))?;
        assert_eq!(fixture.expected(), fixture.actual());

        fixture
            .display
            .update(TransferUpdates::Notify(TransferNotification::Start {
                arena,
                job_id: job1,
            }))
            .await;

        fixture
            .display
            .update(TransferUpdates::Notify(TransferNotification::Stop {
                arena,
                job_id: job1,
                progress: JobProgress::NoPeers,
            }))
            .await;

        exp.reset();
        exp.write_line(&format!(
            "{} [myarena]/foo",
            forced_style("  Connecting").yellow().bold(),
        ))?;
        exp.write_line(&format!(
            "{} [myarena]/bar                               (1.00 MiB/4.00 MiB) 25%",
            forced_style("    Download").cyan().bold(),
        ))?;
        exp.write_line(&format!(
            "{} 2 active jobs",
            forced_style("  Processing").cyan().bold(),
        ))?;
        assert_eq!(fixture.expected(), fixture.actual());

        fixture
            .display
            .update(TransferUpdates::Notify(TransferNotification::Start {
                arena,
                job_id: job1,
            }))
            .await;
        fixture
            .display
            .update(TransferUpdates::Notify(
                TransferNotification::UpdateByteCount {
                    arena,
                    job_id: job1,
                    current_bytes: 1024 * 1024,
                    total_bytes: 4 * 1024 * 1024,
                },
            ))
            .await;

        exp.reset();
        exp.write_line(&format!(
            "{} [myarena]/foo                               (1.00 MiB/4.00 MiB) 25%",
            forced_style("    Download").cyan().bold(),
        ))?;
        exp.write_line(&format!(
            "{} [myarena]/bar                               (1.00 MiB/4.00 MiB) 25%",
            forced_style("    Download").cyan().bold(),
        ))?;
        exp.write_line(&format!(
            "{} 2 active jobs",
            forced_style("  Processing").cyan().bold(),
        ))?;
        assert_eq!(fixture.expected(), fixture.actual());

        fixture
            .display
            .update(TransferUpdates::Notify(TransferNotification::Stop {
                arena,
                job_id: job2,
                progress: JobProgress::Done,
            }))
            .await;

        exp.reset();
        exp.write_line(&format!(
            "{} [myarena]/bar (4.00 MiB)",
            forced_style("Downloaded").green().bold(),
        ))?;
        exp.write_line(&format!(
            "{} [myarena]/foo                               (1.00 MiB/4.00 MiB) 25%",
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
