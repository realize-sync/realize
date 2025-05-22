use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use realize_lib::algo::{FileProgress as AlgoFileProgress, MoveFileError, Progress};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) struct CliProgress {
    multi: MultiProgress,
    total_files: usize,
    total_bytes: u64,
    overall_pb: ProgressBar,
    next_file_index: Arc<AtomicUsize>,
    quiet: bool,
    path_prefix: String,
}

impl CliProgress {
    pub(crate) fn new(quiet: bool) -> Self {
        let multi = MultiProgress::with_draw_target(if quiet {
            ProgressDrawTarget::hidden()
        } else {
            ProgressDrawTarget::stdout()
        });
        let overall_pb = multi.add(ProgressBar::no_length());
        overall_pb.set_style(
            ProgressStyle::with_template(
                "{prefix:<9.cyan.bold} [{wide_bar:.cyan/blue}] {bytes_per_sec} ({bytes}/{total_bytes}) {percent}%",
            )
            .unwrap()
            .progress_chars("=> "),
        );
        overall_pb.set_prefix("Listing");
        Self {
            multi,
            total_files: 0,
            total_bytes: 0,
            overall_pb,
            next_file_index: Arc::new(AtomicUsize::new(1)),
            quiet,
            path_prefix: "".to_string(),
        }
    }

    pub(crate) fn set_path_prefix(&mut self, prefix: String) {
        self.path_prefix = prefix;
    }

    pub(crate) fn finish_and_clear(&self) {
        self.overall_pb.finish_and_clear();
        let _ = self.multi.clear();
    }
}

impl Progress for CliProgress {
    fn set_length(&mut self, total_files: usize, total_bytes: u64) {
        self.total_files += total_files;
        self.total_bytes += total_bytes;
        self.overall_pb.set_length(self.total_bytes);
        self.overall_pb.set_position(0);
        self.overall_pb.set_prefix("Moving");
    }

    fn for_file(&self, path: &std::path::Path, bytes: u64) -> Box<dyn AlgoFileProgress> {
        Box::new(CliFileProgress {
            bar: None,
            next_file_index: self.next_file_index.clone(),
            total_files: self.total_files,
            bytes,
            path: format!("{}{}", self.path_prefix, path.display()),
            multi: self.multi.clone(),
            overall_pb: self.overall_pb.clone(),
            quiet: self.quiet,
        })
    }
}

struct CliFileProgress {
    bar: Option<(usize, ProgressBar)>,
    next_file_index: Arc<AtomicUsize>,
    total_files: usize,
    bytes: u64,
    path: String,
    multi: MultiProgress,
    overall_pb: ProgressBar,
    quiet: bool,
}

impl CliFileProgress {
    fn get_or_create_bar(&mut self) -> &mut ProgressBar {
        let (_, pb) = self.bar.get_or_insert_with(|| {
            let file_index = self.next_file_index.fetch_add(1, Ordering::Relaxed);
            let pb = self.multi.insert_from_back(1, ProgressBar::new(self.bytes));
            let tag = format!("[{}/{}]", file_index, self.total_files);
            pb.set_message(self.path.clone());
            pb.set_style(
                ProgressStyle::with_template(
                    "{prefix:<9.cyan.bold} {tag} {wide_msg} ({bytes}/{total_bytes}) {percent}%",
                )
                .unwrap()
                .progress_chars("=> ")
                .with_key(
                    "tag",
                    move |_state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
                        let _ = w.write_str(&tag);
                    },
                ),
            );
            pb.set_prefix("Checking");

            (file_index, pb)
        });

        pb
    }
}

impl AlgoFileProgress for CliFileProgress {
    fn verifying(&mut self) {
        let pb = self.get_or_create_bar();
        pb.set_prefix("Verifying");
    }
    fn rsyncing(&mut self) {
        let pb = self.get_or_create_bar();
        pb.set_prefix("Rsyncing");
    }
    fn copying(&mut self) {
        let pb = self.get_or_create_bar();
        pb.set_prefix("Copying");
    }
    fn inc(&mut self, bytecount: u64) {
        self.overall_pb.inc(bytecount);
        if let Some((_, pb)) = &mut self.bar {
            pb.inc(bytecount);
        }
    }
    fn success(&mut self) {
        if let Some((index, pb)) = self.bar.take() {
            pb.finish_and_clear();
            if !self.quiet {
                self.multi.suspend(|| {
                    println!(
                        "{:<9} [{}/{}] {}",
                        style("Moved").for_stdout().green().bold(),
                        index,
                        self.total_files,
                        self.path
                    );
                });
            }
        }
    }

    fn error(&mut self, err: &MoveFileError) {
        // Write report even if no bar was ever created.
        let tag = if let Some((index, pb)) = self.bar.take() {
            pb.finish_and_clear();

            format!("[{}/{}] ", index, self.total_files)
        } else {
            "".to_string()
        };
        self.multi.suspend(|| {
            eprintln!(
                "{:<9} {}{}: {}",
                style("ERROR").for_stderr().red().bold(),
                tag,
                self.path,
                err,
            );
        });
    }
}
