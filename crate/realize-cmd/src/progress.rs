use console::style;
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use realize_lib::algo::ProgressEvent;
use realize_lib::model::service::DirectoryId;
use realize_lib::transport::tcp::ClientConnectionState;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

pub(crate) struct CliProgress {
    multi: MultiProgress,
    total_files: usize,
    total_bytes: u64,
    overall_pb: ProgressBar,
    next_file_index: Arc<AtomicUsize>,
    quiet: bool,
    path_prefix: String,
    should_show_dir: bool,
    connection_state: ClientConnectionState,
}

impl CliProgress {
    pub(crate) fn new(quiet: bool, dir_count: usize) -> Self {
        let multi = MultiProgress::with_draw_target(if quiet {
            ProgressDrawTarget::hidden()
        } else {
            ProgressDrawTarget::stdout()
        });
        let overall_pb = multi.add(ProgressBar::no_length());
        overall_pb.set_style(
            ProgressStyle::with_template(
                "{prefix:<10.cyan.bold} [{wide_bar:.cyan/blue}] {bytes_per_sec} ({bytes}/{total_bytes}) {percent}%",
            )
                .unwrap()
                .progress_chars("=> "),
        );
        let res = Self {
            multi,
            total_files: 0,
            total_bytes: 0,
            overall_pb,
            next_file_index: Arc::new(AtomicUsize::new(1)),
            quiet,
            path_prefix: "".to_string(),
            should_show_dir: dir_count > 1,
            connection_state: ClientConnectionState::NotConnected,
        };
        res.update_overall_prefix();

        res
    }

    pub(crate) fn set_path_prefix(&mut self, prefix: String) {
        self.path_prefix = prefix;
    }

    pub(crate) fn finish_and_clear(&self) {
        self.overall_pb.finish_and_clear();
        let _ = self.multi.clear();
    }

    fn set_length(&mut self, dir_id: &DirectoryId, total_files: usize, total_bytes: u64) {
        log::info!(
            "{}: {} files to move ({})",
            dir_id,
            total_files,
            HumanBytes(total_bytes)
        );

        if self.should_show_dir {
            self.set_path_prefix(format!("{}/", dir_id));
        }
        self.total_files += total_files;
        self.total_bytes += total_bytes;
        self.overall_pb.set_length(self.total_bytes);
        self.overall_pb.set_position(0);
        self.update_overall_prefix();
    }

    fn for_file(&self, path: &std::path::Path, bytes: u64, available: u64) -> CliFileProgress {
        let path = format!("{}{}", self.path_prefix, path.display());
        log::info!(
            "Preparing to move {} ({}/{})",
            path,
            HumanBytes(available),
            HumanBytes(bytes)
        );

        CliFileProgress {
            bar: None,
            next_file_index: self.next_file_index.clone(),
            total_files: self.total_files,
            bytes,
            available,
            path,
            multi: self.multi.clone(),
            overall_pb: self.overall_pb.clone(),
            quiet: self.quiet,
        }
    }

    fn set_connection_state(
        &mut self,
        src_state: ClientConnectionState,
        dst_state: ClientConnectionState,
    ) {
        use realize_lib::transport::tcp::ClientConnectionState::*;

        let old_state = self.connection_state;
        self.connection_state = match (src_state, dst_state) {
            (Connected, Connected) => Connected,
            _ => Connecting,
        };
        match (old_state, self.connection_state) {
            (Connected, Connecting) => {
                log::warn!("Connection lost. Reconnecting...");
            }
            (Connecting, Connected) => {
                log::info!("Connected!");
            }
            _ => {}
        }
        self.update_overall_prefix();
    }

    fn update_overall_prefix(&self) {
        use realize_lib::transport::tcp::ClientConnectionState::*;
        self.overall_pb.set_prefix(match self.connection_state {
            NotConnected | Connecting => "Connecting",
            Connected => {
                if self.total_files == 0 {
                    "Listing"
                } else {
                    "Moving"
                }
            }
        });
    }

    pub async fn update(
        &mut self,
        mut rx: Receiver<ProgressEvent>,
        mut src_watch_rx: tokio::sync::watch::Receiver<ClientConnectionState>,
        mut dst_watch_rx: tokio::sync::watch::Receiver<ClientConnectionState>,
    ) {
        use ProgressEvent::*;
        let mut file_progress_map: HashMap<(DirectoryId, PathBuf), CliFileProgress> =
            HashMap::new();

        self.set_connection_state(
            *src_watch_rx.borrow_and_update(),
            *dst_watch_rx.borrow_and_update(),
        );
        loop {
            tokio::select!(
                _ = src_watch_rx.changed() => {
                    self.set_connection_state(
                        *src_watch_rx.borrow_and_update(),
                        *dst_watch_rx.borrow_and_update(),
                    );
                },
                _ = dst_watch_rx.changed() => {
                    self.set_connection_state(
                        *src_watch_rx.borrow_and_update(),
                        *dst_watch_rx.borrow_and_update(),
                    );
                },
                ev = rx.recv() => match ev {
                    None => return,
                    Some(MovingDir {
                        dir_id,
                        total_files,
                        total_bytes,
                        ..
                    }) => {
                        self.set_length(&dir_id, total_files, total_bytes);
                    }
                    Some(MovingFile {
                        dir_id,
                        path,
                        bytes,
                        available,
                        ..
                    }) => {
                        let fp = self.for_file(&path, bytes, available);
                        file_progress_map.insert((dir_id, path), fp);
                    }
                    Some(VerifyingFile { dir_id, path, .. }) => {
                        if let Some(fp) = file_progress_map.get_mut(&(dir_id, path)) {
                            fp.verifying();
                        }
                    }
                    Some(RsyncingFile { dir_id, path, .. }) => {
                        if let Some(fp) = file_progress_map.get_mut(&(dir_id, path)) {
                            fp.rsyncing();
                        }
                    }
                    Some(CopyingFile { dir_id, path, .. }) => {
                        if let Some(fp) = file_progress_map.get_mut(&(dir_id, path)) {
                            fp.copying();
                        }
                    }
                    Some(PendingFile { dir_id, path, .. }) => {
                        if let Some(fp) = file_progress_map.get_mut(&(dir_id, path)) {
                            fp.pending();
                        }
                    }
                    Some(IncrementByteCount {
                        dir_id,
                        path,
                        bytecount,
                        ..
                    }) => {
                        if let Some(fp) = file_progress_map.get_mut(&(dir_id, path)) {
                            fp.inc(bytecount);
                        }
                    }
                    Some(DecrementByteCount {
                        dir_id,
                        path,
                        bytecount,
                        ..
                    }) => {
                        if let Some(fp) = file_progress_map.get_mut(&(dir_id, path)) {
                            fp.dec(bytecount);
                        }
                    }
                    Some(FileSuccess { dir_id, path, .. }) => {
                        if let Some(mut fp) = file_progress_map.remove(&(dir_id, path)) {
                            fp.success();
                        }
                    }
                    Some(FileError {
                        dir_id,
                        path,
                        error,
                        ..
                    }) => {
                        if let Some(mut fp) = file_progress_map.remove(&(dir_id, path)) {
                            // Use a generic error for display
                            fp.error(&error);
                        }
                    }
                },
            );
        }
    }
}

struct CliFileProgress {
    bar: Option<(usize, ProgressBar)>,
    next_file_index: Arc<AtomicUsize>,
    total_files: usize,
    bytes: u64,

    // Bytes already available at the beginning of the operation; They
    // are rsynced instead of copied.
    available: u64,
    path: String,
    multi: MultiProgress,
    overall_pb: ProgressBar,
    quiet: bool,
}

impl CliFileProgress {
    fn get_or_create_bar(&mut self) -> (usize, &mut ProgressBar) {
        // Is it worth creating the bar on demand? Won't MoveFileEvent only be sent
        // when the bar should be shown?
        let (index, pb) = self.bar.get_or_insert_with(|| {
            let file_index = self.next_file_index.fetch_add(1, Ordering::Relaxed);
            let pb = self.multi.insert_from_back(1, ProgressBar::new(self.bytes));
            pb.set_message(self.path.clone());

            set_bar_style(file_index, self.total_files, &pb, false);
            pb.set_prefix("Pending");
            pb.inc(self.available);

            (file_index, pb)
        });

        (*index, pb)
    }
    fn verifying(&mut self) {
        log::info!("Verifying {} ({})", self.path, HumanBytes(self.bytes));

        let (_, pb) = self.get_or_create_bar();

        pb.set_prefix("Verifying");
    }
    fn rsyncing(&mut self) {
        log::info!(
            "Rsyncing {} ({}/{})",
            self.path,
            HumanBytes(self.available),
            HumanBytes(self.bytes)
        );

        let total_files = self.total_files;

        // If we enter this state, things have gone wrong. Change the color to yellow.
        let (index, pb) = self.get_or_create_bar();
        set_bar_style(index, total_files, pb, true);
        pb.set_prefix("Rsyncing");
    }

    fn copying(&mut self) {
        log::info!(
            "Copying {} ({}/{})",
            self.path,
            HumanBytes(self.available),
            HumanBytes(self.bytes)
        );

        let (_, pb) = self.get_or_create_bar();
        pb.set_prefix("Copying");
    }
    fn pending(&mut self) {
        let (_, pb) = self.get_or_create_bar();
        pb.set_prefix("Pending");
    }
    fn inc(&mut self, bytecount: u64) {
        self.overall_pb.inc(bytecount);
        if let Some((_, pb)) = &mut self.bar {
            pb.inc(bytecount);
        }
    }
    fn dec(&mut self, bytecount: u64) {
        self.overall_pb.dec(bytecount);
        if let Some((_, pb)) = &mut self.bar {
            pb.dec(bytecount);
        }
    }
    fn success(&mut self) {
        log::info!("Moved {} ({})", self.path, HumanBytes(self.bytes));

        if let Some((index, pb)) = self.bar.take() {
            pb.finish_and_clear();
            if !self.quiet {
                self.multi.suspend(|| {
                    println!(
                        "{:<10} [{}/{}] {}",
                        style("Moved").for_stdout().green().bold(),
                        index,
                        self.total_files,
                        self.path
                    );
                });
            }
        }
    }

    fn error(&mut self, err: &str) {
        log::warn!(
            "Failed to copy {} ({}): {}",
            self.path,
            HumanBytes(self.bytes),
            err
        );

        // Write report even if no bar was ever created.
        let tag = if let Some((index, pb)) = self.bar.take() {
            pb.finish_and_clear();

            format!("[{}/{}] ", index, self.total_files)
        } else {
            "".to_string()
        };
        self.multi.suspend(|| {
            eprintln!(
                "{:<10} {}{}: {}",
                style("ERROR").for_stderr().red().bold(),
                tag,
                self.path,
                err,
            );
        });
    }
}

/// Set the style of a file copy bar.
///
/// There are two variants: blue (warn=false) and yellow (warn-true).
fn set_bar_style(file_index: usize, total_files: usize, pb: &ProgressBar, warn: bool) {
    let tag = format!("[{}/{}]", file_index, total_files);
    pb.set_style(
        ProgressStyle::with_template(if warn {
            "{prefix:<10.yellow.bold} {tag} {wide_msg} ({bytes}/{total_bytes}) {percent}%"
        } else {
            "{prefix:<10.cyan.bold} {tag} {wide_msg} ({bytes}/{total_bytes}) {percent}%"
        })
        .unwrap()
        .progress_chars("=> ")
        .with_key(
            "tag",
            move |_state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
                let _ = w.write_str(&tag);
            },
        ),
    );
}
