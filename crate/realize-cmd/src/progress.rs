use console::style;
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use realize_lib::logic::consensus::movedirs::ProgressEvent;
use realize_lib::model::service::DirectoryId;
use realize_lib::transport::tcp::ClientConnectionState;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::mpsc::Receiver;

pub(crate) struct CliProgress {
    multi: MultiProgress,
    total_files: usize,
    total_bytes: u64,
    overall_pb: ProgressBar,
    next_file_index: usize,
    quiet: bool,
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
            next_file_index: 1,
            quiet,
            should_show_dir: dir_count > 1,
            connection_state: ClientConnectionState::NotConnected,
        };
        res.update_overall_prefix();

        res
    }

    pub(crate) fn finish_and_clear(&self) {
        self.overall_pb.finish_and_clear();
        let _ = self.multi.clear();
    }

    fn set_length(
        &mut self,
        dir_id: &DirectoryId,
        total_files: usize,
        total_bytes: u64,
        available_bytes: u64,
    ) {
        log::info!(
            "{}: {} files to move ({})",
            dir_id,
            total_files,
            HumanBytes(total_bytes)
        );

        self.total_files += total_files;
        self.total_bytes += total_bytes;
        self.overall_pb.set_length(self.total_bytes);
        self.overall_pb.inc(available_bytes);
        self.update_overall_prefix();
    }

    fn for_file(
        &mut self,
        dir_id: &DirectoryId,
        path: &std::path::Path,
        bytes: u64,
        available: u64,
    ) -> CliFileProgress {
        let path = if self.should_show_dir {
            format!("{}/{}", dir_id, path.display())
        } else {
            format!("{}", path.display())
        };
        log::info!(
            "Preparing to move {} ({}/{})",
            path,
            HumanBytes(available),
            HumanBytes(bytes)
        );

        let mut pb = self.multi.insert_from_back(1, ProgressBar::new(bytes));
        pb.set_message(path.to_string());
        pb.set_prefix("Pending");
        pb.inc(available);
        // overall_pb already took MoveDirEvent.available_bytes into account
        let index = self.next_file_index;
        self.next_file_index += 1;
        let tag = format!("{}/{}", index, self.total_files);
        set_bar_style(&mut pb, &tag, false);

        CliFileProgress {
            pb,
            tag,
            bytes,
            available,
            path,
            overall_pb: self.overall_pb.clone(),
            multi: self.multi.clone(),
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
                        available_bytes,
                        ..
                    }) => {
                        self.set_length(&dir_id, total_files, total_bytes, available_bytes);
                    }
                    Some(MovingFile {
                        dir_id,
                        path,
                        bytes,
                        available,
                        ..
                    }) => {
                        let fp = self.for_file(&dir_id, &path, bytes, available);
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
    pb: ProgressBar,
    tag: String,
    bytes: u64,

    // Bytes already available at the beginning of the operation; They
    // are rsynced instead of copied.
    available: u64,
    path: String,
    overall_pb: ProgressBar,
    multi: MultiProgress,
    quiet: bool,
}

impl CliFileProgress {
    fn verifying(&mut self) {
        log::info!("Verifying {} ({})", self.path, HumanBytes(self.bytes));

        self.pb.set_prefix("Verifying");
    }
    fn rsyncing(&mut self) {
        log::info!(
            "Rsyncing {} ({}/{})",
            self.path,
            HumanBytes(self.available),
            HumanBytes(self.bytes)
        );

        // If we enter this state, things have gone wrong. Change the
        // color to yellow.
        set_bar_style(&mut self.pb, &self.tag, true);
        self.pb.set_prefix("Rsyncing");
    }

    fn copying(&mut self) {
        log::info!(
            "Copying {} ({}/{})",
            self.path,
            HumanBytes(self.available),
            HumanBytes(self.bytes)
        );

        self.pb.set_prefix("Copying");
    }
    fn pending(&mut self) {
        self.pb.set_prefix("Pending");
    }
    fn inc(&mut self, bytecount: u64) {
        self.overall_pb.inc(bytecount);
        self.pb.inc(bytecount);
    }
    fn dec(&mut self, bytecount: u64) {
        self.overall_pb.dec(bytecount);
        self.pb.dec(bytecount);
    }
    fn success(&mut self) {
        log::info!("Moved {} ({})", self.path, HumanBytes(self.bytes));

        self.pb.finish_and_clear();
        if self.quiet {
            return;
        }
        self.multi.suspend(|| {
            println!(
                "{:<10} [{}] {}",
                style("Moved").for_stdout().green().bold(),
                self.tag,
                self.path
            );
        });
    }

    fn error(&mut self, err: &str) {
        log::warn!(
            "Failed to copy {} ({}): {}",
            self.path,
            HumanBytes(self.bytes),
            err
        );

        self.pb.finish_and_clear();
        self.multi.suspend(|| {
            eprintln!(
                "{:<10} [{}] {}: {}",
                style("ERROR").for_stderr().red().bold(),
                self.tag,
                self.path,
                err,
            );
        });
    }
}

/// Set the style of a file copy bar.
///
/// There are two variants: blue (warn=false) and yellow (warn-true).
fn set_bar_style(pb: &ProgressBar, tag: &str, warn: bool) {
    let tag = tag.to_string();
    pb.set_style(
        ProgressStyle::with_template(if warn {
            "{prefix:<10.yellow.bold} [{tag}] {wide_msg} ({bytes}/{total_bytes}) {percent}%"
        } else {
            "{prefix:<10.cyan.bold} [{tag}] {wide_msg} ({bytes}/{total_bytes}) {percent}%"
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
