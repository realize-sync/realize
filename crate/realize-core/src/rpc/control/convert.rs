use std::sync::Arc;

use super::control_capnp;
use super::control_capnp::churten_notification;
use crate::consensus::tracker::JobInfo;
use crate::consensus::types::{ChurtenNotification, JobAction, JobProgress};
use realize_storage::{Job, JobId};
use realize_types::{Arena, Hash, Path};

/// Convert capnp ChurtenNotification to rust.
pub(crate) fn parse_notification(
    reader: churten_notification::Reader<'_>,
) -> Result<ChurtenNotification, capnp::Error> {
    let arena = parse_arena(reader.get_arena()?)?;
    let job_id = JobId(reader.get_job_id());

    match reader.which()? {
        churten_notification::Which::New(new_reader) => {
            let job_reader = new_reader?.get_job()?;
            let job = parse_job(job_reader)?;

            Ok(ChurtenNotification::New {
                arena,
                job_id,
                job: std::sync::Arc::new(job),
            })
        }
        churten_notification::Which::Start(_) => Ok(ChurtenNotification::Start { arena, job_id }),
        churten_notification::Which::Finish(update_reader_result) => {
            let update_reader = update_reader_result?;
            let progress_reader = update_reader.get_progress()?;
            let progress = parse_progress(progress_reader)?;

            Ok(ChurtenNotification::Finish {
                arena,
                job_id,
                progress,
            })
        }
        churten_notification::Which::UpdateByteCount(reader) => {
            let reader = reader?;
            Ok(ChurtenNotification::UpdateByteCount {
                arena,
                job_id,
                current_bytes: reader.get_current_bytes(),
                total_bytes: reader.get_total_bytes(),
                index: reader.get_index(),
            })
        }
        churten_notification::Which::UpdateAction(reader) => {
            let reader = reader?;
            let index = reader.get_index();
            let action = parse_action(reader.get_action()?)?.ok_or_else(|| {
                capnp::Error::failed("A JobAction must be set in UpdateAction".to_string())
            })?;

            Ok(ChurtenNotification::UpdateAction {
                arena,
                job_id,
                action,
                index,
            })
        }
    }
}

/// Convert rust ChurtenNotification to capnp.
pub(crate) fn fill_notification(
    source: ChurtenNotification,
    mut dest: churten_notification::Builder<'_>,
) {
    let arena = source.arena();
    let job_id = source.job_id();

    dest.set_arena(&arena.as_str());
    dest.set_job_id(job_id.as_u64());
    match &source {
        ChurtenNotification::New { job, .. } => {
            fill_job(job, dest.reborrow().init_new().init_job());
        }
        ChurtenNotification::Start { .. } => {
            dest.reborrow().init_start();
        }
        ChurtenNotification::Finish { progress, .. } => {
            fill_progress(progress, dest.reborrow().init_finish().init_progress());
        }
        ChurtenNotification::UpdateAction { action, index, .. } => {
            let mut update = dest.reborrow().init_update_action();
            update.set_action(to_capnp_action(Some(action)));
            update.set_index(*index);
        }
        ChurtenNotification::UpdateByteCount {
            current_bytes,
            total_bytes,
            index,
            ..
        } => {
            let mut update = dest.reborrow().init_update_byte_count();
            update.set_current_bytes(*current_bytes);
            update.set_total_bytes(*total_bytes);
            update.set_index(*index);
        }
    }
}

/// Convert rust JobInfo to capnp.
pub(crate) fn fill_job_info(source: &JobInfo, mut dest: control_capnp::job_info::Builder<'_>) {
    dest.set_arena(&source.arena.as_str());
    dest.set_id(source.id.as_u64());
    fill_job(&source.job, dest.reborrow().init_job());
    fill_progress(&source.progress, dest.reborrow().init_progress());
    dest.set_action(to_capnp_action(source.action.as_ref()));
    dest.set_notification_index(source.notification_index);
    if let Some((current, total)) = source.byte_progress {
        let mut byte_progress = dest.init_byte_progress();
        byte_progress.set_current(current);
        byte_progress.set_total(total);
    }
}

fn fill_job(job: &Arc<Job>, mut builder: control_capnp::job::Builder<'_>) {
    builder.set_path(job.path().as_str());
    builder.set_hash(&job.hash().0);

    match &**job {
        realize_storage::Job::Download(_, _) => {
            builder.init_download();
        }
        realize_storage::Job::Realize(_, _, index_hash) => {
            let mut realize = builder.init_realize();
            if let Some(h) = index_hash {
                realize.set_index_hash(&h.0);
            }
        }
        realize_storage::Job::Unrealize(_, _) => {
            builder.init_unrealize();
        }
    }
}

fn fill_progress(
    source: &JobProgress,
    mut progress_builder: control_capnp::job_progress::Builder<'_>,
) {
    match source {
        JobProgress::Pending => {
            progress_builder.set_type(control_capnp::job_progress::Type::Pending);
        }
        JobProgress::Running => {
            progress_builder.set_type(control_capnp::job_progress::Type::Running);
        }
        JobProgress::Done => {
            progress_builder.set_type(control_capnp::job_progress::Type::Done);
        }
        JobProgress::Abandoned => {
            progress_builder.set_type(control_capnp::job_progress::Type::Abandoned);
        }
        JobProgress::Cancelled => {
            progress_builder.set_type(control_capnp::job_progress::Type::Cancelled);
        }
        JobProgress::Failed(msg) => {
            progress_builder.set_type(control_capnp::job_progress::Type::Failed);
            progress_builder.set_message(msg);
        }
    }
}

fn to_capnp_action(action: Option<&JobAction>) -> control_capnp::JobAction {
    match action {
        Some(JobAction::Download) => control_capnp::JobAction::Download,
        Some(JobAction::Verify) => control_capnp::JobAction::Verify,
        Some(JobAction::Repair) => control_capnp::JobAction::Repair,
        Some(JobAction::Move) => control_capnp::JobAction::Move,
        None => control_capnp::JobAction::None,
    }
}

/// Convert capnp JobInfo to rust.
pub(crate) fn parse_job_info(
    reader: control_capnp::job_info::Reader<'_>,
) -> Result<JobInfo, capnp::Error> {
    let arena = parse_arena(reader.get_arena()?)?;
    let id = JobId(reader.get_id());
    let notification_index = reader.get_notification_index();
    let job_reader = reader.get_job()?;
    let job = parse_job(job_reader)?;
    let progress_reader = reader.get_progress()?;
    let progress = parse_progress(progress_reader)?;
    let action = parse_action(reader.get_action()?)?;
    let byte_progress = if reader.has_byte_progress() {
        let byte_progress_reader = reader.get_byte_progress()?;
        Some((
            byte_progress_reader.get_current(),
            byte_progress_reader.get_total(),
        ))
    } else {
        None
    };

    Ok(JobInfo {
        arena,
        id,
        job: std::sync::Arc::new(job),
        progress,
        action,
        byte_progress,
        notification_index,
    })
}

fn parse_job(job_reader: control_capnp::job::Reader<'_>) -> Result<Job, capnp::Error> {
    let path = parse_path(job_reader.get_path()?)?;
    let hash = parse_hash(job_reader.get_hash()?)?;

    match job_reader.which()? {
        control_capnp::job::Which::Download(_) => Ok(Job::Download(path, hash)),
        control_capnp::job::Which::Realize(realize_reader_result) => {
            let realize_reader = realize_reader_result?;
            let index_hash = if realize_reader.has_index_hash() {
                let index_hash_data = realize_reader.get_index_hash()?;
                Some(Hash(index_hash_data.try_into().map_err(|_| {
                    capnp::Error::failed("Invalid index hash length".to_string())
                })?))
            } else {
                None
            };
            Ok(Job::Realize(path, hash, index_hash))
        }
        control_capnp::job::Which::Unrealize(_) => Ok(Job::Unrealize(path, hash)),
    }
}

fn parse_progress(
    progress_reader: control_capnp::job_progress::Reader<'_>,
) -> Result<JobProgress, capnp::Error> {
    match progress_reader.get_type()? {
        control_capnp::job_progress::Type::Pending => Ok(JobProgress::Pending),
        control_capnp::job_progress::Type::Running => Ok(JobProgress::Running),
        control_capnp::job_progress::Type::Done => Ok(JobProgress::Done),
        control_capnp::job_progress::Type::Abandoned => Ok(JobProgress::Abandoned),
        control_capnp::job_progress::Type::Cancelled => Ok(JobProgress::Cancelled),
        control_capnp::job_progress::Type::Failed => {
            let message = progress_reader.get_message()?.to_str()?.to_string();
            Ok(JobProgress::Failed(message))
        }
    }
}

fn parse_action(action: control_capnp::JobAction) -> Result<Option<JobAction>, capnp::Error> {
    match action {
        control_capnp::JobAction::None => Ok(None),
        control_capnp::JobAction::Download => Ok(Some(JobAction::Download)),
        control_capnp::JobAction::Verify => Ok(Some(JobAction::Verify)),
        control_capnp::JobAction::Repair => Ok(Some(JobAction::Repair)),
        control_capnp::JobAction::Move => Ok(Some(JobAction::Move)),
    }
}

fn parse_arena(reader: capnp::text::Reader<'_>) -> Result<Arena, capnp::Error> {
    Ok(Arena::from(reader.to_str()?))
}

fn parse_path(reader: capnp::text::Reader<'_>) -> Result<Path, capnp::Error> {
    Path::parse(reader.to_str()?).map_err(|e| capnp::Error::failed(e.to_string()))
}

fn parse_hash(hash: &[u8]) -> Result<Hash, capnp::Error> {
    let hash: [u8; 32] = hash
        .try_into()
        .map_err(|_| capnp::Error::failed("invalid hash".to_string()))?;

    Ok(Hash(hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use capnp::message::Builder;
    use capnp::serialize_packed;

    fn create_test_notification() -> ChurtenNotification {
        let arena = Arena::from("test-arena");
        let job_id = JobId(123);
        let path = Path::parse("test/file.txt").unwrap();
        let hash = Hash([0x42; 32]);
        let job = Job::Download(path, hash);

        ChurtenNotification::New {
            arena,
            job_id,
            job: std::sync::Arc::new(job),
        }
    }

    fn create_test_update_notification() -> ChurtenNotification {
        let arena = Arena::from("test-arena");
        let job_id = JobId(123);

        ChurtenNotification::Finish {
            arena,
            job_id,
            progress: JobProgress::Done,
        }
    }

    fn create_test_update_failed_notification() -> ChurtenNotification {
        let arena = Arena::from("test-arena");
        let job_id = JobId(123);

        ChurtenNotification::Finish {
            arena,
            job_id,
            progress: JobProgress::Failed("error".to_string()),
        }
    }

    fn create_test_update_byte_count_notification() -> ChurtenNotification {
        let arena = Arena::from("test-arena");
        let job_id = JobId(123);

        ChurtenNotification::UpdateByteCount {
            arena,
            job_id,
            current_bytes: 42,
            total_bytes: 100,
            index: 3,
        }
    }

    fn create_test_update_action_notification() -> ChurtenNotification {
        let arena = Arena::from("test-arena");
        let job_id = JobId(123);

        ChurtenNotification::UpdateAction {
            arena,
            job_id,
            action: JobAction::Download,
            index: 2,
        }
    }

    fn create_test_realize_notification() -> ChurtenNotification {
        let arena = Arena::from("test-arena");
        let job_id = JobId(123);
        let path = Path::parse("test/file.txt").unwrap();
        let hash = Hash([0x42; 32]);
        let job = Job::Realize(path, hash, Some(Hash([0x24; 32])));

        ChurtenNotification::New {
            arena,
            job_id,
            job: std::sync::Arc::new(job),
        }
    }

    fn create_test_unrealize_notification() -> ChurtenNotification {
        let arena = Arena::from("test-arena");
        let job_id = JobId(123);
        let path = Path::parse("test/file.txt").unwrap();
        let hash = Hash([0x42; 32]);
        let job = Job::Unrealize(path, hash);

        ChurtenNotification::New {
            arena,
            job_id,
            job: std::sync::Arc::new(job),
        }
    }

    fn round_trip_test(original: ChurtenNotification) {
        // Convert to capnp
        let mut message = Builder::new_default();
        let mut builder = message.init_root::<churten_notification::Builder>();
        fill_notification(original.clone(), builder.reborrow());

        // Convert back to rust
        let msg_reader = message.into_reader();
        let reader = msg_reader
            .get_root::<churten_notification::Reader>()
            .unwrap();
        let parsed = parse_notification(reader).unwrap();

        // Compare
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_parse_new_download_notification() {
        round_trip_test(create_test_notification());
    }

    #[test]
    fn test_parse_new_realize_notification() {
        round_trip_test(create_test_realize_notification());
    }

    #[test]
    fn test_parse_new_unrealize_notification() {
        round_trip_test(create_test_unrealize_notification());
    }

    #[test]
    fn test_parse_update_notification() {
        round_trip_test(create_test_update_notification());
    }

    #[test]
    fn test_parse_update_failed_notification() {
        round_trip_test(create_test_update_failed_notification());
    }

    #[test]
    fn test_parse_update_byte_count_notification() {
        round_trip_test(create_test_update_byte_count_notification());
    }

    #[test]
    fn test_parse_update_action_notification() {
        round_trip_test(create_test_update_action_notification());
    }

    #[test]
    fn test_all_job_actions() {
        let arena = Arena::from("test-arena");
        let job_id = JobId(123);
        let index = 3;

        for action in [
            JobAction::Download,
            JobAction::Verify,
            JobAction::Repair,
            JobAction::Move,
        ] {
            let notification = ChurtenNotification::UpdateAction {
                arena,
                job_id,
                action,
                index,
            };
            round_trip_test(notification);
        }
    }

    #[test]
    fn test_all_job_progress_states() {
        let arena = Arena::from("test-arena");
        let job_id = JobId(123);

        for progress in [
            JobProgress::Running,
            JobProgress::Done,
            JobProgress::Abandoned,
            JobProgress::Cancelled,
            JobProgress::Failed("Test error".to_string()),
        ] {
            let notification = ChurtenNotification::Finish {
                arena,
                job_id,
                progress,
            };
            round_trip_test(notification);
        }
    }

    #[test]
    fn test_serialize_packed_round_trip() {
        let original = create_test_notification();

        // Convert to capnp and serialize
        let mut message = Builder::new_default();
        let mut builder = message.init_root::<churten_notification::Builder>();
        fill_notification(original.clone(), builder.reborrow());

        let mut buffer = Vec::new();
        serialize_packed::write_message(&mut buffer, &message).unwrap();

        // Deserialize and parse
        let message_reader =
            serialize_packed::read_message(&mut &buffer[..], capnp::message::ReaderOptions::new())
                .unwrap();
        let reader = message_reader
            .get_root::<churten_notification::Reader>()
            .unwrap();
        let parsed = parse_notification(reader).unwrap();

        // Compare
        assert_eq!(original, parsed);
    }

    fn create_test_job_info() -> JobInfo {
        let arena = Arena::from("test-arena");
        let id = JobId(123);
        let path = Path::parse("test/file.txt").unwrap();
        let hash = Hash([0x42; 32]);
        let job = Job::Download(path, hash);

        JobInfo {
            arena,
            id,
            job: std::sync::Arc::new(job),
            progress: JobProgress::Running,
            action: Some(JobAction::Download),
            byte_progress: Some((42, 100)),
            notification_index: 12,
        }
    }

    fn create_test_realize_job_info() -> JobInfo {
        let arena = Arena::from("test-arena");
        let id = JobId(456);
        let path = Path::parse("test/file.txt").unwrap();
        let hash = Hash([0x42; 32]);
        let job = Job::Realize(path, hash, Some(Hash([0x24; 32])));

        JobInfo {
            arena,
            id,
            job: std::sync::Arc::new(job),
            progress: JobProgress::Done,
            action: Some(JobAction::Verify),
            byte_progress: None,
            notification_index: 4,
        }
    }

    fn create_test_unrealize_job_info() -> JobInfo {
        let arena = Arena::from("test-arena");
        let id = JobId(789);
        let path = Path::parse("test/file.txt").unwrap();
        let hash = Hash([0x42; 32]);
        let job = Job::Unrealize(path, hash);

        JobInfo {
            arena,
            id,
            job: std::sync::Arc::new(job),
            progress: JobProgress::Failed("Test error".to_string()),
            action: None,
            byte_progress: Some((0, 0)),
            notification_index: 9,
        }
    }

    fn job_info_round_trip_test(original: JobInfo) {
        // Convert to capnp
        let mut message = Builder::new_default();
        let mut builder = message.init_root::<control_capnp::job_info::Builder>();
        fill_job_info(&original, builder.reborrow());

        // Convert back to rust
        let msg_reader = message.into_reader();
        let reader = msg_reader
            .get_root::<control_capnp::job_info::Reader>()
            .unwrap();
        let parsed = parse_job_info(reader).unwrap();

        // Compare
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_parse_job_info_download() {
        job_info_round_trip_test(create_test_job_info());
    }

    #[test]
    fn test_parse_job_info_realize() {
        job_info_round_trip_test(create_test_realize_job_info());
    }

    #[test]
    fn test_parse_job_info_unrealize() {
        job_info_round_trip_test(create_test_unrealize_job_info());
    }

    #[test]
    fn test_parse_job_info_all_progress_states() {
        let arena = Arena::from("test-arena");
        let id = JobId(123);
        let path = Path::parse("test/file.txt").unwrap();
        let hash = Hash([0x42; 32]);
        let job = Job::Download(path, hash);

        for progress in [
            JobProgress::Pending,
            JobProgress::Running,
            JobProgress::Done,
            JobProgress::Abandoned,
            JobProgress::Cancelled,
            JobProgress::Failed("Test error".to_string()),
        ] {
            let job_info = JobInfo {
                arena,
                id,
                job: std::sync::Arc::new(job.clone()),
                progress,
                action: Some(JobAction::Download),
                byte_progress: Some((42, 100)),
                notification_index: 12,
            };
            job_info_round_trip_test(job_info);
        }
    }

    #[test]
    fn test_parse_job_info_all_actions() {
        let arena = Arena::from("test-arena");
        let id = JobId(123);
        let path = Path::parse("test/file.txt").unwrap();
        let hash = Hash([0x42; 32]);
        let job = Job::Download(path, hash);

        for action in [
            JobAction::Download,
            JobAction::Verify,
            JobAction::Repair,
            JobAction::Move,
        ] {
            let job_info = JobInfo {
                arena,
                id,
                job: std::sync::Arc::new(job.clone()),
                progress: JobProgress::Running,
                action: Some(action),
                byte_progress: Some((42, 100)),
                notification_index: 10,
            };
            job_info_round_trip_test(job_info);
        }
    }

    #[test]
    fn test_parse_job_info_no_action() {
        let arena = Arena::from("test-arena");
        let id = JobId(123);
        let path = Path::parse("test/file.txt").unwrap();
        let hash = Hash([0x42; 32]);
        let job = Job::Download(path, hash);

        let job_info = JobInfo {
            arena,
            id,
            job: std::sync::Arc::new(job),
            progress: JobProgress::Pending,
            action: None,
            byte_progress: None,
            notification_index: 11,
        };
        job_info_round_trip_test(job_info);
    }
}
