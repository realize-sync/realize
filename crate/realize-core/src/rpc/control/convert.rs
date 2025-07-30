use super::control_capnp;
use super::control_capnp::churten_notification;
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
            let path = parse_path(job_reader.get_path()?)?;
            let hash = parse_hash(job_reader.get_hash()?)?;

            let job = match job_reader.which()? {
                super::control_capnp::job::Which::Download(_) => Job::Download(path, hash),
                super::control_capnp::job::Which::Realize(realize_reader_result) => {
                    let realize_reader = realize_reader_result?;
                    let index_hash = if realize_reader.has_index_hash() {
                        let index_hash_data = realize_reader.get_index_hash()?;
                        Some(Hash(index_hash_data.try_into().map_err(|_| {
                            capnp::Error::failed("Invalid index hash length".to_string())
                        })?))
                    } else {
                        None
                    };
                    Job::Realize(path, hash, index_hash)
                }
                super::control_capnp::job::Which::Unrealize(_) => Job::Unrealize(path, hash),
            };

            Ok(ChurtenNotification::New {
                arena,
                job_id,
                job: std::sync::Arc::new(job),
            })
        }
        churten_notification::Which::Update(update_reader_result) => {
            let update_reader = update_reader_result?;
            let progress = match update_reader.get_progress()? {
                super::control_capnp::JobProgress::Running => JobProgress::Running,
                super::control_capnp::JobProgress::Done => JobProgress::Done,
                super::control_capnp::JobProgress::Abandoned => JobProgress::Abandoned,
                super::control_capnp::JobProgress::Cancelled => JobProgress::Cancelled,
                super::control_capnp::JobProgress::Failed => {
                    let message = update_reader.get_message()?.to_str()?.to_string();
                    JobProgress::Failed(message)
                }
            };

            Ok(ChurtenNotification::Update {
                arena,
                job_id,
                progress,
            })
        }
        churten_notification::Which::UpdateByteCount(byte_count_reader_result) => {
            let byte_count_reader = byte_count_reader_result?;
            Ok(ChurtenNotification::UpdateByteCount {
                arena,
                job_id,
                current_bytes: byte_count_reader.get_current_bytes(),
                total_bytes: byte_count_reader.get_total_bytes(),
            })
        }
        churten_notification::Which::UpdateAction(action_reader) => {
            let action = match action_reader?.get_action()? {
                super::control_capnp::JobAction::Download => JobAction::Download,
                super::control_capnp::JobAction::Verify => JobAction::Verify,
                super::control_capnp::JobAction::Repair => JobAction::Repair,
                super::control_capnp::JobAction::Move => JobAction::Move,
            };

            Ok(ChurtenNotification::UpdateAction {
                arena,
                job_id,
                action,
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
            let mut builder = dest.reborrow().init_new().init_job();
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
        ChurtenNotification::Update { progress, .. } => {
            let mut update = dest.reborrow().init_update();
            match progress {
                JobProgress::Running => {
                    update.set_progress(control_capnp::JobProgress::Running);
                }
                JobProgress::Done => {
                    update.set_progress(control_capnp::JobProgress::Done);
                }
                JobProgress::Abandoned => {
                    update.set_progress(control_capnp::JobProgress::Abandoned);
                }
                JobProgress::Cancelled => {
                    update.set_progress(control_capnp::JobProgress::Cancelled);
                }
                JobProgress::Failed(msg) => {
                    update.set_progress(control_capnp::JobProgress::Failed);
                    update.set_message(&msg);
                }
            }
        }
        ChurtenNotification::UpdateAction { action, .. } => {
            let mut update = dest.reborrow().init_update_action();
            match action {
                JobAction::Download => update.set_action(control_capnp::JobAction::Download),
                JobAction::Verify => update.set_action(control_capnp::JobAction::Verify),
                JobAction::Repair => update.set_action(control_capnp::JobAction::Repair),
                JobAction::Move => update.set_action(control_capnp::JobAction::Move),
            }
        }
        ChurtenNotification::UpdateByteCount {
            current_bytes,
            total_bytes,
            ..
        } => {
            let mut update = dest.reborrow().init_update_byte_count();
            update.set_current_bytes(*current_bytes);
            update.set_total_bytes(*total_bytes);
        }
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

        ChurtenNotification::Update {
            arena,
            job_id,
            progress: JobProgress::Done,
        }
    }

    fn create_test_update_failed_notification() -> ChurtenNotification {
        let arena = Arena::from("test-arena");
        let job_id = JobId(123);

        ChurtenNotification::Update {
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
        }
    }

    fn create_test_update_action_notification() -> ChurtenNotification {
        let arena = Arena::from("test-arena");
        let job_id = JobId(123);

        ChurtenNotification::UpdateAction {
            arena,
            job_id,
            action: JobAction::Download,
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
            let notification = ChurtenNotification::Update {
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
}
