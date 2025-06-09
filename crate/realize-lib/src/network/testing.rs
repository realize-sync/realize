/// Extract I/O error kind from an anyhow error, if possible.
pub(crate) fn io_error_kind(err: Option<anyhow::Error>) -> Option<std::io::ErrorKind> {
    Some(err?.downcast_ref::<std::io::Error>()?.kind())
}
