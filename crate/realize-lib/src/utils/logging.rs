use env_logger::Builder;
use std::io::Write;

/// Create a builder with app-wide defaults.
///
/// Level defaults to WARN. Can be set with the RUST_LOG env variable.
///
/// Output format can be made systemd-friendly by setting
/// RUST_LOG_FORMAT to SYSTEMD.
pub fn init() {
    init_with_info_modules(vec![])
}

/// Create a builder with app-wide defaults.
///
/// Works like [init] and add support for displaying the logs of some
/// modules at info level by default. Setting the env var RUST_LOG
/// overwrites this.
pub fn init_with_info_modules(info_modules: Vec<&str>) {
    let mut builder = env_logger::Builder::new();

    if let Ok(format) = std::env::var("RUST_LOG_FORMAT") {
        if format == "SYSTEMD" {
            enable_systemd_log_format(&mut builder);
        }
    }

    builder.filter_level(log::LevelFilter::Warn);
    for module in info_modules {
        builder.filter_module(module, log::LevelFilter::Info);
    }

    builder.parse_default_env();
    builder.init();
}

/// Set the format of the log output to a systemd-compatible one.
///
/// Convenient output format when working with systemd/syslog
/// for log levels to be recognized. Time isn't useful since
/// it's tracked by the logging facility anyways.
fn enable_systemd_log_format(builder: &mut Builder) {
    builder.format(|fmt, record| {
        writeln!(
            fmt,
            "<{}>{}: {}",
            // Map to syslog priority level (RFC 5424)
            match record.level() {
                log::Level::Error => 3,
                log::Level::Warn => 4,
                log::Level::Info => 5,
                log::Level::Debug => 7,
                log::Level::Trace => 7,
            },
            record.target(),
            record.args()
        )
    });
}
