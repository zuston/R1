use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

use crate::config::{LogConfig, RotationConfig};

const LOG_FILE_NAME: &str = "riffle-server.log";

pub struct LogService;
impl LogService {
    pub fn init_for_test() {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
        let formatting_layer = fmt::layer().pretty().with_writer(std::io::stderr);

        Registry::default()
            .with(env_filter)
            .with(formatting_layer)
            .init();
    }

    pub fn init(log: &LogConfig) -> WorkerGuard {
        let file_appender = match log.rotation {
            RotationConfig::Hourly => tracing_appender::rolling::hourly(&log.path, LOG_FILE_NAME),
            RotationConfig::Daily => tracing_appender::rolling::daily(&log.path, LOG_FILE_NAME),
            RotationConfig::Never => tracing_appender::rolling::never(&log.path, LOG_FILE_NAME),
        };

        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let formatting_layer = fmt::layer().pretty().with_writer(std::io::stderr);

        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
        let file_layer = fmt::layer()
            .with_ansi(false)
            .with_line_number(true)
            .with_writer(non_blocking);

        Registry::default()
            .with(env_filter)
            .with(formatting_layer)
            .with(file_layer)
            .init();

        // Note: _guard is a WorkerGuard which is returned by tracing_appender::non_blocking to
        // ensure buffered logs are flushed to their output in the case of abrupt terminations of a process.
        // See WorkerGuard module for more details.
        _guard
    }
}
