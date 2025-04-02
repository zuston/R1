use crate::app::SHUFFLE_SERVER_ID;
use crate::config::LogConfig;
use logforth::append::rolling_file::{RollingFileWriter, Rotation};
use logforth::append::{rolling_file, RollingFile};
use logforth::non_blocking::WorkerGuard;

const LOG_FILE_NAME: &str = "riffle-server";

pub struct LogService;
impl LogService {
    pub fn init(log: &LogConfig) -> WorkerGuard {
        // todo: obey the rule from the config options

        let default_id = "unknown".to_string();
        let shuffle_server_id = SHUFFLE_SERVER_ID.get().unwrap_or(&default_id);
        let child_folder = format!("{}/{}", &log.path, shuffle_server_id);

        let rolling_writer = RollingFileWriter::builder()
            .rotation(Rotation::Daily)
            .filename_prefix(LOG_FILE_NAME)
            .max_file_size(512 * 1024 * 1024)
            .max_log_files(10)
            .build(&child_folder)
            .unwrap();

        let (non_blocking, _guard) = rolling_file::non_blocking(rolling_writer).finish();

        logforth::builder()
            .dispatch(|d| {
                d.filter(log::LevelFilter::Info)
                    .append(RollingFile::new(non_blocking))
            })
            .apply();

        _guard
    }
}
