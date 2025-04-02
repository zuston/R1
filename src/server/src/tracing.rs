use crate::app::SHUFFLE_SERVER_ID;
use crate::config::Config;
use log::warn;

pub struct FastraceWrapper;

impl FastraceWrapper {
    pub fn init(config: Config) {
        if config.tracing.is_none() {
            warn!("No any tracing config. Ignore initializing...");
            return;
        }
        let config = config.tracing.unwrap();
        let jaeger_endpoint = config.jaeger_reporter_endpoint.as_str();
        let jaeger_service_name = config.jaeger_service_name.as_str();
        let reporter = fastrace_jaeger::JaegerReporter::new(
            jaeger_endpoint.parse().unwrap(),
            format!(
                "{}-{}",
                jaeger_service_name,
                SHUFFLE_SERVER_ID.get().unwrap()
            ),
        )
        .unwrap();
        fastrace::set_reporter(reporter, fastrace::collector::Config::default());
    }
}

impl Drop for FastraceWrapper {
    fn drop(&mut self) {
        fastrace::flush();
    }
}
