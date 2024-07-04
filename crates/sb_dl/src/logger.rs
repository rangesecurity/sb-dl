use std::str::FromStr;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{filter::LevelFilter, prelude::*, Layer};
pub struct LogOpts {
    pub level: String,
    pub file: String,
    pub console: bool,
}

/// initializes logging capabilities but adds a variety of customization, including file+line which sourced the log,
/// a tokio-console used for monitoring async tasks, as well as log-level filtration
pub fn init_log(opts: LogOpts) {
    let mut layers = Vec::with_capacity(2);
    let level_filter = LevelFilter::from_level(tracing::Level::from_str(&opts.level).unwrap());
    let filter = EnvFilter::from_default_env().add_directive(level_filter.into());

    layers.push(
        tracing_subscriber::fmt::layer()
            .with_level(true)
            .with_line_number(true)
            .with_file(true)
            .with_filter(filter)
            .boxed(),
    );
    if !opts.file.is_empty() {
        let log_file = std::fs::File::options()
            .create(true)
            .append(true)
            .open(opts.file)
            .unwrap();
        layers.push(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(log_file)
                .with_filter(EnvFilter::from_default_env().add_directive(level_filter.into()))
                .boxed(),
        );
    }
    if let Err(err) = tracing_subscriber::registry().with(layers).try_init() {
        log::warn!("global subscriber already registered {err:#?}");
    }
}
