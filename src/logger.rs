use crate::config::LoggingConfig;
use std::fs;
use tracing::Level;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

/// Initialize logging based on configuration
pub fn init_logging(config: &LoggingConfig) -> Result<(), String> {
    // Create log directory if it doesn't exist
    fs::create_dir_all(&config.log_dir)
        .map_err(|e| format!("Failed to create log directory: {}", e))?;

    // Parse log level
    let level = match config.level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    // Create file appender based on rotation config
    let file_appender = match config.rotation.to_lowercase().as_str() {
        "daily" => tracing_appender::rolling::daily(&config.log_dir, &config.file_prefix),
        "hourly" => tracing_appender::rolling::hourly(&config.log_dir, &config.file_prefix),
        "never" => tracing_appender::rolling::never(&config.log_dir, &config.file_prefix),
        _ => tracing_appender::rolling::daily(&config.log_dir, &config.file_prefix),
    };

    // Create non-blocking writer for better performance
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    // Build the filter
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.level.clone()));

    // Create file layer
    let file_layer = if config.json_format {
        fmt::layer()
            .json()
            .with_writer(non_blocking)
            .boxed()
    } else {
        fmt::layer()
            .with_writer(non_blocking)
            .with_target(true)
            .with_thread_ids(false)
            .with_ansi(false)
            .boxed()
    };

    // Initialize the subscriber with file logging only
    tracing_subscriber::registry()
        .with(filter)
        .with(file_layer)
        .init();

    // Keep the guard alive
    std::mem::forget(_guard);

    Ok(())
}

/// Macro to log and print welcome messages (only these go to stdout)
#[macro_export]
macro_rules! welcome {
    ($($arg:tt)*) => {
        println!($($arg)*);
    };
}
