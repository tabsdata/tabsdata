//
// Copyright 2024 Tabs Data Inc.
//

use crate::env::{get_current_dir, to_absolute};
use crate::logging::LogOutput::File;
use crate::manifest::Inf;
use crate::manifest::WORKER_INF_FILE;
use once_cell::sync::OnceCell;
use opentelemetry_sdk::logs::LoggerProvider;
use opentelemetry_stdout::LogExporter;
use pico_args::Arguments;
use std::env;
use std::fs::{create_dir_all, OpenOptions};
use std::io::stdout;
use std::path::PathBuf;
use tracing::field::Field;
use tracing::{debug, error, info, trace, warn, Event, Level, Subscriber};
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
// ToDo: Dimas: Accept logging configuration from external file. --> https://tabsdata.atlassian.net/browse/TD-249
// ToDo: Dimas: Configure logging channel through logging configuration file. --> https://tabsdata.atlassian.net/browse/TD-250
// ToDo: Dimas: Allow custom configuration of log format. --> https://tabsdata.atlassian.net/browse/TD-251
// ToDo: Dimas: Support to logging configuration reload. --> https://tabsdata.atlassian.net/browse/TD-252
// ToDo: Dimas: Enable OpenTelemetry Semantic Conventions. --> https://tabsdata.atlassian.net/browse/TD-253
// ToDo: Dimas: Support to configurable exporters as an alternative to file or standard output. --> https://tabsdata.atlassian.net/browse/TD-254

pub const SENSITIVE_MARKER: &str = "@@SENSITIVE@@";

pub const LOG_MESSAGE_FIELD: &str = "message";

pub const WORK_PARAMETER: &str = "--work";

pub const CURRENT_DIR: &str = ".";
pub const DEFAULT_LOG_POSITION: &str = "..";
pub const DEFAULT_LOG_PLACE: &str = "work";

pub const LOG_LOCATION: &str = "log";
pub const LOG_FILE: &str = "td.log";

pub const WORK_ENV: &str = "TD_WORK";

// Global logger provider, initialized once.
static LOGGER_PROVIDER: OnceCell<LoggerGuard> = OnceCell::new();

// Enum to specify log output type
pub enum LogOutput {
    StdOut,
    File(PathBuf),
}

// Struct to hold the logger provider and ensure proper shutdown.
struct LoggerGuard {
    provider: LoggerProvider,
}

// Destructor for the log provider.
impl Drop for LoggerGuard {
    fn drop(&mut self) {
        self.provider
            .shutdown()
            .expect("Error shutting down the logging system");
    }
}

// Struc to support the layer event handler for sensitive log entries filtering.
pub struct SensitiveFilterLayer;

// Layer event handler to filter out sensitive log entries.
impl<S> Layer<S> for SensitiveFilterLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn event_enabled(&self, event: &Event<'_>, _context: Context<'_, S>) -> bool {
        let mut forward = true;
        let mut message = String::new();
        event.record(&mut |field: &Field, value: &dyn std::fmt::Debug| {
            if field.name() == LOG_MESSAGE_FIELD {
                message.push_str(&format!("{:?}", value));
            }
            forward = forward && !format!("{:?}", value).contains(SENSITIVE_MARKER);
        });
        forward
    }
}

// Initialize the logger with the specified max level and writer.
#[allow(unused_variables)]
fn init<W: for<'a> MakeWriter<'a> + Send + Sync + 'static>(
    max_level: Level,
    writer: W,
    supervisor: bool,
) -> LoggerGuard {
    let tabsdata_layer = tracing_subscriber::fmt::layer()
        .with_writer(writer)
        .with_filter(tracing_subscriber::filter::LevelFilter::from_level(
            max_level,
        ));
    let registry = tracing_subscriber::registry()
        .with(SensitiveFilterLayer)
        .with(tabsdata_layer);
    #[cfg(feature = "tokio_console")]
    let registry = registry.with(if supervisor {
        Some(console_subscriber::spawn())
    } else {
        None
    });
    registry.init();
    let exporter = LogExporter::default();
    let provider = LoggerProvider::builder()
        .with_simple_exporter(exporter)
        .build();
    LoggerGuard { provider }
}

// Start the logger with the specified max level and output type.
// Option 'with_tokio_console' enabled requires also binaries with feature 'tokio_console'.
// Otherwise, flag will be ignored defaulting to standard behavior.
pub fn start(max_level: Level, output_type: Option<LogOutput>, with_tokio_console: bool) {
    match output_type {
        None => obtain(
            max_level,
            File(PathBuf::from(CURRENT_DIR)),
            with_tokio_console,
        ),
        Some(channel) => obtain(max_level, channel, with_tokio_console),
    }
}

fn obtain(max_level: Level, output_type: LogOutput, with_tokio_console: bool) {
    let writer = match output_type {
        LogOutput::StdOut => BoxMakeWriter::new(stdout),
        File(path) => {
            let location = obtain_path_location(path);
            if location.is_none() {
                BoxMakeWriter::new(stdout)
            } else {
                create_dir_all(
                    location
                        .clone()
                        .unwrap()
                        .parent()
                        .expect("Failed to resolve log directory {}"),
                )
                .expect("Failed to create log directory {}");
                let file = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(location.unwrap())
                    .unwrap();
                BoxMakeWriter::new(file)
            }
        }
    };
    LOGGER_PROVIDER.get_or_init(|| init(max_level, writer, with_tokio_console));
}

fn obtain_path_location(path: PathBuf) -> Option<PathBuf> {
    let path = if path.is_absolute() {
        Some(path)
    } else {
        obtain_path_location_from_info_file(path.clone())
            .or_else(|| obtain_path_location_from_arguments(path.clone()))
            .or_else(|| obtain_path_location_from_environment(path))
    };
    path.and_then(|path| {
        to_absolute(&path)
            .ok()
            .map(|abs_path| abs_path.join(LOG_LOCATION).join(LOG_FILE))
    })
}

fn obtain_path_location_from_info_file(path: PathBuf) -> Option<PathBuf> {
    let inf_path = get_current_dir().join(WORKER_INF_FILE);
    if inf_path.exists() {
        if let Ok(inf_file) = std::fs::File::open(&inf_path) {
            if let Ok(inf) = serde_yaml::from_reader::<_, Inf>(inf_file) {
                return Some(inf.work.join(path));
            }
        }
    }
    None
}

fn obtain_path_location_from_arguments(path: PathBuf) -> Option<PathBuf> {
    let mut arguments = Arguments::from_env();
    let work: Option<PathBuf> = arguments.opt_value_from_str(WORK_PARAMETER).unwrap_or(None);
    let _ = arguments.finish();
    if work.is_some() {
        return Some(work?.join(path));
    }
    work
}

fn obtain_path_location_from_environment(path: PathBuf) -> Option<PathBuf> {
    if let Ok(work) = env::var(WORK_ENV) {
        Some(PathBuf::from(work).join(path))
    } else {
        None
    }
}

#[allow(unused)]
fn obtain_path_location_from_current_dir() -> Option<PathBuf> {
    Some(
        get_current_dir()
            .join(DEFAULT_LOG_POSITION)
            .join(DEFAULT_LOG_PLACE),
    )
}

// Log a message with the specified level.
// Mainly for testing. Prefer using the tracing macros.
pub fn log(level: Level, message: &str) {
    match level {
        Level::ERROR => error!(message = message),
        Level::WARN => warn!(message = message),
        Level::INFO => info!(message = message),
        Level::DEBUG => debug!(message = message),
        Level::TRACE => trace!(message = message),
    }
}

#[cfg(test)]
#[cfg(feature = "test_logging")]
mod tests {
    use std::io;
    use std::io::Write;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::time::Duration;

    use std::sync::Mutex;

    use super::*;

    lazy_static::lazy_static! {
        static ref TEST_LOGGING_MUTEX: Mutex<()> = Mutex::new(());
    }

    lazy_static::lazy_static! {
        static ref SHARED_LOGGER: Mutex<(Sender<String>, Receiver<String>, LoggerGuard)> = {
            let (sender, receiver) = channel();
            let writer = TestWriter { sender: sender.clone() };
            let logger_provider = init(Level::DEBUG, writer, false);
            Mutex::new((sender, receiver, logger_provider))
        };
    }

    // Custom writer for testing. It sends log messages through a channel.
    struct TestWriter {
        sender: Sender<String>,
    }

    impl Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let msg = String::from_utf8_lossy(buf).to_string();
            self.sender.send(msg).expect("Failed to send log message");
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for TestWriter {
        type Writer = TestWriterGuard<'a>;
        fn make_writer(&'a self) -> Self::Writer {
            TestWriterGuard {
                sender: &self.sender,
            }
        }
    }

    // Guard for the test writer.
    pub struct TestWriterGuard<'a> {
        sender: &'a Sender<String>,
    }

    impl Write for TestWriterGuard<'_> {
        // Write to the sender channel, converting to an UTF-8 string.
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let msg = String::from_utf8_lossy(buf).into_owned();
            self.sender.send(msg).unwrap();
            Ok(buf.len())
        }

        // Flush the writer. This is a no-op for the TestWriterGuard.
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    // Verifies that log messages are actually written.
    #[test]
    fn test_logging_basic() {
        let _guard = TEST_LOGGING_MUTEX.lock().unwrap();
        let (_sender, receiver, _logger_provider) = &*SHARED_LOGGER.lock().unwrap();

        log(Level::INFO, "INFO message");
        log(Level::ERROR, "ERROR message");

        let mut received = Vec::new();
        while let Ok(msg) = receiver.recv_timeout(Duration::from_secs(1)) {
            received.push(msg);
        }

        assert!(received.iter().any(|msg| msg.contains("INFO message")));
        assert!(received.iter().any(|msg| msg.contains("ERROR message")));
    }

    #[test]
    fn test_logging_message_filtering() {
        let _guard = TEST_LOGGING_MUTEX.lock().unwrap();
        let (_sender, receiver, _logger_provider) = &*SHARED_LOGGER.lock().unwrap();

        info!("Show me: sensitive");
        info!("Do not show me: @@SENSITIVE@@");

        let received = receiver.recv_timeout(Duration::from_secs(60)).ok();
        if let Some(message) = received {
            assert!(message.contains("Show me"));
        } else {
            panic!("No message received");
        }
    }

    #[test]
    fn test_logging_values_filtering() {
        let _guard = TEST_LOGGING_MUTEX.lock().unwrap();
        let (_sender, receiver, _logger_provider) = &*SHARED_LOGGER.lock().unwrap();

        info!(key = "sensitive", message = "Show me");
        info!(key = "@@SENSITIVE@@", message = "Do not show me");

        let received = receiver.recv_timeout(Duration::from_secs(60)).ok();
        if let Some(message) = received {
            assert!(message.contains("Show me"));
        } else {
            panic!("No message received");
        }
    }
}
