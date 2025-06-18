//
// Copyright 2024 Tabs Data Inc.
//

use crate::env::{get_current_dir, to_absolute};
use crate::logging::LogOutput::File;
use crate::manifest::Inf;
use crate::manifest::WORKER_INF_FILE;
use crate::settings::{LOG_WITH_ANSI, MANAGER, TRUE};
use once_cell::sync::OnceCell;
use opentelemetry_sdk::logs::LoggerProvider;
use opentelemetry_stdout::LogExporter;
use pico_args::Arguments;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fmt::Debug;
use std::fs::{create_dir_all, read_to_string, OpenOptions};
use std::io::{stdout, ErrorKind};
use std::path::PathBuf;
use std::str::FromStr;
use tracing::field::Field;
use tracing::{debug, error, info, trace, warn, Event, Level, Subscriber};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Layer, Registry};
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

pub const PROFILE_ENV_PATTERN: &str = "TD_LOG_PROFILE_";

pub const LOG_CONFIG_FILE: &str = "log.yaml";

pub const LOG_LOCATION: &str = "log";
pub const LOG_FILE: &str = "td.log";
pub const LOG_EXTENSION: &str = "log";

pub const WORK_ENV: &str = "TD_URI_WORK";

use tracing_subscriber::reload::Handle;
use tracing_subscriber::reload::Layer as ReloadLayer;
use tracing_subscriber::util::SubscriberInitExt;

static LOG_RELOAD_HANDLE: OnceCell<Handle<EnvFilter, Registry>> = OnceCell::new();

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

// Struct to support the layer event handler for sensitive log entries filtering.
pub struct SensitiveFilterLayer;

// Layer event handler to filter out sensitive log entries.
impl<S> Layer<S> for SensitiveFilterLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn event_enabled(&self, event: &Event<'_>, _context: Context<'_, S>) -> bool {
        let mut forward = true;
        let mut message = String::new();
        event.record(&mut |field: &Field, value: &dyn Debug| {
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
    with_tokio_console: bool,
) -> LoggerGuard {
    let log_with_ansi = MANAGER.get(LOG_WITH_ANSI).as_deref() == Some(TRUE);

    let tabsdata_layer = tracing_subscriber::fmt::layer()
        .with_ansi(log_with_ansi)
        .with_writer(writer)
        .with_span_events(FmtSpan::FULL);

    let env_filter = match load() {
        Some(log_config) => {
            let profile = match obtain_name() {
                Some(worker_name) => {
                    let profile_env = format!("{}{}", PROFILE_ENV_PATTERN, &worker_name);
                    env::var(profile_env).unwrap_or(log_config.profile)
                }
                None => log_config.profile,
            };
            match log_config.profiles.get(&profile) {
                Some(log_filter) => {
                    let env_filter = log_filter.directives.iter().fold(
                        EnvFilter::from_default_env().add_directive(
                            Level::from_str(&log_filter.level)
                                .unwrap_or_else(|_| {
                                    panic!("Unable to parse log level `{}`", log_filter.level)
                                })
                                .into(),
                        ),
                        |filter, directive| {
                            filter.add_directive(directive.parse().unwrap_or_else(|_| {
                                panic!("Unable to parse log directive `{}`", directive)
                            }))
                        },
                    );
                    env_filter
                }
                None => EnvFilter::from_default_env().add_directive(max_level.into()),
            }
        }
        None => EnvFilter::from_default_env().add_directive(max_level.into()),
    };

    let (reload_filter, handle) = ReloadLayer::new(env_filter);
    LOG_RELOAD_HANDLE.set(handle).ok();

    let registry = tracing_subscriber::registry()
        .with(reload_filter)
        .with(SensitiveFilterLayer)
        .with(tabsdata_layer);
    #[cfg(feature = "tokio_console")]
    let registry = registry.with(if with_tokio_console {
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
        None => obtain_log_path(
            max_level,
            File(PathBuf::from(CURRENT_DIR)),
            with_tokio_console,
        ),
        Some(channel) => obtain_log_path(max_level, channel, with_tokio_console),
    }
}

fn obtain_name() -> Option<String> {
    let inf_path = get_current_dir().join(WORKER_INF_FILE);
    if !inf_path.exists() {
        return None;
    }
    let file = std::fs::File::open(&inf_path).ok()?;
    let inf: Inf = serde_yaml::from_reader(file).ok()?;
    Some(inf.name.to_uppercase())
}

fn obtain_config_folder() -> Option<PathBuf> {
    let inf_path = get_current_dir().join(WORKER_INF_FILE);
    if !inf_path.exists() {
        return None;
    }
    let file = std::fs::File::open(&inf_path).ok()?;
    let inf: Inf = serde_yaml::from_reader(file).ok()?;
    Some(inf.config)
}

fn obtain_log_path(max_level: Level, output_type: LogOutput, with_tokio_console: bool) {
    let writer = match output_type {
        LogOutput::StdOut => BoxMakeWriter::new(stdout),
        File(path) => {
            let location = obtain_log_location(path);
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

fn obtain_log_location(path: PathBuf) -> Option<PathBuf> {
    let path = if path.is_absolute() {
        Some(path)
    } else {
        obtain_log_location_from_info_file(path.clone())
            .or_else(|| obtain_log_location_from_arguments(path.clone()))
            .or_else(|| obtain_log_location_from_environment(path))
    };
    path.and_then(|path| {
        to_absolute(&path)
            .ok()
            .map(|abs_path| abs_path.join(LOG_LOCATION).join(LOG_FILE))
    })
}

fn obtain_log_location_from_info_file(path: PathBuf) -> Option<PathBuf> {
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

fn obtain_log_location_from_arguments(path: PathBuf) -> Option<PathBuf> {
    let mut arguments = Arguments::from_env();
    let work: Option<PathBuf> = arguments.opt_value_from_str(WORK_PARAMETER).unwrap_or(None);
    let _ = arguments.finish();
    if work.is_some() {
        return Some(work?.join(path));
    }
    work
}

fn obtain_log_location_from_environment(path: PathBuf) -> Option<PathBuf> {
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

pub fn set_log_level(level: Level) {
    if let Some(handle) = LOG_RELOAD_HANDLE.get() {
        let _ = handle.reload(EnvFilter::default().add_directive(level.into()));
    } else {
        error!("Log level reload handle not initialized");
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct LogProfile {
    pub level: String,
    pub directives: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct LogConfig {
    pub profile: String,
    pub profiles: HashMap<String, LogProfile>,
}

pub fn load() -> Option<LogConfig> {
    let config_folder = obtain_config_folder()?;
    let content = match read_to_string(config_folder.join(LOG_CONFIG_FILE)) {
        Ok(content) => content,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            return None;
        }
        Err(e) => {
            panic!(
                "Failed to read log configuration file '{}' in working folder: {}",
                LOG_CONFIG_FILE, e
            );
        }
    };
    match serde_yaml::from_str::<LogConfig>(&content) {
        Ok(config) => Some(config),
        Err(e) => {
            panic!(
                "Failed to parse log configuration file '{}' in working folder: {}",
                LOG_CONFIG_FILE, e
            )
        }
    }
}

/// Convenience function to log [`Result`] values.
///
/// Refer to the [`td_log_monad`] macro for details.
pub fn result<V: Debug, E: Debug>(
    msg: &str,
) -> impl Fn(&Result<V, E>) -> Option<String> + use<'_, V, E> {
    move |res: &Result<V, E>| match res {
        Ok(v) => Some(format!("{} - Ok: {:?}", msg, v)),
        Err(e) => Some(format!("{} - Err: {:?}", msg, e)),
    }
}

/// Convenience function to log [`Result:Ok`] values.
///
/// Refer to the [`td_log_monad`] macro for details.
pub fn ok<V: Debug, E: Debug>(
    msg: &str,
) -> impl Fn(&Result<V, E>) -> Option<String> + use<'_, V, E> {
    move |res: &Result<V, E>| match res {
        Ok(v) => Some(format!("{} - Ok: {:?}", msg, v)),
        Err(_) => None,
    }
}

/// Convenience function to log [`Err`] values.
///
/// Refer to the [`td_log_monad`] macro for details.
pub fn err<V: Debug, E: Debug>(
    msg: &str,
) -> impl Fn(&Result<V, E>) -> Option<String> + use<'_, V, E> {
    move |res: &Result<V, E>| match res {
        Ok(_) => None,
        Err(e) => Some(format!("{} - Err: {:?}", msg, e)),
    }
}

/// Convenience function to log [`Option`] values.
///
/// Refer to the [`td_log_monad`] macro for details.
pub fn option<T: Debug>(msg: &str) -> impl Fn(&Option<T>) -> Option<String> + use<'_, T> {
    |option: &Option<T>| match option {
        Some(v) => Some(format!("{} - Some: {:?}", msg.to_owned(), v)),
        None => Some(format!("{} - None", msg.to_owned())),
    }
}

/// Convenience function to log [`Some`] values.
///
/// Refer to the [`td_log_monad`] macro for details.
pub fn some<T: Debug>(msg: &str) -> impl Fn(&Option<T>) -> Option<String> + use<'_, T> {
    |option: &Option<T>| {
        option
            .as_ref()
            .map(|v| format!("{} - Some: {:?}", msg.to_owned(), v))
    }
}

/// Convenience function to log [`None`] values.
///
/// The function receives a message that will be logged if the option is `None`.
///
/// Refer to the [`td_log_monad`] macro for details.
pub fn none<T: Debug>(msg: &str) -> impl Fn(&Option<T>) -> Option<String> + use<'_, T> {
    |option: &Option<T>| match option {
        Some(_) => None,
        None => Some(format!("{} - None", msg.to_owned())),
    }
}

/// Macro that enables [`Result`] and [`Option`] direct logging in the current module.
///
/// It generates module private structs and traits for [`Result`] and [`Option`]:
///
/// `Result<T,E>` has a `log::<LEVEL>(Fn(&Result<T, E>) -> Option<String>) -> Result<T, E>` method.
///
/// `Option<T>` has a `log::<LEVEL>(Fn(&Option<T>) -> Option<String>) -> Option<T>` method.
///
/// `LEVEL` can be one of the following: `ERROR`, `WARN`, `INFO`, `DEBUG`, or `TRACE`.
///
/// The lambda function receives the [`Result`] or [`Option`] and returns an `Option<String>`.
/// If the function returns `None`, no logging will be performed.
///
/// There are convenience lambda implementations:
///
/// - `ok` for [`Ok`] values,
/// - `err` for [`Err`] values,
/// - `some` for [`Some`] values,
/// - `none(message: &str)` for [`None`] values with a custom message.
///
/// How to use it:
///
/// Invoke the `td_log_monad!();` macro after the module's `use` section.
///
/// For example:
///
/// ```
///     use crate::td_common::logging::*;
///     use crate::*;
///
///     td_log_monad!();
///
///     fn my_function() {
///         let res = Result::<i32, String>::Ok(42).log::<DEBUG>(ok).log::<ERROR>(err);
///
///         let opt = Some("foo").log::<INFO>(some).log::<WARN>(none("No value found"));
///     }
/// ```
///
/// NOTE: the generation of all the structs and traits in each module is for logging to be able
/// to capture the module path at compile time (due to how tracing macros work).
#[macro_export]
macro_rules! td_log_monad {
    () => {
        /// Trait used for [`ResultLogger`] and [`OptionLogger`] blanket trait implementations.
        ///
        /// See [`td_log_monad!`] macro for more information.
        trait LogLevel {
            /// Returns the log level to use.
            fn level() -> tracing::log::Level;
        }

        #[allow(dead_code)]
        #[allow(clippy::upper_case_acronyms)]
        struct ERROR;

        impl LogLevel for ERROR {
            #[inline]
            fn level() -> tracing::log::Level {
                tracing::log::Level::Error
            }
        }

        #[allow(dead_code)]
        #[allow(clippy::upper_case_acronyms)]
        struct WARN;

        impl LogLevel for WARN {
            #[inline]
            fn level() -> tracing::log::Level {
                tracing::log::Level::Warn
            }
        }

        #[allow(dead_code)]
        #[allow(clippy::upper_case_acronyms)]
        struct INFO;

        impl LogLevel for INFO {
            #[inline]
            fn level() -> tracing::log::Level {
                tracing::log::Level::Info
            }
        }

        #[allow(dead_code)]
        #[allow(clippy::upper_case_acronyms)]
        struct DEBUG;

        impl LogLevel for DEBUG {
            #[inline]
            fn level() -> tracing::log::Level {
                tracing::log::Level::Debug
            }
        }

        #[allow(dead_code)]
        #[allow(clippy::upper_case_acronyms)]
        struct TRACE;

        impl LogLevel for TRACE {
            #[inline]
            fn level() -> tracing::log::Level {
                tracing::log::Level::Trace
            }
        }

        /// Trait with blanket implementation for logging [`Result`] types.
        trait ResultLogger<T, E> {
            /// Logs the result using the provided message function and returns the original result.
            #[allow(dead_code)]
            fn log<Level: LogLevel>(self, msg: impl Fn(&Result<T, E>) -> Option<String>) -> Self;
        }

        /// Trait with blanket implementation for logging [`Option`] types.
        trait OptionLogger<T> {
            /// Logs the option using the provided message function and returns the original option.
            #[allow(dead_code)]
            fn log<Level: LogLevel>(self, msg: impl Fn(&Option<T>) -> Option<String>) -> Self;
        }

        #[allow(dead_code)]
        impl<T, E> ResultLogger<T, E> for Result<T, E> {
            fn log<MD: LogLevel>(self, msg_fn: impl Fn(&Result<T, E>) -> Option<String>) -> Self {
                //let module = module_path!();
                let level: tracing::log::Level = MD::level();
                // todo check if module + level logging is enabled
                // if the msg_fn returns None, skip logging
                if let Some(msg) = msg_fn(&self) {
                    match level {
                        tracing::log::Level::Error => tracing::error!(message = msg),
                        tracing::log::Level::Warn => tracing::warn!(message = msg),
                        tracing::log::Level::Info => tracing::info!(message = msg),
                        tracing::log::Level::Debug => tracing::debug!(message = msg),
                        tracing::log::Level::Trace => tracing::trace!(message = msg),
                    }
                }
                self
            }
        }

        #[allow(dead_code)]
        impl<T> OptionLogger<T> for Option<T> {
            fn log<MD: LogLevel>(self, msg_fn: impl Fn(&Option<T>) -> Option<String>) -> Self {
                //let module = module_path!();
                let level: tracing::log::Level = MD::level();
                // todo check if module + level logging is enabled
                // if the msg_fn returns None, skip logging
                if let Some(msg) = msg_fn(&self) {
                    match level {
                        tracing::log::Level::Error => tracing::error!(message = msg),
                        tracing::log::Level::Warn => tracing::warn!(message = msg),
                        tracing::log::Level::Info => tracing::info!(message = msg),
                        tracing::log::Level::Debug => tracing::debug!(message = msg),
                        tracing::log::Level::Trace => tracing::trace!(message = msg),
                    }
                }
                self
            }
        }
    };
}
pub use td_log_monad;

#[cfg(test)]
#[cfg(feature = "test_logging")]
mod tests {
    use std::io;
    use std::io::Write;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::time::Duration;

    use super::*;
    use std::sync::Mutex;

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
        // Write to the sender channel, converting to a UTF-8 string.
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

    mod test_res_opt_logging_module1 {
        use crate::logging::tests::{SHARED_LOGGER, TEST_LOGGING_MUTEX};
        use crate::logging::{err, ok, result};
        use std::time::Duration;

        td_log_monad!();

        #[test]
        fn test_result_log_result() {
            let _guard = TEST_LOGGING_MUTEX.lock().unwrap();
            let (_sender, receiver, _logger_provider) = &*SHARED_LOGGER.lock().unwrap();

            let _ = Result::<String, String>::Ok("OK".to_string()).log::<DEBUG>(result("MSG"));
            let received = receiver.recv_timeout(Duration::from_secs(60)).ok();
            if let Some(message) = received {
                assert!(message.contains("logging::tests::test_res_opt_logging_module1"));
                assert!(message.contains("MSG - Ok: \"OK\""));
                assert!(!message.contains("Err:"));
            } else {
                panic!("No Ok message received");
            }

            let _ = Result::<String, String>::Err("ERR".to_string()).log::<DEBUG>(result("MSG"));
            let received = receiver.recv_timeout(Duration::from_secs(60)).ok();
            if let Some(message) = received {
                assert!(message.contains("logging::tests::test_res_opt_logging_module1"));
                assert!(message.contains("MSG - Err: \"ERR\""));
                assert!(!message.contains("Ok:"));
            } else {
                panic!("No Err message received");
            }
        }

        #[test]
        fn test_result_log_ok() {
            let _guard = TEST_LOGGING_MUTEX.lock().unwrap();
            let (_sender, receiver, _logger_provider) = &*SHARED_LOGGER.lock().unwrap();

            let _ = Result::<String, String>::Ok("OK".to_string())
                .log::<DEBUG>(ok("OK_MSG"))
                .log::<DEBUG>(err("ERR_MSG"));
            let received = receiver.recv_timeout(Duration::from_secs(60)).ok();
            if let Some(message) = received {
                assert!(message.contains("logging::tests::test_res_opt_logging_module1"));
                assert!(message.contains("OK_MSG - Ok: \"OK\""));
                assert!(!message.contains("ERR_MSG"));
            } else {
                panic!("No Ok message received");
            }
        }

        #[test]
        fn test_result_log_err() {
            let _guard = TEST_LOGGING_MUTEX.lock().unwrap();
            let (_sender, receiver, _logger_provider) = &*SHARED_LOGGER.lock().unwrap();

            let _ = Result::<String, String>::Err("ERR".to_string())
                .log::<DEBUG>(ok("OK_MSG"))
                .log::<DEBUG>(err("ERR_MSG"));
            let received = receiver.recv_timeout(Duration::from_secs(60)).ok();
            if let Some(message) = received {
                assert!(message.contains("logging::tests::test_res_opt_logging_module1"));
                assert!(message.contains("ERR_MSG - Err: \"ERR\""));
                assert!(!message.contains("OK_MSG"));
            } else {
                panic!("No Err message received");
            }
        }

        mod test_res_opt_logging_module2 {
            use crate::logging::tests::{SHARED_LOGGER, TEST_LOGGING_MUTEX};
            use crate::logging::{none, option, some};
            use std::time::Duration;

            td_log_monad!();

            #[test]
            fn test_option_option() {
                let _guard = TEST_LOGGING_MUTEX.lock().unwrap();
                let (_sender, receiver, _logger_provider) = &*SHARED_LOGGER.lock().unwrap();

                let _ = Option::<String>::Some("SOME".to_string()).log::<DEBUG>(option("MSG"));
                let received = receiver.recv_timeout(Duration::from_secs(60)).ok();
                if let Some(message) = received {
                    assert!(message.contains("logging::tests::test_res_opt_logging_module1::test_res_opt_logging_module2"));
                    assert!(message.contains("MSG - Some: \"SOME\""));
                    assert!(!message.contains("MSG - None"));
                } else {
                    panic!("No Some message received");
                }

                let _ = Option::<String>::None.log::<DEBUG>(option("MSG"));
                let received = receiver.recv_timeout(Duration::from_secs(60)).ok();
                if let Some(message) = received {
                    assert!(message.contains("logging::tests::test_res_opt_logging_module1::test_res_opt_logging_module2"));
                    assert!(message.contains("MSG - None"));
                    assert!(!message.contains("Some"));
                } else {
                    panic!("No None message received");
                }
            }

            #[test]
            fn test_option_some() {
                let _guard = TEST_LOGGING_MUTEX.lock().unwrap();
                let (_sender, receiver, _logger_provider) = &*SHARED_LOGGER.lock().unwrap();

                let _ = Option::<String>::Some("SOME".to_string())
                    .log::<DEBUG>(some("MSG_SOME"))
                    .log::<DEBUG>(none("MSG_NONE"));
                let received = receiver.recv_timeout(Duration::from_secs(60)).ok();
                if let Some(message) = received {
                    assert!(message.contains("logging::tests::test_res_opt_logging_module1::test_res_opt_logging_module2"));
                    assert!(message.contains("MSG_SOME - Some: \"SOME\""));
                    assert!(!message.contains("MSG_NONE"));
                } else {
                    panic!("No Ok message received");
                }
            }

            #[test]
            fn test_option_none() {
                let _guard = TEST_LOGGING_MUTEX.lock().unwrap();
                let (_sender, receiver, _logger_provider) = &*SHARED_LOGGER.lock().unwrap();

                let _ = Option::<String>::None
                    .log::<DEBUG>(some("MSG_SOME"))
                    .log::<DEBUG>(none("MSG_NONE"));
                let received = receiver.recv_timeout(Duration::from_secs(60)).ok();
                if let Some(message) = received {
                    assert!(message.contains("logging::tests::test_res_opt_logging_module1::test_res_opt_logging_module2"));
                    assert!(message.contains("MSG_NONE - None"));
                    assert!(!message.contains("MSG_SOME"));
                } else {
                    panic!("No Err message received");
                }
            }
        }
    }
}
