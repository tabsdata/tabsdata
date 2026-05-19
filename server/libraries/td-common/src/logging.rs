//
// Copyright 2024 Tabs Data Inc.
//

//! Backend logging: tracing subscriber wiring, file/stdout sinks, the
//! sensitive-marker filter, and panic capture.
//!
//! Messages containing [`SENSITIVE_MARKER`] are dropped by
//! [`SensitiveFilterLayer`]; sanitize URLs and never log raw credentials
//! or tokens.
//!
//! [`start`] installs [`tracing_panic::panic_hook`] so panics on
//! background tokio tasks emit structured `ERROR` events through this
//! pipeline rather than going to stderr only.

use crate::constants::PYTEST_CONTEXT_ACTIVE;
use crate::env::{get_current_dir, to_absolute};
use crate::manifest::{Inf, WORKER_INF_FILE};
use crate::settings::{LOG_WITH_ANSI, MANAGER, TRUE};
use colored::Colorize;
use once_cell::sync::OnceCell;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_stdout::LogExporter;
use pico_args::Arguments;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fmt::Debug;
use std::fs::{OpenOptions, create_dir_all, read_to_string};
use std::io::{ErrorKind, stdout};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tracing::field::Field;
use tracing::level_filters::LevelFilter;
use tracing::{Event, Level, Subscriber, debug, error, info, trace, warn};
use tracing_subscriber::filter::Directive;
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::reload::{Handle, Layer as ReloadLayer};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

pub const SENSITIVE_MARKER: &str = "@@SENSITIVE@@";

pub const LOG_MESSAGE_FIELD: &str = "message";

pub const WORK_PARAMETER: &str = "--work";

pub const CURRENT_DIR: &str = ".";
pub const DEFAULT_LOG_POSITION: &str = "..";
pub const DEFAULT_LOG_PLACE: &str = "work";
pub const TARGET_FOLDER: &str = "target";
pub const TDUDF_FOLDER: &str = "tdudf";

pub const TD_LOG_PROFILE_ENV: &str = "TD_LOG_PROFILE";
pub const TD_LOG_PROFILE_ENV_PATTERN: &str = "TD_LOG_PROFILE_";

pub const CONSOLE_PROFILE: &str = "console";
pub const DEVELOPMENT_PROFILE: &str = "development";
pub const PRODUCTION_PROFILE: &str = "production";

pub const LOG_CONFIG_FILE: &str = "log.yaml";

pub const LOG_LOCATION: &str = "log";
pub const LOG_FILE: &str = "td.log";
pub const LOG_EXTENSION: &str = "log";

pub const DEFAULT_LOG_LEVEL: Level = Level::DEBUG;

pub const WORK_ENV: &str = "TD_URI_WORK";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    #[default]
    Full,
    Compact,
    CompactColored,
}

struct CompactFormat;

impl<S, N> FormatEvent<S, N> for CompactFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let level = match *event.metadata().level() {
            Level::ERROR => 'e',
            Level::WARN => 'w',
            Level::INFO => 'i',
            Level::DEBUG => 'd',
            Level::TRACE => 't',
        };
        write!(writer, "[{level}] ")?;
        context.format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

struct CompactColoredFormat;

impl<S, N> FormatEvent<S, N> for CompactColoredFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let mut message = String::new();
        let mut message_writer = Writer::new(&mut message);
        context.format_fields(message_writer.by_ref(), event)?;
        let colored = match *event.metadata().level() {
            Level::ERROR => message.red(),
            Level::WARN => message.yellow(),
            Level::INFO => message.green(),
            Level::DEBUG => message.blue(),
            Level::TRACE => message.purple(),
        };
        writeln!(writer, "{colored}")
    }
}

fn build_base_layer<S, W>(
    writer: W,
    log_with_ansi: bool,
    base_filter: EnvFilter,
    format: LogFormat,
) -> BoxedLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    match format {
        LogFormat::Full => tracing_subscriber::fmt::layer()
            .with_ansi(log_with_ansi)
            .with_writer(writer)
            .with_filter(base_filter)
            .boxed(),
        LogFormat::Compact => tracing_subscriber::fmt::layer()
            .event_format(CompactFormat)
            .with_ansi(log_with_ansi)
            .with_writer(writer)
            .with_filter(base_filter)
            .boxed(),
        LogFormat::CompactColored => tracing_subscriber::fmt::layer()
            .event_format(CompactColoredFormat)
            .with_ansi(true)
            .with_writer(writer)
            .with_filter(base_filter)
            .boxed(),
    }
}

fn build_sink_layer<S>(
    writer: BoxMakeWriter,
    log_with_ansi: bool,
    format: LogFormat,
) -> BoxedLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    match format {
        LogFormat::Full => tracing_subscriber::fmt::layer()
            .with_ansi(log_with_ansi)
            .with_writer(writer)
            .boxed(),
        LogFormat::Compact => tracing_subscriber::fmt::layer()
            .event_format(CompactFormat)
            .with_ansi(log_with_ansi)
            .with_writer(writer)
            .boxed(),
        LogFormat::CompactColored => tracing_subscriber::fmt::layer()
            .event_format(CompactColoredFormat)
            .with_ansi(true)
            .with_writer(writer)
            .boxed(),
    }
}

static BOOTSTRAP_NOTICE_ENABLED: OnceCell<bool> = OnceCell::new();

fn bootstrap_notice(message: impl Into<String>) {
    if *BOOTSTRAP_NOTICE_ENABLED.get().unwrap_or(&true) {
        eprintln!("[logging] {}", message.into());
    }
}

static LOG_RELOAD_HANDLE: OnceCell<Handle<EnvFilter, Registry>> = OnceCell::new();

// Global logger provider, initialized once.
static LOGGER_PROVIDER: OnceCell<LoggerGuard> = OnceCell::new();

// Struct to hold the logger provider and ensure proper shutdown.
struct LoggerGuard {
    provider: SdkLoggerProvider,
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
                message.push_str(&format!("{value:?}"));
            }
            forward = forward && !format!("{value:?}").contains(SENSITIVE_MARKER);
        });
        forward
    }
}

// Initialize the logger.
#[allow(unused_variables)]
fn init<W: for<'a> MakeWriter<'a> + Send + Sync + 'static>(
    max_level: Level,
    writer: W,
    directives: Vec<String>,
    with_tokio_console: bool,
    log_dir: Option<PathBuf>,
    profile: Option<LogProfile>,
    format: LogFormat,
) -> LoggerGuard {
    let log_with_ansi = MANAGER.get(LOG_WITH_ANSI).as_deref() == Some(TRUE);

    let base_filter = match &profile {
        Some(profile) => build_base_layer_filter(Some(profile)),
        None => build_default_filter(max_level, &directives),
    };

    let base_layer = build_base_layer(writer, log_with_ansi, base_filter, format);

    let (filter, sinks) = match profile {
        Some(profile) => (build_profile_filter(&profile), profile.sinks.clone()),
        None => (build_default_filter(max_level, &directives), Vec::new()),
    };

    let (reload_filter, handle) = ReloadLayer::new(filter);
    LOG_RELOAD_HANDLE.set(handle).ok();

    let mut sink_layers = Vec::new();
    for sink in &sinks {
        let writer = match sink.output {
            LogSinkOutput::StdOut => BoxMakeWriter::new(stdout),
            LogSinkOutput::File => match sink.file.as_deref() {
                Some(file_name) => match resolve_sink_path(file_name, log_dir.as_deref()) {
                    Some(path) => make_file_writer(&path),
                    None => {
                        bootstrap_notice(format!(
                            "Could not resolve directory for sink '{file_name}'; defaulting to stdout."
                        ));
                        BoxMakeWriter::new(stdout)
                    }
                },
                None => {
                    bootstrap_notice("File sink declared without 'file'; defaulting to stdout.");
                    BoxMakeWriter::new(stdout)
                }
            },
        };
        let filter = build_sink_filter(&sink.directives);
        let layer = build_sink_layer(writer, log_with_ansi, format);
        sink_layers.push(SinkLayer { layer, filter });
    }
    let tabsdata_layers = MultiWriterLayer::new(base_layer, sink_layers);

    let registry = tracing_subscriber::registry()
        .with(reload_filter)
        .with(SensitiveFilterLayer)
        .with(tabsdata_layers);
    #[cfg(feature = "tokio_console")]
    let registry = registry.with(if with_tokio_console {
        Some(console_subscriber::spawn())
    } else {
        None
    });

    registry.init();

    let exporter = LogExporter::default();
    let provider = SdkLoggerProvider::builder()
        .with_simple_exporter(exporter)
        .build();

    LoggerGuard { provider }
}

#[derive(Debug, Clone)]
pub struct LoggingOptions {
    pub max_level: Option<Level>,
    pub profile: Option<String>,
    pub with_tokio_console: bool,
    pub enable_bootstrap_notice: bool,
    pub format: LogFormat,
}

impl Default for LoggingOptions {
    fn default() -> Self {
        Self {
            max_level: None,
            profile: None,
            with_tokio_console: false,
            enable_bootstrap_notice: true,
            format: LogFormat::default(),
        }
    }
}

// Start the logger with default options.
pub fn start() {
    start_with(LoggingOptions::default());
}

// Start the logger with the given options.
// Option 'with_tokio_console' enabled requires also binaries with feature 'tokio_console'.
// Otherwise, flag will be ignored defaulting to standard behavior.
pub fn start_with(options: LoggingOptions) {
    let LoggingOptions {
        max_level,
        profile,
        with_tokio_console,
        enable_bootstrap_notice,
        format,
    } = options;

    BOOTSTRAP_NOTICE_ENABLED.get_or_init(|| enable_bootstrap_notice);

    let log_config = load();
    let active_profile_name = determine_profile_name(profile, log_config.as_ref());
    let active_profile = log_config
        .as_ref()
        .and_then(|cfg| cfg.profiles.get(&active_profile_name).cloned());
    let log_dir = resolve_log_directory(PathBuf::from(CURRENT_DIR));
    let writer = select_primary_writer(active_profile.as_ref(), log_dir.as_deref());

    let effective_level = max_level.unwrap_or_else(|| {
        active_profile
            .as_ref()
            .and_then(|profile| Level::from_str(&profile.level).ok())
            .unwrap_or(DEFAULT_LOG_LEVEL)
    });

    LOGGER_PROVIDER.get_or_init(|| {
        init(
            effective_level,
            writer,
            vec![],
            with_tokio_console,
            log_dir.clone(),
            active_profile.clone(),
            format,
        )
    });

    // Route panics through tracing so they hit the same sinks / filters as the rest
    // of the log pipeline (file, JSON, sensitive filter, OTEL). Without this, panics
    // on background tokio tasks go to stderr only.
    std::panic::set_hook(Box::new(tracing_panic::panic_hook));
}

fn select_primary_writer(profile: Option<&LogProfile>, log_dir: Option<&Path>) -> BoxMakeWriter {
    let output = profile.map(|p| p.output).unwrap_or_default();
    match output {
        LogProfileOutput::StdOut => BoxMakeWriter::new(stdout),
        LogProfileOutput::File => {
            if let Some(dir) = log_dir {
                let file_path = dir.join(LOG_FILE);
                make_file_writer(&file_path)
            } else {
                bootstrap_notice(
                    "Log directory not resolved for file output; defaulting to stdout.",
                );
                BoxMakeWriter::new(stdout)
            }
        }
    }
}

fn determine_profile_name(input_profile: Option<String>, log_config: Option<&LogConfig>) -> String {
    if let Some(worker_profile) = determine_worker_profile(log_config) {
        return worker_profile;
    }

    if let Ok(user_profile) = env::var(TD_LOG_PROFILE_ENV) {
        if profile_exists(&user_profile, log_config) {
            return user_profile;
        } else {
            bootstrap_notice(format!(
                "Ignoring TD_LOG_PROFILE '{user_profile}' because it is not defined."
            ));
        }
    }

    if let Some(profile) = input_profile {
        if profile_exists(&profile, log_config) {
            return profile;
        } else {
            bootstrap_notice(format!(
                "Input log profile '{profile}' is not defined. Ignoring it."
            ));
        }
    }

    if is_test_context() && profile_exists(DEVELOPMENT_PROFILE, log_config) {
        return DEVELOPMENT_PROFILE.to_string();
    }

    if profile_exists(PRODUCTION_PROFILE, log_config) {
        return PRODUCTION_PROFILE.to_string();
    }

    if let Some(config) = log_config
        && profile_exists(&config.profile, log_config)
    {
        return config.profile.clone();
    }

    PRODUCTION_PROFILE.to_string()
}

fn determine_worker_profile(log_config: Option<&LogConfig>) -> Option<String> {
    let worker_name = obtain_worker_name()?;
    let profile_env = format!("{}{}", TD_LOG_PROFILE_ENV_PATTERN, worker_name);
    let profile = env::var(profile_env).ok()?;
    if profile_exists(&profile, log_config) {
        Some(profile)
    } else {
        bootstrap_notice(format!(
            "Worker-specific log profile '{profile}' is not defined."
        ));
        None
    }
}

fn profile_exists(profile: &str, log_config: Option<&LogConfig>) -> bool {
    log_config
        .map(|cfg| cfg.profiles.contains_key(profile))
        .unwrap_or(true)
}

#[cfg(not(test))]
fn is_test_context() -> bool {
    false
}

#[cfg(test)]
fn is_test_context() -> bool {
    true
}

fn obtain_worker_name() -> Option<String> {
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

fn resolve_log_directory(path: PathBuf) -> Option<PathBuf> {
    let path = if path.is_absolute() {
        Some(path)
    } else {
        obtain_log_location_from_info_file(path.clone())
            .or_else(|| obtain_log_location_from_arguments(path.clone()))
            .or_else(|| obtain_log_location_from_environment(path.clone()))
            .or_else(|| obtain_log_location_from_pytest(path))
    };
    path.and_then(|path| match to_absolute(&path) {
        Ok(abs_path) => Some(abs_path.join(LOG_LOCATION)),
        Err(err) => {
            bootstrap_notice(format!(
                "Failed to resolve absolute log path for '{}': {err}.",
                path.display()
            ));
            None
        }
    })
}

fn resolve_sink_path(file: &str, log_dir: Option<&Path>) -> Option<PathBuf> {
    let sink_path = PathBuf::from(file);
    if sink_path.is_absolute() {
        Some(sink_path)
    } else {
        log_dir.map(|dir| dir.join(sink_path))
    }
}

fn make_file_writer(path: &Path) -> BoxMakeWriter {
    if let Some(parent) = path.parent()
        && let Err(err) = create_dir_all(parent)
    {
        bootstrap_notice(format!(
            "Failed to create log directory '{}': {err}. Defaulting to stdout.",
            parent.display()
        ));
        return BoxMakeWriter::new(stdout);
    }

    match OpenOptions::new().append(true).create(true).open(path) {
        Ok(file) => BoxMakeWriter::new(file),
        Err(err) => {
            bootstrap_notice(format!(
                "Unable to open log file '{}': {err}. Defaulting to stdout.",
                path.display()
            ));
            BoxMakeWriter::new(stdout)
        }
    }
}

fn build_sink_filter(directives: &[String]) -> EnvFilter {
    let filter = EnvFilter::builder()
        .with_default_directive(
            if directives.is_empty() {
                LevelFilter::INFO
            } else {
                LevelFilter::OFF
            }
            .into(),
        )
        .parse_lossy("");
    directives
        .iter()
        .fold(filter, |acc, directive| match directive.parse() {
            Ok(parsed) => acc.add_directive(parsed),
            Err(err) => {
                bootstrap_notice(format!(
                    "Unable to parse log directive '{directive}': {err}. Ignoring directive."
                ));
                acc
            }
        })
}

type BoxedLayer<S> = Box<dyn Layer<S> + Send + Sync + 'static>;

struct SinkLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    layer: BoxedLayer<S>,
    filter: EnvFilter,
}

struct MultiWriterLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    base: BoxedLayer<S>,
    sinks: Vec<SinkLayer<S>>,
}

impl<S> MultiWriterLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn new(base: BoxedLayer<S>, sinks: Vec<SinkLayer<S>>) -> Self {
        Self { base, sinks }
    }
}

impl<S> Layer<S> for MultiWriterLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_register_dispatch(&self, dispatch: &tracing::Dispatch) {
        self.base.on_register_dispatch(dispatch);
        for sink in &self.sinks {
            sink.layer.on_register_dispatch(dispatch);
        }
    }

    fn on_layer(&mut self, subscriber: &mut S) {
        self.base.on_layer(subscriber);
        for sink in &mut self.sinks {
            sink.layer.on_layer(subscriber);
        }
    }

    fn register_callsite(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        let mut base_interest = self.base.register_callsite(metadata);
        for sink in &self.sinks {
            let sink_interest = sink.layer.register_callsite(metadata);
            if sink_interest.is_always() {
                return tracing::subscriber::Interest::always();
            } else if sink_interest.is_never() {
                continue;
            } else if !base_interest.is_always() {
                base_interest = tracing::subscriber::Interest::sometimes();
            }
        }
        base_interest
    }

    fn enabled(&self, metadata: &tracing::Metadata<'_>, ctx: Context<'_, S>) -> bool {
        for sink in &self.sinks {
            if sink.filter.enabled(metadata, ctx.clone()) {
                return true;
            }
        }
        self.base.enabled(metadata, ctx)
    }

    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        self.base.on_new_span(attrs, id, ctx.clone());
        for sink in &self.sinks {
            sink.layer.on_new_span(attrs, id, ctx.clone());
        }
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        let mut hint = self.base.max_level_hint();
        for sink in &self.sinks {
            if let Some(sink_hint) = sink.layer.max_level_hint() {
                hint = match hint {
                    Some(current) if current >= sink_hint => Some(current),
                    _ => Some(sink_hint),
                };
            }
        }
        hint
    }

    fn on_record(
        &self,
        span: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: Context<'_, S>,
    ) {
        self.base.on_record(span, values, ctx.clone());
        for sink in &self.sinks {
            sink.layer.on_record(span, values, ctx.clone());
        }
    }

    fn on_follows_from(
        &self,
        span: &tracing::span::Id,
        follows: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        self.base.on_follows_from(span, follows, ctx.clone());
        for sink in &self.sinks {
            sink.layer.on_follows_from(span, follows, ctx.clone());
        }
    }

    fn event_enabled(&self, event: &Event<'_>, ctx: Context<'_, S>) -> bool {
        for sink in &self.sinks {
            if sink.filter.enabled(event.metadata(), ctx.clone()) {
                return true;
            }
        }
        self.base.event_enabled(event, ctx)
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut handled = false;
        for sink in &self.sinks {
            if sink.filter.enabled(event.metadata(), ctx.clone()) {
                sink.layer.on_event(event, ctx.clone());
                handled = true;
            }
        }
        if !handled {
            self.base.on_event(event, ctx);
        }
    }

    fn on_enter(&self, id: &tracing::span::Id, ctx: Context<'_, S>) {
        self.base.on_enter(id, ctx.clone());
        for sink in &self.sinks {
            sink.layer.on_enter(id, ctx.clone());
        }
    }

    fn on_exit(&self, id: &tracing::span::Id, ctx: Context<'_, S>) {
        self.base.on_exit(id, ctx.clone());
        for sink in &self.sinks {
            sink.layer.on_exit(id, ctx.clone());
        }
    }

    fn on_close(&self, id: tracing::span::Id, ctx: Context<'_, S>) {
        self.base.on_close(id.clone(), ctx.clone());
        for sink in &self.sinks {
            sink.layer.on_close(id.clone(), ctx.clone());
        }
    }

    fn on_id_change(&self, old: &tracing::span::Id, new: &tracing::span::Id, ctx: Context<'_, S>) {
        self.base.on_id_change(old, new, ctx.clone());
        for sink in &self.sinks {
            sink.layer.on_id_change(old, new, ctx.clone());
        }
    }
}

fn build_default_filter(max_level: Level, directives: &[String]) -> EnvFilter {
    directives.iter().fold(
        EnvFilter::from_default_env().add_directive(Directive::from(max_level)),
        |filter, directive| match directive.parse() {
            Ok(parsed) => filter.add_directive(parsed),
            Err(err) => {
                bootstrap_notice(format!(
                    "Unable to parse log directive '{directive}': {err}. Ignoring directive."
                ));
                filter
            }
        },
    )
}

fn build_profile_filter(profile: &LogProfile) -> EnvFilter {
    let base_level = if !profile.sinks.is_empty()
        && profile
            .sinks
            .iter()
            .any(|sink_config| !sink_config.directives.is_empty())
    {
        Level::TRACE
    } else {
        match Level::from_str(&profile.level) {
            Ok(level) => level,
            Err(err) => {
                bootstrap_notice(format!(
                    "Unable to parse log level '{}': {err}. Falling back to INFO.",
                    profile.level
                ));
                Level::INFO
            }
        }
    };
    profile.directives.iter().fold(
        EnvFilter::from_default_env().add_directive(base_level.into()),
        |filter, directive| match directive.parse() {
            Ok(parsed) => filter.add_directive(parsed),
            Err(err) => {
                bootstrap_notice(format!(
                    "Unable to parse log directive '{directive}': {err}. Ignoring directive."
                ));
                filter
            }
        },
    )
}

fn build_base_layer_filter(profile: Option<&LogProfile>) -> EnvFilter {
    let level = profile
        .and_then(|p| Level::from_str(&p.level).ok())
        .unwrap_or(Level::INFO);

    let directives = profile.map(|p| p.directives.as_slice()).unwrap_or(&[]);

    directives.iter().fold(
        EnvFilter::from_default_env().add_directive(level.into()),
        |filter, directive| match directive.parse() {
            Ok(parsed) => filter.add_directive(parsed),
            Err(err) => {
                bootstrap_notice(format!(
                    "Unable to parse log directive '{directive}': {err}. Ignoring directive."
                ));
                filter
            }
        },
    )
}

fn obtain_log_location_from_info_file(path: PathBuf) -> Option<PathBuf> {
    let inf_path = get_current_dir().join(WORKER_INF_FILE);
    if inf_path.exists()
        && let Ok(inf_file) = std::fs::File::open(&inf_path)
        && let Ok(inf) = serde_yaml::from_reader::<_, Inf>(inf_file)
    {
        return Some(inf.work.join(path));
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

fn obtain_log_location_from_pytest(_: PathBuf) -> Option<PathBuf> {
    if env::var(PYTEST_CONTEXT_ACTIVE).is_ok() {
        Some(get_current_dir().join(TARGET_FOLDER).join(TDUDF_FOLDER))
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
pub struct LogSinkConfig {
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub directives: Vec<String>,
    #[serde(default)]
    pub output: LogSinkOutput,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum LogSinkOutput {
    StdOut,
    #[default]
    File,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum LogProfileOutput {
    StdOut,
    #[default]
    File,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LogProfile {
    pub level: String,
    pub directives: Vec<String>,
    #[serde(default)]
    pub sinks: Vec<LogSinkConfig>,
    #[serde(default)]
    pub output: LogProfileOutput,
}

#[derive(Debug, Deserialize)]
pub struct LogConfig {
    pub profile: String,
    pub profiles: HashMap<String, LogProfile>,
}

pub fn load() -> Option<LogConfig> {
    let config_folder = obtain_config_folder()?;
    let config_path = config_folder.join(LOG_CONFIG_FILE);
    let content = match read_to_string(&config_path) {
        Ok(content) => content,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            return None;
        }
        Err(err) => {
            bootstrap_notice(format!(
                "Failed to read log configuration file '{}': {err}. Falling back to defaults.",
                config_path.display()
            ));
            return None;
        }
    };
    match serde_yaml::from_str::<LogConfig>(&content) {
        Ok(config) => Some(config),
        Err(err) => {
            bootstrap_notice(format!(
                "Failed to parse log configuration file '{}': {err}. Falling back to defaults.",
                config_path.display()
            ));
            None
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
        Ok(v) => Some(format!("{msg} - Ok: {v:?}")),
        Err(e) => Some(format!("{msg} - Err: {e:?}")),
    }
}

/// Convenience function to log [`Result:Ok`] values.
///
/// Refer to the [`td_log_monad`] macro for details.
pub fn ok<V: Debug, E: Debug>(
    msg: &str,
) -> impl Fn(&Result<V, E>) -> Option<String> + use<'_, V, E> {
    move |res: &Result<V, E>| match res {
        Ok(v) => Some(format!("{msg} - Ok: {v:?}")),
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
        Err(e) => Some(format!("{msg} - Err: {e:?}")),
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

        fn log_with_level(level: tracing::log::Level, msg: String) {
            match level {
                tracing::log::Level::Error => tracing::error!(message = msg),
                tracing::log::Level::Warn => tracing::warn!(message = msg),
                tracing::log::Level::Info => tracing::info!(message = msg),
                tracing::log::Level::Debug => tracing::debug!(message = msg),
                tracing::log::Level::Trace => tracing::trace!(message = msg),
            }
        }

        #[allow(dead_code)]
        impl<T, E> ResultLogger<T, E> for Result<T, E> {
            fn log<MD: LogLevel>(self, msg_fn: impl Fn(&Result<T, E>) -> Option<String>) -> Self {
                let level: tracing::log::Level = MD::level();
                if let Some(msg) = msg_fn(&self) {
                    log_with_level(level, msg);
                }
                self
            }
        }

        #[allow(dead_code)]
        impl<T> OptionLogger<T> for Option<T> {
            fn log<MD: LogLevel>(self, msg_fn: impl Fn(&Option<T>) -> Option<String>) -> Self {
                let level: tracing::log::Level = MD::level();
                if let Some(msg) = msg_fn(&self) {
                    log_with_level(level, msg);
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
    use std::sync::mpsc::{Receiver, Sender, channel};
    use std::time::Duration;

    use super::*;
    use std::sync::{LazyLock, Mutex};

    static TEST_LOGGING_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    type SharedLogger = Mutex<(Sender<String>, Receiver<String>, LoggerGuard)>;
    static SHARED_LOGGER: LazyLock<SharedLogger> = LazyLock::new(|| {
        let (sender, receiver) = channel();
        let writer = TestWriter {
            sender: sender.clone(),
        };
        let logger_provider = init(
            Level::DEBUG,
            writer,
            vec!["opentelemetry_sdk=error".to_string()],
            false,
            None,
            None,
            LogFormat::Full,
        );
        Mutex::new((sender, receiver, logger_provider))
    });

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
                    assert!(message.contains(
                        "logging::tests::test_res_opt_logging_module1::test_res_opt_logging_module2"
                    ));
                    assert!(message.contains("MSG - Some: \"SOME\""));
                    assert!(!message.contains("MSG - None"));
                } else {
                    panic!("No Some message received");
                }

                let _ = Option::<String>::None.log::<DEBUG>(option("MSG"));
                let received = receiver.recv_timeout(Duration::from_secs(60)).ok();
                if let Some(message) = received {
                    assert!(message.contains(
                        "logging::tests::test_res_opt_logging_module1::test_res_opt_logging_module2"
                    ));
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
                    assert!(message.contains(
                        "logging::tests::test_res_opt_logging_module1::test_res_opt_logging_module2"
                    ));
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
                    assert!(message.contains(
                        "logging::tests::test_res_opt_logging_module1::test_res_opt_logging_module2"
                    ));
                    assert!(message.contains("MSG_NONE - None"));
                    assert!(!message.contains("MSG_SOME"));
                } else {
                    panic!("No Err message received");
                }
            }
        }
    }
}

#[cfg(test)]
mod sink_tests {
    use super::*;

    #[test]
    fn resolves_relative_sink_path() {
        let log_dir = Path::new("/tmp/tabsdata/log");
        let resolved = resolve_sink_path("sql.log", Some(log_dir)).unwrap();
        assert_eq!(resolved, log_dir.join("sql.log"));
    }

    #[test]
    fn keeps_absolute_sink_path() {
        let absolute = std::env::temp_dir().join("sql.log");
        let resolved = resolve_sink_path(absolute.to_str().unwrap(), None).unwrap();
        assert_eq!(resolved, absolute);
    }

    #[test]
    fn build_sink_filter_defaults_to_trace() {
        let filter = build_sink_filter(&[]);
        assert_eq!(filter.to_string(), "info");
    }

    #[test]
    fn build_sink_filter_includes_directives() {
        let filter = build_sink_filter(&["sqlx::query=trace".to_string()]);
        assert!(
            filter.to_string().contains("sqlx::query"),
            "expected filter to contain sqlx::query directive but got {}",
            filter
        );
    }
}
