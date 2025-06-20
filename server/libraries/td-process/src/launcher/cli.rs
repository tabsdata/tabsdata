//
// Copyright 2025 Tabs Data Inc.
//

use crate::launcher::config;
use crate::launcher::config::Config;
use crate::monitor::resources::{
    ResourcesMonitor, RESOURCES_MONITOR_CHECK_FREQUENCY, TD_RESOURCES_MONITOR_CHECK_FREQUENCY,
};
use clap::Parser;
use clap_derive::{Args, ValueEnum};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::env::{args, current_dir, set_current_dir};
use std::fmt::Debug;
use std::future::Future;
use std::io::{Error, ErrorKind};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::time::Duration;
use td_common::env::get_current_dir;
use td_common::manifest::Inf;
use td_common::manifest::WORKER_INF_FILE;
use td_common::status::ExitStatus;
use tokio::runtime::Runtime;
use tokio::time::sleep;
use tracing::trace;

pub const ARGUMENT_PREFIX: &str = "--";
pub const TRAILING_ARGUMENTS_PREFIX: &str = "--";

/// Trait that must be implemented by the CLI parameters struct, which itself it must be a Clap Args struct.
pub trait Params: clap::Args + Sync + Send + Clone {}

impl<A: Sync + Send + clap::Args + Clone> Params for A {}

/// Default configuration struct for CLI commands that do not have configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NoConfig {}

impl Config for NoConfig {
    fn as_yaml(&self) -> String {
        "# This command does not have configuration".to_string()
    }
}

/// Default parameters struct for CLI commands that do not have parameters.
#[derive(Debug, Clone, Args)]
pub struct NoParams {}

#[derive(Debug, Clone, ValueEnum)]
enum ConfigArg {
    Default,
    Current,
}

#[derive(Debug, clap_derive::Parser)]
#[command(version)]
struct CliParser<P: Params> {
    #[arg(
        value_enum,
        required = false,
        exclusive = true,
        long,
        help = "Print the current or default configuration"
    )]
    config: Option<ConfigArg>,
    #[command(flatten)]
    params: P,
    #[arg(
        long = "stdin-config",
        help = "Whether to ingest additional config from stdin"
    )]
    stdin_config: Option<bool>,
}

/// Entry point for all CLI commands.
///
/// It supports commands with and without configuration and parameters.
///
/// The configuration must be a struct implementing the [Config] trait, thus supporting layered configuration.
///
/// The parameters must be a struct implementing the [Params] trait.
///
/// It supports sync and async environments.
pub struct Cli<C: Config, P: Params> {
    params: PhantomData<P>,
    config: PhantomData<C>,
}

impl<C: Config, P: Params> Cli<C, P> {
    fn print_config(config_name: &str, config_type: &str, config: &C) {
        println!();
        println!("# {} configuration for {}", config_type, config_name);
        println!("#--------------------------------------------");
        println!("{}", config.as_yaml());
        println!("#--------------------------------------------");
        println!();
    }

    fn exec_impl(
        config_name: &str,
        parser: CliParser<P>,
        app: impl FnOnce(C, P) -> ExitStatus,
        config_dir: Option<PathBuf>,
    ) -> ExitStatus {
        let stdin_config = parser.stdin_config.unwrap_or(false);
        match parser.config {
            Some(ConfigArg::Default) => {
                Self::print_config(config_name, "Default", &C::default());
                ExitStatus::Success
            }
            Some(ConfigArg::Current) => {
                let config: C = config::load_config(config_name, config_dir, stdin_config);
                Self::print_config(config_name, "Current", &config);
                ExitStatus::Success
            }
            _ => {
                let config: C = config::load_config(config_name, config_dir, stdin_config);
                app(config, parser.params)
            }
        }
    }

    fn _exec(
        config_name: &str,
        parser: CliParser<P>,
        app: impl FnOnce(C, P) -> ExitStatus,
        config_dir: Option<PathBuf>,
    ) -> ExitStatus {
        Self::exec_impl(config_name, parser, app, config_dir)
    }

    fn _exec_async<R>(
        config_name: &str,
        parser: CliParser<P>,
        app: impl FnOnce(C, P) -> R,
        config_dir: Option<PathBuf>,
    ) -> ExitStatus
    where
        R: Future<Output = ExitStatus>,
    {
        let app = move |config: C, params: P| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(app(config, params))
        };
        Self::exec_impl(config_name, parser, app, config_dir)
    }

    #[cfg(not(test))]
    fn exit(exit_status: ExitStatus) {
        std::process::exit(exit_status.code());
    }

    #[cfg(test)]
    fn exit(_result: ExitStatus) {
        // no-op
    }

    fn cli_parse(config_dir: Option<PathBuf>) -> CliParser<P> {
        let current_dir = obtain_current_dir();
        let _ = (match config_dir {
            None => move_to_dir(obtain_config_dir()),
            Some(_) => move_to_dir(config_dir),
        })
        .is_ok();

        let cli_parser = CliParser::parse();

        match move_to_dir(current_dir) {
            Ok(_) => {}
            Err(e) => {
                panic!("Fatal error moving back to current dir: {}", e);
            }
        }
        cli_parser
    }

    /// Execute the app function providing the configuration and CLI parameters.
    pub fn exec(
        config_name: &str,
        app: impl FnOnce(C, P) -> ExitStatus,
        config_dir: Option<PathBuf>,
    ) {
        Self::log_parameters();
        let result = Self::_exec(
            config_name,
            Self::cli_parse(config_dir.clone()),
            app,
            config_dir,
        );
        Self::exit(result);
    }

    /// Execute the app function within Tokio's async environment, providing the configuration and CLI parameters.
    pub fn exec_async<R>(
        config_name: &str,
        app: impl FnOnce(C, P) -> R,
        config_dir: Option<PathBuf>,
        monitor_folders: Option<PathBuf>,
    ) where
        R: Future<Output = ExitStatus>,
    {
        Self::log_parameters();
        let runtime = Runtime::new().unwrap();
        Self::monitor_resources(&runtime, monitor_folders);
        let result = Self::_exec_async(
            config_name,
            Self::cli_parse(config_dir.clone()),
            app,
            config_dir,
        );
        Self::exit(result);
    }

    /// Logs input arguments and current directory.
    fn log_parameters() {
        let args: Vec<String> = args().collect();
        trace!("Using args: '{}'", args.join(" "));

        let current_dir = get_current_dir();
        trace!("Starting @ folder: '{:?}'", current_dir);
    }

    /// Monitors resources (memory, disk space, etc.) consumption.
    fn monitor_resources(runtime: &Runtime, monitor_folders: Option<PathBuf>) {
        runtime.spawn(async move {
            let mut monitor = ResourcesMonitor::new();
            loop {
                monitor.monitor(&monitor_folders);
                let wait_time = match env::var(TD_RESOURCES_MONITOR_CHECK_FREQUENCY) {
                    Ok(time) => time
                        .parse::<u64>()
                        .unwrap_or(RESOURCES_MONITOR_CHECK_FREQUENCY),
                    Err(_) => RESOURCES_MONITOR_CHECK_FREQUENCY,
                };
                let _ = sleep(Duration::from_secs(wait_time)).await;
            }
        });
    }
}

pub fn obtain_current_dir() -> Option<PathBuf> {
    current_dir().ok()
}

pub fn obtain_config_dir() -> Option<PathBuf> {
    let inf_path = get_current_dir().join(WORKER_INF_FILE);
    if inf_path.exists() {
        if let Ok(inf_file) = std::fs::File::open(&inf_path) {
            if let Ok(inf) = serde_yaml::from_reader::<_, Inf>(inf_file) {
                return Some(inf.config);
            }
        }
    }
    None
}

pub fn move_to_dir(folder: Option<PathBuf>) -> std::io::Result<()> {
    if let Some(folder) = folder {
        match set_current_dir(&folder) {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::other(format!(
                "Failed to set current folder '{:?}' for this worker: {}",
                folder, e
            ))),
        }
    } else {
        Err(Error::new(
            ErrorKind::InvalidInput,
            "Failed to determine folder to move...",
        ))
    }
}

// ToDo: Dimas: TD-349 - Improve arguments forwarding algorithms in cli & td & supervisor
/// Given trailing arguments with syntax:
/// "-- group_1 --arg_1_1_k arg1_1_v ... -- group_2 --arg_2_1_k arg2_1_v ...",
/// it will produce a map with entries for each group, and each group will have a map with the
/// arguments in key/value format in a map.
pub fn parse_extra_arguments(
    arguments_vec: Vec<String>,
) -> Result<HashMap<String, HashMap<String, String>>, String> {
    let mut arguments_map = HashMap::new();
    let mut program = None;
    let mut arguments_iterator = arguments_vec.into_iter();
    while let Some(argument) = arguments_iterator.next() {
        if argument == "--" {
            if let Some(next_argument) = arguments_iterator.next() {
                program = Some(next_argument);
                arguments_map
                    .entry(program.clone().unwrap())
                    .or_insert(HashMap::new());
            } else {
                return Err("Expected a program name after `--`".to_string());
            }
        } else if let Some(ref program_name) = program {
            if argument.starts_with("--") {
                if let Some(argument_value) = arguments_iterator.next() {
                    let argument_name = argument.trim_start_matches("--").to_string();
                    arguments_map
                        .get_mut(program_name)
                        .unwrap()
                        .insert(argument_name, argument_value);
                } else {
                    return Err(format!("Expected a value after argument: '{}'", argument));
                }
            } else {
                return Err(format!(
                    "Unexpected argument without a flag: '{}'",
                    argument
                ));
            }
        } else {
            return Err(format!("Unexpected argument: '{}'", argument));
        }
    }
    Ok(arguments_map)
}

#[cfg(test)]
mod tests {
    use crate::launcher::cli::parse_extra_arguments;
    use crate::launcher::cli::{Cli, CliParser, NoConfig, NoParams, Params};
    use crate::launcher::config::Config;
    use clap::error::Error;
    use clap::Parser;
    use clap_derive::Args;
    use getset::Getters;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use td_common::status::ExitStatus;

    fn simulate_cli<P: Params>(args: Vec<&str>) -> Result<CliParser<P>, Error> {
        CliParser::<P>::try_parse_from(std::iter::once("program").chain(args))
    }

    #[test]
    fn test_no_config_no_params() {
        let cli: CliParser<NoParams> = simulate_cli(vec![]).unwrap();
        assert!(matches!(
            Cli::<NoConfig, NoParams>::exec_impl(
                "config",
                cli,
                |_, _| { ExitStatus::Success },
                None,
            ),
            ExitStatus::Success
        ));
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Getters)]
    #[getset(get = "pub")]
    pub struct MyConfig {
        c1: String,
    }

    impl Default for MyConfig {
        fn default() -> Self {
            MyConfig {
                c1: "default".to_string(),
            }
        }
    }

    impl Config for MyConfig {}

    #[test]
    fn test_config_no_params() {
        let cli: CliParser<NoParams> = simulate_cli(vec![]).unwrap();
        assert!(matches!(
            Cli::<MyConfig, NoParams>::exec_impl(
                "config",
                cli,
                |config, _| {
                    assert_eq!(config.c1(), "default");
                    ExitStatus::Success
                },
                None,
            ),
            ExitStatus::Success
        ));
    }

    #[test]
    fn test_params_no_args() {
        let cli: CliParser<NoParams> = simulate_cli(vec![]).unwrap();
        let mut c1 = None;
        assert!(matches!(
            Cli::<MyConfig, NoParams>::exec_impl(
                "config",
                cli,
                |config, _| {
                    c1 = Some(config.c1().to_string());
                    ExitStatus::Success
                },
                None,
            ),
            ExitStatus::Success
        ));
        assert!(c1.is_some());
    }

    #[test]
    fn test_params_config_current_arg() {
        let cli: CliParser<NoParams> = simulate_cli(vec!["--config", "current"]).unwrap();
        let mut c1 = None;
        assert!(matches!(
            Cli::<MyConfig, NoParams>::exec_impl(
                "config",
                cli,
                |config, _| {
                    c1 = Some(config.c1().to_string());
                    ExitStatus::Success
                },
                None,
            ),
            ExitStatus::Success
        ));
        assert!(c1.is_none());
    }

    #[test]
    fn test_params_config_default_arg() {
        let cli: CliParser<NoParams> = simulate_cli(vec!["--config", "default"]).unwrap();
        let mut c1 = None;
        assert!(matches!(
            Cli::<MyConfig, NoParams>::exec_impl(
                "config",
                cli,
                |config, _| {
                    c1 = Some(config.c1().to_string());
                    ExitStatus::Success
                },
                None,
            ),
            ExitStatus::Success
        ));
        assert!(c1.is_none());
    }

    #[derive(Debug, Args, Clone)]
    struct MyParams {
        #[arg(required = false, long, action = clap::ArgAction::SetFalse)]
        option: Option<bool>,
    }

    #[test]
    fn test_params_params_arg() {
        let cli: CliParser<MyParams> = simulate_cli(vec!["--option"]).unwrap();
        let mut c1 = None;
        let mut option = None;
        assert!(matches!(
            Cli::<MyConfig, MyParams>::exec_impl(
                "config",
                cli,
                |config, params| {
                    c1 = Some(config.c1().to_string());
                    option = params.option;
                    ExitStatus::Success
                },
                None,
            ),
            ExitStatus::Success
        ));
        assert!(c1.is_some());
        assert!(option.is_some());
    }

    #[test]
    fn test_exec_sync() {
        let cli: CliParser<NoParams> = simulate_cli(vec![]).unwrap();
        let mut executed = false;
        Cli::<NoConfig, NoParams>::_exec(
            "config",
            cli,
            |_, _| {
                executed = true;
                ExitStatus::Success
            },
            None,
        );
        assert!(executed);
    }

    #[test]
    fn test_exec_async() {
        let cli: CliParser<NoParams> = simulate_cli(vec![]).unwrap();
        let mut executed = false;
        Cli::<NoConfig, NoParams>::_exec_async(
            "config",
            cli,
            |_, _| async {
                executed = true;
                ExitStatus::Success
            },
            None,
        );
        assert!(executed);
    }

    #[test]
    fn test_failure_exit() {
        let cli: CliParser<NoParams> = simulate_cli(vec![]).unwrap();
        assert!(matches!(
            Cli::<NoConfig, NoParams>::exec_impl(
                "config",
                cli,
                |_, _| { ExitStatus::GeneralError },
                None,
            ),
            ExitStatus::GeneralError
        ));
    }

    #[test]
    fn test_parse_extra_arguments_valid_input() {
        let arguments = vec![
            "--".to_string(),
            "program1".to_string(),
            "--arg1".to_string(),
            "value1".to_string(),
            "--arg2".to_string(),
            "value2".to_string(),
            "--".to_string(),
            "program2".to_string(),
            "--argA".to_string(),
            "valueA".to_string(),
        ];

        let expected: HashMap<String, HashMap<String, String>> = [
            (
                "program1".to_string(),
                [
                    ("arg1".to_string(), "value1".to_string()),
                    ("arg2".to_string(), "value2".to_string()),
                ]
                .iter()
                .cloned()
                .collect(),
            ),
            (
                "program2".to_string(),
                [("argA".to_string(), "valueA".to_string())]
                    .iter()
                    .cloned()
                    .collect(),
            ),
        ]
        .iter()
        .cloned()
        .collect();

        let result = parse_extra_arguments(arguments);
        assert_eq!(result, Ok(expected));
    }

    #[test]
    fn test_parse_extra_arguments_missing_value() {
        let arguments = vec![
            "--".to_string(),
            "program1".to_string(),
            "--arg1".to_string(),
        ];

        let result = parse_extra_arguments(arguments);
        assert_eq!(
            result,
            Err("Expected a value after argument: '--arg1'".to_string())
        );
    }

    #[test]
    fn test_parse_extra_arguments_unexpected_argument_without_flag() {
        let arguments = vec![
            "--".to_string(),
            "program1".to_string(),
            "unexpected".to_string(),
        ];

        let result = parse_extra_arguments(arguments);
        assert_eq!(
            result,
            Err("Unexpected argument without a flag: 'unexpected'".to_string())
        );
    }

    #[test]
    fn test_parse_extra_arguments_missing_program_name() {
        let arguments = vec!["--".to_string()];

        let result = parse_extra_arguments(arguments);
        assert_eq!(
            result,
            Err("Expected a program name after `--`".to_string())
        );
    }

    #[test]
    fn test_parse_extra_arguments_unexpected_argument() {
        let arguments = vec!["arg_without_program".to_string()];

        let result = parse_extra_arguments(arguments);
        assert_eq!(
            result,
            Err("Unexpected argument: 'arg_without_program'".to_string())
        );
    }

    #[test]
    fn test_parse_extra_arguments_empty_input() {
        let arguments: Vec<String> = vec![];
        let result = parse_extra_arguments(arguments);

        let expected: HashMap<String, HashMap<String, String>> = HashMap::new();
        assert_eq!(result, Ok(expected));
    }
}
