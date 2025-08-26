//
// Copyright 2025 Tabs Data Inc.
//

use crate::component::describer::{
    DescriberError, TabsDataWorkerDescriber, TabsDataWorkerDescriberBuilder,
};
use crate::component::tracker::{WorkerStatus, WorkerTracker};
use crate::launch::worker::{TabsDataWorker, Worker};
use crate::resource::instance::{
    get_instance_path_for_instance, get_repository_path_for_instance,
    get_workspace_path_for_instance,
};
use crate::services::bootloader::{
    BOOTLOADER, BOOTLOADER_ARGUMENT_INSTANCE, BOOTLOADER_ARGUMENT_PROFILE,
    BOOTLOADER_ARGUMENT_REPOSITORY, BOOTLOADER_ARGUMENT_WORKSPACE,
};
use crate::services::supervisor::TD_ARGUMENT_KEY;
use crate::services::supervisor::WorkerLocation::Relative;
use clap::{Parser, command};
use clap_derive::{Args, Subcommand};
use colored::Colorize;
use getset::Getters;
use humantime::format_duration;
use indexmap::IndexMap;
use linemux::MuxedLines;
use num_format::{Locale, ToFormattedString};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env::set_current_dir;
use std::fs::create_dir_all;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, exit};
use std::thread::sleep;
use std::{env, fs, io};
use sysinfo::Signal;
use ta_tableframe::api::Extension;
use tabled::{
    Table, Tabled,
    settings::{
        Alignment, Modify,
        object::Columns,
        style::{HorizontalLine, Style, VerticalLine},
    },
};
use td_apiserver::config::DbSchema;
use td_build::version::TABSDATA_VERSION;
use td_common::env::{TABSDATA_HOME_DIR, check_flag_env, get_home_dir, to_absolute};
use td_common::logging::set_log_level;
use td_common::os::{name_program, terminate_process};
use td_common::server::WorkerClass::REGULAR;
use td_common::server::{
    AVAILABLE_ENVIRONMENTS_FOLDER, CONFIG_FOLDER, DATABASE_FILE, DATABASE_FOLDER,
    ENVIRONMENTS_FOLDER, ETC_FOLDER, MSG_FOLDER, STORAGE_FOLDER, TD_DETACHED_SUBPROCESSES,
    WORK_FOLDER,
};
use td_common::status::ExitStatus::{GeneralError, NoAction, Success};
use td_process::launcher::arg::InheritedArgumentKey;
use td_process::launcher::arg::InheritedArgumentKey::*;
use td_process::launcher::cli::{
    ARGUMENT_PREFIX, TRAILING_ARGUMENTS_PREFIX, parse_extra_arguments,
};
use td_process::monitor::processes::{ProcessDistilled, get_process_tree};
use td_process::monitor::space::instance_space;
use td_python::upgrade::{get_source_version, get_target_version, upgrade};
use td_python::venv::prepare;
use te_tableframe::engine::TableFrameExtension;
use terminal_size::{Width, terminal_size};
use textwrap::{Options, WordSeparator, fill};
use thiserror::Error;
use tm_workspace::workspace_root;
use tokio::time::{Duration, Instant};
use tracing::{Level, error, info, warn};
use url::Url;
use walkdir::WalkDir;

pub const SUPERVISOR: &str = "supervisor";

pub const APISERVER: &str = "apiserver";
pub const APISERVER_ARGUMENT_DATABASE_URL: &str = "--database-url";
pub const APISERVER_ARGUMENT_STORAGE_URL: &str = "--storage-url";
pub const APISERVER_ARGUMENT_DB_SCHEMA: &str = "--db-schema";

pub const TD_KEEP: &str = ".tdkeep";

const START_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const START_WAIT: Duration = Duration::from_secs(5);

const STOP_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const STOP_WAIT: Duration = Duration::from_secs(2);

const VERSION: &str = TABSDATA_VERSION;

const BANNER: &str = include_str!(concat!(
    workspace_root!(),
    "/variant/assets/manifest/BANNER"
));

const LICENSE: &str = include_str!(concat!(
    workspace_root!(),
    "/variant/assets/manifest/LICENSE"
));

#[derive(Debug, Error)]
pub enum TabsCliError {
    #[error("Failed to start Tabs Data worker '{command}' in workspace '{workspace}': {cause}")]
    LaunchError {
        command: String,
        workspace: String,
        cause: io::Error,
    },
}

#[derive(Debug, Clone, clap_derive::Parser)]
#[command(
    name = "Tabsdata Server",
    version = "1.3.0",
    about = "Tabsdata Server",
    long_about = "Any Tabsdata instance can be managed with the available commands of this tool. \
                  These commands rely on file 'pid' to control the state of any instance."
)]
struct Arguments {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, Subcommand)]
enum Commands {
    /*
    #[command(about = "Create a Tabsdata settings based on the product defaults")]
    Settings(SettingsArguments),
     */

    /*
    #[command(about = "Create a Tabsdata profile based on the product defaults")]
    Profile(ProfileArguments),
     */
    #[command(about = "Show Tabsdata banner")]
    Banner(BannerArguments),

    #[command(about = "Show Tabsdata license")]
    License(LicenseArguments),

    #[command(about = "Show Tabsdata installation information", name = "info")]
    Information(InformationArguments),

    #[command(about = "Create a Tabsdata instance")]
    Create(CreateArguments),

    #[command(about = "Upgrade a Tabsdata instance")]
    Upgrade(UpgradeArguments),

    #[command(about = "Delete a Tabsdata instance")]
    Delete(DeleteArguments),

    #[command(about = "Start a Tabsdata instance")]
    Start(StartArguments),

    #[command(about = "Restart a Tabsdata instance, stopping gracefully")]
    Restart(RestartArguments),

    #[command(about = "Stop a Tabsdata instance, gracefully")]
    Stop(StopArguments),

    #[command(about = "Show the status of a Tabsdata instance")]
    Status(StatusArguments),

    #[command(about = "Tail the logs of a Tabsdata instance")]
    Log(ControlArguments),

    #[command(
        name = "clean",
        about = "Clean Tabsdata internal Python virtual environments & pip and uv cache"
    )]
    Clean(CleanArguments),
    /*
    #[command(about = "Find Tabsdata instances in the system")]
    Instances,
     */
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct BannerArguments {}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct LicenseArguments {}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct InformationArguments {}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct ProfileArguments {
    /// Name of the profile to create.
    #[arg(
        long,
        name = "name",
        required = true,
        long_help = "Name of the profile to create."
    )]
    name: String,

    /// Folder to contain the created profile.
    #[arg(
        long,
        name = "folder",
        required = true,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Folder to contain the created profile. \
                     It can be absolute or relative. All required parent folders will be created if necessary."
    )]
    folder: PathBuf,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct SettingsArguments {
    /// Name of the instance of the settings file.
    #[arg(
        long,
        name = "instance",
        required = false,
        long_help = "Name of the instance of the settings file. \
                     If specified, the generated file will be settings_<instance>.yaml. \
                     Otherwise, the generated file will be settings.yaml"
    )]
    instance: Option<String>,

    /// Destination folder of the created settings.
    #[arg(
        long,
        name = "folder",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Folder to contain the created settings. \
                     If unspecified, folder ~/.tabsdata will be used. \
                     It can be absolute or relative. All required parent folders will be created if necessary."
    )]
    folder: Option<PathBuf>,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct InstanceArguments {
    /// Name/Location of the Tabsdata instance.
    #[arg(
        long,
        name = "instance",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Name/Location of the Tabsdata instance."
    )]
    instance: Option<PathBuf>,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct UpgradeArguments {
    #[command(flatten)]
    instance: InstanceArguments,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct CleanArguments {
    /// Automatic confirmation.
    #[arg(
        long,
        name = "force",
        required = false,
        default_value_t = false,
        long_help = "Clean without confirmation."
    )]
    force: bool,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct ControlArguments {
    /// Name/Location of the Tabsdata instance.
    #[arg(
        long,
        name = "instance",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Name/Location of the Tabsdata instance."
    )]
    instance: Option<PathBuf>,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct CreateArguments {
    /// Name/Location of the Tabsdata instance.
    #[arg(
        long,
        name = "instance",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Name/Location of the Tabsdata instance. \
                     The instance is stored as a subfolder of the user's home folder, when a relative path. \
                     If unspecified, instance ~/.tabsdata/instances/tabsdata will be used."
    )]
    instance: Option<PathBuf>,

    /// Folder containing the instance's profile.
    #[arg(
        long,
        name = "profile",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Folder containing the instance's profile. \
                    The default Tabsdata profile will we used if unspecified."
    )]
    profile: Option<PathBuf>,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct DeleteArguments {
    #[command(flatten)]
    instance: InstanceArguments,

    /// Automatic confirmation.
    #[arg(
        long,
        name = "force",
        required = false,
        default_value_t = false,
        long_help = "Delete without confirmation."
    )]
    force: bool,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct StartArguments {
    /// Require instance to exists in advance.
    #[arg(
        long,
        name = "existing",
        required = false,
        default_value_t = false,
        long_help = "Whether the instance is expected to already exist. It defaults to false."
    )]
    existing: bool,

    /// Skip waiting for the instance to fully start.
    /// If not set, it will wait until startup completes or 5 minutes have passed.
    #[arg(
        long,
        name = "wait",
        required = false,
        default_value_t = false,
        long_help = "Continue without waiting for the instance to finish starting."
    )]
    no_wait: bool,

    /// Name/Location of the Tabsdata instance.
    #[arg(
        long,
        name = "instance",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Name/Location of the Tabsdata instance. \
                     The instance is stored as a subfolder of the user's home folder, when a relative path. \
                     If unspecified, instance ~/.tabsdata/instances/tabsdata will be used."
    )]
    instance: Option<PathBuf>,

    /*
    /// Folder containing the instance's profile.
    #[arg(
        long,
        name = "profile",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Folder containing the instance's profile. \
                    The default Tabsdata profile will we used if unspecified."
    )]
    profile: Option<PathBuf>,
     */
    /// Additional arguments to pass to the root supervisor and all its workers.
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "-- <arguments>",
        long_help = "Additional arguments to pass to the root supervisor and all its workers launched to for the instance. \
                     Use any additional argument supported the root supervisor or any of its workers. \
                     Use the syntax '-- program_1 --arg_1_1_k arg1_1_v ... -- program_2 --arg_2_1_k arg2_1_v ...'."
    )]
    arguments: Vec<String>,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct RestartArguments {
    #[command(flatten)]
    start: StartArguments,

    #[command(flatten)]
    options: StopOptionsArguments,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct StopArguments {
    #[command(flatten)]
    control: ControlArguments,

    #[command(flatten)]
    options: StopOptionsArguments,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct StopOptionsArguments {
    /// Option to stop forcefully.
    #[arg(long, name = "force", long_help = "Option to stop forcefully.")]
    force: bool,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct StatusArguments {
    #[command(flatten)]
    control: ControlArguments,

    #[command(flatten)]
    options: StatusOptionsArguments,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct StatusOptionsArguments {
    /// Option to show metrics resources consumption too.
    #[arg(
        long,
        name = "usage",
        long_help = "Option to show metrics on resources consumption too."
    )]
    metrics: bool,
}

pub struct TabsDataCli {}

impl TabsDataCli {
    async fn run(&self) {
        let arguments = Arguments::parse();
        match arguments.command {
            /*
            Commands::Settings(arguments) => {
                command_settings(arguments);
            }
             */
            /*
            Commands::Profile(arguments) => {
                command_profile(arguments);
            }
             */
            Commands::Banner(arguments) => {
                command_banner(arguments);
            }
            Commands::License(arguments) => {
                command_license(arguments);
            }
            Commands::Information(arguments) => {
                command_information(arguments);
            }
            Commands::Create(arguments) => {
                command_create(arguments);
            }
            Commands::Upgrade(arguments) => {
                command_upgrade(arguments);
            }
            Commands::Delete(arguments) => {
                command_delete(arguments);
            }
            Commands::Start(arguments) => {
                command_start(arguments);
            }
            Commands::Restart(arguments) => {
                command_restart(arguments);
            }
            Commands::Stop(arguments) => {
                command_stop(arguments);
            }
            Commands::Status(arguments) => {
                command_status(arguments);
            }
            Commands::Log(arguments) => {
                command_log(arguments).await;
            }
            Commands::Clean(arguments) => {
                command_clean(arguments);
            } /*
              Commands::Instances => {
                  command_instances();
              }
               */
        }
    }
}

pub fn show_box(text: &str, min: usize) -> Result<(), io::Error> {
    #[cfg(not(windows))]
    let use_colors = supports_color::on(supports_color::Stream::Stdout).is_some();
    #[cfg(windows)]
    let use_colors = false;

    let width = terminal_size()
        .map(|(Width(w), _)| w as usize - 6)
        .unwrap_or(50)
        .min(min);
    let trimmed_text: String = text
        .lines()
        .map(|line| line.trim())
        .collect::<Vec<_>>()
        .join("\n");
    let wrap_options = Options::new(width)
        .break_words(false)
        .word_splitter(textwrap::WordSplitter::NoHyphenation)
        .word_separator(WordSeparator::AsciiSpace);
    let wrapped_text = fill(trimmed_text.trim(), wrap_options);
    let top_border = if use_colors {
        format!("╭{}╮", "─".repeat(width + 2))
            .blue()
            .bold()
            .to_string()
    } else {
        format!("╭{}╮", "─".repeat(width + 2))
    };
    let bottom_border = if use_colors {
        format!("╰{}╯", "─".repeat(width + 2))
            .blue()
            .bold()
            .to_string()
    } else {
        format!("╰{}╯", "─".repeat(width + 2))
    };
    println!("\n{top_border}");
    for line in wrapped_text.lines() {
        let padded_line = format!("{line:^width$}");
        if use_colors {
            println!(
                "{} {} {}",
                "│".blue().bold(),
                padded_line.truecolor(251, 175, 79).bold(),
                "│".blue().bold()
            );
        } else {
            println!("│ {padded_line} │");
        }
    }
    println!("{bottom_border}");
    Ok(())
}

pub fn show_banner() {
    show_box(BANNER, 80).unwrap()
}

pub fn show_license() {
    show_box(LICENSE, 80).unwrap()
}

pub fn show_information() {
    let mut information = vec![];

    information.push(format!("Version: {VERSION}"));

    let edition = TableFrameExtension.edition();
    information.push(format!("Edition: {edition}"));

    let run_mode = run_mode();
    information.push(format!("Mode: {run_mode}"));

    let pip_uv_repository_envs = get_pip_uv_repository_envs();
    if !pip_uv_repository_envs.is_empty() {
        information.push("".to_string());
        information.push(
            "Found environment variables overriding standard pip/uv repository settings:"
                .to_string(),
        );
        information.push("".to_string());
        information.extend(
            pip_uv_repository_envs
                .iter()
                .map(|(k, v)| format!("{k}: {v}")),
        );
    }

    let information = information.join("\n");

    let width = if pip_uv_repository_envs.is_empty() {
        40
    } else {
        80
    };
    show_box(&information, width).unwrap()
}

/*
fn command_settings(arguments: SettingsArguments) {
    match extract_default_settings(arguments.instance().clone(), arguments.folder().clone()) {
        Ok(settings) => {
            info!(
                "Tabsdata default settings replicated successfully at '{:?}'",
                settings
            );
        }
        Err(e) => {
            error!(
                "Failed to replicate Tabsdata default settings at '{}' - '{:?}': {}",
                arguments
                    .folder()
                    .as_ref()
                    .map_or("<default>".to_string(), |p| format!("{:?}", p)),
                arguments
                    .instance()
                    .as_ref()
                    .map_or("<default>".to_string(), |s| s.to_string()),
                e
            );
            exit(GeneralError.code())
        }
    };
}
 */

/*
fn command_profile(arguments: ProfileArguments) {
    match extract_profile(arguments.folder().join(arguments.name()), true) {
        Ok(_) => {
            info!(
                "Tabsdata profile '{}' at '{:?}' created successfully",
                arguments.folder().display(),
                arguments.name()
            );
        }
        Err(e) => {
            error!(
                "Failed to create Tabsdata profile '{}' at '{:?}': {}",
                arguments.folder().display(),
                arguments.name(),
                e
            );
            exit(GeneralError.code())
        }
    };
}
 */

fn command_banner(_arguments: BannerArguments) {
    show_banner()
}

fn command_license(_arguments: LicenseArguments) {
    show_license()
}

fn command_information(_arguments: InformationArguments) {
    show_information()
}

fn command_create(arguments: CreateArguments) {
    let supervisor_instance = get_instance_path_for_instance(arguments.instance());
    let supervisor_instance_absolute = to_absolute(&supervisor_instance.clone()).unwrap();

    let supervisor_repository =
        get_repository_path_for_instance(&Some(supervisor_instance_absolute.clone()));
    let supervisor_repository_absolute = to_absolute(&supervisor_repository.clone()).unwrap();

    let supervisor_workspace =
        get_workspace_path_for_instance(&Some(supervisor_instance_absolute.clone()));
    let supervisor_workspace_absolute = to_absolute(&supervisor_workspace.clone()).unwrap();

    let supervisor_config = supervisor_workspace.clone().join(CONFIG_FOLDER);
    let supervisor_config_absolute = to_absolute(&supervisor_config.clone()).unwrap();

    let supervisor_work = supervisor_workspace.clone().join(WORK_FOLDER);
    let supervisor_work_absolute = to_absolute(&supervisor_work.clone()).unwrap();

    if supervisor_instance_absolute.exists() {
        error!(
            "Instance folder '{}' already exists. Please use a non existing folder/instance.",
            supervisor_instance_absolute.clone().display(),
        );
        exit(GeneralError.code());
    }

    create_instance_folders(
        supervisor_instance_absolute.clone(),
        supervisor_repository_absolute.clone(),
        supervisor_workspace_absolute.clone(),
        supervisor_config_absolute.clone(),
        supervisor_work_absolute.clone(),
    );
    create_instance(arguments.instance(), arguments.profile());
    create_database(arguments.instance());
}

fn create_instance(instance: &Option<PathBuf>, profile: &Option<PathBuf>) {
    let supervisor_instance = get_instance_path_for_instance(instance);
    let supervisor_instance_absolute = to_absolute(&supervisor_instance.clone()).unwrap();

    let supervisor_repository =
        get_repository_path_for_instance(&Some(supervisor_instance_absolute.clone()));
    let supervisor_repository_absolute = to_absolute(&supervisor_repository.clone()).unwrap();

    let supervisor_workspace =
        get_workspace_path_for_instance(&Some(supervisor_instance_absolute.clone()));
    let supervisor_workspace_absolute = to_absolute(&supervisor_workspace.clone()).unwrap();

    let bootloader = name_program(&PathBuf::from(BOOTLOADER));
    let mut binary = Command::new(bootloader);
    if check_flag_env(TD_DETACHED_SUBPROCESSES) {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

            binary.creation_flags(CREATE_NO_WINDOW);
        }
    }
    let mut command = binary
        .arg(BOOTLOADER_ARGUMENT_INSTANCE)
        .arg(supervisor_instance_absolute.clone())
        .arg(BOOTLOADER_ARGUMENT_REPOSITORY)
        .arg(supervisor_repository_absolute.clone())
        .arg(BOOTLOADER_ARGUMENT_WORKSPACE)
        .arg(supervisor_workspace_absolute.clone());
    if let Some(profile) = profile.clone() {
        command = command
            .arg(BOOTLOADER_ARGUMENT_PROFILE)
            .arg(profile.clone());
    }
    let result = command.output();
    match result {
        Ok(output) => {
            if !output.status.success() {
                show_std_out_and_err(&output);
                error!(
                    "Bad exit code creating instance '{}': {}",
                    supervisor_instance_absolute.clone().display(),
                    output.status
                );
                exit(GeneralError.code())
            };
            info!(
                "Instance '{}' successfully created!",
                supervisor_instance_absolute.clone().display()
            );
        }
        Err(error) => {
            error!(
                "Error creating instance '{}' structure: {}",
                supervisor_instance_absolute.clone().display(),
                error
            );
            exit(GeneralError.code())
        }
    }
}

fn create_database(instance: &Option<PathBuf>) {
    let supervisor_instance = get_instance_path_for_instance(instance);
    let supervisor_instance_absolute = to_absolute(&supervisor_instance.clone()).unwrap();

    let supervisor_repository =
        get_repository_path_for_instance(&Some(supervisor_instance_absolute.clone()));
    let supervisor_repository_absolute = to_absolute(&supervisor_repository.clone()).unwrap();

    let supervisor_database = supervisor_repository_absolute
        .clone()
        .join(DATABASE_FOLDER)
        .join(DATABASE_FILE);
    let supervisor_database_absolute = to_absolute(&supervisor_database.clone()).unwrap();
    let supervisor_database_url = Url::from_file_path(supervisor_database_absolute.clone())
        .expect("Failed to convert database file path to file:// URL");

    let supervisor_storage = supervisor_repository_absolute.clone().join(STORAGE_FOLDER);
    let supervisor_storage_absolute = to_absolute(&supervisor_storage.clone()).unwrap();
    let supervisor_storage_url = Url::from_file_path(supervisor_storage_absolute.clone())
        .expect("Failed to convert storage folder path to file:// URL");

    let apiserver = name_program(&PathBuf::from(APISERVER));
    let mut binary = Command::new(apiserver);
    if check_flag_env(TD_DETACHED_SUBPROCESSES) {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

            binary.creation_flags(CREATE_NO_WINDOW);
        }
    }
    let command = binary
        .arg(APISERVER_ARGUMENT_DATABASE_URL)
        .arg(supervisor_database_url.clone().to_string())
        .arg(APISERVER_ARGUMENT_STORAGE_URL)
        .arg(supervisor_storage_url.clone().to_string())
        .arg(APISERVER_ARGUMENT_DB_SCHEMA)
        .arg(DbSchema::Create.to_string());
    let result = command.output();
    match result {
        Ok(output) => {
            if !output.status.success() {
                show_std_out_and_err(&output);
                error!(
                    "Bad exit code creating database '{}': {}",
                    supervisor_database_absolute.clone().display(),
                    output.status
                );
                exit(GeneralError.code())
            };
            info!(
                "Database '{}' successfully created!",
                supervisor_database_absolute.clone().display()
            );
        }
        Err(error) => {
            error!(
                "Error creating database '{}' structure: {}",
                supervisor_database_absolute.clone().display(),
                error
            );
            exit(GeneralError.code())
        }
    }
}

fn command_upgrade(arguments: UpgradeArguments) {
    let supervisor_instance = get_instance_path_for_instance(&arguments.instance.instance);
    let supervisor_instance_absolute = to_absolute(&supervisor_instance.clone()).unwrap();

    let supervisor_repository =
        get_repository_path_for_instance(&Some(supervisor_instance_absolute.clone()));
    let supervisor_repository_absolute = to_absolute(&supervisor_repository.clone()).unwrap();

    let supervisor_database = supervisor_repository_absolute
        .clone()
        .join(DATABASE_FOLDER)
        .join(DATABASE_FILE);
    let supervisor_database_absolute = to_absolute(&supervisor_database.clone()).unwrap();
    let supervisor_database_url = Url::from_file_path(supervisor_database_absolute.clone())
        .expect("Failed to convert database file path to file:// URL");

    let supervisor_storage = supervisor_repository_absolute.clone().join(STORAGE_FOLDER);
    let supervisor_storage_absolute = to_absolute(&supervisor_storage.clone()).unwrap();
    let supervisor_storage_url = Url::from_file_path(supervisor_storage_absolute.clone())
        .expect("Failed to convert storage folder path to file:// URL");

    let supervisor_workspace = get_workspace_path_for_instance(arguments.instance.instance());
    let supervisor_work = supervisor_workspace.clone().join(WORK_FOLDER);
    let supervisor_tracker = WorkerTracker::new(supervisor_work.clone());

    match supervisor_tracker.check_worker_status() {
        WorkerStatus::Running { pid } => {
            error!(
                "Tabsdata instance '{}' is running with pid {}. You need to stop it before upgrading",
                supervisor_instance_absolute.clone().display(),
                pid,
            );
            exit(GeneralError.code());
        }
        _ => {
            if !needs_upgrade(supervisor_instance_absolute.clone()) {
                info!(
                    "The instance '{}' is already up to date. No need to upgrade.",
                    supervisor_instance_absolute.clone().display()
                );
            } else {
                set_log_level(Level::ERROR);
                match upgrade(&supervisor_instance_absolute, true) {
                    Ok(_) => {
                        set_log_level(Level::INFO);
                        info!(
                            "Instance '{}' successfully upgraded!",
                            supervisor_instance_absolute.display(),
                        );
                    }
                    Err(e) => {
                        error!(
                            "Failed to upgrade instance '{}': {}",
                            supervisor_instance_absolute.display(),
                            e
                        );
                        exit(GeneralError.code());
                    }
                }
            }

            let apiserver = name_program(&PathBuf::from(APISERVER));
            let mut binary = Command::new(apiserver);
            if check_flag_env(TD_DETACHED_SUBPROCESSES) {
                #[cfg(windows)]
                {
                    use std::os::windows::process::CommandExt;
                    use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

                    binary.creation_flags(CREATE_NO_WINDOW);
                }
            }
            set_log_level(Level::ERROR);
            let command = binary
                .arg(APISERVER_ARGUMENT_DATABASE_URL)
                .arg(supervisor_database_url.clone().to_string())
                .arg(APISERVER_ARGUMENT_STORAGE_URL)
                .arg(supervisor_storage_url.clone().to_string())
                .arg(APISERVER_ARGUMENT_DB_SCHEMA)
                .arg(DbSchema::Upgrade.to_string());
            let result = command.output();
            match result {
                Ok(output) => {
                    if !output.status.success() {
                        if let Some(code) = output.status.code()
                            && code == NoAction.code()
                        {
                            set_log_level(Level::INFO);
                            info!(
                                "The database '{}' is already up to date. No need to upgrade.",
                                supervisor_database_absolute.clone().display()
                            );
                            exit(Success.code())
                        }
                        show_std_out_and_err(&output);
                        error!(
                            "Bad exit code upgrading database '{}': {}",
                            supervisor_database_absolute.clone().display(),
                            output.status
                        );
                        exit(GeneralError.code())
                    };
                    set_log_level(Level::INFO);
                    info!(
                        "Database '{}' successfully upgraded!",
                        supervisor_database_absolute.clone().display()
                    );
                }
                Err(error) => {
                    error!(
                        "Error upgrading database '{}' structure: {}",
                        supervisor_database_absolute.clone().display(),
                        error
                    );
                    exit(GeneralError.code())
                }
            }
        }
    };
    exit(Success.code());
}

fn command_delete(arguments: DeleteArguments) {
    let supervisor_instance = get_instance_path_for_instance(&arguments.instance.instance);
    let supervisor_instance_absolute = to_absolute(&supervisor_instance.clone()).unwrap();

    let supervisor_workspace = get_workspace_path_for_instance(arguments.instance.instance());
    let supervisor_work = supervisor_workspace.clone().join(WORK_FOLDER);

    eprintln!("Removing an instance will delete only the local instance resources.");
    eprintln!("Cloud storage and other external resources will be left unmodified.");
    eprintln!("Removing an instance cannot be undone.");

    if !arguments.force {
        eprintln!("Please, confirm with 'yes' to continue...");
        eprint!(">>> ");
        io::stdout().flush().unwrap();
        let mut confirmation = String::new();
        io::stdin().read_line(&mut confirmation).unwrap();
        if confirmation.trim() != "yes" {
            eprintln!("Cancelling operation");
            exit(NoAction.code());
        }
    }

    if !supervisor_instance_absolute.exists() {
        error!(
            "Instance folder '{}' does no exist. Skipping.",
            supervisor_instance_absolute.clone().display(),
        );
        exit(NoAction.code());
    }

    let supervisor_tracker = WorkerTracker::new(supervisor_work.clone());
    match supervisor_tracker.check_worker_status() {
        WorkerStatus::Running { pid } => {
            error!(
                "Tabsdata instance '{}' is running with pid {}. You need to stop it before deleting it.",
                supervisor_instance_absolute.clone().display(),
                pid,
            );
            exit(GeneralError.code());
        }
        _ => {
            fs::remove_dir_all(&supervisor_instance_absolute).unwrap_or_else(|e| {
                error!(
                    "Unexpected error deleting instance '{}': {}",
                    supervisor_instance_absolute.display(),
                    e
                );
                exit(GeneralError.code())
            });
            info!(
                "Instance '{}' deleted successfully!",
                supervisor_instance_absolute.display()
            );
        }
    };
    exit(Success.code());
}

fn command_start(arguments: StartArguments) {
    show_setup_and_launch();

    let supervisor_instance = get_instance_path_for_instance(arguments.instance());
    let supervisor_instance_absolute = to_absolute(&supervisor_instance.clone()).unwrap();

    if needs_upgrade(supervisor_instance_absolute.clone()) {
        warn!("The instance is not up to date. An upgrade is required before starting.");
        warn!("To upgrade, run: 'tdserver upgrade --instance <instance>'.");
        warn!("(or just 'tdserver upgrade' to upgrade the default instance.)");
        warn!("For a dry run before upgrading, use: 'tdserver upgrade --instance <instance>'.");
        warn!("(or just 'tdserver upgrade' for the default instance.)");
        warn!(
            "It is strongly recommended to back up your instance before proceeding with the upgrade."
        );

        exit(GeneralError.code())
    }

    let supervisor_repository =
        get_repository_path_for_instance(&Some(supervisor_instance_absolute.clone()));
    let supervisor_repository_absolute = to_absolute(&supervisor_repository.clone()).unwrap();

    let supervisor_workspace =
        get_workspace_path_for_instance(&Some(supervisor_instance_absolute.clone()));
    let supervisor_workspace_absolute = to_absolute(&supervisor_workspace.clone()).unwrap();

    let supervisor_config = supervisor_workspace.clone().join(CONFIG_FOLDER);
    let supervisor_config_absolute = to_absolute(&supervisor_config.clone()).unwrap();

    let supervisor_work = supervisor_workspace.clone().join(WORK_FOLDER);
    let supervisor_work_absolute = to_absolute(&supervisor_work.clone()).unwrap();

    let forwarded_parameters = forward_parameters(
        arguments.arguments.clone(),
        None,
        &supervisor_instance_absolute,
        &supervisor_repository_absolute,
        &supervisor_workspace_absolute,
    );

    if arguments.existing && !supervisor_instance_absolute.exists() {
        error!(
            "Instance folder '{}' does not exists. You need to create the instance before starting it.",
            supervisor_instance_absolute.clone().display(),
        );
        exit(GeneralError.code());
    }

    let supervisor_tracker = WorkerTracker::new(supervisor_work.clone());
    match supervisor_tracker.check_worker_status() {
        WorkerStatus::Running { pid } => {
            warn!(
                "Tabsdata instance '{}' already running with pid '{}'",
                supervisor_instance_absolute.clone().display(),
                pid
            );
        }
        _ => {
            if !supervisor_instance_absolute.exists() {
                create_instance_folders(
                    supervisor_instance_absolute.clone(),
                    supervisor_repository_absolute.clone(),
                    supervisor_workspace_absolute.clone(),
                    supervisor_config_absolute.clone(),
                    supervisor_work_absolute.clone(),
                );
                create_instance(arguments.instance(), &None);
                create_database(arguments.instance());
            }

            let describer = build_instance_describer(
                forwarded_parameters,
                supervisor_config_absolute,
                supervisor_work_absolute,
            )
            .unwrap();

            prepare(&supervisor_instance, false);

            set_log_level(Level::ERROR);
            match TabsDataWorker::new(describer.clone()).work(None, true) {
                Ok((worker, _out, _err)) => {
                    if arguments.no_wait {
                        set_log_level(Level::INFO);
                        info!(
                            "Tabsdata instance '{}' launched with pid '{:?}'",
                            supervisor_instance_absolute.clone().display(),
                            worker.id().unwrap()
                        );
                        info!("The instance may take some additional time to fully start.");
                        info!(
                            "Startup time can vary depending on whether this is the first launch."
                        );
                        info!(
                            "Initial launches create the base Python virtual environment, which can slow down startup."
                        );
                    } else {
                        set_log_level(Level::INFO);
                        info!("Waiting for the instance to complete startup.");
                        info!("Use the '--no-wait' flag to skip waiting and proceed immediately.");
                        if wait(supervisor_work) {
                            set_log_level(Level::INFO);
                            info!(
                                "Tabsdata instance '{}' started with pid '{:?}'",
                                supervisor_instance_absolute.clone().display(),
                                worker.id().unwrap()
                            );
                        } else {
                            exit(GeneralError.code())
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to run the Tabsdata instance: {}", e);
                    exit(GeneralError.code())
                }
            };
            set_log_level(Level::INFO);
        }
    }
}

fn command_restart(arguments: RestartArguments) {
    let start_arguments = arguments.start.clone();
    let stop_arguments = StopArguments {
        control: ControlArguments {
            instance: arguments.start.instance,
        },
        options: arguments.options,
    };
    command_stop(stop_arguments);
    command_start(start_arguments);
}

fn command_stop(arguments: StopArguments) {
    let supervisor_instance = get_instance_path_for_instance(arguments.control.instance());
    let supervisor_instance_absolute = to_absolute(&supervisor_instance.clone()).unwrap();

    let supervisor_workspace =
        get_workspace_path_for_instance(&arguments.control.instance().clone());
    let supervisor_work = supervisor_workspace.clone().join(WORK_FOLDER);

    let supervisor_tracker = WorkerTracker::new(supervisor_work.clone());
    match supervisor_tracker.check_worker_status() {
        WorkerStatus::Running { pid } => {
            terminate_process(
                pid,
                if arguments.options.force {
                    Signal::Kill
                } else {
                    Signal::Term
                },
            )
            .unwrap();
            let start = Instant::now();
            loop {
                if start.elapsed() > STOP_TIMEOUT {
                    error!(
                        "Failed to stop Tabsdata instance '{}' with pid {} after {:?} seconds",
                        supervisor_instance_absolute.clone().display(),
                        pid,
                        STOP_TIMEOUT
                    );
                    exit(GeneralError.code());
                }
                match supervisor_tracker.check_worker_status() {
                    WorkerStatus::Running { .. } => {
                        info!(
                            "Waiting for Tabsdata instance '{}' with pid {} to stop...",
                            supervisor_instance_absolute.clone().display(),
                            pid
                        );
                        std::thread::sleep(STOP_WAIT);
                    }
                    _ => {
                        break;
                    }
                }
            }
            info!(
                "Tabs Data instance '{}' with pid '{}' stopped",
                supervisor_instance_absolute.clone().display(),
                pid
            );
        }
        other => {
            warn!(
                "Tabsdata instance '{}' not running: '{:?}'",
                supervisor_instance_absolute.clone().display(),
                other
            )
        }
    }
}

fn command_status(arguments: StatusArguments) {
    let supervisor_instance = get_instance_path_for_instance(&arguments.control.instance().clone());
    let supervisor_workspace =
        get_workspace_path_for_instance(&arguments.control.instance().clone());
    let supervisor_work = supervisor_workspace.clone().join(WORK_FOLDER);

    let theme = Style::modern()
        .horizontals([(1, HorizontalLine::inherit(Style::modern()))])
        .verticals([(1, VerticalLine::inherit(Style::modern()))])
        .remove_horizontal();

    let mut status = String::new();

    match status_processes(supervisor_work) {
        (WorkerStatus::Running { pid }, Some(workers)) => {
            let tabled_workers: Vec<WorkerRow> = workers
                .into_iter()
                .flat_map(
                    |(
                        pid,
                        parent_pid,
                        name,
                        program,
                        cwd,
                        cpu,
                        physical_memory,
                        virtual_memory,
                        collection,
                        function,
                        worker,
                        attempt,
                    )| {
                        let exe_row = WorkerRow::new(
                            pid,
                            parent_pid,
                            name.clone(),
                            collection.clone(),
                            function.clone(),
                            worker.clone(),
                            attempt.clone(),
                            cpu,
                            physical_memory,
                            virtual_memory,
                            program.clone(),
                        );
                        let cwd_row = WorkerRow::cwd(cwd);
                        vec![exe_row, cwd_row]
                    },
                )
                .collect();

            let mut table = Table::new(tabled_workers);

            table
                .with((theme.clone(), Alignment::left()))
                .with(Modify::new(Columns::one(0)).with(Alignment::right()))
                .with(Modify::new(Columns::one(1)).with(Alignment::right()))
                .with(Modify::new(Columns::one(6)).with(Alignment::right()))
                .with(Modify::new(Columns::one(7)).with(Alignment::right()))
                .with(Modify::new(Columns::one(8)).with(Alignment::right()))
                .with(Modify::new(Columns::one(9)).with(Alignment::right()));

            status.push_str(&format!(
                "Workers and its sub-workers of instance '{}' - '{}':\n{}",
                pid,
                supervisor_instance.display(),
                table
            ));
        }
        other => {
            status.push_str(&format!(
                "Tabsdata instance '{}' not running: '{:?}'",
                supervisor_instance.clone().display(),
                other
            ));
        }
    }

    if arguments.options.metrics {
        let space_folders = status_space(supervisor_instance);
        let tabled_space: Vec<SpaceRow> = space_folders
            .into_iter()
            .map(|(name, (path, _, human))| SpaceRow {
                name,
                human,
                path: path.display().to_string(),
            })
            .collect();
        if !tabled_space.is_empty() {
            let mut table = Table::new(tabled_space);
            table
                .with((theme.clone(), Alignment::left()))
                .with(Modify::new(Columns::one(1)).with(Alignment::right()));

            status.push_str(&format!("\nRelevant folders disk usage:\n{table}"));
        }
    }
    info!("{status}");
}

fn status_processes(supervisor_work: PathBuf) -> (WorkerStatus, Option<Vec<ProcessDistilled>>) {
    let supervisor_tracker = WorkerTracker::new(supervisor_work.clone());
    match supervisor_tracker.check_worker_status() {
        WorkerStatus::Running { pid } => {
            (WorkerStatus::Running { pid }, Some(get_process_tree(pid)))
        }
        other => (other, None),
    }
}

fn status_space(instance: PathBuf) -> IndexMap<String, (PathBuf, u64, String)> {
    instance_space(&instance)
}

fn wait(supervisor_work: PathBuf) -> bool {
    let start_time = Instant::now();
    loop {
        let (status, tree) = status_processes(supervisor_work.clone());
        if let WorkerStatus::Running { .. } = status
            && let Some(children) = &tree
            && children
                .iter()
                .any(|(_, _, name, ..)| name.trim().trim_matches('"').contains(APISERVER))
        {
            return true;
        }
        if Instant::now().duration_since(start_time) >= START_TIMEOUT {
            info!(
                "The supervisor hasn't started after {}. Exiting.",
                format_duration(START_TIMEOUT)
            );
            return false;
        }

        let elapsed = Instant::now().duration_since(start_time);
        info!(
            "The supervisor hasn’t started after {} out of {}. Waiting…",
            format_duration(Duration::from_secs(elapsed.as_secs())),
            format_duration(START_TIMEOUT)
        );

        sleep(START_WAIT);
    }
}

async fn command_log(arguments: ControlArguments) {
    let supervisor_workspace = get_workspace_path_for_instance(&arguments.instance().clone());

    let mut lines = match MuxedLines::new() {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to create the tail processor: {:?}", e);
            exit(GeneralError.code());
        }
    };

    let mut known_files: HashSet<PathBuf> = HashSet::new();

    async fn scan_for_new_log_files(
        workspace: &PathBuf,
        known_files: &mut HashSet<PathBuf>,
        lines: &mut MuxedLines,
    ) {
        for entry in WalkDir::new(workspace).into_iter().filter_map(|e| e.ok()) {
            let entry_path = entry.path().to_path_buf();
            if entry_path.is_file()
                && entry_path.extension().and_then(|ext| ext.to_str()) == Some("log")
                && !known_files.contains(&entry_path)
            {
                match lines.add_file(&entry_path).await {
                    Ok(_) => {
                        info!("Watching new log file {:?}", entry_path);
                        known_files.insert(entry_path);
                    }
                    Err(e) => {
                        error!(
                            "Failed to add watcher for log file {:?}: {:?}",
                            entry_path, e
                        );
                    }
                };
            }
        }
    }

    scan_for_new_log_files(&supervisor_workspace, &mut known_files, &mut lines).await;
    let mut scan_interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = scan_interval.tick() => {
                scan_for_new_log_files(&supervisor_workspace, &mut known_files, &mut lines).await;
            }
            result = lines.next_line() => {
                if let Ok(Some(line)) = result {
                    println!("Source: {} - Line: {}", line.source().display(), line.line());
                }
            }
        }
    }
}

fn needs_upgrade(instance: PathBuf) -> bool {
    if !instance.exists() {
        return false;
    }
    let source = match get_source_version(&instance) {
        Ok(version) => version,
        Err(err) => {
            error!("Failed to get source version: {}", err);
            exit(GeneralError.code());
        }
    };
    let target = match get_target_version() {
        Ok(version) => version,
        Err(err) => {
            error!("Failed to get target version: {}", err);
            exit(GeneralError.code());
        }
    };
    source < target
}

fn create_instance_folders(
    supervisor_instance: PathBuf,
    supervisor_repository: PathBuf,
    supervisor_workspace: PathBuf,
    supervisor_config: PathBuf,
    supervisor_work: PathBuf,
) {
    match create_dir_all(supervisor_instance.clone()) {
        Ok(_) => (),
        Err(e) => {
            error!(
                "Failed to create instance folder '{}' for Tabsdata instance: {}",
                supervisor_instance.clone().display(),
                e
            );
            exit(GeneralError.code());
        }
    }
    match create_dir_all(supervisor_repository.clone()) {
        Ok(_) => (),
        Err(e) => {
            error!(
                "Failed to create repository folder '{}' for Tabsdata instance: {}",
                supervisor_repository.clone().display(),
                e
            );
            exit(GeneralError.code());
        }
    }
    match create_dir_all(supervisor_workspace.clone()) {
        Ok(_) => (),
        Err(e) => {
            error!(
                "Failed to create workspace folder '{}' for Tabsdata instance: {}",
                supervisor_workspace.clone().display(),
                e
            );
            exit(GeneralError.code());
        }
    }
    match create_dir_all(supervisor_config.clone()) {
        Ok(_) => (),
        Err(e) => {
            error!(
                "Failed to create config folder '{}' for Tabsdata instance: {}",
                supervisor_config.clone().display(),
                e
            );
            exit(GeneralError.code());
        }
    }
    match create_dir_all(supervisor_work.clone()) {
        Ok(_) => (),
        Err(e) => {
            error!(
                "Failed to create work folder '{}' for Tabsdata instance: {}",
                supervisor_work.clone().display(),
                e
            );
            exit(GeneralError.code());
        }
    }
    match set_current_dir(supervisor_work.clone()) {
        Ok(_) => (),
        Err(e) => {
            error!(
                "Failed to set current folder '{}' for the Tabsdata instance: {}",
                supervisor_config.clone().display(),
                e
            );
            exit(GeneralError.code());
        }
    }
}

fn forward_parameters(
    arguments: Vec<String>,
    profile: Option<PathBuf>,
    instance: &Path,
    repository: &Path,
    workspace: &Path,
) -> Vec<String> {
    let mut arguments_map = parse_extra_arguments(arguments.clone()).unwrap();
    let common_extra_arguments = arguments_map
        .entry(TD_ARGUMENT_KEY.to_string())
        .or_default();
    forward_parameter(profile.clone(), Profile, common_extra_arguments);
    forward_parameter(
        Some(instance.to_path_buf()),
        Instance,
        common_extra_arguments,
    );
    forward_parameter(
        Some(repository.to_path_buf()),
        Repository,
        common_extra_arguments,
    );
    forward_parameter(
        Some(workspace.to_path_buf()),
        Workspace,
        common_extra_arguments,
    );

    let forward_arguments = &mut Vec::new();

    forward_argument(
        Some(to_absolute(&instance.to_path_buf()).unwrap()),
        Instance,
        forward_arguments,
    );
    forward_argument(
        Some(to_absolute(&repository.to_path_buf()).unwrap()),
        Repository,
        forward_arguments,
    );
    forward_argument(
        Some(to_absolute(&workspace.to_path_buf()).unwrap()),
        Workspace,
        forward_arguments,
    );
    forward_argument(profile.clone(), Profile, forward_arguments);
    forward_arguments.push(TRAILING_ARGUMENTS_PREFIX.to_string());
    for (key, value) in arguments_map {
        forward_arguments.push(ARGUMENT_PREFIX.to_string());
        forward_arguments.push(key.clone());
        for (sub_key, sub_value) in value {
            forward_arguments.push(format!("--{sub_key}"));
            forward_arguments.push(sub_value);
        }
    }
    forward_arguments.clone()
}

fn forward_parameter(
    value: Option<PathBuf>,
    key: InheritedArgumentKey,
    map: &mut HashMap<String, String>,
) {
    if let Some(path) = value {
        let path_str = path.as_os_str().to_string_lossy();
        let trimmed_path = path_str.trim();
        if path_str != trimmed_path {
            error!(
                "Paths cannot contain leading or trailing spaces: '{}'",
                path_str
            );
            exit(GeneralError.code())
        }
        if !trimmed_path.is_empty() {
            map.insert(
                key.as_ref().to_string(),
                to_absolute(&path).unwrap().to_string_lossy().to_string(),
            );
        }
    }
}

fn forward_argument(value: Option<PathBuf>, key: InheritedArgumentKey, vector: &mut Vec<String>) {
    if let Some(path) = value {
        let path_str = path.as_os_str().to_string_lossy();
        let trimmed_path = path_str.trim();
        if path_str != trimmed_path {
            error!(
                "Paths cannot contain leading or trailing spaces: '{}'",
                path_str
            );
            exit(GeneralError.code())
        }
        if !trimmed_path.is_empty() {
            vector.push(format!("--{}", key.as_ref()));
            vector.push(
                to_absolute(&path)
                    .unwrap()
                    .as_os_str()
                    .to_string_lossy()
                    .to_string(),
            );
        }
    }
}

fn build_instance_describer(
    forwarded_parameters: Vec<String>,
    supervisor_config: PathBuf,
    supervisor_work: PathBuf,
) -> Result<TabsDataWorkerDescriber, DescriberError> {
    let describer = TabsDataWorkerDescriberBuilder::default()
        .class(REGULAR)
        .name(SUPERVISOR.to_string())
        .location(Relative)
        .program(PathBuf::from(SUPERVISOR))
        .set_state(None)
        .get_states(vec![])
        .arguments(forwarded_parameters)
        .markers(vec![])
        .config(supervisor_config.clone())
        .work(supervisor_work.clone())
        .queue(supervisor_work.clone().join(MSG_FOLDER))
        .etc(supervisor_work.clone().join(ETC_FOLDER))
        .build();
    if describer.is_err() {
        error!(
            "Failed to create describer for the Tabsdata instance: {:?}",
            describer.err()
        );
        exit(GeneralError.code());
    };
    describer
}

fn command_clean(arguments: CleanArguments) {
    clean_envs(arguments.clone());
    clean_cache(arguments.clone());
    exit(Success.code());
}

fn clean_envs(arguments: CleanArguments) {
    eprintln!("Make sure there is no running instance before continuing.");
    eprintln!("Removing Tabsdata internal Python virtual environments cannot be undone.");

    if !arguments.force {
        eprintln!("Please, confirm with 'yes' to continue...");
        eprint!(">>> ");
        io::stdout().flush().unwrap();
        let mut confirmation = String::new();
        io::stdin().read_line(&mut confirmation).unwrap();
        if confirmation.trim() != "yes" {
            eprintln!("Cancelling operation");
            return;
        }
    }

    set_log_level(Level::INFO);
    info!("Removing all Tabsdata internal Python virtual environments...");

    let available_environments_folder = get_home_dir()
        .join(TABSDATA_HOME_DIR)
        .join(AVAILABLE_ENVIRONMENTS_FOLDER);
    if available_environments_folder.exists() {
        info!(
            "Deleting folder '{}'...",
            available_environments_folder.display()
        );
        fs::remove_dir_all(&available_environments_folder).unwrap_or_else(|e| {
            error!(
                "Unexpected error deleting folder '{}': {}",
                available_environments_folder.display(),
                e
            );
            exit(GeneralError.code())
        });
        info!(
            "Folder '{}' deleted successfully!",
            available_environments_folder.display()
        );
    } else {
        info!(
            "Folder '{}' does not exist; skipping.",
            available_environments_folder.display()
        );
    }

    let environments_folder = get_home_dir()
        .join(TABSDATA_HOME_DIR)
        .join(ENVIRONMENTS_FOLDER);
    if environments_folder.exists() {
        info!("Deleting folder '{}'...", environments_folder.display());
        fs::remove_dir_all(&environments_folder).unwrap_or_else(|e| {
            error!(
                "Unexpected error deleting folder '{}': {}",
                environments_folder.display(),
                e
            );
            exit(GeneralError.code())
        });
        info!(
            "Folder '{}' deleted successfully!",
            environments_folder.display()
        );
    } else {
        info!(
            "Folder '{}' does not exist; skipping.",
            environments_folder.display()
        );
    }

    info!("All Tabsdata internal Python virtual environments removed successfully!");
}

fn clean_cache(_: CleanArguments) {
    let mut ok: bool = true;
    let mut binary = Command::new("uv");
    if check_flag_env(TD_DETACHED_SUBPROCESSES) {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

            binary.creation_flags(CREATE_NO_WINDOW);
        }
    }
    let command = binary.arg("cache").arg("clean");
    let result = command.output();
    match result {
        Ok(output) => {
            show_std_out_and_err(&output);
        }
        Err(_) => {
            ok = false;
            error!("Error purging uv cache");
        }
    }
    let mut binary = Command::new("pip");
    if check_flag_env(TD_DETACHED_SUBPROCESSES) {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

            binary.creation_flags(CREATE_NO_WINDOW);
        }
    }
    let command = binary.arg("cache").arg("purge");
    let result = command.output();
    match result {
        Ok(output) => {
            show_std_out_and_err(&output);
        }
        Err(_) => {
            ok = false;
            error!("Error purging pip cache");
        }
    }
    if !ok {
        exit(GeneralError.code())
    }
}

/*
fn command_instances() {
    let start_time = Instant::now();
    let instances = Arc::new(Mutex::new(Vec::new()));
    let progress_bar = ProgressBar::new_spinner();
    progress_bar.set_style(
        ProgressStyle::default_spinner()
            .template("[{elapsed_precise}] {spinner} Instances found: {pos}")
            .expect("Invalid progress bar template")
            .tick_chars("|/-\\"),
    );
    progress_bar.enable_steady_tick(Duration::from_millis(100));
    let walker = WalkBuilder::new(ROOT)
        .hidden(false)
        .parents(false)
        .ignore(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .follow_links(false)
        .build_parallel();
    walker.run(|| {
        let token = TD_KEEP;
        let instances = Arc::clone(&instances);
        let progress_bar = progress_bar.clone();
        Box::new(move |entry| {
            if let Ok(dir_entry) = entry {
                if dir_entry.file_type().map(|t| t.is_file()).unwrap_or(false)
                    && dir_entry.file_name() == token
                {
                    if let Some(parent) = dir_entry.path().parent() {
                        instances.lock().unwrap().push(parent.to_path_buf());
                        progress_bar.inc(1);
                    }
                }
            }
            WalkState::Continue
        })
    });
    progress_bar.finish_and_clear();
    let instances = instances.lock().unwrap();
    if instances.is_empty() {
        info!("No Tabsdata instance found");
    } else {
        let tabled_instances: Vec<InstanceRow> = instances
            .iter()
            .map(|path| InstanceRow {
                path: path.display().to_string(),
            })
            .collect();

        let theme = Style::modern()
            .horizontals([(1, HorizontalLine::inherit(Style::modern()))])
            .remove_horizontal();
        let mut table = Table::new(tabled_instances);

        table.with((theme, Alignment::left()));

        info!(
            "Some Tabsdata instances found: {}:\n{}",
            instances.len(),
            table
        );
    }
    info!("Search completed in {:.2?} seconds", start_time.elapsed());
}
 */

#[derive(Tabled)]
struct WorkerRow {
    #[tabled(rename = "Process")]
    pid: String,
    #[tabled(rename = "Parent")]
    ppid: String,
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Collection")]
    collection: String,
    #[tabled(rename = "Function")]
    function: String,
    #[tabled(rename = "Worker")]
    worker: String,
    #[tabled(rename = "Attempt")]
    attempt: String,
    #[tabled(rename = "CPU")]
    cpu: String,
    #[tabled(rename = "Physical Memory")]
    p_memory: String,
    #[tabled(rename = "Virtual Memory")]
    v_memory: String,
    #[tabled(rename = "Program / Working Directory")]
    program: String,
}

impl WorkerRow {
    #[allow(clippy::too_many_arguments)]
    fn new(
        pid: sysinfo::Pid,
        ppid: sysinfo::Pid,
        name: String,
        collection: String,
        function: String,
        worker: String,
        attempt: String,
        cpu: i32,
        pmem: u64,
        vmem: u64,
        program: String,
    ) -> Self {
        Self {
            pid: format!("{}", pid.as_u32()),
            ppid: format!("{}", ppid.as_u32()),
            name: name.replace('"', ""),
            collection,
            function,
            worker,
            attempt,
            cpu: format!("{cpu}%"),
            p_memory: format!(
                "{} mb",
                (pmem / (1024 * 1024)).to_formatted_string(&Locale::en)
            ),
            v_memory: format!(
                "{} mb",
                (vmem / (1024 * 1024)).to_formatted_string(&Locale::en)
            ),
            program: program
                .strip_prefix('"')
                .and_then(|n| n.strip_suffix('"'))
                .unwrap_or(&program)
                .to_string(),
        }
    }

    fn cwd(cwd: String) -> Self {
        Self {
            pid: "".to_string(),
            ppid: "".to_string(),
            name: "".to_string(),
            collection: "".to_string(),
            function: "".to_string(),
            worker: "".to_string(),
            attempt: "".to_string(),
            cpu: "".to_string(),
            p_memory: "".to_string(),
            v_memory: "".to_string(),
            program: cwd
                .strip_prefix('"')
                .and_then(|n| n.strip_suffix('"'))
                .unwrap_or(&cwd)
                .to_string(),
        }
    }
}

#[derive(Debug, tabled::Tabled)]
struct SpaceRow {
    #[tabled(rename = "Category")]
    name: String,
    #[tabled(rename = "Size")]
    human: String,
    #[tabled(rename = "Path")]
    path: String,
}

/*
#[derive(Tabled)]
struct InstanceRow {
    #[tabled(rename = "Instance")]
    path: String,
}
 */

#[cfg(not(any(test, feature = "mock-env")))]
pub fn run_mode() -> String {
    "Production".to_string()
}

#[cfg(any(test, feature = "mock-env"))]
pub fn run_mode() -> String {
    "Development (mock-env feature enabled)".to_string()
}

#[cfg(not(any(test, feature = "mock-env")))]
pub fn is_dev_mode() -> bool {
    false
}

pub fn get_pip_uv_repository_envs() -> BTreeMap<String, String> {
    const PIP_UV_ENV_VARS: [&str; 11] = [
        "PIP_INDEX_URL",
        "PIP_PYPI_URL",
        "PIP_EXTRA_INDEX_URL",
        "PIP_NO_INDEX",
        "PIP_FIND_LINKS",
        "UV_DEFAULT_INDEX",
        "UV_INDEX",
        "UV_INDEX_STRATEGY",
        "UV_INDEX_URL",
        "UV_EXTRA_INDEX_URL",
        "UV_FIND_LINKS",
    ];

    let mut repository_envs = BTreeMap::new();
    for pip_uv_env_var in PIP_UV_ENV_VARS {
        if let Ok(pip_uv_env_value) = env::var(pip_uv_env_var) {
            repository_envs.insert(pip_uv_env_var.to_string(), pip_uv_env_value);
        }
    }

    repository_envs
}

pub fn show_setup_and_launch() {
    info!("Setting up and launching instance...")
}

pub fn show_std_out_and_err(output: &Output) {
    match String::from_utf8(output.clone().stdout) {
        Ok(output) => {
            if !output.trim().is_empty() {
                info!("\n\n{}", output);
            }
        }
        Err(e) => {
            error!("Error processing system standard output: {}", e);
        }
    };
    match String::from_utf8(output.clone().stderr) {
        Ok(output) => {
            if !output.trim().is_empty() {
                error!("\n\n{}", output);
            }
        }
        Err(e) => {
            error!("Error processing system standard error: {}", e);
        }
    };
}

pub async fn start() {
    TabsDataCli {}.run().await;
}
