// Copyright 2024 Tabs Data Inc.
//

use crate::bin::supervisor::WorkerLocation::RELATIVE;
use crate::bin::supervisor::TD_ARGUMENT_KEY;
use crate::logic::platform::component::argument::InheritedArgumentKey;
use crate::logic::platform::component::argument::InheritedArgumentKey::*;
use crate::logic::platform::component::describer::TabsDataWorkerDescriberBuilder;
use crate::logic::platform::component::tracker::{WorkerStatus, WorkerTracker};
use crate::logic::platform::launch::worker::{TabsDataWorker, Worker};
use crate::logic::platform::resource::instance::{
    get_instance_path_for_instance, get_repository_path_for_instance,
    get_workspace_path_for_instance, CONFIG_FOLDER, MSG_FOLDER, WORK_FOLDER,
};
use crate::logic::platform::resource::settings::{extract_default_settings, extract_profile};
use clap::{command, Parser};
use clap_derive::{Args, Subcommand};
use getset::Getters;
use ignore::{WalkBuilder, WalkState};
use indicatif::{ProgressBar, ProgressStyle};
use linemux::MuxedLines;
use num_format::{Locale, ToFormattedString};
use std::collections::{HashMap, HashSet};
use std::env::set_current_dir;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::{env, io};
use sysinfo::Signal;
use ta_tableframe::api::Extension;
use tabled::{
    settings::{
        object::Columns,
        style::{HorizontalLine, Style, VerticalLine},
        Alignment, Modify,
    },
    Table, Tabled,
};
use td_common::cli::{parse_extra_arguments, ARGUMENT_PREFIX, TRAILING_ARGUMENTS_PREFIX};
use td_common::env::to_absolute;
use td_common::files::ROOT;
use td_common::os::{get_process_tree, terminate_process};
use td_common::status::ExitStatus::{GeneralError, Success};
use td_python::upgrade::{get_source_version, get_target_version, upgrade};
use td_python::venv::prepare;
use te_tableframe::engine::TableFrameExtension;
use thiserror::Error;
use tokio::time::{Duration, Instant};
use tracing::{error, info, warn};
use walkdir::WalkDir;

pub const SUPERVISOR: &str = "supervisor";

pub const TD_KEEP: &str = ".tdkeep";

const STOP_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const STOP_WAIT: Duration = Duration::from_secs(1);

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
    name = "Tabsdata Server CLI",
    version = "0.1.0",
    about = "Tabsdata Server Command Line Interface",
    long_about = "Any Tabsdata instance can be managed with the available commands of this tool. \
                  These commands rely on file pid to control the state of any instance."
)]
struct Arguments {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, Subcommand)]
enum Commands {
    #[command(about = "Create a Tabsdata profile based on the product defaults)")]
    Profile(ProfileArguments),

    #[command(about = "Create a Tabsdata settings based on the product defaults)")]
    Settings(SettingsArguments),

    #[command(about = "Upgrade a Tabsdata instance (with optional additional arguments)")]
    Upgrade(UpgradeArguments),

    #[command(about = "Start a Tabsdata instance (with optional additional arguments)")]
    Start(StartArguments),

    #[command(about = "Restart (stopping gracefully) a Tabsdata instance")]
    Restart(RestartArguments),

    #[command(about = "Stop (graceful) a Tabsdata instance")]
    Stop(StopArguments),

    #[command(about = "Get the status of a Tabsdata instance")]
    Status(ControlArguments),

    #[command(about = "Tail the logs of a Tabsdata instance")]
    Log(ControlArguments),

    #[command(about = "Find Tabsdata instances in the system")]
    Instances,
}

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
struct UpgradeOptionsArguments {
    /// Option to perform actual upgrade instead of performing a dry run.
    #[arg(
        long,
        name = "execute",
        long_help = "Option to perform actual upgrade."
    )]
    execute: bool,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct UpgradeArguments {
    #[command(flatten)]
    instance: InstanceArguments,

    #[command(flatten)]
    options: UpgradeOptionsArguments,
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

    /// Folder containing the instance's transient data.
    #[arg(
        long,
        name = "workspace",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Folder containing the instance's transient data. \
                     If unspecified, the subfolder 'workspace' inside the instance's folder will be used."
    )]
    workspace: Option<PathBuf>,
}

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
struct StartArguments {
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

    /// Folder containing the instance's persistent data.
    #[arg(
        long,
        name = "repository",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Folder containing the instance's persistent data. \
                     If unspecified, the subfolder 'repository' inside the instance folder will be used."
    )]
    repository: Option<PathBuf>,

    /// Folder containing the instance's transient data.
    #[arg(
        long,
        name = "workspace",
        required = false,
        value_parser = clap::value_parser!(PathBuf),
        long_help = "Folder containing the instance's transient data. \
                     If unspecified, the subfolder 'workspace' inside the instance folder will be used."
    )]
    workspace: Option<PathBuf>,

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

pub struct TabsDataCli {}

impl TabsDataCli {
    async fn run(&self) {
        let arguments = Arguments::parse();
        match arguments.command {
            Commands::Profile(arguments) => {
                command_profile(arguments);
            }
            Commands::Settings(arguments) => {
                command_settings(arguments);
            }
            Commands::Upgrade(arguments) => {
                command_upgrade(arguments);
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
            Commands::Instances => {
                command_instances();
            }
        }
    }
}

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

fn command_upgrade(arguments: UpgradeArguments) {
    let supervisor_instance = get_instance_path_for_instance(&arguments.instance.instance);
    let supervisor_instance_absolute = to_absolute(&supervisor_instance.clone()).unwrap();

    if !needs_upgrade(supervisor_instance_absolute.clone()) {
        info!("The instance is already up to date. No need to upgrade.");
        exit(Success.code())
    }

    let supervisor_workspace =
        get_workspace_path_for_instance(&None, arguments.instance.instance());
    let supervisor_work = supervisor_workspace.clone().join(WORK_FOLDER);
    let supervisor_tracker = WorkerTracker::new(supervisor_work.clone());
    match supervisor_tracker.check_worker_status() {
        WorkerStatus::Running { pid } => {
            error!(
                "Tabsdata instance '{}' is running with pid {}. You need to stop it before upgrading",
                supervisor_workspace.clone().display(),
                pid,
            );
            exit(GeneralError.code());
        }
        _ => match upgrade(&supervisor_instance_absolute, arguments.options.execute) {
            Ok(_) => (),
            Err(e) => {
                error!(
                    "Failed to upgrade instance '{}: {}",
                    supervisor_instance_absolute.display(),
                    e
                );
                exit(GeneralError.code());
            }
        },
    };
}

fn command_start(arguments: StartArguments) {
    show_mode();
    show_pip_uv_repository_mode();
    show_setup_and_launch();

    let supervisor_instance = get_instance_path_for_instance(arguments.instance());
    let supervisor_instance_absolute = to_absolute(&supervisor_instance.clone()).unwrap();

    if needs_upgrade(supervisor_instance_absolute.clone()) {
        warn!("The instance is not up to date. An upgrade is required before starting.");
        warn!("To upgrade, run: 'tdserver upgrade --instance <instance> --execute'.");
        warn!("(or just 'tdserver upgrade --execute' to upgrade the default instance.)");
        warn!("For a dry run before upgrading, use: 'tdserver upgrade --instance <instance>'.");
        warn!("(or just 'tdserver upgrade' for the default instance.)");
        warn!("It is strongly recommended to back up your instance before proceeding with the upgrade.");

        exit(GeneralError.code())
    }

    let supervisor_repository = get_repository_path_for_instance(
        arguments.repository(),
        &Some(supervisor_instance_absolute.clone()),
    );
    let supervisor_repository_absolute = to_absolute(&supervisor_repository.clone()).unwrap();

    let supervisor_workspace = get_workspace_path_for_instance(
        arguments.workspace(),
        &Some(supervisor_instance_absolute.clone()),
    );
    let supervisor_workspace_absolute = to_absolute(&supervisor_workspace.clone()).unwrap();

    let supervisor_config = supervisor_workspace.clone().join(CONFIG_FOLDER);
    let supervisor_config_absolute = to_absolute(&supervisor_config.clone()).unwrap();

    let supervisor_work = supervisor_workspace.clone().join(WORK_FOLDER);
    let supervisor_work_absolute = to_absolute(&supervisor_work.clone()).unwrap();

    let forwarded_parameters = forward_parameters(
        arguments,
        &supervisor_instance_absolute,
        &supervisor_repository_absolute,
        &supervisor_workspace_absolute,
    );

    let supervisor_tracker = WorkerTracker::new(supervisor_work.clone());
    match supervisor_tracker.check_worker_status() {
        WorkerStatus::Running { pid } => {
            warn!(
                "Tabsdata instance '{}' already running with pid '{}'",
                supervisor_workspace.clone().display(),
                pid
            );
        }
        _ => {
            match create_dir_all(supervisor_instance_absolute.clone()) {
                Ok(_) => (),
                Err(e) => {
                    error!(
                        "Failed to create instance folder '{}' for Tabsdata instance: {}",
                        supervisor_instance_absolute.clone().display(),
                        e
                    );
                    exit(GeneralError.code());
                }
            }
            match create_dir_all(supervisor_repository_absolute.clone()) {
                Ok(_) => (),
                Err(e) => {
                    error!(
                        "Failed to create repository folder '{}' for Tabsdata instance: {}",
                        supervisor_repository_absolute.clone().display(),
                        e
                    );
                    exit(GeneralError.code());
                }
            }
            match create_dir_all(supervisor_workspace_absolute.clone()) {
                Ok(_) => (),
                Err(e) => {
                    error!(
                        "Failed to create workspace folder '{}' for Tabsdata instance: {}",
                        supervisor_workspace_absolute.clone().display(),
                        e
                    );
                    exit(GeneralError.code());
                }
            }
            match create_dir_all(supervisor_config_absolute.clone()) {
                Ok(_) => (),
                Err(e) => {
                    error!(
                        "Failed to create config folder '{}' for Tabsdata instance: {}",
                        supervisor_config_absolute.clone().display(),
                        e
                    );
                    exit(GeneralError.code());
                }
            }
            match create_dir_all(supervisor_work_absolute.clone()) {
                Ok(_) => (),
                Err(e) => {
                    error!(
                        "Failed to create work folder '{}' for Tabsdata instance: {}",
                        supervisor_work_absolute.clone().display(),
                        e
                    );
                    exit(GeneralError.code());
                }
            }
            match set_current_dir(supervisor_work_absolute.clone()) {
                Ok(_) => (),
                Err(e) => {
                    error!(
                        "Failed to set current folder '{}' for the Tabsdata instance: {}",
                        supervisor_config_absolute.clone().display(),
                        e
                    );
                    exit(GeneralError.code());
                }
            }
            let describer = TabsDataWorkerDescriberBuilder::default()
                .name(SUPERVISOR.to_string())
                .location(RELATIVE)
                .program(PathBuf::from(SUPERVISOR))
                .arguments(forwarded_parameters)
                .config(supervisor_config_absolute.clone())
                .work(supervisor_work_absolute.clone())
                .queue(supervisor_work_absolute.clone().join(MSG_FOLDER))
                .build();
            if describer.is_err() {
                error!(
                    "Failed to create describer for the Tabsdata instance: {:?}",
                    describer.err()
                );
                exit(GeneralError.code());
            };
            let describer = describer.unwrap();

            prepare(&supervisor_instance);

            match TabsDataWorker::new(describer.clone()).work() {
                Ok(worker) => {
                    info!(
                        "Tabsdata instance '{}' started with pid '{:?}'",
                        supervisor_workspace.clone().display(),
                        worker.id()
                    );
                }
                Err(e) => {
                    error!("Failed to run the Tabsdata instance: {}", e);
                    exit(GeneralError.code())
                }
            };
        }
    }

    fn forward_parameters(
        arguments: StartArguments,
        instance: &Path,
        repository: &Path,
        workspace: &Path,
    ) -> Vec<String> {
        let mut arguments_map = parse_extra_arguments(arguments.arguments().clone()).unwrap();
        let common_extra_arguments = arguments_map
            .entry(TD_ARGUMENT_KEY.to_string())
            .or_default();
        forward_parameter(arguments.profile().clone(), Profile, common_extra_arguments);
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
        forward_argument(arguments.profile().clone(), Profile, forward_arguments);
        forward_arguments.push(TRAILING_ARGUMENTS_PREFIX.to_string());
        for (key, value) in arguments_map {
            forward_arguments.push(ARGUMENT_PREFIX.to_string());
            forward_arguments.push(key.clone());
            for (sub_key, sub_value) in value {
                forward_arguments.push(format!("--{}", sub_key));
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

    fn forward_argument(
        value: Option<PathBuf>,
        key: InheritedArgumentKey,
        vector: &mut Vec<String>,
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
}

fn command_restart(arguments: RestartArguments) {
    let start_arguments = arguments.start.clone();
    let stop_arguments = StopArguments {
        control: ControlArguments {
            instance: arguments.start.instance,
            workspace: arguments.start.workspace,
        },
        options: arguments.options,
    };
    command_stop(stop_arguments);
    command_start(start_arguments);
}

fn command_stop(arguments: StopArguments) {
    let supervisor_workspace = get_workspace_path_for_instance(
        arguments.control.workspace(),
        &arguments.control.instance().clone(),
    );
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
                        supervisor_workspace.clone().display(),
                        pid,
                        STOP_TIMEOUT
                    );
                    exit(GeneralError.code());
                }
                match supervisor_tracker.check_worker_status() {
                    WorkerStatus::Running { .. } => {
                        info!(
                            "Waiting for Tabsdata instance '{}' with pid {} to stop...",
                            supervisor_workspace.clone().display(),
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
                "Tabs Data workspace '{}' with pid '{}' stopped",
                supervisor_workspace.clone().display(),
                pid
            );
        }
        other => {
            warn!(
                "Tabsdata instance '{}' not running: '{:?}'",
                supervisor_workspace.clone().display(),
                other
            )
        }
    }
}

fn command_status(arguments: ControlArguments) {
    show_mode();
    show_pip_uv_repository_mode();
    let supervisor_workspace =
        get_workspace_path_for_instance(arguments.workspace(), &arguments.instance().clone());
    let supervisor_work = supervisor_workspace.clone().join(WORK_FOLDER);

    let supervisor_tracker = WorkerTracker::new(supervisor_work.clone());
    match supervisor_tracker.check_worker_status() {
        WorkerStatus::Running { pid } => {
            let workers = get_process_tree(pid);

            let tabled_workers: Vec<WorkerRow> = workers
                .into_iter()
                .map(
                    |(pid, parent_pid, name, program, physical_memory, virtual_memory)| {
                        WorkerRow::new(
                            pid,
                            parent_pid,
                            name,
                            program,
                            physical_memory,
                            virtual_memory,
                        )
                    },
                )
                .collect();

            let theme = Style::modern()
                .horizontals([(1, HorizontalLine::inherit(Style::modern()))])
                .verticals([(1, VerticalLine::inherit(Style::modern()))])
                .remove_horizontal();
            let mut table = Table::new(tabled_workers);

            table
                .with((theme, Alignment::left()))
                .with(Modify::new(Columns::single(0)).with(Alignment::right()))
                .with(Modify::new(Columns::single(1)).with(Alignment::right()))
                .with(Modify::new(Columns::single(4)).with(Alignment::right()))
                .with(Modify::new(Columns::single(5)).with(Alignment::right()));

            info!(
                "Workers '{}' and its sub-workers at workspace '{}':\n{}",
                pid,
                supervisor_workspace.clone().display(),
                table
            );
        }
        other => {
            warn!(
                "Tabsdata instance '{}' not running: '{:?}'",
                supervisor_workspace.clone().display(),
                other
            )
        }
    }
}

async fn command_log(arguments: ControlArguments) {
    let supervisor_workspace =
        get_workspace_path_for_instance(arguments.workspace(), &arguments.instance().clone());

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

#[derive(Tabled)]
struct WorkerRow {
    #[tabled(rename = "Process")]
    pid: u32,
    #[tabled(rename = "Parent")]
    ppid: u32,
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Program")]
    program: String,
    #[tabled(rename = "Physical Memory")]
    p_memory: String,
    #[tabled(rename = "Virtual Memory")]
    v_memory: String,
}

impl WorkerRow {
    fn new(
        pid: sysinfo::Pid,
        ppid: sysinfo::Pid,
        name: String,
        program: String,
        pmem: u64,
        vmem: u64,
    ) -> Self {
        Self {
            pid: pid.as_u32(),
            ppid: ppid.as_u32(),
            name: name
                .strip_prefix('"')
                .and_then(|n| n.strip_suffix('"'))
                .unwrap_or(&name)
                .to_string(),
            program: program
                .strip_prefix('"')
                .and_then(|n| n.strip_suffix('"'))
                .unwrap_or(&program)
                .to_string(),
            p_memory: format!(
                "{} mb",
                (pmem / (1024 * 1024)).to_formatted_string(&Locale::en)
            ),
            v_memory: format!(
                "{} mb",
                (vmem / (1024 * 1024)).to_formatted_string(&Locale::en)
            ),
        }
    }
}

#[derive(Tabled)]
struct InstanceRow {
    #[tabled(rename = "Instance")]
    path: String,
}

#[cfg(not(any(test, feature = "mock-env")))]
pub fn show_mode() {
    warn!(
        "Activated tabsdata {} in production mode",
        TableFrameExtension.edition()
    );
}

#[cfg(any(test, feature = "mock-env"))]
pub fn show_mode() {
    info!(
        "Activated tabsdata {} in development mode",
        TableFrameExtension.edition()
    );
}

pub fn show_pip_uv_repository_mode() {
    const PIP_ENV_VARS: [&str; 5] = [
        "PIP_INDEX_URL",
        "PIP_EXTRA_INDEX_URL",
        "UV_DEFAULT_INDEX",
        "UV_INDEX_URL",
        "UV_EXTRA_INDEX_URL",
    ];
    for env_var in PIP_ENV_VARS {
        if let Ok(env_value) = env::var(env_var) {
            warn!(
                "You are using a non-standard pip/uv setup: '{}' = '{}'",
                env_var, env_value
            );
        }
    }
}

pub fn show_setup_and_launch() {
    info!("Setting up and launching instance...")
}

pub async fn start() {
    TabsDataCli {}.run().await;
}
