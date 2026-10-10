//
// Copyright 2026 Tabsdata Inc.
//

pub mod arguments;
pub mod commands;
pub mod common;
pub mod injector;
pub mod render;
pub mod widgets;

use crate::cerror;

use crate::base::constants::{KM_IO_LOG_PREFIX, LOG_FOLDER, TDKSERVER_LOG_FILE, VERBOSE_FLAG};
use crate::client::tdkserver::arguments::{Commands, GlobalArguments, parse_arguments};
use crate::client::tdkserver::widgets::{set_keep_progress, show_standout};
// use crate::client::tdkserver::commands::cluster::command_cluster;
// use crate::client::tdkserver::commands::instance::command_instance;
// use crate::client::tdkserver::commands::secret::command_secret;
// use crate::client::tdkserver::commands::server::command_server;
use crate::client::tdkserver::commands::about::command_about;
use crate::client::tdkserver::commands::banner::command_banner;
use crate::client::tdkserver::commands::config::command_config;
use crate::client::tdkserver::commands::help::{HelpArguments, command_help};
use crate::client::tdkserver::commands::service::assess::command_assess;
use crate::client::tdkserver::commands::service::create::command_create;
use crate::client::tdkserver::commands::service::delete::command_delete;
use crate::client::tdkserver::commands::service::info::command_info;
use crate::client::tdkserver::commands::service::init::command_init;
use crate::client::tdkserver::commands::service::inspect::command_inspect;
use crate::client::tdkserver::commands::service::list::command_list;
use crate::client::tdkserver::commands::service::quickstart::command_quickstart;
// use crate::client::tdkserver::commands::service::register::command_register;
use crate::client::tdkserver::commands::service::restart::command_restart;
use crate::client::tdkserver::commands::service::start::command_start;
use crate::client::tdkserver::commands::service::status::command_status;
use crate::client::tdkserver::commands::service::stop::command_stop;
use crate::client::tdkserver::commands::service::stubs::command_stubs;
use crate::client::tdkserver::commands::service::trash::command_trash;
use crate::client::tdkserver::commands::service::upgrade::command_upgrade;
// use crate::client::tdkserver::commands::service::unregister::command_unregister;
use crate::client::tdkserver::commands::session::set::command_set;
use crate::client::tdkserver::commands::session::settings::command_settings;
use crate::client::tdkserver::commands::session::unset::command_unset;
// use crate::client::tdkserver::commands::session::command_session;
use crate::client::tdkserver::commands::shell::command_shell;
use crate::client::tdkserver::commands::venvs::command_venv;
// use crate::client::tdkserver::commands::template::command_template;
use crate::core::system::exit::{CommandError, CommandResult, render_error};
use td_common::crypto::install_crypto_provider;
use td_common::env::TABSDATA_HOME_DIR;
use td_common::status::ExitStatus;

pub struct TabsDataKubernetesClient;

impl TabsDataKubernetesClient {
    async fn run(&self) -> ExitStatus {
        let arguments = parse_arguments();
        let globals = arguments.globals;
        let verbose = globals.verbose;
        set_keep_progress(globals.keep_progress);
        match self.dispatch(arguments.command, globals).await {
            Ok(()) => ExitStatus::Success,
            Err(error) => {
                let status = *error.status();
                report_command_termination(&error, verbose);
                status
            }
        }
    }

    async fn dispatch(&self, command: Option<Commands>, globals: GlobalArguments) -> CommandResult {
        match command {
            None => command_help(HelpArguments::summary(), &globals).await,
            Some(Commands::Help(arguments)) => command_help(arguments, &globals).await,
            Some(Commands::Banner) => command_banner(&globals).await,
            Some(Commands::About) => command_about(&globals).await,
            Some(Commands::Info(arguments)) => command_info(arguments, &globals).await,
            Some(Commands::Stubs(arguments)) => command_stubs(arguments, &globals).await,
            Some(Commands::Assess(arguments)) => command_assess(arguments, &globals).await,
            Some(Commands::Quickstart(arguments)) => command_quickstart(arguments, &globals).await,
            Some(Commands::Init(arguments)) => command_init(arguments, &globals).await,
            Some(Commands::Create(arguments)) => command_create(arguments, &globals).await,
            Some(Commands::Delete(arguments)) => command_delete(arguments, &globals).await,
            Some(Commands::Start(arguments)) => command_start(arguments, &globals).await,
            Some(Commands::Stop(arguments)) => command_stop(arguments, &globals).await,
            Some(Commands::Restart(arguments)) => command_restart(arguments, &globals).await,
            Some(Commands::Upgrade(arguments)) => command_upgrade(arguments, &globals).await,
            Some(Commands::Status(arguments)) => command_status(arguments, &globals).await,
            Some(Commands::Inspect(arguments)) => command_inspect(arguments, &globals).await,
            // Some(Commands::Register(arguments)) => command_register(arguments, &globals).await,
            // Some(Commands::Unregister(arguments)) => {
            //     command_unregister(arguments, &globals).await
            // }
            Some(Commands::Trash(arguments)) => command_trash(arguments, &globals).await,
            Some(Commands::Config(command)) => command_config(command, &globals).await,
            Some(Commands::Venv(command)) => command_venv(command, &globals).await,
            Some(Commands::Set(arguments)) => command_set(arguments, &globals).await,
            Some(Commands::Unset(arguments)) => command_unset(arguments, &globals).await,
            Some(Commands::Settings(arguments)) => command_settings(arguments, &globals).await,
            Some(Commands::List) => command_list(&globals).await,
            Some(Commands::Shell) => command_shell(&globals).await,
            // Some(Commands::Template(command)) => command_template(command, &globals).await,
            // Some(Commands::Cluster(command)) => command_cluster(command, &globals).await,
            // Some(Commands::Instance(command)) => command_instance(command, &globals).await,
            // Some(Commands::Server(command)) => command_server(command, &globals).await,
            // Some(Commands::Secret(command)) => command_secret(command, &globals).await,
            // Some(Commands::Session(command)) => command_session(command, &globals).await,
        }
    }
}

pub async fn start() {
    #[cfg(windows)]
    let _ = colored::control::set_virtual_terminal(true);

    install_crypto_provider();
    let status = TabsDataKubernetesClient.run().await;
    if status != ExitStatus::Success {
        std::process::exit(status.code());
    }
}

fn report_command_termination(error: &CommandError, verbose: bool) {
    match error.status() {
        ExitStatus::Cancelled => match error.source() {
            Some(source) => {
                tracing::info!("Operation cancelled: {}", render_error(source, verbose))
            }
            None => tracing::info!("Operation cancelled."),
        },
        ExitStatus::NoAction => {
            if let Some(source) = error.source() {
                tracing::info!("{}", render_error(source, verbose));
            }
        }
        _ => {
            match error.source() {
                Some(source) => cerror!("Command aborted: {}", render_error(source, verbose)),
                None => cerror!("Command aborted."),
            }
            show_failure_hint(verbose);
        }
    }
}

fn show_failure_hint(verbose: bool) {
    let mut lines = vec!["For more detail on this failure:".to_owned()];
    if !verbose {
        lines.push(format!("  Run the command again with {VERBOSE_FLAG}."));
    }
    lines.push(format!(
        "  Read {TDKSERVER_LOG_FILE} under {TABSDATA_HOME_DIR}."
    ));
    lines.push(format!(
        "  Read the {KM_IO_LOG_PREFIX} files in the instance {LOG_FOLDER} folder."
    ));
    show_standout(&lines);
}
