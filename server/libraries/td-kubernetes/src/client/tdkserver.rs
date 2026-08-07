//
// Copyright 2026 Tabsdata Inc.
//

pub mod arguments;
pub mod commands;
pub mod common;
pub mod render;
pub mod widgets;

use clap::{CommandFactory, Parser};

use crate::cerror;
use crate::client::tdkserver::arguments::{Arguments, Commands, GlobalArguments};
// use crate::client::tdkserver::commands::cluster::command_cluster;
// use crate::client::tdkserver::commands::config::command_config;
// use crate::client::tdkserver::commands::instance::command_instance;
// use crate::client::tdkserver::commands::secret::command_secret;
// use crate::client::tdkserver::commands::server::command_server;
use crate::client::tdkserver::commands::service::create::command_create;
use crate::client::tdkserver::commands::service::get::command_get;
use crate::client::tdkserver::commands::service::info::command_info;
use crate::client::tdkserver::commands::service::init::command_init;
use crate::client::tdkserver::commands::service::list::command_list;
use crate::client::tdkserver::commands::service::put::command_put;
use crate::client::tdkserver::commands::service::quickstart::command_quickstart;
// use crate::client::tdkserver::commands::service::register::command_register;
use crate::client::tdkserver::commands::service::restart::command_restart;
use crate::client::tdkserver::commands::service::start::command_start;
use crate::client::tdkserver::commands::service::status::command_status;
use crate::client::tdkserver::commands::service::stop::command_stop;
use crate::client::tdkserver::commands::service::trash::command_trash;
// use crate::client::tdkserver::commands::service::unregister::command_unregister;
use crate::client::tdkserver::commands::service::view::command_view;
use crate::client::tdkserver::commands::session::set::command_set;
use crate::client::tdkserver::commands::session::settings::command_settings;
use crate::client::tdkserver::commands::session::unset::command_unset;
// use crate::client::tdkserver::commands::session::command_session;
use crate::client::tdkserver::commands::shell::command_shell;
// use crate::client::tdkserver::commands::template::command_template;
use crate::core::system::exit::{CommandError, CommandResult, render_error};
use td_common::crypto::install_crypto_provider;
use td_common::status::ExitStatus;

pub struct TabsDataKubernetesClient;

impl TabsDataKubernetesClient {
    async fn run(&self) -> ExitStatus {
        let arguments = Arguments::parse();
        let globals = arguments.globals;
        let verbose = globals.verbose;
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
            None => {
                Arguments::command().print_help().ok();
                println!();
                Ok(())
            }
            Some(Commands::Quickstart(arguments)) => command_quickstart(arguments, &globals).await,
            Some(Commands::Init(arguments)) => command_init(arguments, &globals).await,
            Some(Commands::Create(arguments)) => command_create(arguments, &globals).await,
            Some(Commands::Start(arguments)) => command_start(arguments, &globals).await,
            Some(Commands::Stop(arguments)) => command_stop(arguments, &globals).await,
            Some(Commands::Restart(arguments)) => command_restart(arguments, &globals).await,
            Some(Commands::Status(arguments)) => command_status(arguments, &globals).await,
            Some(Commands::Info(arguments)) => command_info(arguments, &globals).await,
            // Some(Commands::Register(arguments)) => command_register(arguments, &globals).await,
            // Some(Commands::Unregister(arguments)) => {
            //     command_unregister(arguments, &globals).await
            // }
            Some(Commands::Get(arguments)) => command_get(arguments, &globals).await,
            Some(Commands::Put(arguments)) => command_put(arguments, &globals).await,
            Some(Commands::View(arguments)) => command_view(arguments, &globals).await,
            Some(Commands::Trash(arguments)) => command_trash(arguments, &globals).await,
            Some(Commands::Shell) => command_shell(&globals).await,
            Some(Commands::Set(arguments)) => command_set(arguments, &globals).await,
            Some(Commands::Unset(arguments)) => command_unset(arguments, &globals).await,
            Some(Commands::Settings(arguments)) => command_settings(arguments, &globals).await,
            Some(Commands::List) => command_list(&globals).await,
            // Some(Commands::Template(command)) => command_template(command, &globals).await,
            // Some(Commands::Cluster(command)) => command_cluster(command, &globals).await,
            // Some(Commands::Instance(command)) => command_instance(command, &globals).await,
            // Some(Commands::Server(command)) => command_server(command, &globals).await,
            // Some(Commands::Config(command)) => command_config(command, &globals).await,
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
        _ => match error.source() {
            Some(source) => cerror!("Command aborted: {}", render_error(source, verbose)),
            None => cerror!("Command aborted."),
        },
    }
}
