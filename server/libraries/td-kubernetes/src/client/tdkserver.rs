//
// Copyright 2026 Tabs Data Inc.
//

pub mod arguments;
pub mod commands;
pub mod common;
pub mod widgets;

use clap::{CommandFactory, Parser};

use crate::cerror;
use crate::client::tdkserver::arguments::{Arguments, Commands};
use crate::client::tdkserver::commands::config::command_config;
use crate::client::tdkserver::commands::secret::command_secret;
use crate::client::tdkserver::commands::server::command_server;
use crate::client::tdkserver::commands::session::command_session;
use crate::client::tdkserver::commands::shell::command_shell;
use crate::core::system::exit::{CommandError, Result};
use crate::kube::api::client::install_crypto_provider;
use td_common::status::ExitStatus;

pub struct TabsDataKubernetesClient;

impl TabsDataKubernetesClient {
    async fn run(&self) -> Result {
        let arguments = Arguments::parse();
        let globals = arguments.globals;
        match arguments.command {
            None => {
                Arguments::command().print_help().ok();
                println!();
                Ok(())
            }
            Some(Commands::Config(command)) => command_config(command, &globals).await,
            Some(Commands::Secret(command)) => command_secret(command, &globals).await,
            Some(Commands::Server(command)) => command_server(command, &globals).await,
            Some(Commands::Session(command)) => command_session(command, &globals).await,
            Some(Commands::Shell) => command_shell(&globals).await,
        }
    }
}

pub async fn start() {
    #[cfg(windows)]
    let _ = colored::control::set_virtual_terminal(true);

    install_crypto_provider();
    if let Err(error) = TabsDataKubernetesClient.run().await {
        report_command_termination(&error);
        std::process::exit(error.status().code());
    }
}

fn report_command_termination(error: &CommandError) {
    match error.status() {
        ExitStatus::Cancelled => tracing::info!("Operation cancelled."),
        ExitStatus::NoAction => {}
        _ => cerror!("Command aborted."),
    }
}
