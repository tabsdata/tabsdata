//
// Copyright 2026 Tabs Data Inc.
//

pub mod arguments;
pub mod commands;
pub mod common;
pub mod widgets;

use clap::{CommandFactory, Parser};

use crate::client::tdkserver::arguments::{Arguments, Commands};
use crate::client::tdkserver::commands::create::command_create;
use crate::client::tdkserver::commands::shell::command_shell;
use crate::client::tdkserver::commands::start::command_start;
use crate::client::tdkserver::commands::stop::command_stop;
use crate::client::tdkserver::commands::unuse::command_unuse;
use crate::client::tdkserver::commands::r#use::command_use;
use crate::client::tdkserver::commands::using::command_using;
use crate::cerror;
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
            Some(Commands::Shell) => command_shell(&globals).await,
            Some(Commands::Create(arguments)) => command_create(arguments, &globals).await,
            Some(Commands::Use(arguments)) => command_use(arguments, &globals).await,
            Some(Commands::Using(arguments)) => command_using(arguments).await,
            Some(Commands::Unuse(arguments)) => command_unuse(arguments).await,
            Some(Commands::Start(arguments)) => command_start(arguments, &globals).await,
            Some(Commands::Stop(arguments)) => command_stop(arguments, &globals).await,
        }
    }
}

pub async fn start() {
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
