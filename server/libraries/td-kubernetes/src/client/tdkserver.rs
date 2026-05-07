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
use crate::exit::Outcome;
use crate::kubernetes::client::install_crypto_provider;

pub struct TabsDataKubernetesClient;

impl TabsDataKubernetesClient {
    async fn run(&self) -> Outcome {
        let arguments = Arguments::parse();
        match arguments.command {
            None => {
                Arguments::command().print_help().ok();
                println!();
                Ok(())
            }
            Some(Commands::Shell) => command_shell().await,
            Some(Commands::Create(arguments)) => command_create(arguments).await,
        }
    }
}

pub async fn start() {
    install_crypto_provider();
    if let Err(error) = TabsDataKubernetesClient.run().await {
        std::process::exit(error.status().code());
    }
}
