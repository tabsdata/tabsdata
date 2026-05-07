//
// Copyright 2026 Tabs Data Inc.
//

mod arguments;
mod common;
mod create;
mod shell;
pub mod widgets;

use clap::{CommandFactory, Parser};

use crate::client::tdkserver::arguments::{Arguments, Commands};
use crate::client::tdkserver::create::command_create;
use crate::client::tdkserver::shell::command_shell;
use crate::kubernetes::client::install_crypto_provider;

pub struct TabsDataKubernetesClient;

impl TabsDataKubernetesClient {
    async fn run(&self) {
        let arguments = Arguments::parse();
        match arguments.command {
            None => {
                Arguments::command().print_help().ok();
                println!();
            }
            Some(Commands::Shell) => command_shell().await,
            Some(Commands::Create(arguments)) => command_create(arguments).await,
        }
    }
}

pub async fn start() {
    install_crypto_provider();
    TabsDataKubernetesClient.run().await;
}
