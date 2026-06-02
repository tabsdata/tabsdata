//
// Copyright 2026 Tabs Data Inc.
//

pub mod create;
pub mod restart;
pub mod start;
pub mod stop;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::server::create::{CreateArguments, command_create};
use crate::client::tdkserver::commands::server::restart::{RestartArguments, command_restart};
use crate::client::tdkserver::commands::server::start::{StartArguments, command_start};
use crate::client::tdkserver::commands::server::stop::{StopArguments, command_stop};
use crate::core::system::exit::Result;

#[derive(Debug, Clone, Subcommand)]
pub enum ServerCommands {
    #[command(about = "Create a tabsdata instance")]
    Create(CreateArguments),
    #[command(about = "Restart a tabsdata instance")]
    Restart(RestartArguments),
    #[command(about = "Start a tabsdata instance")]
    Start(StartArguments),
    #[command(about = "Stop a tabsdata instance")]
    Stop(StopArguments),
}

pub async fn command_server(command: ServerCommands, globals: &GlobalArguments) -> Result {
    match command {
        ServerCommands::Create(arguments) => command_create(arguments, globals).await,
        ServerCommands::Restart(arguments) => command_restart(arguments, globals).await,
        ServerCommands::Start(arguments) => command_start(arguments, globals).await,
        ServerCommands::Stop(arguments) => command_stop(arguments, globals).await,
    }
}
