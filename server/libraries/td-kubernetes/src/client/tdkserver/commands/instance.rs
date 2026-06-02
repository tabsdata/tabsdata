//
// Copyright 2026 Tabs Data Inc.
//

pub mod restart;
pub mod start;
pub mod stop;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::instance::restart::{RestartArguments, command_restart};
use crate::client::tdkserver::commands::instance::start::{StartArguments, command_start};
use crate::client::tdkserver::commands::instance::stop::{StopArguments, command_stop};
use crate::core::system::exit::CommandResult;

#[derive(Debug, Clone, Subcommand)]
pub enum InstanceCommands {
    #[command(about = "Restart the tabsdata instance")]
    Restart(RestartArguments),
    #[command(about = "Start the tabsdata instance")]
    Start(StartArguments),
    #[command(about = "Stop the tabsdata instance")]
    Stop(StopArguments),
}

pub async fn command_instance(
    command: InstanceCommands,
    globals: &GlobalArguments,
) -> CommandResult {
    match command {
        InstanceCommands::Restart(arguments) => command_restart(arguments, globals).await,
        InstanceCommands::Start(arguments) => command_start(arguments, globals).await,
        InstanceCommands::Stop(arguments) => command_stop(arguments, globals).await,
    }
}
