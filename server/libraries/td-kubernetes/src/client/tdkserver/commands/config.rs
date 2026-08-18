//
// Copyright 2026 Tabsdata Inc.
//

pub mod check;
pub mod get;
pub mod put;
pub mod view;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::config::check::{CheckArguments, command_check};
use crate::client::tdkserver::commands::config::get::{GetArguments, command_get};
use crate::client::tdkserver::commands::config::put::{PutArguments, command_put};
use crate::client::tdkserver::commands::config::view::{ViewArguments, command_view};
use crate::core::system::exit::CommandResult;

#[derive(Debug, Clone, Subcommand)]
pub enum ConfigCommands {
    #[command(
        about = "Check the values still to be provided in the configs of a tabsdata instance"
    )]
    Check(CheckArguments),
    #[command(about = "Download the configmaps of a tabsdata instance for editing")]
    Get(GetArguments),
    #[command(about = "Replace the configmaps of a tabsdata instance from edited files")]
    Put(PutArguments),
    #[command(about = "View the configmaps of a tabsdata instance")]
    View(ViewArguments),
}

pub async fn command_config(command: ConfigCommands, globals: &GlobalArguments) -> CommandResult {
    match command {
        ConfigCommands::Check(arguments) => command_check(arguments, globals).await,
        ConfigCommands::Get(arguments) => command_get(arguments, globals).await,
        ConfigCommands::Put(arguments) => command_put(arguments, globals).await,
        ConfigCommands::View(arguments) => command_view(arguments, globals).await,
    }
}
