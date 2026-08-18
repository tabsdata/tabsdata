//
// Copyright 2026 Tabsdata Inc.
//

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::venvs::list::{ListArguments, command_venv_list};
use crate::client::tdkserver::commands::venvs::update::{UpdateArguments, command_venv_update};
use crate::core::system::exit::CommandResult;

pub mod list;
pub mod update;

#[derive(Debug, Clone, Subcommand)]
pub enum VenvCommands {
    #[command(about = "Update the complementary packages of a tabsdata virtual environment")]
    Update(UpdateArguments),
    #[command(about = "List the tabsdata virtual environments")]
    List(ListArguments),
}

pub async fn command_venv(command: VenvCommands, globals: &GlobalArguments) -> CommandResult {
    match command {
        VenvCommands::Update(arguments) => command_venv_update(arguments, globals).await,
        VenvCommands::List(arguments) => command_venv_list(arguments, globals).await,
    }
}
