//
// Copyright 2026 Tabsdata Inc.
//

pub mod set;
pub mod settings;
pub mod unset;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::session::set::{SetArguments, command_set};
use crate::client::tdkserver::commands::session::settings::{SettingsArguments, command_settings};
use crate::client::tdkserver::commands::session::unset::{UnpinArguments, command_unset};
use crate::core::system::exit::CommandResult;

#[derive(Debug, Clone, Subcommand)]
pub enum SessionCommands {
    #[command(about = "Set a session setting used when commands omit it")]
    Set(SetArguments),
    #[command(about = "Show the tabsdata session settings")]
    Settings(SettingsArguments),
    #[command(about = "Unset a session setting")]
    Unset(UnpinArguments),
}

pub async fn command_session(command: SessionCommands, globals: &GlobalArguments) -> CommandResult {
    match command {
        SessionCommands::Set(arguments) => command_set(arguments, globals).await,
        SessionCommands::Settings(arguments) => command_settings(arguments, globals).await,
        SessionCommands::Unset(arguments) => command_unset(arguments, globals).await,
    }
}
