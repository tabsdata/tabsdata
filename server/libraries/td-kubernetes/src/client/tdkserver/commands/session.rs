//
// Copyright 2026 Tabsdata Inc.
//

pub mod pin;
pub mod show;
pub mod unpin;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::session::pin::{PinArguments, command_pin};
use crate::client::tdkserver::commands::session::show::{ShowArguments, command_show};
use crate::client::tdkserver::commands::session::unpin::{UnpinArguments, command_unpin};
use crate::core::system::exit::CommandResult;

#[derive(Debug, Clone, Subcommand)]
pub enum SessionCommands {
    #[command(about = "Pin default values used when commands omit them")]
    Pin(PinArguments),
    #[command(about = "Show the pinned default values")]
    Show(ShowArguments),
    #[command(about = "Unpin default values")]
    Unpin(UnpinArguments),
}

pub async fn command_session(command: SessionCommands, globals: &GlobalArguments) -> CommandResult {
    match command {
        SessionCommands::Pin(arguments) => command_pin(arguments, globals).await,
        SessionCommands::Show(arguments) => command_show(arguments, globals).await,
        SessionCommands::Unpin(arguments) => command_unpin(arguments, globals).await,
    }
}
