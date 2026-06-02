//
// Copyright 2026 Tabs Data Inc.
//

pub mod pin;
pub mod show;
pub mod unpin;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::autoconfirm::pin::{PinArguments, command_pin};
use crate::client::tdkserver::commands::autoconfirm::show::{ShowArguments, command_show};
use crate::client::tdkserver::commands::autoconfirm::unpin::{UnpinArguments, command_unpin};
use crate::core::system::exit::CommandResult;

#[derive(Debug, Clone, Subcommand)]
pub enum AutoconfirmCommands {
    #[command(about = "Pin the autoconfirm preference")]
    Pin(PinArguments),
    #[command(about = "Show the pinned autoconfirm preference")]
    Show(ShowArguments),
    #[command(about = "Unpin the autoconfirm preference")]
    Unpin(UnpinArguments),
}

pub async fn command_autoconfirm(
    command: AutoconfirmCommands,
    _globals: &GlobalArguments,
) -> CommandResult {
    match command {
        AutoconfirmCommands::Pin(arguments) => command_pin(arguments).await,
        AutoconfirmCommands::Show(arguments) => command_show(arguments).await,
        AutoconfirmCommands::Unpin(arguments) => command_unpin(arguments).await,
    }
}
