//
// Copyright 2026 Tabs Data Inc.
//

pub mod pin;
pub mod show;
pub mod unpin;

use clap_derive::Subcommand;

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::session::pin::{PinArguments, command_pin};
use crate::client::tdkserver::commands::session::show::{ShowArguments, command_show};
use crate::client::tdkserver::commands::session::unpin::{UnpinArguments, command_unpin};
use crate::core::system::exit::Result;

#[derive(Debug, Clone, Subcommand)]
pub enum SessionCommands {
    #[command(about = "Pin a tabsdata session as active")]
    Pin(PinArguments),
    #[command(about = "Show the active tabsdata session")]
    Show(ShowArguments),
    #[command(about = "Unpin the active tabsdata session")]
    Unpin(UnpinArguments),
}

pub async fn command_session(command: SessionCommands, globals: &GlobalArguments) -> Result {
    match command {
        SessionCommands::Pin(arguments) => command_pin(arguments, globals).await,
        SessionCommands::Show(arguments) => command_show(arguments).await,
        SessionCommands::Unpin(arguments) => command_unpin(arguments).await,
    }
}
