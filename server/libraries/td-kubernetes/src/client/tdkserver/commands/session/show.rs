//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use colored::Colorize;

use crate::client::session::pin::PinContext;
use crate::core::system::exit::CommandResult;

#[derive(Debug, Clone, Args)]
pub struct ShowArguments {}

pub async fn command_show(_arguments: ShowArguments) -> CommandResult {
    match PinContext::pinned_instance() {
        Some(name) => println!("Pinned tabsdata instance: {name}"),
        None => println!("{}", "No tabsdata instance is pinned.".dimmed()),
    }
    Ok(())
}
