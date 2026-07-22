//
// Copyright 2026 Tabsdata Inc.
//

use clap_derive::Args;

use crate::client::session::pin::PinContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::core::system::exit::CommandResult;
use crate::cwarn;

#[derive(Debug, Clone, Args)]
pub struct ShowArguments {}

pub async fn command_show(_arguments: ShowArguments, _globals: &GlobalArguments) -> CommandResult {
    let pins = PinContext::pins();
    if pins.is_empty() {
        cwarn!("No tabsdata values are pinned.");
        return Ok(());
    }
    pins.report("Pinned tabsdata values:");
    Ok(())
}
