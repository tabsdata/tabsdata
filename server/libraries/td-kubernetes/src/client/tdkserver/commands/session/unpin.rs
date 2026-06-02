//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::client::session::pin::PinContext;
use crate::core::system::exit::CommandResult;
use crate::cwarn;

#[derive(Debug, Clone, Args)]
pub struct UnpinArguments {}

pub async fn command_unpin(_arguments: UnpinArguments) -> CommandResult {
    if PinContext::unpin_session()? {
        info!("Cleared the active tabsdata session.");
    } else {
        cwarn!("No active tabsdata session is set.");
    }
    Ok(())
}
