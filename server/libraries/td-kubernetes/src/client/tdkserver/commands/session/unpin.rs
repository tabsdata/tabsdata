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
    if PinContext::unpin_instance()? {
        info!("Cleared the pinned tabsdata instance.");
    } else {
        cwarn!("No tabsdata instance is pinned.");
    }
    Ok(())
}
