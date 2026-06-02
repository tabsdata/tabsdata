//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::client::session::pin::PinContext;
use crate::core::system::exit::Result;
use crate::cwarn;

#[derive(Debug, Clone, Args)]
pub struct UnpinArguments {}

pub async fn command_unpin(_arguments: UnpinArguments) -> Result {
    if PinContext::unpin_autoconfirm()? {
        info!("Cleared the pinned autoconfirm.");
    } else {
        cwarn!("No autoconfirm is pinned.");
    }
    Ok(())
}
