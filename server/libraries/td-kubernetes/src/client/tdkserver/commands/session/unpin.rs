//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::client::tdkpin::TdkPin;
use crate::core::system::exit::Result;
use crate::cwarn;

#[derive(Debug, Clone, Args)]
pub struct UnpinArguments {}

pub async fn command_unpin(_arguments: UnpinArguments) -> Result {
    if TdkPin::unpin_session()? {
        info!("Cleared the active tabsdata session.");
    } else {
        cwarn!("No active tabsdata session is set.");
    }
    Ok(())
}
