//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::client::tdkcontext::{TdkContext, tdkcontext_path};
use crate::core::system::exit::Result;
use crate::cwarn;

#[derive(Debug, Clone, Args)]
pub struct UnpinArguments {}

pub async fn command_unpin(_arguments: UnpinArguments) -> Result {
    if TdkContext::delete()? {
        info!(
            "Cleared the active tabsdata session at {}.",
            tdkcontext_path().display()
        );
    } else {
        cwarn!("No active tabsdata session is set.");
    }
    Ok(())
}
