//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::client::tdkcontext::{TdkContext, tdkcontext_path};
use crate::core::system::exit::Result;
use crate::cwarn;

#[derive(Debug, Clone, Args)]
pub struct UnuseArguments {}

pub async fn command_unuse(_arguments: UnuseArguments) -> Result {
    if TdkContext::delete()? {
        info!(
            "Cleared the active context at {}.",
            tdkcontext_path().display()
        );
    } else {
        cwarn!("No active context is set.");
    }
    Ok(())
}
