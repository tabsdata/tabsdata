//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use getset::Getters;
use tracing::info;

use crate::exit::Outcome;
use crate::tdkcontext::{TdkContext, tdkcontext_path};

#[derive(Debug, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct UnuseArguments {}

pub async fn command_unuse(_arguments: UnuseArguments) -> Outcome {
    if TdkContext::delete()? {
        info!(
            "Cleared the active context at {}.",
            tdkcontext_path().display()
        );
    } else {
        info!("No active context is set.");
    }
    Ok(())
}
