//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use getset::Getters;
use tracing::info;

use crate::client::tdkcontext::{TdkContext, tdkcontext_path};
use crate::cwarn;
use crate::system::exit::Outcome;

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
        cwarn!("No active context is set.");
    }
    Ok(())
}
