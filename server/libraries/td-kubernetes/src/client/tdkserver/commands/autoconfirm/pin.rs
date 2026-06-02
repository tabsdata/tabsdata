//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::client::session::pin::PinContext;
use crate::core::system::exit::Result;

#[derive(Debug, Clone, Args)]
pub struct PinArguments {
    #[arg(
        long,
        name = "value",
        required = false,
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        long_help = "Whether autoconfirm is enabled. When enabled, commands proceed \
                     without asking for confirmation, as if '--yes' was passed. \
                     Defaults to 'true'. Pass '--value false' to disable."
    )]
    pub value: bool,
}

pub async fn command_pin(arguments: PinArguments) -> Result {
    let PinArguments { value } = arguments;

    PinContext::pin_autoconfirm(value)?;
    if value {
        info!("Pinned autoconfirm as enabled.");
    } else {
        info!("Pinned autoconfirm as disabled.");
    }
    Ok(())
}
