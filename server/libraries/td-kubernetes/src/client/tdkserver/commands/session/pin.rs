//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::client::session::pin::PinContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::InstanceNameArguments;
use crate::core::system::exit::CommandResult;
use td_common::settings::DEFAULT_INSTANCE;

#[derive(Debug, Clone, Args)]
pub struct PinArguments {
    #[command(flatten)]
    pub instance: InstanceNameArguments,
}

pub async fn command_pin(arguments: PinArguments, _globals: &GlobalArguments) -> CommandResult {
    let name = arguments
        .instance
        .instance
        .unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
    PinContext::pin_instance(name.clone())?;
    info!("Pinned tabsdata instance '{name}'.");
    Ok(())
}
