//
// Copyright 2026 Tabs Data Inc.
//

use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::InstanceNameArguments;
use crate::client::tdkserver::commands::session::pin::{
    PinArguments, command_pin as command_session_pin,
};
use crate::core::system::exit::CommandResult;

pub async fn command_pin(
    arguments: InstanceNameArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    command_session_pin(PinArguments { instance: arguments }, globals).await
}
