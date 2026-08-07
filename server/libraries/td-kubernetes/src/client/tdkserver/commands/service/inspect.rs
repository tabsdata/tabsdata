//
// Copyright 2026 Tabsdata Inc.
//

use td_common::settings::DEFAULT_INSTANCE;

use crate::client::registry::{InstanceSettings, instance_folder};
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::{InstanceNameArguments, resolved_instance};
use crate::client::tdkserver::render::report;
use crate::core::system::exit::CommandResult;
use crate::runtime::provider::provider_for_instance;

pub async fn command_inspect(
    arguments: InstanceNameArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let settings = SettingsContext::settings();
    let instance = resolved_instance(arguments.instance, &settings);
    let verbose = globals.verbose;

    let name = instance
        .value
        .unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());

    let settings = InstanceSettings::load(&name)?;
    let folder = instance_folder(&name)?;

    let alive = match provider_for_instance(Some(name.clone()), verbose, None).await {
        Ok(provider) => Some(provider.service_alive().await),
        Err(_) => None,
    };

    report::component::service::instance_inspect(&settings, &folder, alive);

    Ok(())
}
