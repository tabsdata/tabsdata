//
// Copyright 2026 Tabsdata Inc.
//

use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::{
    InstanceNameArguments, instance_label, resolved_instance,
};
use crate::client::tdkserver::render::report;
use crate::core::system::exit::CommandResult;
use crate::cwarn;
use crate::kube::api::cronjob::list_cronjobs;
use crate::kube::api::job::list_jobs;
use crate::kube::api::pod::list_pods;
use crate::runtime::provider::provider_for_instance_any_state;

pub async fn command_status(
    arguments: InstanceNameArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let settings = SettingsContext::settings();
    let instance = resolved_instance(arguments.instance, &settings);
    let verbose = globals.verbose;
    let provider = provider_for_instance_any_state(instance.value, verbose).await?;
    if !provider.cluster_alive().await {
        cwarn!(
            "Instance '{}' is not running.",
            instance_label(&provider.summary())
        );
        return Ok(());
    }
    let context = provider.kube_context().await?;
    let pods = list_pods(&context).await?;
    let jobs = list_jobs(&context).await?;
    let cronjobs = list_cronjobs(&context).await?;
    report::component::service::status::cluster_status(&pods, &jobs, &cronjobs)?;
    Ok(())
}
