//
// Copyright 2026 Tabs Data Inc.
//

use crate::client::session::pin::PinContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::{InstanceNameArguments, resolved_instance};
use crate::client::tdkserver::render::report;
use crate::core::system::exit::CommandResult;
use crate::kube::api::job::list_jobs;
use crate::kube::api::pod::list_pods;
use crate::runtime::provider::provider_for_instance;

pub async fn command_status(
    arguments: InstanceNameArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let pins = PinContext::pins();
    let instance = resolved_instance(arguments.instance, &pins);
    let verbose = globals.verbose;
    let provider = provider_for_instance(instance.value, verbose).await?;
    let context = provider.kube_context().await?;
    let pods = list_pods(&context).await?;
    let jobs = list_jobs(&context).await?;
    report::status::cluster_status(&pods, &jobs);
    Ok(())
}
