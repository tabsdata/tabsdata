//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use td_error::TdError;

use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::registry::{
    InstanceSettings, InstanceState, instance_init_folder, instance_stage_folder,
};
use crate::client::tdkserver::arguments::{ClusterArguments, GlobalArguments, InstanceArguments};
use crate::core::system::exit::CommandResult;
use crate::runtime::gateway::resolver::fusion;
use crate::runtime::instance::profile::load_profile;
use crate::runtime::provider::provider_for;

#[derive(Debug, Clone, Args)]
pub struct CreateArguments {
    #[arg(
        long,
        name = "instance",
        required = true,
        long_help = "Name of the instance to create."
    )]
    pub instance: String,
}

pub async fn command_create(
    arguments: CreateArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let CreateArguments { instance } = arguments;

    let settings = InstanceSettings::load_required(&instance, &[InstanceState::Initialized])?;

    let init = instance_init_folder(&settings.name)?;
    let stage = instance_stage_folder(&settings.name)?;

    let profile = load_profile(settings.provider, globals.verbose)?;

    let cluster = ClusterArguments {
        provider: Some(settings.provider),
        context: None,
        kubeconfig: None,
        current: false,
        namespace: None,
    };
    let provider = provider_for(
        cluster,
        InstanceArguments::new(Some(settings.name.clone())),
        globals.verbose,
    )
    .await?;
    let system = provider.system_values()?;

    let reporter = InteractiveReporter::new();

    let step = reporter.step("Fusing tabsdata instance configuration");
    if let Err(error) = fusion::fuse(&profile, &system, &init, &stage) {
        step.fail();
        return Err(TdError::from(error).into());
    }
    step.done("Fused tabsdata instance configuration");

    Ok(())
}
