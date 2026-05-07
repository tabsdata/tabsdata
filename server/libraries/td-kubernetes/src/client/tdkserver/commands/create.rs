//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use tracing::info;

use crate::client::tdkserver::arguments::{ClusterArguments, InstanceArguments};
use crate::client::tdkserver::common::prompt_yes_no;
use crate::constants::PROFILE_PRODUCTION;
use crate::provider::provider_for;
use crate::system::exit::Result;

#[derive(Debug, Clone, Args)]
pub struct CreateArguments {
    #[command(flatten)]
    pub cluster: ClusterArguments,

    #[command(flatten)]
    pub instance: InstanceArguments,

    #[cfg(td_k8s_development)]
    #[arg(
        long,
        name = "profile",
        required = false,
        long_help = "Profile to use when generating kubernetes manifests. \
                     When unspecified, defaults to 'production'."
    )]
    pub profile: Option<String>,

    #[arg(
        long,
        name = "preview",
        required = false,
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        long_help = "Show the summary of resolved values and prompt for confirmation \
                     before creating the instance. Defaults to 'true'. \
                     Pass '--preview false' to skip the summary and prompt."
    )]
    pub preview: bool,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        default_value_t = false,
        long_help = "Skip the interactive confirmation prompt and proceed immediately. \
                     Has no effect when --preview is 'false'."
    )]
    pub yes: bool,
}

#[cfg(td_k8s_development)]
fn resolve_profile(arguments: &CreateArguments) -> String {
    arguments
        .profile
        .as_deref()
        .unwrap_or(PROFILE_PRODUCTION)
        .to_owned()
}

#[cfg(not(td_k8s_development))]
fn resolve_profile(_arguments: &CreateArguments) -> String {
    PROFILE_PRODUCTION.to_owned()
}

pub async fn command_create(arguments: CreateArguments) -> Result {
    let profile = resolve_profile(&arguments);
    let CreateArguments {
        cluster,
        instance,
        preview,
        yes,
        ..
    } = arguments;

    let provider = provider_for(cluster, instance).await?;
    provider.ensure_creatable_runtime().await?;

    if preview {
        provider.preview_create_runtime_summary(&profile);
        if !yes && !prompt_yes_no("Proceed with the creation?") {
            info!("Operation cancelled.");
            return Ok(());
        }
    }

    let tx = provider.prepare_cluster().await?;
    provider.prepare_instance(&profile).await?;
    tx.commit();
    provider.report_created_runtime();

    Ok(())
}
