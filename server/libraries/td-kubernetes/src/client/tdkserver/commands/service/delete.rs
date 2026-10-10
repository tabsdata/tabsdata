//
// Copyright 2026 Tabsdata Inc.
//

use clap_derive::Args;
use td_common::settings::DEFAULT_INSTANCE;
use tracing::info;

use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::registry::{InstanceSettings, InstanceState, instance_folder};
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::trash::trash_instance_folder;
use crate::client::tdkserver::commands::service::{
    instance_markers, resolved_details, resolved_instance, resolved_yes,
};
use crate::client::tdkserver::common::{confirm, confirm_name};
use crate::client::tdkserver::render::{preview, report};
use crate::core::system::exit::{CommandError, CommandResult, render_error};
use crate::cwarn;
use crate::kube::api::entity::configmap::delete_configmaps;
use crate::kube::api::entity::secret::delete_secrets;
use crate::runtime::provider::{Provider, provider_for_instance_mutating_any_state};

#[derive(Debug, Clone, Args)]
pub struct DeleteArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance to delete. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "trash",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 20,
        long_help = "Move the instance folder to the trash once deleted, leaving nothing behind. \
                     Defaults to 'false'."
    )]
    pub trash: Option<bool>,

    #[arg(
        long,
        name = "details",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 110,
        long_help = "Show the summary of resolved values before deleting the instance. \
                     Defaults to 'true'. Pass '--details false' to skip the summary."
    )]
    pub details: Option<bool>,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 120,
        long_help = "Skip the interactive confirmation prompt and proceed immediately."
    )]
    pub yes: Option<bool>,
}

pub async fn command_delete(
    arguments: DeleteArguments,
    globals: &GlobalArguments,
) -> CommandResult {
    let DeleteArguments {
        instance,
        trash,
        details,
        yes,
    } = arguments;
    let trash = trash.unwrap_or(false);

    let settings = SettingsContext::settings();
    let instance = resolved_instance(instance, &settings);
    let details = resolved_details(details, &settings);
    let yes = resolved_yes(yes, &settings);
    let verbose = globals.verbose;

    let name = instance
        .value
        .clone()
        .unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
    if !instance_folder(&name)?.exists() {
        cwarn!("Instance '{name}' does not exist. Nothing to delete.");
        return Ok(());
    }

    let provider = provider_for_instance_mutating_any_state(instance.value, verbose).await?;

    if details.value {
        let markers = instance_markers(&provider.view(), instance.in_settings);
        preview::delete_service(&provider.view(), &markers, trash);
    }
    if !confirm(yes.value, "Proceed with the deletion?")? {
        info!("Operation cancelled.");
        return Ok(());
    }
    if !confirm_name(yes.value, &name)? {
        info!("Operation cancelled.");
        return Ok(());
    }

    let reporter = InteractiveReporter::new();
    delete_resources(provider.as_ref(), &reporter, &name).await?;
    report::component::service::deleted_service(&provider.view());

    if trash {
        let folder = instance_folder(&name)?;
        let destination = trash_instance_folder(&name, &folder)?;
        info!(
            "Instance '{name}' moved to trash: {}",
            destination.display()
        );
    }

    Ok(())
}

async fn delete_resources(
    provider: &dyn Provider,
    reporter: &dyn Reporter,
    name: &str,
) -> CommandResult {
    provider.stop_service(reporter).await?;

    let context = provider.kube_context().await?;
    let step = reporter.step("Deleting tabsdata instance configmaps and secrets");
    if let Err(error) = delete_configmaps(&context).await {
        step.fail();
        return Err(error.into());
    }
    if let Err(error) = delete_secrets(&context).await {
        step.fail();
        return Err(error.into());
    }
    step.done("Deleted tabsdata instance configmaps and secrets");

    let mut settings = InstanceSettings::load(name)?;
    settings.state = InstanceState::Initialized;
    settings.save()
}

pub(crate) async fn delete_if_possible(name: &str, verbose: bool) {
    match InstanceSettings::load(name) {
        Ok(settings) if settings.state == InstanceState::Initialized => return,
        Ok(_) => {}
        Err(error) => {
            cwarn!(
                "Skipping the deletion of the instance resources; could not read the instance \
                 settings: {}",
                render(&error, verbose)
            );
            return;
        }
    }
    let provider =
        match provider_for_instance_mutating_any_state(Some(name.to_owned()), verbose).await {
            Ok(provider) => provider,
            Err(error) => {
                cwarn!(
                    "Skipping the deletion of the instance resources; could not prepare the \
                     instance: {}",
                    render(&error, verbose)
                );
                return;
            }
        };
    let reporter = InteractiveReporter::new();
    if let Err(error) = delete_resources(provider.as_ref(), &reporter, name).await {
        cwarn!(
            "Could not delete the instance resources from the cluster; delete them with \
             'tdkserver delete --instance {name}': {}",
            render(&error, verbose)
        );
    }
}

fn render(error: &CommandError, verbose: bool) -> String {
    match error.source() {
        Some(source) => render_error(source, verbose),
        None => error.status().to_string(),
    }
}
