//
// Copyright 2026 Tabs Data Inc.
//

use clap_derive::Args;
use td_common::settings::DEFAULT_INSTANCE;
use td_common::status::ExitStatus::{Cancelled, GeneralError, NoAction};
use tracing::info;

use crate::base::error::cli::CliError;
use crate::base::error::provider::ProviderError;
use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::registry::{InstanceSettings, instance_folder, instance_init_folder};
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::template::get::get_templates;
use crate::client::tdkserver::common::{effective_yes, prompt_yes_no};
use crate::client::tdkserver::render::preview;
use crate::core::system::exit::{CommandResult, abort, abort_with};
use crate::runtime::provider::ClusterProvider;
use crate::runtime::provider::axes::Locality;

const DEFAULT_PROVIDER: ClusterProvider = ClusterProvider::Kminus;

#[derive(Debug, Clone, Args)]
pub struct InitArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        long_help = "Name of the instance whose editable input config folder is produced. \
                     Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "provider",
        required = false,
        value_enum,
        long_help = "Provider of the instance: 'kminus', 'docker', 'eks' or 'fargate'. \
                     Shapes the produced input config. Defaults to 'kminus'."
    )]
    pub provider: Option<ClusterProvider>,

    #[arg(
        long,
        name = "preview",
        required = false,
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        long_help = "Show the summary of resolved values before initializing the instance. \
                     Defaults to 'true'. Pass '--preview false' to skip the summary."
    )]
    pub preview: bool,

    #[arg(
        long,
        name = "yes",
        short = 'y',
        required = false,
        default_value_t = false,
        long_help = "Skip the interactive confirmation prompt and proceed immediately."
    )]
    pub yes: bool,
}

pub async fn command_init(arguments: InitArguments, globals: &GlobalArguments) -> CommandResult {
    let InitArguments {
        instance,
        provider,
        preview,
        yes,
    } = arguments;

    let instance_defaulted = instance.is_none();
    let provider_defaulted = provider.is_none();
    let instance = instance.unwrap_or_else(|| DEFAULT_INSTANCE.to_owned());
    let provider = provider.unwrap_or(DEFAULT_PROVIDER);

    if preview {
        preview::init_service(
            &instance,
            instance_defaulted,
            provider.as_str(),
            provider_defaulted,
        );
    }
    if !effective_yes(yes) && !prompt_yes_no("Proceed with the initialization?") {
        info!("Operation cancelled.");
        return abort(Cancelled);
    }

    let folder = instance_folder(&instance)?;
    if folder.exists() {
        return abort_with(
            NoAction,
            ProviderError::InstanceFolderExists(folder.display().to_string()),
        );
    }

    let init_folder = instance_init_folder(&instance)?;

    let reporter = InteractiveReporter::new();

    let step = reporter.step("Creating tabsdata instance folder");
    if let Err(error) = std::fs::create_dir_all(&init_folder) {
        step.fail();
        return abort_with(
            GeneralError,
            CliError::FolderCreateFailed {
                folder: init_folder.display().to_string(),
                cause: error.to_string(),
            },
        );
    }
    step.done(&format!(
        "Created tabsdata instance folder at {}",
        folder.display()
    ));

    get_templates(&reporter, provider, &init_folder, globals.verbose)?;

    let step = reporter.step("Writing tabsdata instance settings");
    let settings = InstanceSettings {
        name: instance.clone(),
        provider,
        locality: Locality::Local,
    };
    if let Err(error) = settings.save() {
        step.fail();
        return Err(error);
    }
    step.done("Wrote tabsdata instance settings");

    Ok(())
}
