//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::HashMap;
use std::fs;

use clap_derive::Args;
use td_error::TdError;
use tracing::info;

use crate::base::constants::{CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY};
use crate::base::error::cli::CliError;
use crate::base::naming::name_from_kube_resource_name;
use crate::base::reporter::{InteractiveReporter, Reporter};
use crate::client::registry::instance_edit_input_folder;
use crate::client::session::pin::PinContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::resolved_instance;
use crate::core::system::exit::CommandResult;
use crate::cwarn;
use crate::kube::api::secret::get_secret;
use crate::kube::api::{LabelFilter, get_configmaps};
use crate::runtime::gateway::resolver::reveal::reveal;
use crate::runtime::gateway::resolver::schema::Schema;
use crate::runtime::instance::profile::load_profile;
use crate::runtime::provider::provider_for_instance;
use crate::runtime::secrets::source::to_secret_name;

const SCHEMA_EXTENSION: &str = ".yamls";
const CONFIG_EXTENSION: &str = ".yaml";

#[derive(Debug, Clone, Args)]
pub struct GetArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance whose configmaps are downloaded. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "values",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 70,
        long_help = "Materialize the values the user can modify into the downloaded files. \
                     Values the user cannot modify are kept as placeholders. Defaults to 'false'."
    )]
    pub values: Option<bool>,
}

pub async fn command_get(arguments: GetArguments, globals: &GlobalArguments) -> CommandResult {
    let GetArguments { instance, values } = arguments;
    let values = values.unwrap_or(false);

    let pins = PinContext::pins();
    let instance = resolved_instance(instance, &pins);
    let verbose = globals.verbose;

    let provider = provider_for_instance(instance.value, verbose).await?;
    let name = provider.summary().instance_name.unwrap_or_default();

    let profile = load_profile(provider.cluster_provider(), verbose)?;
    let context = provider.kube_context().await?;

    let reporter = InteractiveReporter::new();
    let step = reporter.step("Downloading tabsdata configmaps");

    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY.kind);
    let configmaps = match get_configmaps(&context, Some(filter)).await {
        Ok(configmaps) => configmaps,
        Err(error) => {
            step.fail();
            return Err(TdError::from(error).into());
        }
    };

    let edit_input = instance_edit_input_folder(&name)?;
    let configs_folder = edit_input.join(KPROFILE_CONFIGS_FAMILY.folder);
    if let Err(error) = fs::create_dir_all(&configs_folder) {
        step.fail();
        return Err(TdError::from(CliError::FolderCreateFailed {
            folder: configs_folder.display().to_string(),
            cause: error.to_string(),
        })
        .into());
    }

    let mut count = 0;
    for (configmap_name, entries) in configmaps {
        let Ok(file) = name_from_kube_resource_name(
            &configmap_name,
            KPROFILE_CONFIGS_FAMILY.scope,
            KPROFILE_CONFIGS_FAMILY.kind,
        ) else {
            continue;
        };
        let file = file.to_owned();
        let Some(template) = profile.configs.get(&file) else {
            continue;
        };

        let configmap = match entries.into_iter().next() {
            Some((_key, value)) => String::from_utf8(value).map_err(|error| {
                TdError::from(CliError::FileParseFailed {
                    file: file.clone(),
                    cause: error.to_string(),
                })
            })?,
            None => continue,
        };

        let schema = parse_schema(&file, &profile.configs)?;
        let secret_values = if values {
            read_secret_values(&context, &file).await?
        } else {
            HashMap::new()
        };

        let rendered =
            reveal(template, &configmap, &schema, values, &secret_values).map_err(TdError::from)?;

        let target = configs_folder.join(&file);
        fs::write(&target, rendered).map_err(|error| {
            TdError::from(CliError::FileWriteFailed {
                file: target.display().to_string(),
                cause: error.to_string(),
            })
        })?;
        count += 1;
    }

    if count == 0 {
        step.fail();
        cwarn!("No tabsdata configmaps found for instance '{name}'.");
        return Ok(());
    }

    for (file, content) in &profile.configs {
        if !file.ends_with(SCHEMA_EXTENSION) {
            continue;
        }
        let target = configs_folder.join(file);
        fs::write(&target, content).map_err(|error| {
            TdError::from(CliError::FileWriteFailed {
                file: target.display().to_string(),
                cause: error.to_string(),
            })
        })?;
    }

    step.done(&format!("Downloaded {count} tabsdata configmaps"));
    info!("Configmaps written to {}", configs_folder.display());
    Ok(())
}

fn parse_schema(
    file: &str,
    configs: &std::collections::BTreeMap<String, String>,
) -> CommandResult<Schema> {
    let Some(stem) = file.strip_suffix(CONFIG_EXTENSION) else {
        return Ok(Schema::default());
    };
    let Some(schema_text) = configs.get(&format!("{stem}{SCHEMA_EXTENSION}")) else {
        return Ok(Schema::default());
    };
    let schema = Schema::parse(file, schema_text).map_err(TdError::from)?;
    Ok(schema)
}

async fn read_secret_values(
    context: &crate::kube::context::KubeContext,
    file: &str,
) -> CommandResult<HashMap<String, String>> {
    let secret_name = to_secret_name(file).map_err(TdError::from)?;
    let Some(data) = get_secret(context, &secret_name)
        .await
        .map_err(TdError::from)?
    else {
        return Ok(HashMap::new());
    };
    let mut values = HashMap::new();
    for (key, value) in data {
        if let Ok(text) = String::from_utf8(value) {
            values.insert(key, text);
        }
    }
    Ok(values)
}
