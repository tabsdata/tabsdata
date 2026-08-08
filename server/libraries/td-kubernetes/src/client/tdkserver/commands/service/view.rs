//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::{BTreeMap, HashMap};

use clap_derive::Args;
use td_error::TdError;

use crate::base::constants::{CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY};
use crate::base::error::cli::CliError;
use crate::base::naming::name_from_kube_resource_name;
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::resolved_instance;
use crate::client::tdkserver::render::report::helper::yaml::{ReportDocument, render_documents};
use crate::core::system::exit::CommandResult;
use crate::cwarn;
use crate::kube::api::secret::get_secret;
use crate::kube::api::{LabelFilter, get_configmaps};
use crate::runtime::gateway::resolver::derived::amend;
use crate::runtime::gateway::resolver::reveal::reveal;
use crate::runtime::gateway::resolver::schema::Schema;
use crate::runtime::instance::profile::load_profile;
use crate::runtime::provider::provider_for_instance;
use crate::runtime::secrets::source::to_secret_name;

const SCHEMA_EXTENSION: &str = ".yamls";
const CONFIG_EXTENSION: &str = ".yaml";

#[derive(Debug, Clone, Args)]
pub struct ViewArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance whose configmaps are viewed. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "with-secrets",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 70,
        long_help = "Show secret values resolved. Values resolved only when applied are shown \
                     unresolved. Defaults to 'false'."
    )]
    pub with_secrets: Option<bool>,

    #[arg(
        long,
        name = "with-protected",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 80,
        long_help = "Show protected values resolved. Protected values are derived by the system \
                     and cannot be edited, and are shown as their placeholder unless this flag \
                     is given. Defaults to 'false'."
    )]
    pub with_protected: Option<bool>,
}

pub async fn command_view(arguments: ViewArguments, globals: &GlobalArguments) -> CommandResult {
    let ViewArguments {
        instance,
        with_secrets,
        with_protected,
    } = arguments;
    let with_secrets = with_secrets.unwrap_or(false);
    let with_protected = with_protected.unwrap_or(false);

    let settings = SettingsContext::settings();
    let instance = resolved_instance(instance, &settings);
    let verbose = globals.verbose;

    let provider = provider_for_instance(instance.value, verbose, None).await?;
    let name = provider.summary().instance_name.unwrap_or_default();

    let profile = load_profile(provider.cluster_provider(), verbose)?;
    let context = provider.kube_context().await?;

    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY.kind);
    let configmaps = get_configmaps(&context, Some(filter))
        .await
        .map_err(TdError::from)?;

    let mut documents: Vec<(String, String, String)> = Vec::new();
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
        let secret_values = if with_secrets {
            read_secret_values(&context, &file).await?
        } else {
            HashMap::new()
        };

        let rendered = reveal(
            template,
            &configmap,
            &schema,
            with_secrets,
            with_protected,
            &secret_values,
        )
        .map_err(TdError::from)?;
        let rendered = if with_protected {
            amend(&file, &rendered).map_err(TdError::from)?
        } else {
            rendered
        };
        documents.push((file, configmap_name, rendered));
    }

    if documents.is_empty() {
        cwarn!("No tabsdata configmaps found for instance '{name}'.");
        return Ok(());
    }

    documents.sort_by(|left, right| left.0.cmp(&right.0));
    let report: Vec<ReportDocument> = documents
        .iter()
        .map(|(title, subtitle, content)| ReportDocument {
            title,
            subtitle: Some(subtitle),
            content,
        })
        .collect();
    render_documents(&report)
}

fn parse_schema(file: &str, configs: &BTreeMap<String, String>) -> CommandResult<Schema> {
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
