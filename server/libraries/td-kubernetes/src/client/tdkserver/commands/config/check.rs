//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use clap_derive::Args;
use td_error::TdError;

use crate::base::constants::KPROFILE_CONFIGS_FAMILY;
#[cfg(feature = "enterprise")]
use crate::base::constants::{SECRET_CATEGORY_ANTHROPIC_KEY, SECRET_CATEGORY_OPENAI_KEY};
use crate::base::error::cli::CliError;
use crate::client::registry::{instance_edit_input_folder, instance_init_input_folder};
use crate::client::session::settings::SettingsContext;
use crate::client::tdkserver::arguments::GlobalArguments;
use crate::client::tdkserver::commands::service::resolved_instance;
use crate::client::tdkserver::render::report::helper::check::{
    CheckEntry, CheckReport, CheckRule, render_check,
};
use crate::core::system::exit::CommandResult;
use crate::cwarn;
use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::gateway::resolver::walker::{Rule, placeholder_occurrences};
use crate::runtime::instance::profile::load_profile;
use crate::runtime::provider::provider_for_instance_any_state;

const CONFIG_EXTENSION: &str = ".yaml";

#[derive(Debug, Clone, Args)]
pub struct CheckArguments {
    #[arg(
        long,
        name = "instance",
        required = false,
        display_order = 10,
        long_help = "Name of the instance whose configs are checked. Defaults to 'tabsdata'."
    )]
    pub instance: Option<String>,

    #[arg(
        long,
        name = "init",
        required = false,
        conflicts_with = "edit",
        display_order = 20,
        long_help = "Check the configs of the instance creation input. This is the default."
    )]
    pub init: bool,

    #[arg(
        long,
        name = "edit",
        required = false,
        conflicts_with = "init",
        display_order = 30,
        long_help = "Check the configs of the instance edition input, instead of the creation one."
    )]
    pub edit: bool,

    #[arg(
        long,
        name = "all",
        required = false,
        num_args = 0..=1,
        default_missing_value = "true",
        display_order = 70,
        long_help = "Show also the values already provided, besides the pending ones. \
                     Defaults to 'false'."
    )]
    pub all: Option<bool>,
}

pub async fn command_check(arguments: CheckArguments, globals: &GlobalArguments) -> CommandResult {
    let CheckArguments {
        instance,
        init: _,
        edit,
        all,
    } = arguments;
    let all = all.unwrap_or(false);

    let settings = SettingsContext::settings();
    let instance = resolved_instance(instance, &settings);
    let provider = provider_for_instance_any_state(instance.value, globals.verbose).await?;
    let name = provider.summary().instance_name.unwrap_or_default();

    let profile = load_profile(provider.cluster_provider(), globals.verbose)?;

    let (stage, folder) = if edit {
        ("edit", instance_edit_input_folder(&name)?)
    } else {
        ("init", instance_init_input_folder(&name)?)
    };
    let configs = folder.join(KPROFILE_CONFIGS_FAMILY.folder);
    if !configs.is_dir() {
        cwarn!(
            "No {stage} configs to check for instance '{name}': '{}' does not exist.",
            configs.display()
        );
        return Ok(());
    }

    let sources = placeholders_in_folder(&configs)?;
    let settled = settled_categories(&profile.configs, &sources);
    let pending: Vec<CheckEntry> = collect_entries(sources)
        .into_iter()
        .filter(|entry| !settled.contains(&entry.category))
        .collect();
    let provided = if all {
        let expected = collect_entries(placeholders_in_templates(&profile.configs));
        remaining(expected, &pending)
    } else {
        Vec::new()
    };

    if pending.is_empty() && provided.is_empty() {
        cwarn!("No configurable values found in the {stage} configs of instance '{name}'.");
        return Ok(());
    }

    let report = CheckReport {
        instance: &name,
        stage,
        folder: &configs.display().to_string(),
        pending: &pending,
        provided: &provided,
        all,
        notes: &notes(&pending),
    };
    render_check(&report)
}

#[cfg(feature = "enterprise")]
fn notes(pending: &[CheckEntry]) -> Vec<String> {
    let injected: Vec<&str> = [SECRET_CATEGORY_ANTHROPIC_KEY, SECRET_CATEGORY_OPENAI_KEY]
        .into_iter()
        .filter(|category| pending.iter().any(|entry| entry.category == *category))
        .collect();
    if injected.is_empty() {
        return Vec::new();
    }
    vec![format!(
        "'quickstart' injects {} straight into the instance secrets and the instance \
         vault, so they stay pending here even once supplied.",
        injected.join(" and ")
    )]
}

#[cfg(not(feature = "enterprise"))]
fn notes(_pending: &[CheckEntry]) -> Vec<String> {
    Vec::new()
}

fn remaining(expected: Vec<CheckEntry>, pending: &[CheckEntry]) -> Vec<CheckEntry> {
    expected
        .into_iter()
        .filter(|entry| {
            !pending
                .iter()
                .any(|other| other.category == entry.category && other.kind == entry.kind)
        })
        .collect()
}

fn settled_categories(
    templates: &BTreeMap<String, String>,
    sources: &[(String, String)],
) -> BTreeSet<String> {
    let mut settled = BTreeSet::new();
    for (file, content) in sources {
        let Some(template) = templates.get(file) else {
            continue;
        };
        let expected = collect_entries(vec![(file.clone(), template.clone())]);
        let outstanding = collect_entries(vec![(file.clone(), content.clone())]);
        for entry in expected {
            if !outstanding
                .iter()
                .any(|other| other.category == entry.category && other.kind == entry.kind)
            {
                settled.insert(entry.category);
            }
        }
    }
    settled
}

fn placeholders_in_folder(configs: &Path) -> CommandResult<Vec<(String, String)>> {
    let mut sources = Vec::new();
    for file in config_files(configs)? {
        let Some(name) = file.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let content = fs::read_to_string(&file).map_err(|error| {
            TdError::from(CliError::FileParseFailed {
                file: name.to_owned(),
                cause: error.to_string(),
            })
        })?;
        sources.push((name.to_owned(), content));
    }
    Ok(sources)
}

fn placeholders_in_templates(configs: &BTreeMap<String, String>) -> Vec<(String, String)> {
    configs
        .iter()
        .filter(|(name, _)| name.ends_with(CONFIG_EXTENSION))
        .map(|(name, content)| (name.clone(), content.clone()))
        .collect()
}

fn collect_entries(sources: Vec<(String, String)>) -> Vec<CheckEntry> {
    let mut found: BTreeMap<String, CheckEntry> = BTreeMap::new();
    for (name, content) in sources {
        for placeholder in placeholder_occurrences(&content) {
            let Ok(kind) = placeholder.kind.parse::<PlaceholderKind>() else {
                continue;
            };
            if !kind.is_editable() {
                continue;
            }
            let category = placeholder
                .category
                .clone()
                .unwrap_or_else(|| placeholder.kind.clone());
            let entry = found.entry(category.clone()).or_insert_with(|| CheckEntry {
                kind: placeholder.kind.clone(),
                category,
                rule: check_rule(kind, placeholder.rule),
                files: Vec::new(),
            });
            if !entry.files.contains(&name) {
                entry.files.push(name.clone());
            }
        }
    }
    let mut entries: Vec<CheckEntry> = found.into_values().collect();
    for entry in &mut entries {
        entry.files.sort();
    }
    entries.sort_by(|left, right| {
        rank(left.rule)
            .cmp(&rank(right.rule))
            .then_with(|| left.category.cmp(&right.category))
    });
    entries
}

fn check_rule(kind: PlaceholderKind, rule: Rule) -> CheckRule {
    if kind.is_auto() {
        return CheckRule::Delegated;
    }
    match rule {
        Rule::Mandatory => CheckRule::Mandatory,
        Rule::Optional => CheckRule::Optional,
    }
}

fn rank(rule: CheckRule) -> u8 {
    match rule {
        CheckRule::Mandatory => 0,
        CheckRule::Optional => 1,
        CheckRule::Delegated => 2,
    }
}

fn config_files(configs: &Path) -> CommandResult<Vec<PathBuf>> {
    let listing = fs::read_dir(configs).map_err(|error| {
        TdError::from(CliError::FileParseFailed {
            file: configs.display().to_string(),
            cause: error.to_string(),
        })
    })?;
    let mut files: Vec<PathBuf> = listing
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(CONFIG_EXTENSION))
        })
        .collect();
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests;
