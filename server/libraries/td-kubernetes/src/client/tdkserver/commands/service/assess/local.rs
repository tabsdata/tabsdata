//
// Copyright 2026 Tabsdata Inc.
//

use std::time::Instant;

use td_common::settings::DEFAULT_INSTANCE;

use crate::base::constants::KPROFILE_CONFIGS_FAMILY;
use crate::client::registry::{
    InstanceSettings, InstanceState, instance_folder, instance_init_input_folder,
};
use crate::client::tdkserver::arguments::ClusterArguments;
use crate::client::tdkserver::commands::config::check::pending_mandatory_placeholders;
use crate::client::tdkserver::render::report::helper::assess::{
    AssessCheck, AssessHelp, CheckState,
};
use crate::core::system::exit::CommandResult;
use crate::runtime::instance::profile::load_profile;
use crate::runtime::provider::ClusterProvider;

pub const CONCERN: &str = "Local registry";

pub const INSTANCE_STATE: &str = "Instance state";
pub const DEFINITION_MATCH: &str = "Definition match";
pub const MANDATORY_VALUES: &str = "Mandatory values";

const CODE_STATE: &str = "assess::local::instance-state";
const CODE_SETTINGS: &str = "assess::local::settings-mismatch";
const CODE_PLACEHOLDERS: &str = "assess::local::pending-values";

pub fn assess_instance_state(name: &str) -> CommandResult<(AssessCheck, Option<InstanceSettings>)> {
    let started = Instant::now();
    let label = INSTANCE_STATE.to_owned();
    let folder = instance_folder(name)?;

    if !folder.is_dir() {
        return Ok((
            AssessCheck {
                concern: CONCERN,
                check: label,
                state: CheckState::Failed,
                seconds: started.elapsed().as_secs_f64(),
                code: Some(CODE_STATE),
                title: Some(format!(
                    "The instance '{name}' is not initialized, so there is nothing to create"
                )),
                facts: vec![format!("Expected folder: {}", folder.display())],
                help: vec![initialize_hint(name)],
                url: None,
            },
            None,
        ));
    }

    let persisted = InstanceSettings::load(name)?;
    if persisted.state == InstanceState::Initialized {
        return Ok((
            AssessCheck::passed(CONCERN, label, started.elapsed().as_secs_f64()),
            Some(persisted),
        ));
    }

    let state = persisted.state.as_str().to_owned();
    Ok((
        AssessCheck {
            concern: CONCERN,
            check: label,
            state: CheckState::Failed,
            seconds: started.elapsed().as_secs_f64(),
            code: Some(CODE_STATE),
            title: Some(format!(
                "The instance '{name}' is already {state}, so it cannot be created again"
            )),
            facts: vec![
                format!(
                    "State: {state}, expected: {}",
                    InstanceState::Initialized.as_str()
                ),
                format!("Folder: {}", folder.display()),
            ],
            help: vec![
                AssessHelp::plain("Only an initialized instance can be created"),
                command_hint("Delete the instance first", "delete", name),
            ],
            url: None,
        },
        Some(persisted),
    ))
}

pub fn assess_definition_match(
    name: &str,
    cluster: &ClusterArguments,
    persisted: Option<&InstanceSettings>,
) -> AssessCheck {
    let started = Instant::now();
    let label = DEFINITION_MATCH.to_owned();

    let Some(persisted) = persisted else {
        return AssessCheck {
            concern: CONCERN,
            check: label,
            state: CheckState::Failed,
            seconds: started.elapsed().as_secs_f64(),
            code: Some(CODE_SETTINGS),
            title: Some(format!(
                "There are no persisted settings for the instance '{name}'"
            )),
            facts: Vec::new(),
            help: vec![initialize_hint(name)],
            url: None,
        };
    };

    let provided = &persisted.cluster.provided;
    let mut mismatches = Vec::new();

    if let Some(given) = cluster.provider
        && given != persisted.provider
    {
        mismatches.push(difference(
            "Provider",
            given.as_str(),
            persisted.provider.as_str(),
        ));
    }
    if let Some(given) = cluster.context.as_deref()
        && Some(given) != provided.context.as_deref()
    {
        mismatches.push(difference(
            "Context",
            given,
            provided.context.as_deref().unwrap_or_default(),
        ));
    }
    if let Some(given) = cluster.kubeconfig.as_deref()
        && Some(given) != provided.kubeconfig.as_deref()
    {
        mismatches.push(difference(
            "Kubeconfig",
            &given.display().to_string(),
            &provided
                .kubeconfig
                .as_deref()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
        ));
    }
    if let Some(given) = cluster.namespace.as_deref()
        && Some(given) != provided.namespace.as_deref()
    {
        mismatches.push(difference(
            "Namespace",
            given,
            provided.namespace.as_deref().unwrap_or_default(),
        ));
    }
    if cluster.current != provided.current {
        mismatches.push(difference(
            "Current",
            &cluster.current.to_string(),
            &provided.current.to_string(),
        ));
    }

    if mismatches.is_empty() {
        return AssessCheck::passed(CONCERN, label, started.elapsed().as_secs_f64());
    }

    AssessCheck {
        concern: CONCERN,
        check: label,
        state: CheckState::Failed,
        seconds: started.elapsed().as_secs_f64(),
        code: Some(CODE_SETTINGS),
        title: Some(format!(
            "The given arguments differ from what the instance '{name}' was initialized with"
        )),
        facts: mismatches,
        help: vec![
            AssessHelp::plain(
                "Run again with the persisted values, or omit them so they are taken from the \
                 instance",
            ),
            command_hint("See the persisted values", "inspect", name),
        ],
        url: None,
    }
}

pub fn assess_pending_values(name: &str, cluster: &ClusterArguments) -> CommandResult<AssessCheck> {
    let started = Instant::now();
    let label = MANDATORY_VALUES.to_owned();

    let provider = cluster.provider.unwrap_or(ClusterProvider::Kminus);
    let profile = load_profile(provider, false)?;
    let configs = instance_init_input_folder(name)?.join(KPROFILE_CONFIGS_FAMILY.folder);

    if !configs.is_dir() {
        return Ok(AssessCheck {
            concern: CONCERN,
            check: label,
            state: CheckState::Failed,
            seconds: started.elapsed().as_secs_f64(),
            code: Some(CODE_PLACEHOLDERS),
            title: Some(format!(
                "The input config folder of the instance '{name}' does not exist"
            )),
            facts: vec![format!("Expected folder: {}", configs.display())],
            help: vec![initialize_hint(name)],
            url: None,
        });
    }

    let pending = pending_mandatory_placeholders(&profile, &configs)?;
    if pending.is_empty() {
        return Ok(AssessCheck::passed(
            CONCERN,
            label,
            started.elapsed().as_secs_f64(),
        ));
    }

    let count = pending.len();
    let facts = pending
        .iter()
        .map(|entry| format!("{} in {}", entry.category, entry.files.join(", ")))
        .collect();
    Ok(AssessCheck {
        concern: CONCERN,
        check: label,
        state: CheckState::Failed,
        seconds: started.elapsed().as_secs_f64(),
        code: Some(CODE_PLACEHOLDERS),
        title: Some(match count {
            1 => "One mandatory value is not provided yet".to_owned(),
            _ => format!("{count} mandatory values are not provided yet"),
        }),
        facts,
        help: vec![
            AssessHelp::plain(format!("Edit the files under {}", configs.display())),
            command_hint("List every configurable value", "config check", name),
        ],
        url: None,
    })
}

fn initialize_hint(name: &str) -> AssessHelp {
    command_hint("Initialize the instance first", "init", name)
}

fn command_hint(lead: &str, command: &str, name: &str) -> AssessHelp {
    let command = if name == DEFAULT_INSTANCE {
        format!("tdkserver {command}")
    } else {
        format!("tdkserver {command} --instance {name}")
    };
    AssessHelp::with_command(format!("{lead}, with command"), command)
}

fn difference(field: &str, given: &str, persisted: &str) -> String {
    format!("{field}: given '{given}', persisted '{persisted}'")
}
