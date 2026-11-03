//
// Copyright 2026 Tabsdata Inc.
//

use std::str::FromStr;

use once_cell::sync::Lazy;
use regex::Regex;
use serde::Deserialize;
use serde_yaml::Value;

use crate::base::error::provider::ProviderError;
use crate::runtime::gateway::resolver::{SystemKey, SystemValues};
use crate::runtime::instance::profile::RenderedConfigmap;

pub const WINDOWS_PATH_LIMIT: usize = 259;

const POD_RUN_FOLDER: &str = "pod-run";
const CONTAINER_WORK_FOLDER: &str = "work";
const POD_UID_PLACEHOLDER: &str = "00000000-0000-0000-0000-000000000000";
const CONTAINER_ROOT: &str = "$(CONTAINER_ROOT)/";
const PLACEHOLDER_OPENER: &str = "${{";
const WINDOWS_SEPARATOR: &str = r"\";

static SYSTEM_PLACEHOLDER: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\$\{\{system:([A-Za-z0-9._-]*)\}\}")
        .expect("The system placeholder pattern is invalid.")
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutablePath {
    pub pod: String,
    pub container: String,
    pub command: String,
    pub path: String,
}

impl ExecutablePath {
    pub fn length(&self) -> usize {
        self.path.encode_utf16().count()
    }
}

pub fn windows_executable_paths(
    data_folder: &str,
    pods: &RenderedConfigmap,
    system: &SystemValues,
) -> Result<Vec<ExecutablePath>, ProviderError> {
    let pod_run = [data_folder, POD_RUN_FOLDER, POD_UID_PLACEHOLDER].join(WINDOWS_SEPARATOR);
    let mut paths = Vec::new();
    for (pod, yaml) in pods {
        for document in serde_yaml::Deserializer::from_str(yaml) {
            let document = Value::deserialize(document).map_err(|error| {
                ProviderError::PodSpecParseFailed {
                    pod: pod.clone(),
                    cause: error.to_string(),
                }
            })?;
            let Some(pod_spec) = workload_pod_spec(&document) else {
                continue;
            };
            let containers = ["initContainers", "containers"]
                .into_iter()
                .filter_map(|key| pod_spec[key].as_sequence())
                .flatten();
            for container in containers {
                let Some(command) = container["command"][0].as_str() else {
                    continue;
                };
                let Some(program) = command.strip_prefix(CONTAINER_ROOT) else {
                    continue;
                };
                let Some(name) = container["name"].as_str() else {
                    continue;
                };
                let program = resolve_system_placeholders(program, system).ok_or_else(|| {
                    ProviderError::ExecutablePlaceholderUnresolved {
                        pod: pod.clone(),
                        container: name.to_owned(),
                        command: command.to_owned(),
                    }
                })?;
                let path = [
                    pod_run.as_str(),
                    CONTAINER_WORK_FOLDER,
                    name,
                    &program.replace('/', WINDOWS_SEPARATOR),
                ]
                .join(WINDOWS_SEPARATOR);
                paths.push(ExecutablePath {
                    pod: pod.clone(),
                    container: name.to_owned(),
                    command: command.to_owned(),
                    path,
                });
            }
        }
    }
    Ok(paths)
}

pub fn ensure_windows_executable_paths_fit(
    instance: &str,
    data_folder: &str,
    pods: &RenderedConfigmap,
    system: &SystemValues,
) -> Result<(), ProviderError> {
    let paths = windows_executable_paths(data_folder, pods, system)?;
    let Some(longest) = paths.iter().max_by_key(|path| path.length()) else {
        return Ok(());
    };
    if longest.length() <= WINDOWS_PATH_LIMIT {
        return Ok(());
    }
    Err(ProviderError::ExecutablePathTooLong {
        instance: instance.to_owned(),
        path: longest.path.clone(),
        length: longest.length(),
        limit: WINDOWS_PATH_LIMIT,
    })
}

fn workload_pod_spec(document: &Value) -> Option<&Value> {
    let spec = &document["spec"];
    match document["kind"].as_str()? {
        "Deployment" | "Job" => Some(&spec["template"]["spec"]),
        "CronJob" => Some(&spec["jobTemplate"]["spec"]["template"]["spec"]),
        "Pod" => Some(spec),
        _ => None,
    }
}

fn resolve_system_placeholders(program: &str, system: &SystemValues) -> Option<String> {
    let mut unresolved = false;
    let resolved = SYSTEM_PLACEHOLDER.replace_all(program, |captures: &regex::Captures| {
        let value = SystemKey::from_str(&captures[1])
            .ok()
            .and_then(|key| system.lookup(key));
        unresolved |= value.is_none();
        value.unwrap_or_default().to_owned()
    });
    (!unresolved && !resolved.contains(PLACEHOLDER_OPENER)).then(|| resolved.into_owned())
}

#[cfg(test)]
mod tests;
