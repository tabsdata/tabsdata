//
// Copyright 2026 Tabs Data Inc.
//

use std::io::Read;

use k8s_openapi::api::batch::v1::Job;
use kube::Api;
use kube::api::PostParams;
use td_common::status::ExitStatus::{GeneralError, TabsDataError};

use crate::cerror;
use crate::instance::profile::Profile;
use crate::kubernetes::client::build_kube_client;
use crate::pod::ValidationError;
use crate::provider::Provider;
use crate::system::exit::{Result, abort};

const INIT_PHASE_PREFIX: &str = "pods/init/";

pub async fn apply_init_phase(provider: &dyn Provider, profile: &Profile) -> Result {
    let topology = provider.topology();
    let config = provider.kube_config().await?;
    let client = build_kube_client(&config)?;
    let api: Api<Job> = Api::namespaced(client, &config.default_namespace);
    for path in profile.manifest.entries.keys() {
        if !path.starts_with(INIT_PHASE_PREFIX) {
            continue;
        }
        let yaml = read_pod_manifest(profile, path)?;
        let yaml = resolve_pod_manifest(&yaml)?;
        let mut job = deserialize_init_job(path, &yaml)?;
        topology.enrich_init_job(&mut job)?;
        report_validation(path, topology.validate_init_job(&job))?;
        if let Err(error) = api.create(&PostParams::default(), &job).await {
            cerror!(
                "Failed to apply init Job from '{path}' to namespace '{}': {error}",
                config.default_namespace
            );
            return abort(GeneralError);
        }
    }
    Ok(())
}

fn resolve_pod_manifest(yaml: &str) -> Result<String> {
    Ok(yaml.to_owned())
}

fn report_validation(path: &str, errors: Vec<ValidationError>) -> Result {
    if errors.is_empty() {
        return Ok(());
    }
    cerror!("Init pod manifest at '{path}' failed topology validation:");
    for error in &errors {
        cerror!("  - {}: {}", error.path, error.message);
    }
    abort(TabsDataError)
}

fn read_pod_manifest(profile: &Profile, path: &str) -> Result<String> {
    let target = match profile.tree.join(path) {
        Ok(target) => target,
        Err(error) => {
            cerror!("Failed to resolve virtual path '{path}': {error}");
            return abort(GeneralError);
        }
    };
    let mut reader = match target.open_file() {
        Ok(reader) => reader,
        Err(error) => {
            cerror!(
                "Failed to open virtual file '{}': {error}",
                target.as_str()
            );
            return abort(GeneralError);
        }
    };
    let mut yaml = String::new();
    if let Err(error) = reader.read_to_string(&mut yaml) {
        cerror!(
            "Failed to read virtual file '{}' as UTF-8: {error}",
            target.as_str()
        );
        return abort(GeneralError);
    }
    Ok(yaml)
}

fn deserialize_init_job(path: &str, yaml: &str) -> Result<Job> {
    match serde_yaml::from_str(yaml) {
        Ok(job) => Ok(job),
        Err(error) => {
            cerror!("Failed to deserialize init pod manifest at '{path}' as Job: {error}");
            abort(TabsDataError)
        }
    }
}
