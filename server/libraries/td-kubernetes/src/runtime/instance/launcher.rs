//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeSet;

use indicatif::MultiProgress;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{Pod, Service, ServiceAccount};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::api::rbac::v1::{Role, RoleBinding};
use kube::runtime::wait::Condition;
use serde::de::DeserializeOwned;

use crate::base::constants::DbManagerMode;
use crate::base::constants::SERVICE_TYPE_LOAD_BALANCER;
use crate::base::constants::{
    POD_PHASE_FAILED, POD_YAML_COMPONENT_LABEL, POD_YAML_FIELD_KIND, SEQUENCE_LABEL, TIER_LABEL,
};
use crate::base::error::launcher::LauncherError;
use crate::base::error::provider::ProviderError;
use crate::client::tdkserver::widgets::phase_spinner_in;
use crate::core::pod::ValidationError;
use crate::core::system::exit::{CommandError, CommandResult};
use crate::kube::api::deployment::is_deployment_available;
use crate::kube::api::{
    cronjob as cronjob_api, deployment as deployment_api, disruption as disruption_api,
    job as job_api, pod as pod_api, rbac as rbac_api, service as service_api,
};
use crate::kube::context::KubeContext;
use crate::runtime::gateway::messaging::messenger::template::TemplateKind;
use crate::runtime::gateway::resolver::{ConfigurationResolver, SystemValues};
use crate::runtime::instance::injection::{
    inject_into_cronjob, inject_into_deployment, inject_into_job, inject_into_pod,
    list_available_secrets,
};
use crate::runtime::instance::ports::AssignedPort;
use crate::runtime::instance::profile::Profile;
use crate::runtime::instance::tier::Tier;
use crate::runtime::provider::ClusterProvider;

const DATABASE_COMPONENT: &str = "database";
const DBMANAGER_COMPONENT: &str = "dbmanager";
const PYMANAGER_COMPONENT: &str = "pymanager";

const JOB_CONDITION_COMPLETE: &str = "Complete";
const JOB_CONDITION_FAILED: &str = "Failed";
const JOB_CONDITION_STATUS_TRUE: &str = "True";
const POD_CONDITION_READY: &str = "Ready";
const POD_CONDITION_STATUS_TRUE: &str = "True";

pub async fn ensure_ports_unclaimed(
    context: &KubeContext,
    assigned: &[AssignedPort],
    filter: &[&str],
) -> CommandResult {
    let services = service_api::list_services_everywhere(context, None).await?;
    for entry in assigned {
        if !filter.is_empty() && !filter.contains(&entry.role.as_str()) {
            continue;
        }
        for service in &services {
            if service.metadata.namespace.as_deref() == Some(context.namespace()) {
                continue;
            }
            if !claims_port_unbound(service, entry.port) {
                continue;
            }
            return Err(ProviderError::PortClaimedByService {
                port: entry.port,
                caption: entry.caption.clone(),
                namespace: service.metadata.namespace.clone().unwrap_or_default(),
                service: service.metadata.name.clone().unwrap_or_default(),
            }
            .into());
        }
    }
    Ok(())
}

fn claims_port_unbound(service: &Service, port: u16) -> bool {
    let Some(spec) = &service.spec else {
        return false;
    };
    if spec.type_.as_deref() != Some(SERVICE_TYPE_LOAD_BALANCER) {
        return false;
    }
    let claims = spec
        .ports
        .iter()
        .flatten()
        .any(|declared| declared.port == i32::from(port));
    if !claims {
        return false;
    }
    let bound = service
        .status
        .as_ref()
        .and_then(|status| status.load_balancer.as_ref())
        .and_then(|balancer| balancer.ingress.as_ref())
        .is_some_and(|ingress| !ingress.is_empty());
    !bound
}

pub async fn start_workload(
    context: &KubeContext,
    cluster_provider: ClusterProvider,
    system: &SystemValues,
    profile: &Profile,
    multi: &MultiProgress,
) -> CommandResult {
    delete_workload_controllers(context, multi).await?;
    delete_ephemeral_workload(context, multi).await?;
    delete_non_ephemeral_workload(context, multi).await?;
    create_non_ephemeral_workload(context, cluster_provider, system, profile, multi).await
}

async fn create_non_ephemeral_workload(
    context: &KubeContext,
    cluster_provider: ClusterProvider,
    system: &SystemValues,
    profile: &Profile,
    multi: &MultiProgress,
) -> CommandResult {
    apply_authorization_phase(context, system, profile, multi).await?;
    let server_pods = apply_server_phase(context, cluster_provider, system, profile, multi).await?;
    wait_server_phase(context, &server_pods, multi).await?;
    apply_endpoint_phase(context, system, profile, multi).await?;
    run_init_phase(context, cluster_provider, system, profile, multi).await?;
    let regular_pods = apply_regular_phase(
        context,
        cluster_provider,
        system,
        profile,
        multi,
        RegularKind::Pod,
    )
    .await?;
    wait_regular_phase(context, &regular_pods, multi).await?;
    let regular_cronjobs = apply_regular_phase(
        context,
        cluster_provider,
        system,
        profile,
        multi,
        RegularKind::CronJob,
    )
    .await?;
    wait_regular_phase(context, &regular_cronjobs, multi).await?;
    Ok(())
}

pub async fn stop_workload(context: &KubeContext, multi: &MultiProgress) -> CommandResult<bool> {
    let present = !present_non_ephemeral_names(context).await?.is_empty();
    delete_workload_controllers(context, multi).await?;
    delete_ephemeral_workload(context, multi).await?;
    delete_non_ephemeral_workload(context, multi).await?;
    Ok(present)
}

pub async fn apply_authorization_phase(
    context: &KubeContext,
    system: &SystemValues,
    profile: &Profile,
    multi: &MultiProgress,
) -> CommandResult {
    let entries = pods_for_tier(profile, Tier::Authorization)?;
    if entries.is_empty() {
        return Ok(());
    }
    let phase = phase_spinner_in(
        multi,
        &format!("Applying {} authorization(s)", entries.len()),
    );
    let mut ordered: Vec<(&str, &str, TemplateKind)> = Vec::new();
    for (key, yaml) in entries {
        let kind = kind_of_entry(key, yaml)?;
        ordered.push((key, yaml, kind));
    }
    ordered.sort_by_key(|(_, _, kind)| match kind {
        TemplateKind::ServiceAccount => 0,
        TemplateKind::Role => 1,
        TemplateKind::RoleBinding => 2,
        TemplateKind::PodDisruptionBudget => 3,
        _ => 4,
    });
    for (key, yaml, kind) in ordered {
        let bar = phase.add_resource(key);
        let outcome = match kind {
            TemplateKind::ServiceAccount => {
                let account =
                    deserialize_pod_entry::<ServiceAccount>(key, yaml, Tier::Authorization, system)
                        .await?;
                rbac_api::upsert_service_account(context, &account, None)
                    .await
                    .map(|_| ())
            }
            TemplateKind::Role => {
                let role =
                    deserialize_pod_entry::<Role>(key, yaml, Tier::Authorization, system).await?;
                rbac_api::upsert_role(context, &role, None)
                    .await
                    .map(|_| ())
            }
            TemplateKind::RoleBinding => {
                let binding =
                    deserialize_pod_entry::<RoleBinding>(key, yaml, Tier::Authorization, system)
                        .await?;
                rbac_api::upsert_role_binding(context, &binding, None)
                    .await
                    .map(|_| ())
            }
            TemplateKind::PodDisruptionBudget => {
                let budget = deserialize_pod_entry::<PodDisruptionBudget>(
                    key,
                    yaml,
                    Tier::Authorization,
                    system,
                )
                .await?;
                disruption_api::upsert_disruption_budget(context, &budget, None)
                    .await
                    .map(|_| ())
            }
            _ => {
                bar.finish_with_message(format!("{key} (skipped)"));
                continue;
            }
        };
        match outcome {
            Ok(()) => bar.finish_with_message(format!("{key} (applied)")),
            Err(error) => {
                bar.finish_with_message(format!("{key} (failed)"));
                return Err(error.into());
            }
        }
    }
    drop(phase);
    Ok(())
}

pub async fn apply_endpoint_phase(
    context: &KubeContext,
    system: &SystemValues,
    profile: &Profile,
    multi: &MultiProgress,
) -> CommandResult {
    let entries = pods_for_tier(profile, Tier::Endpoint)?;
    if entries.is_empty() {
        return Ok(());
    }
    let phase = phase_spinner_in(multi, &format!("Applying {} endpoint(s)", entries.len()));
    for (key, yaml) in entries {
        let bar = phase.add_resource(key);
        let service = deserialize_pod_entry::<Service>(key, yaml, Tier::Endpoint, system).await?;
        match service_api::upsert_service(context, &service, None).await {
            Ok(_) => bar.finish_with_message(format!("{key} (applied)")),
            Err(error) => {
                bar.finish_with_message(format!("{key} (failed)"));
                return Err(error.into());
            }
        }
    }
    drop(phase);
    Ok(())
}

async fn apply_init_phase(
    context: &KubeContext,
    cluster_provider: ClusterProvider,
    system: &SystemValues,
    profile: &Profile,
) -> CommandResult<Vec<(i64, Vec<(String, Job)>)>> {
    let available_secrets = list_available_secrets(context).await?;
    let mut prepared: Vec<(i64, String, Job)> = Vec::new();
    for (key, yaml) in pods_for_tier(profile, Tier::Init)? {
        let sequence = init_sequence(key, yaml)?;
        let mut job = deserialize_pod_entry::<Job>(key, yaml, Tier::Init, system).await?;
        cluster_provider.enrich_init_job(&mut job)?;
        inject_into_job(profile, key, &mut job, &available_secrets)?;
        validate_pod(key, cluster_provider.validate_init_job(&job))?;
        prepared.push((sequence, key.to_owned(), job));
    }
    prepared.sort_by(|left, right| left.0.cmp(&right.0));
    for pair in prepared.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(LauncherError::DuplicateInitSequence {
                first: pair[0].1.clone(),
                second: pair[1].1.clone(),
                sequence: pair[0].0,
                label: SEQUENCE_LABEL.to_owned(),
            }
            .into());
        }
    }
    let mut groups: Vec<(i64, Vec<(String, Job)>)> = Vec::new();
    for (sequence, key, job) in prepared {
        match groups.last_mut() {
            Some((current, batch)) if *current == sequence => batch.push((key, job)),
            _ => groups.push((sequence, vec![(key, job)])),
        }
    }
    Ok(groups)
}

async fn apply_init_group(
    context: &KubeContext,
    group: Vec<(String, Job)>,
    multi: &MultiProgress,
) -> CommandResult<Vec<String>> {
    let phase = phase_spinner_in(multi, &format!("Applying {} init job(s)", group.len()));
    let mut applied: Vec<String> = Vec::new();
    for (key, job) in group {
        let bar = phase.add_resource(&key);
        let created =
            match job_api::create_job(context, &job, Vec::new(), Vec::new(), None, None).await {
                Ok(created) => {
                    bar.finish_with_message(format!("{key} (applied)"));
                    created
                }
                Err(error) => {
                    bar.finish_with_message(format!("{key} (failed)"));
                    return Err(error.into());
                }
            };
        let name = created
            .metadata
            .name
            .ok_or_else(|| LauncherError::JobMissingName { key: key.clone() })?;
        applied.push(name);
    }
    Ok(applied)
}

pub async fn run_init_phase(
    context: &KubeContext,
    cluster_provider: ClusterProvider,
    system: &SystemValues,
    profile: &Profile,
    multi: &MultiProgress,
) -> CommandResult {
    let groups = apply_init_phase(context, cluster_provider, system, profile).await?;
    for (_, group) in groups {
        let applied = apply_init_group(context, group, multi).await?;
        wait_init_phase(context, &applied, multi).await?;
    }
    Ok(())
}

pub async fn wait_init_phase(
    context: &KubeContext,
    jobs: &[String],
    multi: &MultiProgress,
) -> CommandResult {
    let phase = phase_spinner_in(multi, &format!("Waiting for {} init job(s)", jobs.len()));
    for name in jobs {
        let bar = phase.add_resource(name);
        let settled =
            match job_api::await_job_condition_forever(context, name, is_job_settled()).await {
                Ok(Some(job)) => job,
                Ok(None) => {
                    bar.finish_with_message(format!("{name} (deleted)"));
                    return Err(
                        LauncherError::JobDeletedBeforeCompletion { name: name.clone() }.into(),
                    );
                }
                Err(error) => {
                    bar.finish_with_message(format!("{name} (errored)"));
                    return Err(error.into());
                }
            };
        if has_failed_condition(&settled) {
            bar.finish_with_message(format!("{name} (failed)"));
            return Err(LauncherError::JobFailed { name: name.clone() }.into());
        }
        bar.finish_with_message(format!("{name} (completed)"));
    }
    Ok(())
}

pub async fn apply_server_phase(
    context: &KubeContext,
    cluster_provider: ClusterProvider,
    system: &SystemValues,
    profile: &Profile,
    multi: &MultiProgress,
) -> CommandResult<Vec<(TemplateKind, String)>> {
    let available_secrets = list_available_secrets(context).await?;
    let mut prepared: Vec<(String, ServerWorkload)> = Vec::new();
    for (key, yaml) in pods_for_tier(profile, Tier::Server)? {
        match kind_of_entry(key, yaml)? {
            TemplateKind::Deployment => {
                let mut deployment =
                    deserialize_pod_entry::<Deployment>(key, yaml, Tier::Server, system).await?;
                cluster_provider.enrich_regular_deployment(&mut deployment)?;
                inject_into_deployment(profile, key, &mut deployment, &available_secrets)?;
                validate_pod(
                    key,
                    cluster_provider.validate_regular_deployment(&deployment),
                )?;
                prepared.push((
                    key.to_owned(),
                    ServerWorkload::Deployment(Box::new(deployment)),
                ));
            }
            TemplateKind::Pod => {
                let mut pod = deserialize_pod_entry::<Pod>(key, yaml, Tier::Server, system).await?;
                cluster_provider.enrich_regular_pod(&mut pod)?;
                inject_into_pod(profile, key, &mut pod, &available_secrets)?;
                validate_pod(key, cluster_provider.validate_regular_pod(&pod))?;
                prepared.push((key.to_owned(), ServerWorkload::Pod(Box::new(pod))));
            }
            kind => {
                return Err(LauncherError::PodDeserializeFailed {
                    key: key.to_owned(),
                    tier: Tier::Server.to_string(),
                    cause: format!("unsupported kind '{kind:?}' for the server tier"),
                }
                .into());
            }
        }
    }
    let phase = phase_spinner_in(multi, &format!("Applying {} server pod(s)", prepared.len()));
    let mut applied: Vec<(TemplateKind, String)> = Vec::new();
    for (key, workload) in prepared {
        let bar = phase.add_resource(&key);
        let named = match workload {
            ServerWorkload::Deployment(deployment) => {
                match deployment_api::create_deployment(
                    context,
                    &deployment,
                    Vec::new(),
                    Vec::new(),
                    None,
                    None,
                )
                .await
                {
                    Ok(created) => {
                        bar.finish_with_message(format!("{key} (applied)"));
                        (
                            TemplateKind::Deployment,
                            created.metadata.name.ok_or_else(|| {
                                LauncherError::DeploymentMissingName { key: key.clone() }
                            })?,
                        )
                    }
                    Err(error) => {
                        bar.finish_with_message(format!("{key} (failed)"));
                        return Err(error.into());
                    }
                }
            }
            ServerWorkload::Pod(pod) => {
                match pod_api::create_pod(context, &pod, Vec::new(), Vec::new(), None, None).await {
                    Ok(created) => {
                        bar.finish_with_message(format!("{key} (applied)"));
                        (
                            TemplateKind::Pod,
                            created
                                .metadata
                                .name
                                .ok_or_else(|| LauncherError::PodMissingName {
                                    key: key.clone(),
                                })?,
                        )
                    }
                    Err(error) => {
                        bar.finish_with_message(format!("{key} (failed)"));
                        return Err(error.into());
                    }
                }
            }
        };
        applied.push(named);
    }
    drop(phase);
    Ok(applied)
}

pub async fn wait_server_phase(
    context: &KubeContext,
    workloads: &[(TemplateKind, String)],
    multi: &MultiProgress,
) -> CommandResult {
    let phase = phase_spinner_in(
        multi,
        &format!("Waiting for {} server pod(s)", workloads.len()),
    );
    for (kind, name) in workloads {
        let bar = phase.add_resource(name);
        if matches!(kind, TemplateKind::Deployment) {
            match deployment_api::await_deployment_condition_forever(
                context,
                name,
                is_deployment_available(),
            )
            .await
            {
                Ok(Some(_)) => {
                    bar.finish_with_message(format!("{name} (ready)"));
                    continue;
                }
                Ok(None) => {
                    bar.finish_with_message(format!("{name} (deleted)"));
                    return Err(LauncherError::DeploymentDeletedBeforeAvailable {
                        name: name.clone(),
                    }
                    .into());
                }
                Err(error) => {
                    bar.finish_with_message(format!("{name} (errored)"));
                    return Err(error.into());
                }
            }
        }
        let settled =
            match pod_api::await_pod_condition_forever(context, name, is_pod_settled()).await {
                Ok(Some(pod)) => pod,
                Ok(None) => {
                    bar.finish_with_message(format!("{name} (deleted)"));
                    return Err(LauncherError::PodDeletedBeforeReady { name: name.clone() }.into());
                }
                Err(error) => {
                    bar.finish_with_message(format!("{name} (errored)"));
                    return Err(error.into());
                }
            };
        if has_failed_phase(&settled) {
            bar.finish_with_message(format!("{name} (failed)"));
            return Err(LauncherError::PodFailed { name: name.clone() }.into());
        }
        bar.finish_with_message(format!("{name} (ready)"));
    }
    drop(phase);
    Ok(())
}

fn is_job_settled() -> impl Condition<Job> + Clone {
    |job: Option<&Job>| {
        let Some(status) = job.and_then(|current| current.status.as_ref()) else {
            return false;
        };
        let Some(conditions) = status.conditions.as_ref() else {
            return false;
        };
        conditions.iter().any(|condition| {
            (condition.type_ == JOB_CONDITION_COMPLETE || condition.type_ == JOB_CONDITION_FAILED)
                && condition.status == JOB_CONDITION_STATUS_TRUE
        })
    }
}

fn has_failed_condition(job: &Job) -> bool {
    let Some(status) = job.status.as_ref() else {
        return false;
    };
    let Some(conditions) = status.conditions.as_ref() else {
        return false;
    };
    conditions.iter().any(|condition| {
        condition.type_ == JOB_CONDITION_FAILED && condition.status == JOB_CONDITION_STATUS_TRUE
    })
}

fn validate_pod(key: &str, errors: Vec<ValidationError>) -> CommandResult {
    if errors.is_empty() {
        return Ok(());
    }
    let rendered = errors
        .iter()
        .map(|error| format!("  - {}: {}", error.path, error.message))
        .collect::<Vec<_>>()
        .join("\n");
    Err(LauncherError::PodValidationFailed {
        key: key.to_owned(),
        errors: rendered,
    }
    .into())
}

fn init_sequence(key: &str, yaml: &str) -> Result<i64, LauncherError> {
    let value: serde_yaml::Value =
        serde_yaml::from_str(yaml).map_err(|error| LauncherError::PodYamlParseFailed {
            key: key.to_owned(),
            cause: error.to_string(),
        })?;
    let label = value
        .get("metadata")
        .and_then(|metadata| metadata.get("labels"))
        .and_then(|labels| labels.get(SEQUENCE_LABEL));
    let Some(label) = label else {
        return Err(LauncherError::MissingInitSequence {
            key: key.to_owned(),
            label: SEQUENCE_LABEL.to_owned(),
        });
    };
    if let Some(sequence) = label.as_i64() {
        return Ok(sequence);
    }
    let rendered = label.as_str().unwrap_or_default();
    rendered
        .trim()
        .parse::<i64>()
        .map_err(|_| LauncherError::InvalidInitSequence {
            key: key.to_owned(),
            value: rendered.to_owned(),
            label: SEQUENCE_LABEL.to_owned(),
        })
}

fn pods_for_tier(profile: &Profile, tier: Tier) -> Result<Vec<(&str, &str)>, LauncherError> {
    let mut matched = Vec::new();
    for (key, yaml) in &profile.pods {
        let value: serde_yaml::Value =
            serde_yaml::from_str(yaml).map_err(|error| LauncherError::PodYamlParseFailed {
                key: key.clone(),
                cause: error.to_string(),
            })?;
        let label_value = value
            .get("metadata")
            .and_then(|metadata| metadata.get("labels"))
            .and_then(|labels| labels.get(TIER_LABEL))
            .and_then(|value| value.as_str());
        let Some(label_value) = label_value else {
            return Err(LauncherError::PodMissingTierLabel {
                key: key.clone(),
                label: TIER_LABEL.to_owned(),
            });
        };
        let entry_tier =
            Tier::from_label(label_value).ok_or_else(|| LauncherError::PodUnknownTierLabel {
                key: key.clone(),
                label: TIER_LABEL.to_owned(),
                value: label_value.to_owned(),
            })?;
        if entry_tier == tier {
            matched.push((key.as_str(), yaml.as_str()));
        }
    }
    Ok(matched)
}

async fn deserialize_pod_entry<T: DeserializeOwned>(
    key: &str,
    yaml: &str,
    tier: Tier,
    system: &SystemValues,
) -> Result<T, LauncherError> {
    let resolver = ConfigurationResolver::new(system.clone());
    let materialized =
        resolver
            .materialize(yaml)
            .await
            .map_err(|error| LauncherError::PodMaterializeFailed {
                key: key.to_owned(),
                tier: tier.to_string(),
                cause: error.to_string(),
            })?;
    serde_yaml::from_str(&materialized).map_err(|error| LauncherError::PodDeserializeFailed {
        key: key.to_owned(),
        tier: tier.to_string(),
        cause: error.to_string(),
    })
}

enum ServerWorkload {
    Pod(Box<Pod>),
    Deployment(Box<Deployment>),
}

enum RegularWorkload {
    Pod(Box<Pod>),
    Deployment(Box<Deployment>),
    CronJob(Box<CronJob>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegularKind {
    Pod,
    CronJob,
}

pub async fn apply_regular_phase(
    context: &KubeContext,
    cluster_provider: ClusterProvider,
    system: &SystemValues,
    profile: &Profile,
    multi: &MultiProgress,
    kind: RegularKind,
) -> CommandResult<Vec<(TemplateKind, String)>> {
    let available_secrets = list_available_secrets(context).await?;
    let mut prepared: Vec<(String, RegularWorkload)> = Vec::new();
    for (key, yaml) in pods_for_tier(profile, Tier::Regular)? {
        match (kind, kind_of_entry(key, yaml)?) {
            (RegularKind::CronJob, TemplateKind::CronJob) => {
                let mut cronjob =
                    deserialize_pod_entry::<CronJob>(key, yaml, Tier::Regular, system).await?;
                cluster_provider.enrich_regular_cronjob(&mut cronjob)?;
                inject_into_cronjob(profile, key, &mut cronjob, &available_secrets)?;
                validate_pod(key, cluster_provider.validate_regular_cronjob(&cronjob))?;
                prepared.push((key.to_owned(), RegularWorkload::CronJob(Box::new(cronjob))));
            }
            (RegularKind::Pod, TemplateKind::Deployment) => {
                let mut deployment =
                    deserialize_pod_entry::<Deployment>(key, yaml, Tier::Regular, system).await?;
                cluster_provider.enrich_regular_deployment(&mut deployment)?;
                inject_into_deployment(profile, key, &mut deployment, &available_secrets)?;
                validate_pod(
                    key,
                    cluster_provider.validate_regular_deployment(&deployment),
                )?;
                prepared.push((
                    key.to_owned(),
                    RegularWorkload::Deployment(Box::new(deployment)),
                ));
            }
            (RegularKind::Pod, TemplateKind::Pod | TemplateKind::Job) => {
                let mut pod =
                    deserialize_pod_entry::<Pod>(key, yaml, Tier::Regular, system).await?;
                cluster_provider.enrich_regular_pod(&mut pod)?;
                inject_into_pod(profile, key, &mut pod, &available_secrets)?;
                validate_pod(key, cluster_provider.validate_regular_pod(&pod))?;
                prepared.push((key.to_owned(), RegularWorkload::Pod(Box::new(pod))));
            }
            _ => {}
        }
    }
    let phase = phase_spinner_in(
        multi,
        &format!("Applying {} regular workload(s)", prepared.len()),
    );
    let creates = prepared.into_iter().map(|(key, workload)| {
        let context = context.clone();
        let bar = phase.add_resource(&key);
        async move {
            match workload {
                RegularWorkload::Pod(pod) => {
                    let created = match pod_api::create_pod(
                        &context,
                        &pod,
                        Vec::new(),
                        Vec::new(),
                        None,
                        None,
                    )
                    .await
                    {
                        Ok(created) => created,
                        Err(error) => {
                            bar.finish_with_message(format!("{key} (failed)"));
                            return Err::<(TemplateKind, String), CommandError>(error.into());
                        }
                    };
                    match created.metadata.name {
                        Some(name) => {
                            bar.finish_with_message(format!("{key} (applied)"));
                            Ok((TemplateKind::Pod, name))
                        }
                        None => {
                            bar.finish_with_message(format!("{key} (no name)"));
                            Err(LauncherError::PodMissingName { key: key.clone() }.into())
                        }
                    }
                }
                RegularWorkload::Deployment(deployment) => {
                    let created = match deployment_api::create_deployment(
                        &context,
                        &deployment,
                        Vec::new(),
                        Vec::new(),
                        None,
                        None,
                    )
                    .await
                    {
                        Ok(created) => created,
                        Err(error) => {
                            bar.finish_with_message(format!("{key} (failed)"));
                            return Err::<(TemplateKind, String), CommandError>(error.into());
                        }
                    };
                    match created.metadata.name {
                        Some(name) => {
                            bar.finish_with_message(format!("{key} (applied)"));
                            Ok((TemplateKind::Deployment, name))
                        }
                        None => {
                            bar.finish_with_message(format!("{key} (no name)"));
                            Err(LauncherError::DeploymentMissingName { key: key.clone() }.into())
                        }
                    }
                }
                RegularWorkload::CronJob(cronjob) => {
                    let created = match cronjob_api::create_cronjob(
                        &context,
                        &cronjob,
                        Vec::new(),
                        Vec::new(),
                        None,
                        None,
                    )
                    .await
                    {
                        Ok(created) => created,
                        Err(error) => {
                            bar.finish_with_message(format!("{key} (failed)"));
                            return Err(error.into());
                        }
                    };
                    match created.metadata.name {
                        Some(name) => {
                            bar.finish_with_message(format!("{key} (applied)"));
                            Ok((TemplateKind::CronJob, name))
                        }
                        None => {
                            bar.finish_with_message(format!("{key} (no name)"));
                            Err(LauncherError::CronJobMissingName { key: key.clone() }.into())
                        }
                    }
                }
            }
        }
    });
    let result = futures::future::try_join_all(creates).await;
    drop(phase);
    result
}

pub async fn wait_regular_phase(
    context: &KubeContext,
    workloads: &[(TemplateKind, String)],
    multi: &MultiProgress,
) -> CommandResult {
    let phase = phase_spinner_in(
        multi,
        &format!("Waiting for {} regular workload(s)", workloads.len()),
    );
    let waits = workloads.iter().map(|(kind, name)| {
        let context = context.clone();
        let bar = phase.add_resource(name);
        async move {
            match kind {
                TemplateKind::CronJob => {
                    match cronjob_api::await_cronjob_condition_forever(
                        &context,
                        name,
                        cronjob_exists(),
                    )
                    .await
                    {
                        Ok(Some(_)) => {
                            bar.finish_with_message(format!("{name} (scheduled)"));
                            Ok(())
                        }
                        Ok(None) => {
                            bar.finish_with_message(format!("{name} (deleted)"));
                            Err::<(), CommandError>(
                                LauncherError::CronJobDeletedBeforeScheduled { name: name.clone() }
                                    .into(),
                            )
                        }
                        Err(error) => {
                            bar.finish_with_message(format!("{name} (errored)"));
                            Err(error.into())
                        }
                    }
                }
                TemplateKind::Deployment => {
                    match deployment_api::await_deployment_condition_forever(
                        &context,
                        name,
                        is_deployment_available(),
                    )
                    .await
                    {
                        Ok(Some(_)) => {
                            bar.finish_with_message(format!("{name} (ready)"));
                            Ok(())
                        }
                        Ok(None) => {
                            bar.finish_with_message(format!("{name} (deleted)"));
                            Err::<(), CommandError>(
                                LauncherError::DeploymentDeletedBeforeAvailable {
                                    name: name.clone(),
                                }
                                .into(),
                            )
                        }
                        Err(error) => {
                            bar.finish_with_message(format!("{name} (errored)"));
                            Err(error.into())
                        }
                    }
                }
                TemplateKind::Pod | TemplateKind::Job => {
                    let settled = match pod_api::await_pod_condition_forever(
                        &context,
                        name,
                        is_pod_settled(),
                    )
                    .await
                    {
                        Ok(Some(pod)) => pod,
                        Ok(None) => {
                            bar.finish_with_message(format!("{name} (deleted)"));
                            return Err::<(), CommandError>(
                                LauncherError::PodDeletedBeforeReady { name: name.clone() }.into(),
                            );
                        }
                        Err(error) => {
                            bar.finish_with_message(format!("{name} (errored)"));
                            return Err(error.into());
                        }
                    };
                    if has_failed_phase(&settled) {
                        bar.finish_with_message(format!("{name} (failed)"));
                        return Err(LauncherError::PodFailed { name: name.clone() }.into());
                    }
                    bar.finish_with_message(format!("{name} (ready)"));
                    Ok(())
                }
                TemplateKind::Service
                | TemplateKind::ServiceAccount
                | TemplateKind::Role
                | TemplateKind::RoleBinding
                | TemplateKind::PodDisruptionBudget => {
                    bar.finish_with_message(format!("{name} (unexpected)"));
                    Err(LauncherError::UnawaitableWorkloadKind {
                        name: name.clone(),
                        kind: format!("{kind:?}"),
                    }
                    .into())
                }
            }
        }
    });
    let result = futures::future::try_join_all(waits).await;
    drop(phase);
    result?;
    Ok(())
}

fn cronjob_exists() -> impl Condition<CronJob> + Clone {
    |cronjob: Option<&CronJob>| cronjob.is_some()
}

fn kind_of_entry(key: &str, yaml: &str) -> Result<TemplateKind, LauncherError> {
    let value: serde_yaml::Value =
        serde_yaml::from_str(yaml).map_err(|error| LauncherError::PodYamlParseFailed {
            key: key.to_owned(),
            cause: error.to_string(),
        })?;
    let kind_text = value
        .get(POD_YAML_FIELD_KIND)
        .and_then(serde_yaml::Value::as_str)
        .unwrap_or_default();
    TemplateKind::from_kind(kind_text).ok_or_else(|| LauncherError::PodDeserializeFailed {
        key: key.to_owned(),
        tier: Tier::Regular.to_string(),
        cause: format!("unsupported kind '{kind_text}'"),
    })
}

fn is_job_absent() -> impl Condition<Job> + Clone {
    |job: Option<&Job>| job.is_none()
}

fn job_active_pods(job: &Job) -> i32 {
    job.status
        .as_ref()
        .and_then(|status| status.active)
        .unwrap_or_default()
}

fn is_job_reclaimable(job: &Job) -> bool {
    is_job_settled().matches_object(Some(job)) && job_active_pods(job) == 0
}

async fn clear_settled_job(
    context: &KubeContext,
    component: &str,
    name: &str,
) -> CommandResult<bool> {
    let Some(existing) = job_api::get_job(context, name).await? else {
        return Ok(false);
    };
    if !is_job_reclaimable(&existing) {
        let active = job_active_pods(&existing);
        return Err(LauncherError::ProvisionJobInProgress {
            component: component.to_owned(),
            name: name.to_owned(),
            started: existing
                .status
                .as_ref()
                .and_then(|status| status.start_time.as_ref())
                .map(|time| format!("at {}", time.0.to_rfc3339()))
                .unwrap_or_else(|| "at an unknown time".to_owned()),
            active,
        }
        .into());
    }
    job_api::delete_job(context, name).await?;
    job_api::await_job_condition_forever(context, name, is_job_absent()).await?;
    Ok(true)
}

pub async fn provision_pyvenvs(
    context: &KubeContext,
    cluster_provider: ClusterProvider,
    system: &SystemValues,
    profile: &Profile,
    multi: &MultiProgress,
) -> CommandResult {
    let available_secrets = list_available_secrets(context).await?;

    let (setup_key, setup_yaml) = pod_for_component(profile, PYMANAGER_COMPONENT)?;
    let mut setup =
        deserialize_pod_entry::<Job>(setup_key, setup_yaml, Tier::Setup, system).await?;
    cluster_provider.enrich_init_job(&mut setup)?;
    inject_into_job(profile, setup_key, &mut setup, &available_secrets)?;
    let setup_name = job_name(setup_key, &setup)?;

    let phase = phase_spinner_in(multi, "Provisioning tabsdata python environments");

    let bar = phase.add_resource(setup_key);
    if clear_settled_job(context, PYMANAGER_COMPONENT, &setup_name).await? {
        bar.set_message(format!("{setup_key} (replacing previous run)"));
    }
    job_api::create_job(context, &setup, Vec::new(), Vec::new(), None, None).await?;
    let settled =
        job_api::await_job_condition_forever(context, &setup_name, is_job_settled()).await?;
    if settled.as_ref().is_some_and(has_failed_condition) {
        bar.finish_with_message(format!("{setup_key} (failed)"));
        return Err(LauncherError::JobFailed { name: setup_name }.into());
    }
    bar.finish_with_message(format!("{setup_key} (completed)"));

    drop(phase);
    Ok(())
}

async fn prepare_database_workload(
    cluster_provider: ClusterProvider,
    system: &SystemValues,
    profile: &Profile,
    available_secrets: &BTreeSet<String>,
    key: &str,
    yaml: &str,
) -> CommandResult<ServerWorkload> {
    match kind_of_entry(key, yaml)? {
        TemplateKind::Deployment => {
            let mut deployment =
                deserialize_pod_entry::<Deployment>(key, yaml, Tier::Server, system).await?;
            cluster_provider.enrich_regular_deployment(&mut deployment)?;
            inject_into_deployment(profile, key, &mut deployment, available_secrets)?;
            Ok(ServerWorkload::Deployment(Box::new(deployment)))
        }
        TemplateKind::Pod => {
            let mut pod = deserialize_pod_entry::<Pod>(key, yaml, Tier::Server, system).await?;
            cluster_provider.enrich_regular_pod(&mut pod)?;
            inject_into_pod(profile, key, &mut pod, available_secrets)?;
            Ok(ServerWorkload::Pod(Box::new(pod)))
        }
        kind => Err(LauncherError::PodDeserializeFailed {
            key: key.to_owned(),
            tier: Tier::Server.to_string(),
            cause: format!("unsupported kind '{kind:?}' for the server tier"),
        }
        .into()),
    }
}

fn database_workload_name(key: &str, workload: &ServerWorkload) -> Result<String, LauncherError> {
    match workload {
        ServerWorkload::Deployment(deployment) => deployment_name(key, deployment),
        ServerWorkload::Pod(pod) => pod_name(key, pod),
    }
}

async fn start_database_workload(
    context: &KubeContext,
    workload: &ServerWorkload,
    name: &str,
) -> CommandResult {
    match workload {
        ServerWorkload::Deployment(deployment) => {
            deployment_api::create_deployment(
                context,
                deployment,
                Vec::new(),
                Vec::new(),
                None,
                None,
            )
            .await?;
            deployment_api::await_deployment_condition_forever(
                context,
                name,
                is_deployment_available(),
            )
            .await?;
        }
        ServerWorkload::Pod(pod) => {
            pod_api::create_pod(context, pod, Vec::new(), Vec::new(), None, None).await?;
            pod_api::await_pod_condition_forever(context, name, is_pod_settled()).await?;
        }
    }
    Ok(())
}

async fn delete_database_workload(
    context: &KubeContext,
    workload: &ServerWorkload,
    name: &str,
) -> CommandResult {
    match workload {
        ServerWorkload::Deployment(_) => {
            deployment_api::delete_deployment(context, name).await?;
        }
        ServerWorkload::Pod(_) => {
            pod_api::delete_pod(context, name).await?;
        }
    }
    Ok(())
}

async fn await_database_workload_absent(
    context: &KubeContext,
    workload: &ServerWorkload,
    name: &str,
) -> CommandResult {
    match workload {
        ServerWorkload::Deployment(_) => {
            deployment_api::await_deployment_condition_forever(
                context,
                name,
                is_deployment_absent(),
            )
            .await?;
        }
        ServerWorkload::Pod(_) => {
            pod_api::await_pod_condition_forever(context, name, is_pod_absent()).await?;
        }
    }
    Ok(())
}

pub async fn provision_database(
    context: &KubeContext,
    cluster_provider: ClusterProvider,
    system: &SystemValues,
    profile: &Profile,
    multi: &MultiProgress,
    mode: DbManagerMode,
) -> CommandResult {
    let mut system = system.clone();
    system.dbmanager_mode = Some(mode.as_str().to_owned());
    let system = &system;
    let available_secrets = list_available_secrets(context).await?;
    let hosts_database = cluster_provider.hosts_database();

    let server = if hosts_database {
        let (server_key, server_yaml) = pod_for_component(profile, DATABASE_COMPONENT)?;
        let workload = prepare_database_workload(
            cluster_provider,
            system,
            profile,
            &available_secrets,
            server_key,
            server_yaml,
        )
        .await?;
        let server_name = database_workload_name(server_key, &workload)?;
        Some((server_key, workload, server_name))
    } else {
        None
    };

    let (setup_key, setup_yaml) = pod_for_component(profile, DBMANAGER_COMPONENT)?;
    let mut setup =
        deserialize_pod_entry::<Job>(setup_key, setup_yaml, Tier::Setup, system).await?;
    cluster_provider.enrich_init_job(&mut setup)?;
    inject_into_job(profile, setup_key, &mut setup, &available_secrets)?;
    let setup_name = job_name(setup_key, &setup)?;

    let phase = phase_spinner_in(multi, "Provisioning tabsdata database");

    if let Some((server_key, workload, server_name)) = &server {
        let bar = phase.add_resource(server_key);
        start_database_workload(context, workload, server_name).await?;
        bar.finish_with_message(format!("{server_key} (started)"));
    }

    let bar = phase.add_resource(setup_key);
    if clear_settled_job(context, DBMANAGER_COMPONENT, &setup_name).await? {
        bar.set_message(format!("{setup_key} (replacing previous run)"));
    }
    job_api::create_job(context, &setup, Vec::new(), Vec::new(), None, None).await?;
    let settled =
        job_api::await_job_condition_forever(context, &setup_name, is_job_settled()).await?;
    if settled.as_ref().is_some_and(has_failed_condition) {
        bar.finish_with_message(format!("{setup_key} (failed)"));
        if let Some((_, workload, server_name)) = &server {
            delete_database_workload(context, workload, server_name).await?;
        }
        return Err(LauncherError::JobFailed { name: setup_name }.into());
    }
    bar.finish_with_message(format!("{setup_key} (completed)"));

    if let Some((server_key, workload, server_name)) = &server {
        let bar = phase.add_resource(server_key);
        delete_database_workload(context, workload, server_name).await?;
        await_database_workload_absent(context, workload, server_name).await?;
        bar.finish_with_message(format!("{server_key} (stopped)"));
    }

    drop(phase);
    Ok(())
}

fn is_pod_absent() -> impl Condition<Pod> + Clone {
    |pod: Option<&Pod>| pod.is_none()
}

pub async fn start_database(
    context: &KubeContext,
    cluster_provider: ClusterProvider,
    system: &SystemValues,
    profile: &Profile,
    multi: &MultiProgress,
) -> CommandResult {
    let available_secrets = list_available_secrets(context).await?;
    let (server_key, server_yaml) = pod_for_component(profile, DATABASE_COMPONENT)?;
    let workload = prepare_database_workload(
        cluster_provider,
        system,
        profile,
        &available_secrets,
        server_key,
        server_yaml,
    )
    .await?;
    let server_name = database_workload_name(server_key, &workload)?;

    let phase = phase_spinner_in(multi, "Starting tabsdata database");
    let bar = phase.add_resource(server_key);
    start_database_workload(context, &workload, &server_name).await?;
    bar.finish_with_message(format!("{server_key} (started)"));
    drop(phase);
    Ok(())
}

pub async fn stop_database(
    context: &KubeContext,
    profile: &Profile,
    multi: &MultiProgress,
) -> CommandResult {
    let (server_key, server_yaml) = pod_for_component(profile, DATABASE_COMPONENT)?;
    let service: Pod =
        serde_yaml::from_str(server_yaml).map_err(|error| LauncherError::PodDeserializeFailed {
            key: server_key.to_owned(),
            tier: Tier::Server.to_string(),
            cause: error.to_string(),
        })?;
    let server_name = pod_name(server_key, &service)?;

    let phase = phase_spinner_in(multi, "Stopping tabsdata database");
    let bar = phase.add_resource(server_key);
    pod_api::delete_pod(context, &server_name).await?;
    bar.finish_with_message(format!("{server_key} (stopped)"));
    drop(phase);
    Ok(())
}

fn pod_for_component<'a>(
    profile: &'a Profile,
    component: &str,
) -> Result<(&'a str, &'a str), LauncherError> {
    for (key, yaml) in &profile.pods {
        let value: serde_yaml::Value =
            serde_yaml::from_str(yaml).map_err(|error| LauncherError::PodYamlParseFailed {
                key: key.clone(),
                cause: error.to_string(),
            })?;
        let label_value = value
            .get("metadata")
            .and_then(|metadata| metadata.get("labels"))
            .and_then(|labels| labels.get(POD_YAML_COMPONENT_LABEL))
            .and_then(|value| value.as_str());
        if label_value == Some(component) {
            return Ok((key.as_str(), yaml.as_str()));
        }
    }
    Err(LauncherError::ComponentPodMissing {
        component: component.to_owned(),
    })
}

fn deployment_name(key: &str, deployment: &Deployment) -> Result<String, LauncherError> {
    deployment
        .metadata
        .name
        .clone()
        .ok_or_else(|| LauncherError::DeploymentMissingName {
            key: key.to_owned(),
        })
}

fn is_deployment_absent() -> impl Condition<Deployment> + Clone {
    |deployment: Option<&Deployment>| deployment.is_none()
}

fn pod_name(key: &str, pod: &Pod) -> Result<String, LauncherError> {
    pod.metadata
        .name
        .clone()
        .ok_or_else(|| LauncherError::PodMissingName {
            key: key.to_owned(),
        })
}

fn job_name(key: &str, job: &Job) -> Result<String, LauncherError> {
    job.metadata
        .name
        .clone()
        .ok_or_else(|| LauncherError::JobMissingName {
            key: key.to_owned(),
        })
}

fn is_pod_settled() -> impl Condition<Pod> + Clone {
    |pod: Option<&Pod>| {
        let Some(status) = pod.and_then(|current| current.status.as_ref()) else {
            return false;
        };
        if status.phase.as_deref() == Some(POD_PHASE_FAILED) {
            return true;
        }
        let Some(conditions) = status.conditions.as_ref() else {
            return false;
        };
        conditions.iter().any(|condition| {
            condition.type_ == POD_CONDITION_READY && condition.status == POD_CONDITION_STATUS_TRUE
        })
    }
}

fn has_failed_phase(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        == Some(POD_PHASE_FAILED)
}

#[cfg(test)]
mod tests;

async fn present_non_ephemeral_names(context: &KubeContext) -> CommandResult<BTreeSet<String>> {
    let pods_list = async { pod_api::list_pods_non_ephemeral(context).await };
    let deployments_list = async { deployment_api::list_deployments_non_ephemeral(context).await };
    let jobs_list = async { job_api::list_jobs_non_ephemeral(context).await };
    let cronjobs_list = async { cronjob_api::list_cronjobs_non_ephemeral(context).await };
    let (pods, deployments, jobs, cronjobs) =
        futures::future::try_join4(pods_list, deployments_list, jobs_list, cronjobs_list).await?;
    let mut names = BTreeSet::new();
    names.extend(pods.into_iter().filter_map(|pod| pod.metadata.name));
    names.extend(
        deployments
            .into_iter()
            .filter_map(|deployment| deployment.metadata.name),
    );
    names.extend(jobs.into_iter().filter_map(|job| job.metadata.name));
    names.extend(
        cronjobs
            .into_iter()
            .filter_map(|cronjob| cronjob.metadata.name),
    );
    Ok(names)
}

async fn delete_workload_controllers(
    context: &KubeContext,
    multi: &MultiProgress,
) -> CommandResult {
    let phase = phase_spinner_in(multi, "Deleting tabsdata workload controllers");
    let cronjobs_bar = phase.add_resource("CronJobs");
    match cronjob_api::delete_cronjobs(context).await {
        Ok(()) => cronjobs_bar.finish_with_message("CronJobs (deleted)"),
        Err(error) => {
            cronjobs_bar.finish_with_message("CronJobs (failed)");
            drop(phase);
            return Err(CommandError::from(error));
        }
    }
    let jobs_bar = phase.add_resource("Jobs");
    match job_api::delete_jobs(context).await {
        Ok(()) => jobs_bar.finish_with_message("Jobs (deleted)"),
        Err(error) => {
            jobs_bar.finish_with_message("Jobs (failed)");
            drop(phase);
            return Err(CommandError::from(error));
        }
    }
    drop(phase);
    Ok(())
}

async fn delete_ephemeral_workload(context: &KubeContext, multi: &MultiProgress) -> CommandResult {
    let phase = phase_spinner_in(multi, "Deleting tabsdata ephemeral workload");
    let pods_bar = phase.add_resource("Pods");
    let result = match pod_api::delete_pods_ephemeral(context).await {
        Ok(()) => {
            pods_bar.finish_with_message("Pods (deleted)");
            Ok(())
        }
        Err(error) => {
            pods_bar.finish_with_message("Pods (failed)");
            Err(CommandError::from(error))
        }
    };
    drop(phase);
    result
}

async fn delete_non_ephemeral_workload(
    context: &KubeContext,
    multi: &MultiProgress,
) -> CommandResult {
    let phase = phase_spinner_in(multi, "Deleting tabsdata non-ephemeral workload");
    let pods_bar = phase.add_resource("Pods");
    let deployments_bar = phase.add_resource("Deployments");
    let jobs_bar = phase.add_resource("Jobs");
    let cronjobs_bar = phase.add_resource("CronJobs");
    let services_bar = phase.add_resource("Services");
    let authorizations_bar = phase.add_resource("Authorizations");
    let pods_delete = async {
        match pod_api::delete_pods_non_ephemeral(context).await {
            Ok(()) => {
                pods_bar.finish_with_message("Pods (deleted)");
                Ok(())
            }
            Err(error) => {
                pods_bar.finish_with_message("Pods (failed)");
                Err(CommandError::from(error))
            }
        }
    };
    let deployments_delete = async {
        match deployment_api::delete_deployments_non_ephemeral(context).await {
            Ok(()) => {
                deployments_bar.finish_with_message("Deployments (deleted)");
                Ok(())
            }
            Err(error) => {
                deployments_bar.finish_with_message("Deployments (failed)");
                Err(CommandError::from(error))
            }
        }
    };
    let jobs_delete = async {
        match job_api::delete_jobs_non_ephemeral(context).await {
            Ok(()) => {
                jobs_bar.finish_with_message("Jobs (deleted)");
                Ok(())
            }
            Err(error) => {
                jobs_bar.finish_with_message("Jobs (failed)");
                Err(CommandError::from(error))
            }
        }
    };
    let cronjobs_delete = async {
        match cronjob_api::delete_cronjobs_non_ephemeral(context).await {
            Ok(()) => {
                cronjobs_bar.finish_with_message("CronJobs (deleted)");
                Ok(())
            }
            Err(error) => {
                cronjobs_bar.finish_with_message("CronJobs (failed)");
                Err(CommandError::from(error))
            }
        }
    };
    let services_delete = async {
        match service_api::delete_services(context).await {
            Ok(_) => {
                services_bar.finish_with_message("Services (deleted)");
                Ok(())
            }
            Err(error) => {
                services_bar.finish_with_message("Services (failed)");
                Err(CommandError::from(error))
            }
        }
    };
    let result = futures::future::try_join5(
        pods_delete,
        deployments_delete,
        jobs_delete,
        cronjobs_delete,
        services_delete,
    )
    .await;
    if result.is_ok() {
        match rbac_api::delete_authorizations(context).await {
            Ok(_) => authorizations_bar.finish_with_message("Authorizations (deleted)"),
            Err(error) => {
                authorizations_bar.finish_with_message("Authorizations (failed)");
                drop(phase);
                return Err(CommandError::from(error));
            }
        }
    } else {
        authorizations_bar.finish_with_message("Authorizations (skipped)");
    }
    drop(phase);
    result?;
    Ok(())
}
