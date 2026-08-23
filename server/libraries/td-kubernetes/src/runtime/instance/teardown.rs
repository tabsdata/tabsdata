//
// Copyright 2026 Tabsdata Inc.
//

use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{MultiProgress, ProgressBar};
use kube::core::Selector;
use tokio::time::Instant;

use crate::base::constants::{
    DELETE_WORKLOAD_POLL_INTERVAL, DELETE_WORKLOAD_WAIT_TIMEOUT, POD_YAML_COMPONENT_LABEL,
    TIER_LABEL,
};
use crate::base::error::launcher::LauncherError;
use crate::client::tdkserver::common::confirm_resume;
use crate::client::tdkserver::widgets::{PhaseSpinner, phase_spinner_in};
use crate::core::system::exit::{CommandError, CommandResult};
use crate::kube::api::{
    LabelFilter, cronjob as cronjob_api, deployment as deployment_api, job as job_api,
    pod as pod_api, rbac as rbac_api, service as service_api, tabsdata_selector,
};
use crate::kube::context::KubeContext;
use crate::runtime::instance::tier::Tier;

const TEARDOWN_ATTEMPTS: usize = 3;

const APISERVER_COMPONENT: &str = "apiserver";
#[cfg(feature = "enterprise")]
const AIAGENT_COMPONENT: &str = "aiagent";
const DATABASE_COMPONENT: &str = "database";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    CronJob,
    Job,
    Deployment,
    Pod,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::CronJob => "CronJobs",
            Kind::Job => "Jobs",
            Kind::Deployment => "Deployments",
            Kind::Pod => "Pods",
        }
    }

    fn singular(self) -> &'static str {
        match self {
            Kind::CronJob => "cronjob",
            Kind::Job => "job",
            Kind::Deployment => "deployment",
            Kind::Pod => "pod",
        }
    }

    async fn names(self, context: &KubeContext, selector: &Selector) -> CommandResult<Vec<String>> {
        let names = match self {
            Kind::CronJob => cronjob_api::list_cronjobs_matching(context, selector.clone())
                .await?
                .into_iter()
                .filter_map(|entry| entry.metadata.name)
                .collect(),
            Kind::Job => job_api::list_jobs_matching(context, selector.clone())
                .await?
                .into_iter()
                .filter_map(|entry| entry.metadata.name)
                .collect(),
            Kind::Deployment => {
                deployment_api::list_deployments_matching(context, selector.clone())
                    .await?
                    .into_iter()
                    .filter_map(|entry| entry.metadata.name)
                    .collect()
            }
            Kind::Pod => pod_api::list_pods_matching(context, selector.clone())
                .await?
                .into_iter()
                .filter_map(|entry| entry.metadata.name)
                .collect(),
        };
        Ok(names)
    }

    async fn delete(self, context: &KubeContext, name: &str) -> CommandResult {
        match self {
            Kind::CronJob => cronjob_api::delete_cronjob(context, name).await?,
            Kind::Job => job_api::delete_job(context, name).await?,
            Kind::Deployment => deployment_api::delete_deployment(context, name).await?,
            Kind::Pod => pod_api::delete_pod(context, name).await?,
        }
        Ok(())
    }
}

const WORKLOAD_KINDS: [Kind; 4] = [Kind::CronJob, Kind::Job, Kind::Deployment, Kind::Pod];

struct Stage {
    label: String,
    selector: Selector,
    kinds: Vec<Kind>,
}

impl Stage {
    fn component(component: &str) -> Self {
        Self {
            label: component.to_owned(),
            selector: tabsdata_selector(Some(LabelFilter::new(
                POD_YAML_COMPONENT_LABEL,
                component,
            ))),
            kinds: WORKLOAD_KINDS.to_vec(),
        }
    }

    fn tier(tier: Tier) -> Self {
        Self {
            label: format!("the {} tier", tier.as_str()),
            selector: tabsdata_selector(Some(LabelFilter::new(TIER_LABEL, tier.as_str()))),
            kinds: WORKLOAD_KINDS.to_vec(),
        }
    }

    fn everything(kind: Kind) -> Self {
        Self {
            label: format!("the remaining {}s", kind.singular()),
            selector: tabsdata_selector(None),
            kinds: vec![kind],
        }
    }
}

fn stages() -> Vec<Stage> {
    vec![
        Stage::component(APISERVER_COMPONENT),
        #[cfg(feature = "enterprise")]
        Stage::component(AIAGENT_COMPONENT),
        Stage::component(DATABASE_COMPONENT),
        Stage::tier(Tier::Regular),
        Stage::everything(Kind::CronJob),
        Stage::everything(Kind::Job),
        Stage::everything(Kind::Deployment),
        Stage::everything(Kind::Pod),
    ]
}

pub async fn delete_workload(context: &KubeContext, multi: &MultiProgress) -> CommandResult {
    for stage in stages() {
        delete_stage(context, multi, &stage).await?;
    }
    delete_workload_remnants(context, multi).await
}

async fn delete_stage(
    context: &KubeContext,
    multi: &MultiProgress,
    stage: &Stage,
) -> CommandResult {
    let mut pending = Vec::new();
    for kind in &stage.kinds {
        let names = kind.names(context, &stage.selector).await?;
        if !names.is_empty() {
            pending.push((*kind, names));
        }
    }
    if pending.is_empty() {
        return Ok(());
    }
    let total: usize = pending.iter().map(|(_, names)| names.len()).sum();
    let phase = phase_spinner_in(
        multi,
        &format!("Deleting {total} resource(s) of {}", stage.label),
    );
    for (kind, names) in pending {
        match delete_until_gone(context, &phase, stage, kind, names).await {
            Ok(()) => {}
            Err(error) => {
                drop(phase);
                return Err(error);
            }
        }
    }
    drop(phase);
    Ok(())
}

async fn delete_until_gone(
    context: &KubeContext,
    phase: &PhaseSpinner,
    stage: &Stage,
    kind: Kind,
    gathered: Vec<String>,
) -> CommandResult {
    let bar = phase.add_resource(kind.as_str());
    let mut requested = gathered;
    let mut gone = 0usize;
    let mut swept = 0usize;
    let mut lingering = Vec::new();
    for _ in 1..=TEARDOWN_ATTEMPTS {
        if requested.is_empty() {
            break;
        }
        let batch = requested.len();
        let mut failure: Option<CommandError> = None;
        {
            let mut deletions = requested
                .iter()
                .map(|name| kind.delete(context, name))
                .collect::<FuturesUnordered<_>>();
            let mut issued = 0usize;
            while let Some(outcome) = deletions.next().await {
                match outcome {
                    Ok(()) => {
                        issued += 1;
                        bar.set_message(format!("{} ({issued}/{batch} requested)", kind.as_str()));
                    }
                    Err(error) => failure = failure.or(Some(error)),
                }
            }
        }
        if let Some(error) = failure {
            bar.finish_with_message(format!("{} (failed)", kind.as_str()));
            return Err(error);
        }
        lingering = match await_gone(context, &bar, stage, kind, &requested, gone).await {
            Ok(lingering) => lingering,
            Err(error) => {
                bar.finish_with_message(format!("{} (failed)", kind.as_str()));
                return Err(error);
            }
        };
        gone += batch - lingering.len();
        if !lingering.is_empty() {
            break;
        }
        requested = match kind.names(context, &stage.selector).await {
            Ok(names) => names,
            Err(error) => {
                bar.finish_with_message(format!("{} (failed)", kind.as_str()));
                return Err(error);
            }
        };
        swept += 1;
    }
    if !lingering.is_empty() {
        bar.finish_with_message(format!("{} (failed)", kind.as_str()));
        return Err(LauncherError::WorkloadStillPresent {
            count: lingering.len(),
            kind: kind.as_str().to_lowercase(),
            stage: stage.label.clone(),
            attempts: TEARDOWN_ATTEMPTS,
        }
        .into());
    }
    bar.finish_with_message(format!("{} ({gone} deleted)", kind.as_str()));
    if reports_quiet_sweep(swept) {
        phase.add_note(&format!(
            "No further {} of {} appeared.",
            kind.as_str().to_lowercase(),
            stage.label
        ));
    }
    Ok(())
}

fn reports_quiet_sweep(swept: usize) -> bool {
    swept > 1
}

async fn await_gone(
    context: &KubeContext,
    bar: &ProgressBar,
    stage: &Stage,
    kind: Kind,
    requested: &[String],
    gone: usize,
) -> CommandResult<Vec<String>> {
    let started = Instant::now();
    let mut deadline = started + DELETE_WORKLOAD_WAIT_TIMEOUT;
    loop {
        let present = kind.names(context, &stage.selector).await?;
        let lingering = still_present(requested, &present);
        bar.set_message(format!(
            "{} ({}/{} deleted)",
            kind.as_str(),
            gone + requested.len() - lingering.len(),
            gone + requested.len()
        ));
        if lingering.is_empty() {
            return Ok(lingering);
        }
        if Instant::now() >= deadline {
            let what = format!(
                "{} {} of {}",
                lingering.len(),
                kind.as_str().to_lowercase(),
                stage.label
            );
            if !confirm_resume(&what, started.elapsed()).await {
                return Ok(lingering);
            }
            deadline = Instant::now() + DELETE_WORKLOAD_WAIT_TIMEOUT;
        }
        tokio::time::sleep(DELETE_WORKLOAD_POLL_INTERVAL).await;
    }
}

fn still_present(requested: &[String], present: &[String]) -> Vec<String> {
    requested
        .iter()
        .filter(|name| present.contains(name))
        .cloned()
        .collect()
}

async fn delete_workload_remnants(context: &KubeContext, multi: &MultiProgress) -> CommandResult {
    let phase = phase_spinner_in(multi, "Deleting tabsdata workload remnants");
    let services_bar = phase.add_resource("Services");
    match service_api::delete_services(context).await {
        Ok(deleted) => services_bar.finish_with_message(format!("Services ({deleted} deleted)")),
        Err(error) => {
            services_bar.finish_with_message("Services (failed)");
            drop(phase);
            return Err(CommandError::from(error));
        }
    }
    let authorizations_bar = phase.add_resource("Authorizations");
    match rbac_api::delete_authorizations(context).await {
        Ok(deleted) => {
            authorizations_bar.finish_with_message(format!("Authorizations ({deleted} deleted)"))
        }
        Err(error) => {
            authorizations_bar.finish_with_message("Authorizations (failed)");
            drop(phase);
            return Err(CommandError::from(error));
        }
    }
    drop(phase);
    Ok(())
}

#[cfg(test)]
mod tests;
