//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::path::PathBuf;

use chrono::Utc;
use indoc::formatdoc;
use k8s_openapi::api::batch::v1::{CronJob, Job, JobCondition, JobStatus};
use k8s_openapi::api::core::v1::{Pod, PodCondition, PodStatus};
use kube::runtime::wait::Condition;
use strum::IntoEnumIterator;
use tm_workspace::workspace_root;

use super::*;
use crate::client::session::pod::{
    POD_NAME_ENV, POD_NAMESPACE_ENV, POD_SERVICE_ACCOUNT_ENV, POD_TABSDATA_INSTANCE_ENV,
};
use crate::runtime::instance::manifest::Manifest;
use crate::runtime::instance::profile::Profile;
use crate::runtime::instance::tier::Tier;

fn job_with_conditions(conditions: &[(&str, &str)]) -> Job {
    let conditions = conditions
        .iter()
        .map(|(type_, status)| JobCondition {
            type_: (*type_).to_string(),
            status: (*status).to_string(),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    Job {
        status: Some(JobStatus {
            conditions: Some(conditions),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn job_with_conditions_and_active(conditions: &[(&str, &str)], active: i32) -> Job {
    let mut job = job_with_conditions(conditions);
    if let Some(status) = job.status.as_mut() {
        status.active = Some(active);
    }
    job
}

fn pod_with_status(phase: Option<&str>, ready: Option<&str>) -> Pod {
    let conditions = ready.map(|status| {
        vec![PodCondition {
            type_: POD_CONDITION_READY.to_string(),
            status: status.to_string(),
            ..Default::default()
        }]
    });
    Pod {
        status: Some(PodStatus {
            phase: phase.map(|value| value.to_string()),
            conditions,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn profile_with_pods(entries: &[(&str, &str)]) -> Profile {
    let mut pods = BTreeMap::new();
    for (key, yaml) in entries {
        pods.insert((*key).to_owned(), (*yaml).to_owned());
    }
    Profile {
        pods,
        assets: BTreeMap::new(),
        configs: BTreeMap::new(),
        manifest: Manifest::initial_install("1.0.0", Utc::now(), std::iter::empty()),
    }
}

fn job_yaml(name: &str, tier: &str) -> String {
    format!(
        "apiVersion: batch/v1
kind: Job
metadata:
  name: {name}
  labels:
    tabsdata.com/tier: {tier}
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: bootstrap
          image: busybox
"
    )
}

fn pod_yaml(name: &str, tier: &str) -> String {
    format!(
        "apiVersion: v1
kind: Pod
metadata:
  name: {name}
  labels:
    tabsdata.com/tier: {tier}
spec:
  containers:
    - name: apiserver
      image: tabsdata/apiserver
"
    )
}

fn cronjob_yaml(name: &str, tier: &str) -> String {
    format!(
        "apiVersion: batch/v1
kind: CronJob
metadata:
  name: {name}
  labels:
    tabsdata.com/tier: {tier}
spec:
  schedule: \"*/5 * * * *\"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: cron
              image: busybox
"
    )
}

#[test]
fn kind_of_entry_reads_cronjob() {
    let yaml = cronjob_yaml("cron-publisher", "regular");
    assert_eq!(
        kind_of_entry("cron-publisher.yaml", &yaml).expect("kind"),
        TemplateKind::CronJob
    );
}

#[test]
fn kind_of_entry_reads_pod() {
    let yaml = pod_yaml("apiserver", "regular");
    assert_eq!(
        kind_of_entry("apiserver.yaml", &yaml).expect("kind"),
        TemplateKind::Pod
    );
}

#[test]
fn kind_of_entry_rejects_unknown_kind() {
    let yaml = "\
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: storage
";
    assert!(kind_of_entry("storage.yaml", yaml).is_err());
}

#[tokio::test]
async fn deserialize_pod_entry_parses_valid_cronjob() {
    let yaml = cronjob_yaml("cron-publisher", "regular");
    let cronjob: CronJob = deserialize_pod_entry(
        "cron-publisher.yaml",
        &yaml,
        Tier::Regular,
        &SystemValues::default(),
    )
    .await
    .expect("valid CronJob");
    assert_eq!(cronjob.metadata.name.as_deref(), Some("cron-publisher"));
}

#[test]
fn cronjob_exists_matches_present_and_absent() {
    assert!(cronjob_exists().matches_object(Some(&CronJob::default())));
    assert!(!cronjob_exists().matches_object(None));
}

#[test]
fn is_job_settled_true_when_complete_is_true() {
    let job = job_with_conditions(&[("Complete", "True")]);
    assert!(is_job_settled().matches_object(Some(&job)));
}

#[test]
fn is_job_settled_true_when_failed_is_true() {
    let job = job_with_conditions(&[("Failed", "True")]);
    assert!(is_job_settled().matches_object(Some(&job)));
}

#[test]
fn is_job_settled_false_when_complete_is_false() {
    let job = job_with_conditions(&[("Complete", "False")]);
    assert!(!is_job_settled().matches_object(Some(&job)));
}

#[test]
fn is_job_settled_false_for_unrelated_condition() {
    let job = job_with_conditions(&[("Suspended", "True")]);
    assert!(!is_job_settled().matches_object(Some(&job)));
}

#[test]
fn is_job_settled_false_when_status_missing() {
    let job = Job::default();
    assert!(!is_job_settled().matches_object(Some(&job)));
}

#[test]
fn is_job_reclaimable_true_when_settled_and_idle() {
    let job = job_with_conditions(&[("Complete", "True")]);
    assert!(is_job_reclaimable(&job));
}

#[test]
fn is_job_reclaimable_true_for_a_settled_failed_run() {
    let job = job_with_conditions(&[("Failed", "True")]);
    assert!(is_job_reclaimable(&job));
}

#[test]
fn is_job_reclaimable_false_while_pods_are_active() {
    let job = job_with_conditions_and_active(&[("Complete", "True")], 1);
    assert!(!is_job_reclaimable(&job));
}

#[test]
fn is_job_reclaimable_false_while_still_running() {
    let job = job_with_conditions_and_active(&[], 1);
    assert!(!is_job_reclaimable(&job));
}

#[test]
fn is_job_reclaimable_false_when_status_missing() {
    assert!(!is_job_reclaimable(&Job::default()));
}

#[test]
fn active_pods_defaults_to_zero() {
    assert_eq!(job_active_pods(&Job::default()), 0);
    assert_eq!(job_active_pods(&job_with_conditions_and_active(&[], 3)), 3);
}

#[test]
fn is_job_absent_matches_only_a_missing_object() {
    assert!(is_job_absent().matches_object(None));
    assert!(!is_job_absent().matches_object(Some(&Job::default())));
}

#[test]
fn is_job_settled_false_when_object_missing() {
    assert!(!is_job_settled().matches_object(None));
}

#[test]
fn has_failed_condition_true_for_failed_true() {
    let job = job_with_conditions(&[("Failed", "True")]);
    assert!(has_failed_condition(&job));
}

#[test]
fn has_failed_condition_false_for_complete_true() {
    let job = job_with_conditions(&[("Complete", "True")]);
    assert!(!has_failed_condition(&job));
}

#[test]
fn has_failed_condition_false_when_failed_status_is_false() {
    let job = job_with_conditions(&[("Failed", "False")]);
    assert!(!has_failed_condition(&job));
}

#[test]
fn is_pod_settled_true_when_ready_is_true() {
    let pod = pod_with_status(Some("Running"), Some("True"));
    assert!(is_pod_settled().matches_object(Some(&pod)));
}

#[test]
fn is_pod_settled_true_when_phase_is_failed() {
    let pod = pod_with_status(Some("Failed"), None);
    assert!(is_pod_settled().matches_object(Some(&pod)));
}

#[test]
fn is_pod_settled_false_when_ready_is_false() {
    let pod = pod_with_status(Some("Running"), Some("False"));
    assert!(!is_pod_settled().matches_object(Some(&pod)));
}

#[test]
fn is_pod_settled_false_for_pending() {
    let pod = pod_with_status(Some("Pending"), None);
    assert!(!is_pod_settled().matches_object(Some(&pod)));
}

#[test]
fn is_pod_settled_false_when_object_missing() {
    assert!(!is_pod_settled().matches_object(None));
}

#[test]
fn has_failed_phase_true_for_failed() {
    let pod = pod_with_status(Some("Failed"), None);
    assert!(has_failed_phase(&pod));
}

#[test]
fn has_failed_phase_false_for_running() {
    let pod = pod_with_status(Some("Running"), Some("True"));
    assert!(!has_failed_phase(&pod));
}

#[test]
fn has_failed_phase_false_when_status_missing() {
    let pod = Pod::default();
    assert!(!has_failed_phase(&pod));
}

#[test]
fn pods_for_tier_returns_only_matching_entries() {
    let profile = profile_with_pods(&[
        ("inspector.yaml", &job_yaml("inspector", "init")),
        ("apiserver.yaml", &pod_yaml("apiserver", "regular")),
    ]);
    let init: Vec<&str> = pods_for_tier(&profile, Tier::Init)
        .expect("filter init")
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(init, vec!["inspector.yaml"]);
    let regular: Vec<&str> = pods_for_tier(&profile, Tier::Regular)
        .expect("filter regular")
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(regular, vec!["apiserver.yaml"]);
}

#[test]
fn pods_for_tier_rejects_missing_tier_label() {
    let yaml = "\
apiVersion: v1
kind: Pod
metadata:
  name: nameless
spec:
  containers:
    - name: c
      image: busybox
";
    let profile = profile_with_pods(&[("nameless.yaml", yaml)]);
    let result = pods_for_tier(&profile, Tier::Init);
    assert!(result.is_err());
}

#[test]
fn pods_for_tier_rejects_unknown_tier_label() {
    let yaml = "\
apiVersion: v1
kind: Pod
metadata:
  name: garbage
  labels:
    tabsdata.com/tier: bogus
spec:
  containers:
    - name: c
      image: busybox
";
    let profile = profile_with_pods(&[("garbage.yaml", yaml)]);
    let result = pods_for_tier(&profile, Tier::Init);
    assert!(result.is_err());
}

#[test]
fn pods_for_tier_returns_ephemeral_entries() {
    let profile = profile_with_pods(&[
        ("inspector.yaml", &job_yaml("inspector", "init")),
        ("function.yaml", &job_yaml("function", "ephemeral")),
    ]);
    let ephemeral: Vec<&str> = pods_for_tier(&profile, Tier::Ephemeral)
        .expect("filter ephemeral")
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(ephemeral, vec!["function.yaml"]);
}

#[tokio::test]
async fn deserialize_pod_entry_parses_valid_job() {
    let yaml = job_yaml("init-bootstrap", "init");
    let job: Job = deserialize_pod_entry(
        "bootstrap.yaml",
        &yaml,
        Tier::Init,
        &SystemValues::default(),
    )
    .await
    .expect("valid Job");
    assert_eq!(job.metadata.name.as_deref(), Some("init-bootstrap"));
}

#[tokio::test]
async fn deserialize_pod_entry_aborts_for_invalid_yaml() {
    let result = deserialize_pod_entry::<Job>(
        "broken.yaml",
        "::: not yaml :::",
        Tier::Init,
        &SystemValues::default(),
    )
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn deserialize_pod_entry_parses_valid_pod() {
    let yaml = pod_yaml("apiserver", "regular");
    let pod: Pod = deserialize_pod_entry(
        "apiserver.yaml",
        &yaml,
        Tier::Regular,
        &SystemValues::default(),
    )
    .await
    .expect("valid Pod");
    assert_eq!(pod.metadata.name.as_deref(), Some("apiserver"));
}

fn init_template(sequence: Option<&str>) -> String {
    let label = match sequence {
        Some(value) => format!("\n    {SEQUENCE_LABEL}: {value}"),
        None => String::new(),
    };
    formatdoc!(
        "
            apiVersion: batch/v1
            kind: Job
            metadata:
              name: job
              labels:
                {TIER_LABEL}: init{label}
        "
    )
}

#[test]
fn init_sequence_accepts_quoted_and_bare_numbers() {
    assert_eq!(
        init_sequence("job", &init_template(Some("\"7\""))).expect("quoted"),
        7
    );
    assert_eq!(
        init_sequence("job", &init_template(Some("7"))).expect("bare"),
        7
    );
}

#[test]
fn init_sequence_rejects_a_non_numeric_label() {
    let error = init_sequence("job", &init_template(Some("\"first\""))).expect_err("must reject");
    assert!(matches!(error, LauncherError::InvalidInitSequence { .. }));
}

#[test]
fn init_sequence_requires_the_label() {
    let error = init_sequence("job", &init_template(None)).expect_err("must reject");
    assert!(matches!(error, LauncherError::MissingInitSequence { .. }));
}

#[cfg(feature = "enterprise")]
fn labelled(label: Option<&str>) -> String {
    let line = label
        .map(|value| format!("\n    tabsdata.com/aiagent: {value}"))
        .unwrap_or_default();
    format!(
        "apiVersion: apps/v1
kind: Deployment
metadata:
  name: srv-aiagent
  labels:
    tabsdata.com/tier: regular{line}
spec:
  replicas: 1
"
    )
}

#[test]
#[cfg(feature = "enterprise")]
fn a_true_aiagent_label_marks_a_workload_as_key_dependent() {
    assert!(needs_ai_keys(&labelled(Some("true"))));
}

#[test]
#[cfg(feature = "enterprise")]
fn a_false_aiagent_label_leaves_a_workload_unconditional() {
    assert!(!needs_ai_keys(&labelled(Some("false"))));
}

#[test]
#[cfg(feature = "enterprise")]
fn an_absent_aiagent_label_leaves_a_workload_unconditional() {
    assert!(!needs_ai_keys(&labelled(None)));
}

#[test]
#[cfg(feature = "enterprise")]
fn a_quoted_aiagent_label_is_not_a_boolean() {
    assert!(!needs_ai_keys(&labelled(Some("\"true\""))));
}

#[test]
#[cfg(feature = "enterprise")]
fn unparseable_yaml_leaves_a_workload_unconditional() {
    assert!(!needs_ai_keys("this: [is: not: valid"));
}

#[cfg(feature = "enterprise")]
fn secret(entries: &[(&str, &str)]) -> BTreeMap<String, Vec<u8>> {
    entries
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.as_bytes().to_vec()))
        .collect()
}

#[test]
#[cfg(feature = "enterprise")]
fn an_openai_key_alone_is_a_provider_key() {
    assert!(provider_key_supplied(&secret(&[(
        "AI_PROVIDERS__OPENAI__API_KEY",
        "sk-x"
    )])));
}

#[test]
#[cfg(feature = "enterprise")]
fn an_anthropic_key_alone_is_a_provider_key() {
    assert!(provider_key_supplied(&secret(&[(
        "AI_PROVIDERS__ANTHROPIC__API_KEY",
        "sk-ant-x"
    )])));
}

#[test]
#[cfg(feature = "enterprise")]
fn any_named_provider_key_counts() {
    assert!(provider_key_supplied(&secret(&[(
        "AI_PROVIDERS__AZURE_EU__API_KEY",
        "k"
    )])));
}

#[test]
#[cfg(feature = "enterprise")]
fn empty_or_blank_keys_do_not_count() {
    assert!(!provider_key_supplied(&secret(&[
        ("AI_PROVIDERS__OPENAI__API_KEY", ""),
        ("AI_PROVIDERS__ANTHROPIC__API_KEY", "  \n"),
    ])));
}

#[test]
#[cfg(feature = "enterprise")]
fn no_secret_entries_supply_no_key() {
    assert!(!provider_key_supplied(&secret(&[])));
}

#[test]
#[cfg(feature = "enterprise")]
fn other_entries_are_not_provider_keys() {
    assert!(!provider_key_supplied(&secret(&[
        ("CONFIRM__KEYS", "abc"),
        ("PROVIDERS__OPENAI__API_KEY", "sk-x"),
        ("AI_PROVIDERS____API_KEY", "sk-x"),
        ("AI_PROVIDERS__OPENAI__API_BASE", "http://x"),
    ])));
}

const CONFIG_FILE_ARGUMENT: &str = "--config-file";

fn shipped_job(component: &str, provider: &str) -> Job {
    let path = PathBuf::from(workspace_root!())
        .join("variant/resources/kprofile/pods")
        .join(format!("{component}_{provider}.yaml"));
    let yaml = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("{} is readable: {error}", path.display()));
    serde_yaml::from_str(&yaml)
        .unwrap_or_else(|error| panic!("{} is a job: {error}", path.display()))
}

fn pod_spec(job: &Job) -> &k8s_openapi::api::core::v1::PodSpec {
    job.spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .expect("the job has a pod template")
}

fn config_files(job: &Job) -> Vec<String> {
    let arguments = pod_spec(job).containers[0].args.clone().unwrap_or_default();
    arguments
        .windows(2)
        .filter(|pair| pair[0] == CONFIG_FILE_ARGUMENT)
        .map(|pair| {
            pair[1]
                .rsplit('/')
                .next()
                .expect("a path has a file name")
                .to_owned()
        })
        .collect()
}

fn mounted_configmaps(job: &Job) -> Vec<String> {
    pod_spec(job)
        .volumes
        .iter()
        .flatten()
        .filter_map(|volume| volume.projected.as_ref())
        .flat_map(|projected| projected.sources.iter().flatten())
        .filter_map(|source| source.config_map.as_ref())
        .map(|configmap| configmap.name.clone())
        .collect()
}

fn environment_names(job: &Job) -> Vec<String> {
    pod_spec(job).containers[0]
        .env
        .iter()
        .flatten()
        .map(|variable| variable.name.clone())
        .collect()
}

#[test]
fn every_shipped_tdmanager_pod_reads_the_database_configuration_the_dbmanager_pod_reads() {
    for provider in ClusterProvider::iter().map(ClusterProvider::as_str) {
        let dbmanager = shipped_job(DBMANAGER_COMPONENT, provider);
        let tdmanager = shipped_job(TDMANAGER_COMPONENT, provider);
        let files = config_files(&tdmanager);
        let configmaps = mounted_configmaps(&tdmanager);
        for file in config_files(&dbmanager) {
            assert!(
                files.contains(&file),
                "tdmanager on {provider} is not given {file}: {files:?}"
            );
            assert!(
                configmaps
                    .iter()
                    .any(|configmap| configmap.ends_with(&format!("-{file}"))),
                "tdmanager on {provider} does not mount {file}: {configmaps:?}"
            );
        }
    }
}

#[test]
fn every_shipped_tdmanager_pod_sets_the_environment_of_its_pod_context() {
    for provider in ClusterProvider::iter().map(ClusterProvider::as_str) {
        let names = environment_names(&shipped_job(TDMANAGER_COMPONENT, provider));
        for variable in [
            POD_NAME_ENV,
            POD_NAMESPACE_ENV,
            POD_SERVICE_ACCOUNT_ENV,
            POD_TABSDATA_INSTANCE_ENV,
        ] {
            assert!(
                names.iter().any(|name| name == variable),
                "tdmanager on {provider} does not set {variable}: {names:?}"
            );
        }
    }
}

fn shipped_pod_templates() -> Vec<PathBuf> {
    let folder = PathBuf::from(workspace_root!()).join("variant/resources/kprofile/pods");
    let mut templates: Vec<PathBuf> = std::fs::read_dir(&folder)
        .unwrap_or_else(|error| panic!("{} is listable: {error}", folder.display()))
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "yaml")
        })
        .collect();
    templates.sort();
    templates
}

fn system_values_for_every_placeholder() -> SystemValues {
    let text = || Some("x".to_owned());
    let port = || Some("1".to_owned());
    SystemValues {
        repository_url: text(),
        repository_path: text(),
        database_path: text(),
        storage_url: text(),
        storage_path: text(),
        runtimes_root: text(),
        pyenv_base_root: text(),
        pyenv_td_root: text(),
        pyenv_ag_root: text(),
        pyenv_fn_root: text(),
        pyenv_scripts_folder: text(),
        pyenv_libexec_folder: text(),
        exe_suffix: text(),
        instance_name: text(),
        namespace_name: text(),
        cluster_name: text(),
        tabsdata_version: text(),
        apiserver_public_port: port(),
        apiserver_private_port: port(),
        #[cfg(feature = "enterprise")]
        aiagent_private_port: port(),
        #[cfg(feature = "enterprise")]
        aitdk_private_port: port(),
        #[cfg(feature = "enterprise")]
        embedder_private_port: port(),
        database_host: text(),
        database_port: port(),
        database_name: text(),
        database_ssl_mode: text(),
        database_ssl_root_cert: text(),
        kminus_api_port: port(),
        kminus_prconfig_path: text(),
        project_secret_name: text(),
        connection_configmap_name: text(),
        connection_secret_name: text(),
        probe_request_configmap_name: text(),
        cron_schedule: text(),
        dbmanager_mode: text(),
        tdmanager_rules: text(),
        uv_default_index: text(),
        uv_index: text(),
        uv_index_strategy: text(),
    }
}

async fn deserialize_as_its_kind(
    key: &str,
    yaml: &str,
    system: &SystemValues,
) -> Result<(), LauncherError> {
    let tier = Tier::Regular;
    match kind_of_entry(key, yaml)? {
        TemplateKind::Pod => deserialize_pod_entry::<Pod>(key, yaml, tier, system)
            .await
            .map(drop),
        TemplateKind::Deployment => deserialize_pod_entry::<Deployment>(key, yaml, tier, system)
            .await
            .map(drop),
        TemplateKind::Job => deserialize_pod_entry::<Job>(key, yaml, tier, system)
            .await
            .map(drop),
        TemplateKind::CronJob => deserialize_pod_entry::<CronJob>(key, yaml, tier, system)
            .await
            .map(drop),
        TemplateKind::Service => deserialize_pod_entry::<Service>(key, yaml, tier, system)
            .await
            .map(drop),
        TemplateKind::ServiceAccount => {
            deserialize_pod_entry::<ServiceAccount>(key, yaml, tier, system)
                .await
                .map(drop)
        }
        TemplateKind::Role => deserialize_pod_entry::<Role>(key, yaml, tier, system)
            .await
            .map(drop),
        TemplateKind::RoleBinding => deserialize_pod_entry::<RoleBinding>(key, yaml, tier, system)
            .await
            .map(drop),
        TemplateKind::PodDisruptionBudget => {
            deserialize_pod_entry::<PodDisruptionBudget>(key, yaml, tier, system)
                .await
                .map(drop)
        }
    }
}

#[tokio::test]
async fn every_shipped_pod_template_deserializes_as_its_kind() {
    let system = system_values_for_every_placeholder();
    let templates = shipped_pod_templates();
    assert!(!templates.is_empty(), "no pod template was found");
    let mut failures = Vec::new();
    for path in templates {
        let yaml = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} is readable: {error}", path.display()));
        if let Err(error) =
            deserialize_as_its_kind(&path.display().to_string(), &yaml, &system).await
        {
            failures.push(error.to_string());
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
