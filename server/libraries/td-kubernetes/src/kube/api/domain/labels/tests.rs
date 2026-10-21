//
// Copyright 2026 Tabsdata Inc.
//

use indoc::indoc;

use super::*;
use crate::base::constants::POD_YAML_COMPONENT_LABEL;

#[test]
fn non_ephemeral_selector_excludes_ephemeral_tier() {
    let rendered = tabsdata_non_ephemeral_selector().to_string();
    assert!(rendered.contains(&format!("{PART_OF_LABEL}={PART_OF_TABSDATA}")));
    assert!(rendered.contains(&format!("{TIER_LABEL} notin (ephemeral)")));
}

#[test]
fn the_worker_selector_narrows_the_listing_to_observable_workloads() {
    let rendered = tabsdata_worker_selector().to_string();
    assert!(
        rendered.contains(&format!("{PART_OF_LABEL}={PART_OF_TABSDATA}")),
        "{rendered}"
    );
    assert!(rendered.contains(POD_YAML_WORKER_LABEL), "{rendered}");
    assert!(rendered.contains(LABEL_WORKER_ID), "{rendered}");
}

#[test]
fn the_worker_selector_is_stricter_than_the_plain_one() {
    let plain = tabsdata_selector(None).to_string();
    let worker = tabsdata_worker_selector().to_string();
    assert!(worker.len() > plain.len(), "{worker} vs {plain}");
}

fn named(name: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_owned()),
        ..ObjectMeta::default()
    }
}

fn log_id(metadata: Option<&ObjectMeta>) -> Option<&str> {
    metadata?
        .labels
        .as_ref()?
        .get(LOG_ID_LABEL)
        .map(String::as_str)
}

#[test]
fn a_pod_is_labeled_with_its_own_name() {
    let mut pod = Pod {
        metadata: named("srv-database"),
        ..Pod::default()
    };
    label_log_id(&mut pod).expect("the name fits a label value");
    assert_eq!(log_id(Some(&pod.metadata)), Some("srv-database"));
}

#[test]
fn a_job_labels_its_pod_template_with_the_job_name() {
    let mut job: Job = serde_yaml::from_str(indoc! {"
        metadata:
          name: td-function-transformer-abc
        spec:
          template:
            spec:
              containers: []
    "})
    .expect("the job parses");
    label_log_id(&mut job).expect("the name fits a label value");
    let template = job
        .spec
        .as_ref()
        .expect("the job has a spec")
        .template
        .metadata
        .as_ref();
    assert_eq!(log_id(template), Some("td-function-transformer-abc"));
    assert_eq!(log_id(Some(&job.metadata)), None);
}

#[test]
fn a_cronjob_labels_the_pod_template_of_its_job_template_with_the_cronjob_name() {
    let mut cronjob: CronJob = serde_yaml::from_str(indoc! {r#"
        metadata:
          name: td-cron-publisher-abc
        spec:
          schedule: "* * * * *"
          jobTemplate:
            metadata:
              labels:
                app.kubernetes.io/component: cron-publisher
            spec:
              template:
                metadata:
                  labels:
                    app.kubernetes.io/component: cron-publisher
                spec:
                  containers: []
    "#})
    .expect("the cronjob parses");
    label_log_id(&mut cronjob).expect("the name fits a label value");
    let job_spec = cronjob
        .spec
        .job_template
        .spec
        .as_ref()
        .expect("the job template has a spec");
    let template = job_spec.template.metadata.as_ref();
    assert_eq!(log_id(template), Some("td-cron-publisher-abc"));
    let labels = template
        .and_then(|metadata| metadata.labels.as_ref())
        .expect("the template has labels");
    assert_eq!(labels[POD_YAML_COMPONENT_LABEL], "cron-publisher");
    assert_eq!(log_id(Some(&cronjob.metadata)), None);
}

#[test]
fn a_deployment_labels_its_pod_template_with_the_deployment_name() {
    let mut deployment: Deployment = serde_yaml::from_str(indoc! {"
        metadata:
          name: srv-apiserver
        spec:
          selector: {}
          template:
            spec:
              containers: []
    "})
    .expect("the deployment parses");
    label_log_id(&mut deployment).expect("the name fits a label value");
    let spec = deployment.spec.as_ref().expect("the deployment has a spec");
    assert_eq!(
        log_id(spec.template.metadata.as_ref()),
        Some("srv-apiserver")
    );
}

#[test]
fn a_name_as_long_as_a_label_value_allows_is_labeled() {
    let name = "a".repeat(LABEL_VALUE_MAX_LENGTH);
    let mut pod = Pod {
        metadata: named(&name),
        ..Pod::default()
    };
    label_log_id(&mut pod).expect("the name fits a label value");
    assert_eq!(log_id(Some(&pod.metadata)), Some(name.as_str()));
}

#[test]
fn a_name_too_long_for_a_label_value_is_rejected() {
    let mut pod = Pod {
        metadata: named(&"a".repeat(LABEL_VALUE_MAX_LENGTH + 1)),
        ..Pod::default()
    };
    let error = label_log_id(&mut pod).expect_err("the name is too long for a label value");
    assert!(
        matches!(error, KubeStampError::LabelValueTooLong { length, maximum, .. }
            if length == LABEL_VALUE_MAX_LENGTH + 1 && maximum == LABEL_VALUE_MAX_LENGTH),
        "{error}"
    );
    assert_eq!(log_id(Some(&pod.metadata)), None);
}

#[test]
fn an_unnamed_pod_is_not_labeled() {
    let mut pod = Pod::default();
    label_log_id(&mut pod).expect("an unnamed pod is skipped");
    assert_eq!(log_id(Some(&pod.metadata)), None);
}

#[test]
fn a_resource_that_runs_no_pods_is_not_labeled() {
    let mut configmap = ConfigMap {
        metadata: named("td-configmap"),
        ..ConfigMap::default()
    };
    label_log_id(&mut configmap).expect("a configmap is skipped");
    assert_eq!(log_id(Some(&configmap.metadata)), None);
}
