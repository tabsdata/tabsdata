//
// Copyright 2026 Tabsdata Inc.
//

use super::*;

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

fn pod_log_id(metadata: Option<&ObjectMeta>) -> Option<&str> {
    metadata?
        .labels
        .as_ref()?
        .get(POD_LOG_ID_LABEL)
        .map(String::as_str)
}

#[test]
fn a_pod_is_labeled_with_its_own_name() {
    let mut pod = Pod {
        metadata: named("srv-database"),
        ..Pod::default()
    };
    label_pod_log_id_of_pod(&mut pod);
    assert_eq!(pod_log_id(Some(&pod.metadata)), Some("srv-database"));
}

#[test]
fn a_job_labels_its_pod_template_with_the_job_name() {
    let mut job: Job = serde_json::from_value(serde_json::json!({
        "metadata": {"name": "td-function-transformer-abc"},
        "spec": {"template": {"spec": {"containers": []}}},
    }))
    .unwrap();
    label_pod_log_id_of_job(&mut job);
    let template = job.spec.as_ref().unwrap().template.metadata.as_ref();
    assert_eq!(pod_log_id(template), Some("td-function-transformer-abc"));
    assert_eq!(pod_log_id(Some(&job.metadata)), None);
}

#[test]
fn a_cronjob_labels_the_pod_template_of_its_job_template_with_the_cronjob_name() {
    let mut cronjob: CronJob = serde_json::from_value(serde_json::json!({
        "metadata": {"name": "td-cron-publisher-abc"},
        "spec": {
            "schedule": "* * * * *",
            "jobTemplate": {
                "metadata": {"labels": {"app.kubernetes.io/component": "cron-publisher"}},
                "spec": {"template": {
                    "metadata": {"labels": {"app.kubernetes.io/component": "cron-publisher"}},
                    "spec": {"containers": []},
                }},
            },
        },
    }))
    .unwrap();
    label_pod_log_id_of_cronjob(&mut cronjob);
    let job_spec = cronjob.spec.job_template.spec.as_ref().unwrap();
    let template = job_spec.template.metadata.as_ref();
    assert_eq!(pod_log_id(template), Some("td-cron-publisher-abc"));
    assert_eq!(
        template.unwrap().labels.as_ref().unwrap()["app.kubernetes.io/component"],
        "cron-publisher"
    );
    assert_eq!(pod_log_id(Some(&cronjob.metadata)), None);
}

#[test]
fn a_deployment_labels_its_pod_template_with_the_deployment_name() {
    let mut deployment: Deployment = serde_json::from_value(serde_json::json!({
        "metadata": {"name": "srv-apiserver"},
        "spec": {
            "selector": {},
            "template": {"spec": {"containers": []}},
        },
    }))
    .unwrap();
    label_pod_log_id_of_deployment(&mut deployment);
    let template = deployment.spec.as_ref().unwrap().template.metadata.as_ref();
    assert_eq!(pod_log_id(template), Some("srv-apiserver"));
}

#[test]
fn a_name_too_long_for_a_label_value_is_not_labeled() {
    let mut pod = Pod {
        metadata: named(&"a".repeat(LABEL_VALUE_MAX_LENGTH + 1)),
        ..Pod::default()
    };
    label_pod_log_id_of_pod(&mut pod);
    assert_eq!(pod_log_id(Some(&pod.metadata)), None);
}

#[test]
fn an_unnamed_pod_is_not_labeled() {
    let mut pod = Pod::default();
    label_pod_log_id_of_pod(&mut pod);
    assert_eq!(pod_log_id(Some(&pod.metadata)), None);
}
