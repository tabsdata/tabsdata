//
// Copyright 2026 Tabsdata Inc.
//

use std::fs;
use std::path::PathBuf;

use k8s_openapi::api::batch::v1::Job;

use super::*;
use crate::runtime::gateway::messaging::messenger::request::PROBE_WORKER;

const PROVIDERS: [&str; 6] = ["aks", "docker", "eks", "fargate", "gke", "kminus"];

fn pods_folder() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../variant/resources/kprofile/pods")
}

#[test]
fn every_provider_ships_a_connection_probe_job() {
    for provider in PROVIDERS {
        let path = pods_folder().join(format!("{PROBE_WORKER}_{provider}.yaml"));
        let manifest = fs::read_to_string(&path).expect("probe template");
        let template = parse_template(&manifest)
            .expect("parses")
            .expect("is a workload template");
        assert_eq!(template.component, PROBE_WORKER, "{provider}");
        assert_eq!(template.kind, TemplateKind::Job, "{provider}");

        let job: Job = serde_yaml::from_str(&manifest).expect("a Job manifest");
        let spec = job.spec.expect("job spec");
        assert_eq!(spec.backoff_limit, Some(0), "{provider}");
        assert_eq!(spec.active_deadline_seconds, Some(330), "{provider}");
        let pod = spec.template.spec.expect("pod spec");
        let probe = pod
            .containers
            .iter()
            .find(|container| container.name == "1000-probe")
            .expect("the probe container");
        assert!(
            probe.command.as_ref().unwrap()[0].contains("td-probe-connection"),
            "{provider}"
        );
        let secrets: Vec<_> = probe
            .env_from
            .iter()
            .flatten()
            .filter_map(|source| source.secret_ref.as_ref())
            .map(|secret| secret.name.as_str())
            .collect();
        assert_eq!(
            secrets,
            ["${{system:tdk_connection_secret_name}}"],
            "{provider}"
        );
        let volumes: Vec<_> = pod
            .volumes
            .iter()
            .flatten()
            .map(|v| v.name.as_str())
            .collect();
        assert!(volumes.contains(&"connection"), "{provider}");
        assert!(volumes.contains(&"probe-request"), "{provider}");
    }
}
