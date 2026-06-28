//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::ByteString;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapEnvSource, Container, EnvFromSource, Pod, Secret, SecretEnvSource,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use td_bridge::messaging::{CronStartRequest, FunctionStartRequest, StageTriggerStartRequest};
use td_common::worker::{Callback, HttpCallback};

use crate::base::error::kubernetes::messaging::KubeMessagingError;
use crate::base::labels::LABEL_WORKER_ID;
use crate::runtime::gateway::messaging::messenger::request::WorkerStartRequest;

const ENV_TD_WORKER: &str = "TD_WORKER";
const ENV_TD_WORKER_ID: &str = "TD_WORKER_ID";
const ENV_TD_DECORATOR: &str = "TD_DECORATOR";

const ENV_TD_RESOLVER_URL: &str = "TD_RESOLVER_URL";
const ENV_TD_RESOLVER_METHOD: &str = "TD_RESOLVER_METHOD";
const ENV_TD_RESOLVER_AUTHORIZATION: &str = "TD_RESOLVER_AUTHORIZATION";
const ENV_TD_TRIGGER_URL: &str = "TD_TRIGGER_URL";
const ENV_TD_TRIGGER_METHOD: &str = "TD_TRIGGER_METHOD";
const ENV_TD_TRIGGER_AUTHORIZATION: &str = "TD_TRIGGER_AUTHORIZATION";
const ENV_TD_RESPONDER_URL: &str = "TD_RESPONDER_URL";
const ENV_TD_RESPONDER_METHOD: &str = "TD_RESPONDER_METHOD";
const ENV_TD_RESPONDER_AUTHORIZATION: &str = "TD_RESPONDER_AUTHORIZATION";

const HEADER_AUTHORIZATION: &str = "authorization";

const MAIN_NAME_SUFFIX: &str = "-main";
const LATCH_NAME_SUFFIX: &str = "-latch";
const ENV_TD_LATCH: &str = "TD_LATCH";
const LATCH_VALUE_READY: &str = "ready";
const SECRET_ENV_PREFIX: &str = "SECRET__";

pub struct FunctionRunKit {
    pub main_configmap: ConfigMap,
    pub main_secret: Secret,
    pub latch_configmap: ConfigMap,
    pub envfrom: Vec<EnvFromSource>,
    pub labels: Vec<(String, String)>,
}

pub(crate) fn build_worker_run_kit(
    pod_name: &str,
    namespace: &str,
    request: &impl WorkerStartRequest,
) -> Result<FunctionRunKit, KubeMessagingError> {
    let worker = request.worker();
    let worker_id = request.worker_id();

    let mut config_data = BTreeMap::new();
    config_data.insert(ENV_TD_WORKER.to_owned(), worker.to_owned());
    config_data.insert(ENV_TD_WORKER_ID.to_owned(), worker_id.clone());
    if let Some(decorator) = request.decorator() {
        config_data.insert(ENV_TD_DECORATOR.to_owned(), decorator.to_owned());
    }

    let mut secret_data: BTreeMap<String, ByteString> = BTreeMap::new();
    if let Some(resolver) = request.resolver() {
        insert_resolver_secrets(&mut secret_data, expect_http(resolver, worker, &worker_id)?);
    }
    if let Some(trigger) = request.trigger() {
        insert_trigger_secrets(&mut secret_data, expect_http(trigger, worker, &worker_id)?);
    }
    if let Some(responder) = request.responder() {
        insert_responder_secrets(
            &mut secret_data,
            expect_http(responder, worker, &worker_id)?,
        );
    }

    let mut latch_data = BTreeMap::new();
    latch_data.insert(ENV_TD_LATCH.to_owned(), LATCH_VALUE_READY.to_owned());

    let main_name = format!("{pod_name}{MAIN_NAME_SUFFIX}");
    let latch_name = format!("{pod_name}{LATCH_NAME_SUFFIX}");

    let mut labels = request.labels();
    labels.push((LABEL_WORKER_ID.to_owned(), worker_id));

    Ok(FunctionRunKit {
        main_configmap: build_configmap(&main_name, namespace, config_data),
        main_secret: build_secret(&main_name, namespace, secret_data),
        latch_configmap: build_configmap(&latch_name, namespace, latch_data),
        envfrom: vec![
            envfrom_configmap(&main_name),
            envfrom_secret(&main_name),
            envfrom_configmap(&latch_name),
        ],
        labels,
    })
}

pub fn build_function_run_kit(
    pod_name: &str,
    namespace: &str,
    request: &FunctionStartRequest,
) -> Result<FunctionRunKit, KubeMessagingError> {
    build_worker_run_kit(pod_name, namespace, request)
}

pub fn build_stream_run_kit(
    pod_name: &str,
    namespace: &str,
    request: &StageTriggerStartRequest,
) -> Result<FunctionRunKit, KubeMessagingError> {
    build_worker_run_kit(pod_name, namespace, request)
}

pub fn build_cron_run_kit(
    pod_name: &str,
    namespace: &str,
    request: &CronStartRequest,
) -> Result<FunctionRunKit, KubeMessagingError> {
    build_worker_run_kit(pod_name, namespace, request)
}

pub fn inject_labels_into_pod(pod: &mut Pod, labels: &[(String, String)]) {
    apply_labels(&mut pod.metadata, labels);
}

pub fn inject_labels_into_job(job: &mut Job, labels: &[(String, String)]) {
    apply_labels(&mut job.metadata, labels);
    if let Some(spec) = job.spec.as_mut() {
        let template_metadata = spec
            .template
            .metadata
            .get_or_insert_with(ObjectMeta::default);
        apply_labels(template_metadata, labels);
    }
}

fn apply_labels(metadata: &mut ObjectMeta, labels: &[(String, String)]) {
    let entry = metadata.labels.get_or_insert_with(BTreeMap::new);
    for (key, value) in labels {
        entry.insert(key.clone(), value.clone());
    }
}

pub fn inject_envfrom_into_pod(pod: &mut Pod, envfrom: &[EnvFromSource]) {
    let Some(spec) = pod.spec.as_mut() else {
        return;
    };
    inject_envfrom_into_containers(spec.init_containers.as_mut(), envfrom);
    inject_envfrom_into_containers(Some(&mut spec.containers), envfrom);
}

pub fn inject_envfrom_into_job(job: &mut Job, envfrom: &[EnvFromSource]) {
    let Some(template) = job
        .spec
        .as_mut()
        .and_then(|spec| spec.template.spec.as_mut())
    else {
        return;
    };
    inject_envfrom_into_containers(template.init_containers.as_mut(), envfrom);
    inject_envfrom_into_containers(Some(&mut template.containers), envfrom);
}

fn inject_envfrom_into_containers(
    containers: Option<&mut Vec<Container>>,
    envfrom: &[EnvFromSource],
) {
    let Some(containers) = containers else {
        return;
    };
    for container in containers {
        container
            .env_from
            .get_or_insert_with(Vec::new)
            .extend(envfrom.iter().cloned());
    }
}

fn expect_http<'a>(
    callback: &'a Callback,
    worker: &str,
    worker_id: &str,
) -> Result<&'a HttpCallback, KubeMessagingError> {
    match callback {
        Callback::Http(http) => Ok(http),
        #[allow(unreachable_patterns)]
        _ => Err(KubeMessagingError::UnsupportedCallbackVariant {
            worker: worker.to_owned(),
            worker_id: worker_id.to_owned(),
        }),
    }
}

fn insert_secret_entry(data: &mut BTreeMap<String, ByteString>, key: &str, value: &str) {
    data.insert(key.to_owned(), ByteString(value.as_bytes().to_vec()));
}

fn insert_callback_secrets(
    data: &mut BTreeMap<String, ByteString>,
    (url_key, method_key, authorization_key): (&str, &str, &str),
    callback: &HttpCallback,
) {
    insert_secret_entry(data, url_key, callback.url.as_str());
    insert_secret_entry(data, method_key, callback.method.as_str());
    if let Some(authorization) = callback.headers.get(HEADER_AUTHORIZATION) {
        insert_secret_entry(data, authorization_key, authorization);
    }
}

fn insert_resolver_secrets(data: &mut BTreeMap<String, ByteString>, resolver: &HttpCallback) {
    insert_callback_secrets(
        data,
        (
            ENV_TD_RESOLVER_URL,
            ENV_TD_RESOLVER_METHOD,
            ENV_TD_RESOLVER_AUTHORIZATION,
        ),
        resolver,
    );
}

fn insert_trigger_secrets(data: &mut BTreeMap<String, ByteString>, trigger: &HttpCallback) {
    insert_callback_secrets(
        data,
        (
            ENV_TD_TRIGGER_URL,
            ENV_TD_TRIGGER_METHOD,
            ENV_TD_TRIGGER_AUTHORIZATION,
        ),
        trigger,
    );
}

fn insert_responder_secrets(data: &mut BTreeMap<String, ByteString>, responder: &HttpCallback) {
    insert_callback_secrets(
        data,
        (
            ENV_TD_RESPONDER_URL,
            ENV_TD_RESPONDER_METHOD,
            ENV_TD_RESPONDER_AUTHORIZATION,
        ),
        responder,
    );
}

fn build_configmap(name: &str, namespace: &str, data: BTreeMap<String, String>) -> ConfigMap {
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    }
}

fn build_secret(name: &str, namespace: &str, data: BTreeMap<String, ByteString>) -> Secret {
    Secret {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    }
}

fn envfrom_configmap(name: &str) -> EnvFromSource {
    EnvFromSource {
        config_map_ref: Some(ConfigMapEnvSource {
            name: name.to_owned(),
            optional: Some(false),
        }),
        prefix: None,
        secret_ref: None,
    }
}

fn envfrom_secret(name: &str) -> EnvFromSource {
    EnvFromSource {
        secret_ref: Some(SecretEnvSource {
            name: name.to_owned(),
            optional: Some(false),
        }),
        prefix: Some(SECRET_ENV_PREFIX.to_owned()),
        config_map_ref: None,
    }
}
