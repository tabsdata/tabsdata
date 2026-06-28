//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::ByteString;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapEnvSource, ConfigMapVolumeSource, Container, EnvFromSource, KeyToPath, Pod,
    Secret, SecretEnvSource, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde::Serialize;
use td_bridge::messaging::{CronStartRequest, FunctionStartRequest, StageTriggerStartRequest};
use td_common::worker::{Callback, HttpCallback};
use td_objects::dxo::yaml::location::Location;

use crate::base::constants::TABSDATA_LABEL_NAMESPACE;
use crate::base::error::kubernetes::messaging::KubeMessagingError;

use const_format::concatcp;

pub(crate) const LABEL_COLLECTION_ID: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "collection-id");
pub(crate) const LABEL_COLLECTION: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "collection");
pub(crate) const LABEL_FUNCTION_ID: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "function-id");
pub(crate) const LABEL_FUNCTION: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "function");
pub(crate) const LABEL_FUNCTION_VERSION_ID: &str =
    concatcp!(TABSDATA_LABEL_NAMESPACE, "function-version-id");
pub(crate) const LABEL_WORKER_ID: &str = concatcp!(TABSDATA_LABEL_NAMESPACE, "worker-id");

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
const REQUEST_NAME_SUFFIX: &str = "-request";
const ENV_TD_LATCH: &str = "TD_LATCH";
const LATCH_VALUE_READY: &str = "ready";
const SECRET_ENV_PREFIX: &str = "SECRET__";

/// ConfigMap key + in-pod filename for the streamer's `request.yaml`.
/// The ConfigMap is volume-mounted at `REQUEST_MOUNT_DIR` so the
/// streamer pod's containers can read it via
/// `--request <REQUEST_MOUNT_DIR>/<key>` without any HTTP resolver
/// round-trip (a streamer's envelope is static; see TD-2570).
const REQUEST_CONFIGMAP_KEY: &str = "request.yaml";
/// Directory the ConfigMap volume mounts at. Kubernetes mounts a
/// ConfigMap volume as a directory whose entries are the configmap's
/// keys; with `items: [{key, path}]` only `request.yaml` appears in
/// it. We mount a directory rather than `subPath`-mounting a single
/// file because kminus's pod-runner rejects subPath. Separate from
/// `/tabsdata/shared/` (an emptyDir) so the two mounts don't collide.
const REQUEST_MOUNT_DIR: &str = "/tabsdata/request";
const REQUEST_VOLUME_NAME: &str = "request";

/// Envelope tag the streamer's `_lib/request.py` loader expects.
const STREAMER_ENVELOPE_TAG: &str = "StreamerV1";

pub struct FunctionRunKit {
    pub main_configmap: ConfigMap,
    pub main_secret: Secret,
    pub latch_configmap: ConfigMap,
    /// Per-pod ConfigMap whose single `request.yaml` entry is mounted
    /// as a file at `REQUEST_MOUNT_PATH` on every container.
    /// `Some` only for the streamer pod kit -- the streamer is the
    /// only worker that ships its `request.yaml` via configmap rather
    /// than fetching it from the apiserver (TD-2570: a streamer has
    /// no late binding to defer, so the envelope is fully knowable at
    /// Job-create time).
    pub request_configmap: Option<ConfigMap>,
    pub envfrom: Vec<EnvFromSource>,
    pub labels: Vec<(String, String)>,
}

/// `StreamerV1` envelope the gateway writes into the streamer pod's
/// `request.yaml`. Mirrors the Python loader's `StreamerInfo` shape
/// in `_lib/request.py` -- if you change either, change both.
#[derive(Serialize)]
struct StreamerRequestInfo<'a> {
    project_id: String,
    collection_id: String,
    collection: String,
    function_version_id: String,
    function_defined_on: i64,
    function: String,
    function_bundle: &'a Location,
    function_stage: &'a Location,
    scheduled_on: i64,
    worker_id: String,
}

pub fn build_function_run_kit(
    pod_name: &str,
    namespace: &str,
    request: &FunctionStartRequest,
) -> Result<FunctionRunKit, KubeMessagingError> {
    let worker = request.worker.as_ref();
    let worker_id = request.worker_id.to_string();
    let resolver = expect_http(&request.resolver, worker, &worker_id)?;
    let responder = expect_http(&request.responder, worker, &worker_id)?;

    let mut config_data = BTreeMap::new();
    config_data.insert(ENV_TD_WORKER.to_owned(), worker.to_owned());
    config_data.insert(ENV_TD_WORKER_ID.to_owned(), worker_id.clone());
    config_data.insert(
        ENV_TD_DECORATOR.to_owned(),
        request.decorator.as_ref().to_owned(),
    );

    let mut secret_data: BTreeMap<String, ByteString> = BTreeMap::new();
    insert_resolver_secrets(&mut secret_data, resolver);
    insert_responder_secrets(&mut secret_data, responder);

    let mut latch_data = BTreeMap::new();
    latch_data.insert(ENV_TD_LATCH.to_owned(), LATCH_VALUE_READY.to_owned());

    let main_name = format!("{pod_name}{MAIN_NAME_SUFFIX}");
    let latch_name = format!("{pod_name}{LATCH_NAME_SUFFIX}");

    let labels = vec![
        (
            LABEL_COLLECTION_ID.to_owned(),
            request.collection_id.to_string(),
        ),
        (LABEL_COLLECTION.to_owned(), request.collection.to_string()),
        (
            LABEL_FUNCTION_ID.to_owned(),
            request.function_id.to_string(),
        ),
        (LABEL_FUNCTION.to_owned(), request.function.to_string()),
        (LABEL_WORKER_ID.to_owned(), worker_id.clone()),
    ];

    Ok(FunctionRunKit {
        main_configmap: build_configmap(&main_name, namespace, config_data),
        main_secret: build_secret(&main_name, namespace, secret_data),
        latch_configmap: build_configmap(&latch_name, namespace, latch_data),
        request_configmap: None,
        envfrom: vec![
            envfrom_configmap(&main_name),
            envfrom_secret(&main_name),
            envfrom_configmap(&latch_name),
        ],
        labels,
    })
}

pub fn build_stream_run_kit(
    pod_name: &str,
    namespace: &str,
    request: &StageTriggerStartRequest,
) -> Result<FunctionRunKit, KubeMessagingError> {
    let worker = request.worker.as_ref();
    let worker_id = request.worker_id.to_string();
    let trigger = expect_http(&request.trigger, worker, &worker_id)?;
    let responder = expect_http(&request.responder, worker, &worker_id)?;

    let mut config_data = BTreeMap::new();
    config_data.insert(ENV_TD_WORKER.to_owned(), worker.to_owned());
    config_data.insert(ENV_TD_WORKER_ID.to_owned(), worker_id.clone());
    config_data.insert(
        ENV_TD_DECORATOR.to_owned(),
        request.decorator.as_ref().to_owned(),
    );

    let mut secret_data: BTreeMap<String, ByteString> = BTreeMap::new();
    insert_trigger_secrets(&mut secret_data, trigger);
    insert_responder_secrets(&mut secret_data, responder);

    let mut latch_data = BTreeMap::new();
    latch_data.insert(ENV_TD_LATCH.to_owned(), LATCH_VALUE_READY.to_owned());

    let main_name = format!("{pod_name}{MAIN_NAME_SUFFIX}");
    let latch_name = format!("{pod_name}{LATCH_NAME_SUFFIX}");
    let request_name = format!("{pod_name}{REQUEST_NAME_SUFFIX}");

    // Serialize a `StreamerV1` envelope from the lean request struct
    // -- the streamer pod reads it from a configmap-mounted file
    // instead of fetching it from the apiserver. Mirrors the Python
    // `_lib/request.py:StreamerInfo` model field-for-field; if you
    // change either, change both.
    let envelope_yaml = build_streamer_envelope_yaml(request, &worker_id)?;
    let mut request_data = BTreeMap::new();
    request_data.insert(REQUEST_CONFIGMAP_KEY.to_owned(), envelope_yaml);

    let labels = vec![
        (
            LABEL_COLLECTION_ID.to_owned(),
            request.collection_id.to_string(),
        ),
        (LABEL_COLLECTION.to_owned(), request.collection.to_string()),
        (
            LABEL_FUNCTION_VERSION_ID.to_owned(),
            request.function_version_id.to_string(),
        ),
        (LABEL_FUNCTION.to_owned(), request.function.to_string()),
        (LABEL_WORKER_ID.to_owned(), worker_id.clone()),
    ];

    Ok(FunctionRunKit {
        main_configmap: build_configmap(&main_name, namespace, config_data),
        main_secret: build_secret(&main_name, namespace, secret_data),
        latch_configmap: build_configmap(&latch_name, namespace, latch_data),
        request_configmap: Some(build_configmap(&request_name, namespace, request_data)),
        envfrom: vec![
            envfrom_configmap(&main_name),
            envfrom_secret(&main_name),
            envfrom_configmap(&latch_name),
        ],
        labels,
    })
}

pub fn build_cron_run_kit(
    pod_name: &str,
    namespace: &str,
    request: &CronStartRequest,
) -> Result<FunctionRunKit, KubeMessagingError> {
    let worker = request.worker.as_ref();
    let worker_id = request.worker_id.to_string();
    let trigger = expect_http(&request.trigger, worker, &worker_id)?;
    let responder = expect_http(&request.responder, worker, &worker_id)?;

    let mut config_data = BTreeMap::new();
    config_data.insert(ENV_TD_WORKER.to_owned(), worker.to_owned());
    config_data.insert(ENV_TD_WORKER_ID.to_owned(), worker_id.clone());

    let mut secret_data: BTreeMap<String, ByteString> = BTreeMap::new();
    insert_trigger_secrets(&mut secret_data, trigger);
    insert_responder_secrets(&mut secret_data, responder);

    let mut latch_data = BTreeMap::new();
    latch_data.insert(ENV_TD_LATCH.to_owned(), LATCH_VALUE_READY.to_owned());

    let main_name = format!("{pod_name}{MAIN_NAME_SUFFIX}");
    let latch_name = format!("{pod_name}{LATCH_NAME_SUFFIX}");

    let labels = vec![
        (
            LABEL_COLLECTION_ID.to_owned(),
            request.collection_id.to_string(),
        ),
        (LABEL_COLLECTION.to_owned(), request.collection.to_string()),
        (
            LABEL_FUNCTION_VERSION_ID.to_owned(),
            request.function_version_id.to_string(),
        ),
        (LABEL_FUNCTION.to_owned(), request.function.to_string()),
        (LABEL_WORKER_ID.to_owned(), worker_id.clone()),
    ];

    Ok(FunctionRunKit {
        main_configmap: build_configmap(&main_name, namespace, config_data),
        main_secret: build_secret(&main_name, namespace, secret_data),
        latch_configmap: build_configmap(&latch_name, namespace, latch_data),
        request_configmap: None,
        envfrom: vec![
            envfrom_configmap(&main_name),
            envfrom_secret(&main_name),
            envfrom_configmap(&latch_name),
        ],
        labels,
    })
}

/// Serialize the streamer's `request.yaml` as a YAML string ready to
/// drop into a ConfigMap entry. Output is the single-variant envelope
/// `StreamerV1` whose payload matches the Python loader's
/// `_lib/request.py:StreamerInfo` model field-for-field.
fn build_streamer_envelope_yaml(
    request: &StageTriggerStartRequest,
    worker_id: &str,
) -> Result<String, KubeMessagingError> {
    // The lean `StageTriggerStartRequest` does not carry
    // `project_id` or `function_defined_on`. They're not consumed by
    // any streamer-pod container today (`run_streamer` /
    // `localize_fn_bundle` / `run_manifest_trigger`), and the Python
    // `StreamerInfo` model marks them with `extra="ignore"`-style
    // tolerance via `_AnyRequestInfo`. We deliberately omit them
    // here rather than fake values; if a future consumer needs them,
    // they should be added to the bridge struct and the Python
    // model in lockstep.
    let info = StreamerRequestInfo {
        project_id: String::new(),
        collection_id: request.collection_id.to_string(),
        collection: request.collection.to_string(),
        function_version_id: request.function_version_id.to_string(),
        function_defined_on: 0,
        function: request.function.to_string(),
        function_bundle: &request.function_bundle,
        function_stage: &request.function_stage,
        scheduled_on: 0,
        worker_id: worker_id.to_owned(),
    };
    // Serialise as a single-variant envelope: `{StreamerV1: {info: {...}}}`.
    let mut envelope = serde_yaml::Mapping::new();
    let mut payload = serde_yaml::Mapping::new();
    payload.insert(
        serde_yaml::Value::String("info".to_owned()),
        serde_yaml::to_value(&info).map_err(|e| KubeMessagingError::SerializationFailed {
            what: "streamer request.yaml info".to_owned(),
            cause: e.to_string(),
        })?,
    );
    envelope.insert(
        serde_yaml::Value::String(STREAMER_ENVELOPE_TAG.to_owned()),
        serde_yaml::Value::Mapping(payload),
    );
    serde_yaml::to_string(&serde_yaml::Value::Mapping(envelope)).map_err(|e| {
        KubeMessagingError::SerializationFailed {
            what: "streamer request.yaml envelope".to_owned(),
            cause: e.to_string(),
        }
    })
}

/// Inject the streamer pod's `request.yaml` configmap volume and
/// per-container volumeMount. Idempotent and a no-op when the kit
/// carries no `request_configmap`.
pub fn inject_request_volume_into_pod(pod: &mut Pod, configmap_name: &str) {
    let Some(spec) = pod.spec.as_mut() else {
        return;
    };
    spec.volumes
        .get_or_insert_with(Vec::new)
        .push(build_request_volume(configmap_name));
    inject_request_mount_into_containers(spec.init_containers.as_mut());
    inject_request_mount_into_containers(Some(&mut spec.containers));
}

pub fn inject_request_volume_into_job(job: &mut Job, configmap_name: &str) {
    let Some(template) = job
        .spec
        .as_mut()
        .and_then(|spec| spec.template.spec.as_mut())
    else {
        return;
    };
    template
        .volumes
        .get_or_insert_with(Vec::new)
        .push(build_request_volume(configmap_name));
    inject_request_mount_into_containers(template.init_containers.as_mut());
    inject_request_mount_into_containers(Some(&mut template.containers));
}

fn build_request_volume(configmap_name: &str) -> Volume {
    Volume {
        name: REQUEST_VOLUME_NAME.to_owned(),
        config_map: Some(ConfigMapVolumeSource {
            name: configmap_name.to_owned(),
            items: Some(vec![KeyToPath {
                key: REQUEST_CONFIGMAP_KEY.to_owned(),
                path: REQUEST_CONFIGMAP_KEY.to_owned(),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn inject_request_mount_into_containers(containers: Option<&mut Vec<Container>>) {
    let Some(containers) = containers else {
        return;
    };
    for container in containers {
        container
            .volume_mounts
            .get_or_insert_with(Vec::new)
            .push(VolumeMount {
                name: REQUEST_VOLUME_NAME.to_owned(),
                mount_path: REQUEST_MOUNT_DIR.to_owned(),
                read_only: Some(true),
                ..Default::default()
            });
    }
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

fn insert_resolver_secrets(data: &mut BTreeMap<String, ByteString>, resolver: &HttpCallback) {
    insert_secret_entry(data, ENV_TD_RESOLVER_URL, resolver.url.as_str());
    insert_secret_entry(data, ENV_TD_RESOLVER_METHOD, resolver.method.as_str());
    if let Some(authorization) = resolver.headers.get(HEADER_AUTHORIZATION) {
        insert_secret_entry(data, ENV_TD_RESOLVER_AUTHORIZATION, authorization);
    }
}

fn insert_trigger_secrets(data: &mut BTreeMap<String, ByteString>, trigger: &HttpCallback) {
    insert_secret_entry(data, ENV_TD_TRIGGER_URL, trigger.url.as_str());
    insert_secret_entry(data, ENV_TD_TRIGGER_METHOD, trigger.method.as_str());
    if let Some(authorization) = trigger.headers.get(HEADER_AUTHORIZATION) {
        insert_secret_entry(data, ENV_TD_TRIGGER_AUTHORIZATION, authorization);
    }
}

fn insert_responder_secrets(data: &mut BTreeMap<String, ByteString>, responder: &HttpCallback) {
    insert_secret_entry(data, ENV_TD_RESPONDER_URL, responder.url.as_str());
    insert_secret_entry(data, ENV_TD_RESPONDER_METHOD, responder.method.as_str());
    if let Some(authorization) = responder.headers.get(HEADER_AUTHORIZATION) {
        insert_secret_entry(data, ENV_TD_RESPONDER_AUTHORIZATION, authorization);
    }
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
