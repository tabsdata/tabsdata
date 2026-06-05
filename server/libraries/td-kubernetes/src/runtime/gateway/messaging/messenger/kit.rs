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
use td_common::server::{Callback, FunctionStartRequest, HttpCallback};

use crate::base::error::kubernetes::messaging::KubeMessagingError;

const ENV_TD_WORKER: &str = "TD_WORKER";
const ENV_TD_RUNID: &str = "TD_RUNID";
const ENV_TD_DECORATOR: &str = "TD_DECORATOR";

const ENV_TD_RESOLVER_URL: &str = "TD_RESOLVER_URL";
const ENV_TD_RESOLVER_METHOD: &str = "TD_RESOLVER_METHOD";
const ENV_TD_RESOLVER_AUTHORIZATION: &str = "TD_RESOLVER_AUTHORIZATION";
const ENV_TD_RESPONDER_URL: &str = "TD_RESPONDER_URL";
const ENV_TD_RESPONDER_METHOD: &str = "TD_RESPONDER_METHOD";
const ENV_TD_RESPONDER_AUTHORIZATION: &str = "TD_RESPONDER_AUTHORIZATION";

const HEADER_AUTHORIZATION: &str = "authorization";

const LATCH_NAME_SUFFIX: &str = "-latch";
const ENV_TD_LATCH: &str = "TD_LATCH";
const LATCH_VALUE_READY: &str = "ready";

pub struct FunctionRunKit {
    pub main_configmap: ConfigMap,
    pub main_secret: Secret,
    pub latch_configmap: ConfigMap,
    pub envfrom: Vec<EnvFromSource>,
}

pub fn build_function_run_kit(
    pod_name: &str,
    namespace: &str,
    request: &FunctionStartRequest,
) -> Result<FunctionRunKit, KubeMessagingError> {
    let resolver = expect_http(&request.resolver, &request.worker, &request.run_id)?;
    let responder = expect_http(&request.responder, &request.worker, &request.run_id)?;

    let mut config_data = BTreeMap::new();
    config_data.insert(ENV_TD_WORKER.to_owned(), request.worker.clone());
    config_data.insert(ENV_TD_RUNID.to_owned(), request.run_id.clone());
    config_data.insert(
        ENV_TD_DECORATOR.to_owned(),
        request.decorator.as_ref().to_owned(),
    );

    let mut secret_data: BTreeMap<String, ByteString> = BTreeMap::new();
    insert_secret_entry(&mut secret_data, ENV_TD_RESOLVER_URL, resolver.url.as_str());
    insert_secret_entry(
        &mut secret_data,
        ENV_TD_RESOLVER_METHOD,
        resolver.method.as_str(),
    );
    if let Some(authorization) = resolver.headers.get(HEADER_AUTHORIZATION) {
        insert_secret_entry(&mut secret_data, ENV_TD_RESOLVER_AUTHORIZATION, authorization);
    }
    insert_secret_entry(&mut secret_data, ENV_TD_RESPONDER_URL, responder.url.as_str());
    insert_secret_entry(
        &mut secret_data,
        ENV_TD_RESPONDER_METHOD,
        responder.method.as_str(),
    );
    if let Some(authorization) = responder.headers.get(HEADER_AUTHORIZATION) {
        insert_secret_entry(
            &mut secret_data,
            ENV_TD_RESPONDER_AUTHORIZATION,
            authorization,
        );
    }

    let mut latch_data = BTreeMap::new();
    latch_data.insert(ENV_TD_LATCH.to_owned(), LATCH_VALUE_READY.to_owned());

    let latch_name = format!("{pod_name}{LATCH_NAME_SUFFIX}");

    Ok(FunctionRunKit {
        main_configmap: build_configmap(pod_name, namespace, config_data),
        main_secret: build_secret(pod_name, namespace, secret_data),
        latch_configmap: build_configmap(&latch_name, namespace, latch_data),
        envfrom: vec![
            envfrom_configmap(pod_name),
            envfrom_secret(pod_name),
            envfrom_configmap(&latch_name),
        ],
    })
}

pub fn inject_envfrom_into_pod(pod: &mut Pod, envfrom: &[EnvFromSource]) {
    let Some(spec) = pod.spec.as_mut() else {
        return;
    };
    inject_envfrom_into_containers(spec.init_containers.as_mut(), envfrom);
    inject_envfrom_into_containers(Some(&mut spec.containers), envfrom);
}

pub fn inject_envfrom_into_job(job: &mut Job, envfrom: &[EnvFromSource]) {
    let Some(template) = job.spec.as_mut().and_then(|spec| spec.template.spec.as_mut()) else {
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
    runid: &str,
) -> Result<&'a HttpCallback, KubeMessagingError> {
    match callback {
        Callback::Http(http) => Ok(http),
        #[allow(unreachable_patterns)]
        _ => Err(KubeMessagingError::UnsupportedCallbackVariant {
            worker: worker.to_owned(),
            runid: runid.to_owned(),
        }),
    }
}

fn insert_secret_entry(data: &mut BTreeMap<String, ByteString>, key: &str, value: &str) {
    data.insert(key.to_owned(), ByteString(value.as_bytes().to_vec()));
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
        prefix: None,
        config_map_ref: None,
    }
}
