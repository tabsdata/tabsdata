//
// Copyright 2026 Tabsdata Inc.
//

use serde_yaml::Value;

use crate::base::constants::{CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY};
use crate::base::naming::name_from_kube_resource_name;
use crate::kube::api::domain::labels::LabelFilter;
use crate::kube::api::entity::configmap::get_configmaps;
use crate::kube::context::KubeContext;

const APISERVER_SERVICE_CONFIG_FILE: &str = "apiserver.service.yaml";
const ADDRESSES_KEY: &str = "addresses";
const WILDCARD_HOST: &str = "0.0.0.0";
const LOCALHOST: &str = "localhost";

pub async fn apiserver_public_url(context: &KubeContext) -> Option<String> {
    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, KPROFILE_CONFIGS_FAMILY.kind);
    let configmaps = get_configmaps(context, Some(filter)).await.ok()?;
    for (configmap_name, items) in configmaps {
        let file = name_from_kube_resource_name(
            &configmap_name,
            KPROFILE_CONFIGS_FAMILY.scope,
            KPROFILE_CONFIGS_FAMILY.kind,
        )
        .ok()?;
        if file != APISERVER_SERVICE_CONFIG_FILE {
            continue;
        }
        for (_key, value) in items {
            let yaml = String::from_utf8(value).ok()?;
            if let Some(url) = url_from_service_config(&yaml) {
                return Some(url);
            }
        }
    }
    None
}

fn url_from_service_config(yaml: &str) -> Option<String> {
    let document: Value = serde_yaml::from_str(yaml).ok()?;
    let address = document
        .get(ADDRESSES_KEY)?
        .as_sequence()?
        .first()?
        .as_str()?;
    let (host, port) = address.rsplit_once(':')?;
    let host = if host == WILDCARD_HOST {
        LOCALHOST
    } else {
        host
    };
    Some(format!("http://{host}:{port}"))
}
