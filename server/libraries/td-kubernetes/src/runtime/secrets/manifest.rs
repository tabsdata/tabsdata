//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use crate::base::constants::{PART_OF_LABEL, PART_OF_TABSDATA, SECRET_FAMILY_LABEL};
use crate::base::error::secret::SecretError;
use crate::runtime::secrets::discovery::SecretLeaf;

pub const SECRET_MANIFEST_TYPE: &str = "Opaque";

pub fn build_secret_manifest(
    name: &str,
    namespace: &str,
    family: &str,
    leaves: &[SecretLeaf],
    values: Option<&BTreeMap<String, String>>,
) -> Secret {
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert(PART_OF_LABEL.to_owned(), PART_OF_TABSDATA.to_owned());
    labels.insert(SECRET_FAMILY_LABEL.to_owned(), family.to_owned());

    let mut string_data: BTreeMap<String, String> = BTreeMap::new();
    for leaf in leaves {
        let value = values
            .and_then(|map| map.get(&leaf.key))
            .cloned()
            .unwrap_or_default();
        string_data.insert(leaf.key.clone(), value);
    }

    Secret {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            labels: Some(labels),
            ..Default::default()
        },
        string_data: Some(string_data),
        type_: Some(SECRET_MANIFEST_TYPE.to_owned()),
        ..Default::default()
    }
}

pub fn serialize_secret_manifest(secret: &Secret) -> Result<String, SecretError> {
    serde_yaml::to_string(secret)
        .map_err(|error| SecretError::ManifestSerializeFailed(error.to_string()))
}

#[cfg(test)]
mod tests;
