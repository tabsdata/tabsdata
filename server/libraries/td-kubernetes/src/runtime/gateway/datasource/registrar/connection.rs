//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use async_trait::async_trait;
use td_bridge::connection::{ConnectionStore, merge_kept, read_of};
use td_error::TdError;
use td_objects::dxo::connection::{ConnectionRead, ConnectionUpsert, KeyValue};
use td_objects::types::basic::{CollectionId, CollectionName, ConnectionId, ProjectId};

use crate::base::error::connection::KubeConnectionError;
use crate::base::labels::{
    LABEL_COLLECTION, LABEL_COLLECTION_ID, LABEL_CONNECTION_ID, LABEL_PROJECT_ID,
};
use crate::base::naming::to_connection_object_name;
use crate::kube::api::{configmap, secret};
use crate::kube::context::KubeContext;

const CONNECTION_CONFIGMAP_KEY: &str = "connection.yaml";

pub struct KubernetesConnectionStore {
    context: KubeContext,
}

impl KubernetesConnectionStore {
    pub fn new(context: KubeContext) -> Self {
        Self { context }
    }

    fn name_object(&self, connection_id: &ConnectionId) -> Result<String, TdError> {
        to_connection_object_name(&connection_id.to_string()).map_err(|error| {
            KubeConnectionError::NameDerivationFailed {
                cause: error.to_string(),
            }
            .into()
        })
    }

    async fn read_object(
        &self,
        connection_id: &ConnectionId,
    ) -> Result<Option<ConnectionUpsert>, TdError> {
        let name = self.name_object(connection_id)?;
        let Some(stored_secrets) = secret::get_secret(&self.context, &name).await? else {
            return Ok(None);
        };
        Ok(Some(ConnectionUpsert {
            connection: None,
            secrets: Some(decode_secret_entries(stored_secrets)?),
            vars: None,
        }))
    }
}

#[async_trait]
impl ConnectionStore for KubernetesConnectionStore {
    async fn upsert(
        &self,
        project_id: &ProjectId,
        connection_id: &ConnectionId,
        collection: &CollectionName,
        collection_id: &CollectionId,
        connection: &ConnectionUpsert,
    ) -> Result<ConnectionRead, TdError> {
        let document = connection
            .connection
            .as_ref()
            .ok_or(KubeConnectionError::MissingDocument)?;

        let mut connection = connection.clone();
        connection.vars = None;
        let old_object = self.read_object(connection_id).await?;
        let new_object = merge_kept(&connection, old_object.as_ref())?;
        let name = self.name_object(connection_id)?;
        let labels = connection_labels(project_id, connection_id, collection, collection_id);
        let document_yaml = serde_yaml::to_string(document).map_err(|error| {
            KubeConnectionError::DocumentSerializationFailed {
                cause: error.to_string(),
            }
        })?;
        configmap::upsert_configmap(
            &self.context,
            &name,
            BTreeMap::from([(CONNECTION_CONFIGMAP_KEY.to_owned(), document_yaml)]),
            labels.clone(),
            None,
            None,
        )
        .await?;
        secret::upsert_secret(
            &self.context,
            &name,
            entries_to_map(new_object.secrets.as_deref()),
            labels,
            None,
            None,
        )
        .await?;
        Ok(read_of(&new_object))
    }

    async fn fetch(
        &self,
        _: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<ConnectionRead, TdError> {
        let name = self.name_object(connection_id)?;
        let document = configmap::get_configmap(&self.context, &name)
            .await?
            .ok_or(KubeConnectionError::NotConfigured)?;
        let secret_names = secret::get_secret(&self.context, &name)
            .await?
            .map(|data| data.into_keys().collect())
            .unwrap_or_default();
        Ok(ConnectionRead {
            id: None,
            connection: parse_document(&document)?,
            secret_names,
            var_names: Vec::new(),
        })
    }

    async fn delete(
        &self,
        _project_id: &ProjectId,
        connection_id: &ConnectionId,
    ) -> Result<(), TdError> {
        let name = self.name_object(connection_id)?;
        if configmap::get_configmap(&self.context, &name)
            .await?
            .is_some()
        {
            configmap::delete_configmap(&self.context, &name).await?;
        }
        if secret::get_secret(&self.context, &name).await?.is_some() {
            secret::delete_secret(&self.context, &name).await?;
        }
        Ok(())
    }
}

fn connection_labels(
    project_id: &ProjectId,
    connection_id: &ConnectionId,
    collection: &CollectionName,
    collection_id: &CollectionId,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        (LABEL_PROJECT_ID.to_owned(), project_id.to_string()),
        (LABEL_COLLECTION.to_owned(), collection.to_string()),
        (LABEL_COLLECTION_ID.to_owned(), collection_id.to_string()),
        (LABEL_CONNECTION_ID.to_owned(), connection_id.to_string()),
    ])
}

fn entries_to_map(entries: Option<&[KeyValue]>) -> BTreeMap<String, String> {
    entries
        .unwrap_or(&[])
        .iter()
        .filter_map(|kv| {
            kv.value
                .as_ref()
                .map(|value| (kv.key.clone(), value.clone()))
        })
        .collect()
}

fn decode_secret_entries(data: BTreeMap<String, Vec<u8>>) -> Result<Vec<KeyValue>, TdError> {
    data.into_iter()
        .map(|(key, bytes)| {
            let value =
                String::from_utf8(bytes).map_err(|error| KubeConnectionError::SecretNotUtf8 {
                    key: key.clone(),
                    cause: error.to_string(),
                })?;
            Ok(KeyValue {
                key,
                value: Some(value),
            })
        })
        .collect()
}

fn parse_document(data: &BTreeMap<String, String>) -> Result<serde_json::Value, TdError> {
    let raw = data.get(CONNECTION_CONFIGMAP_KEY).ok_or_else(|| {
        KubeConnectionError::DocumentKeyMissing {
            key: CONNECTION_CONFIGMAP_KEY.to_owned(),
        }
    })?;
    serde_yaml::from_str(raw).map_err(|error| {
        KubeConnectionError::DocumentCorrupt {
            cause: error.to_string(),
        }
        .into()
    })
}

#[cfg(test)]
mod tests;
