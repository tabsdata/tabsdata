//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use async_trait::async_trait;
use td_bridge::secret::{SecretStore, merge_kept, read_of};
use td_error::TdError;
use td_objects::dxo::secret::{
    SecretEntry, SecretHydrated, SecretOwner, SecretRead, SecretScope, SecretUpsert,
};
use td_objects::types::basic::{EntityName, SecretKey, SecretName};

use crate::base::annotations::ANNOTATION_PROJECT;
use crate::base::constants::Scope;
use crate::base::error::secret::SecretError;
use crate::base::labels::{LABEL_PROJECT_ID, LABEL_SCOPE};
use crate::base::naming::to_secrets_object_name;
use crate::kube::api::entity::secret;
use crate::kube::context::KubeContext;

const SECRET_KEY_SEPARATOR: &str = "__";
const SECRET_SIZE_LIMIT: usize = 1024 * 1024;

pub struct KubernetesSecretStore {
    context: KubeContext,
}

impl KubernetesSecretStore {
    pub fn new(context: KubeContext) -> Self {
        Self { context }
    }

    async fn read_object(&self, owner: &SecretOwner) -> Result<Vec<SecretHydrated>, TdError> {
        match secret::get_secret(&self.context, &object_name(owner)?).await? {
            Some(data) => decode(data),
            None => Ok(Vec::new()),
        }
    }
}

#[async_trait]
impl SecretStore for KubernetesSecretStore {
    async fn upsert(
        &self,
        owner: &SecretOwner,
        entity: &EntityName,
        secrets: &[SecretUpsert],
    ) -> Result<Vec<SecretRead>, TdError> {
        let stored = self.read_object(owner).await?;
        let merged = merge_kept(secrets, &stored)?;
        secret::upsert_secret(
            &self.context,
            &object_name(owner)?,
            encode(&merged)?,
            labels(owner),
            annotations(owner, entity),
            None,
            None,
            None,
        )
        .await?;
        Ok(read_of(&merged))
    }

    async fn fetch(&self, owner: &SecretOwner) -> Result<Vec<SecretRead>, TdError> {
        Ok(read_of(&self.read_object(owner).await?))
    }

    async fn fetch_hydrated(&self, owner: &SecretOwner) -> Result<Vec<SecretHydrated>, TdError> {
        self.read_object(owner).await
    }

    async fn delete(&self, owner: &SecretOwner) -> Result<(), TdError> {
        secret::delete_secret(&self.context, &object_name(owner)?).await?;
        Ok(())
    }
}

fn scope(owner: &SecretOwner) -> Scope {
    match owner.scope {
        SecretScope::Project => Scope::Project,
    }
}

fn object_name(owner: &SecretOwner) -> Result<String, TdError> {
    to_secrets_object_name(scope(owner), &owner.entity_id.to_string()).map_err(|error| {
        SecretError::EntitySecretsNameDerivationFailed {
            cause: error.to_string(),
        }
        .into()
    })
}

fn labels(owner: &SecretOwner) -> BTreeMap<String, String> {
    let entity_label = match owner.scope {
        SecretScope::Project => LABEL_PROJECT_ID,
    };
    BTreeMap::from([
        (LABEL_SCOPE.to_owned(), scope(owner).as_str().to_owned()),
        (entity_label.to_owned(), owner.entity_id.to_string()),
    ])
}

fn annotations(owner: &SecretOwner, entity: &EntityName) -> BTreeMap<String, String> {
    let entity_annotation = match owner.scope {
        SecretScope::Project => ANNOTATION_PROJECT,
    };
    BTreeMap::from([(entity_annotation.to_owned(), entity.to_string())])
}

fn encode(secrets: &[SecretHydrated]) -> Result<BTreeMap<String, String>, TdError> {
    let data: BTreeMap<String, String> = secrets
        .iter()
        .flat_map(|secret| {
            secret.entries.iter().filter_map(move |entry| {
                entry.value.as_ref().map(|value| {
                    (
                        format!("{}{SECRET_KEY_SEPARATOR}{}", secret.name, entry.key),
                        value.clone(),
                    )
                })
            })
        })
        .collect();
    let size = data
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum();
    if size > SECRET_SIZE_LIMIT {
        return Err(SecretError::EntitySecretsTooLarge {
            size,
            limit: SECRET_SIZE_LIMIT,
        }
        .into());
    }
    Ok(data)
}

fn decode(data: BTreeMap<String, Vec<u8>>) -> Result<Vec<SecretHydrated>, TdError> {
    let mut secrets: Vec<SecretHydrated> = Vec::new();
    for (data_key, bytes) in data {
        let (name, key) = data_key.split_once(SECRET_KEY_SEPARATOR).ok_or_else(|| {
            SecretError::EntitySecretKeyMalformed {
                key: data_key.clone(),
            }
        })?;
        let invalid = |error: TdError| SecretError::EntitySecretKeyInvalid {
            key: data_key.clone(),
            cause: error.to_string(),
        };
        let name = SecretName::try_from(name).map_err(invalid)?;
        let key = SecretKey::try_from(key).map_err(invalid)?;
        let value =
            String::from_utf8(bytes).map_err(|error| SecretError::EntitySecretValueNotUtf8 {
                key: data_key.clone(),
                cause: error.to_string(),
            })?;
        let entry = SecretEntry {
            key,
            value: Some(value),
        };
        match secrets.iter_mut().find(|secret| secret.name == name) {
            Some(secret) => secret.entries.push(entry),
            None => secrets.push(SecretHydrated {
                name,
                entries: vec![entry],
            }),
        }
    }
    Ok(secrets)
}

#[cfg(test)]
mod tests;
