//
// Copyright 2026 Tabsdata Inc.
//

use async_trait::async_trait;
use td_error::{ApiError, TdError, api_error};
use td_objects::dxo::secret::{
    SecretEntry, SecretHydrated, SecretOwner, SecretRead, SecretUpsert, validate_secrets,
};
use td_objects::types::basic::EntityName;

pub mod file;
#[cfg(any(test, feature = "test-utils"))]
pub mod memory;
pub mod noop;
pub mod project;

#[async_trait]
pub trait SecretStore: Send + Sync {
    async fn upsert(
        &self,
        owner: &SecretOwner,
        entity: &EntityName,
        secrets: &[SecretUpsert],
    ) -> Result<Vec<SecretRead>, TdError>;

    async fn fetch(&self, owner: &SecretOwner) -> Result<Vec<SecretRead>, TdError>;

    async fn fetch_hydrated(&self, owner: &SecretOwner) -> Result<Vec<SecretHydrated>, TdError>;

    async fn delete(&self, owner: &SecretOwner) -> Result<(), TdError>;
}

pub fn read_of(secrets: &[SecretHydrated]) -> Vec<SecretRead> {
    secrets
        .iter()
        .map(|secret| SecretRead {
            name: secret.name.clone(),
            keys: secret
                .entries
                .iter()
                .map(|entry| entry.key.clone())
                .collect(),
        })
        .collect()
}

pub fn merge_kept(
    desired: &[SecretUpsert],
    stored: &[SecretHydrated],
) -> Result<Vec<SecretHydrated>, TdError> {
    validate_secrets(desired)?;
    desired
        .iter()
        .map(|secret| {
            let previous = stored.iter().find(|stored| stored.name == secret.name);
            let entries = secret
                .entries
                .iter()
                .map(|entry| {
                    if entry.value.is_some() {
                        return Ok(entry.clone());
                    }
                    previous
                        .and_then(|previous| {
                            previous
                                .entries
                                .iter()
                                .find(|stored| stored.key == entry.key)
                        })
                        .and_then(|stored| stored.value.clone())
                        .map(|value| SecretEntry {
                            key: entry.key.clone(),
                            value: Some(value),
                        })
                        .ok_or_else(|| {
                            api_error!(
                                ApiError::InputError,
                                "cannot keep the value of the key '{}' in the secret '{}': none is stored",
                                entry.key,
                                secret.name
                            )
                        })
                })
                .collect::<Result<Vec<_>, TdError>>()?;
            Ok(SecretHydrated {
                name: secret.name.clone(),
                entries,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests;
