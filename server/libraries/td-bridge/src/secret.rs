//
// Copyright 2026 Tabsdata Inc.
//

use async_trait::async_trait;
use td_error::TdError;
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
    Ok(desired
        .iter()
        .map(|secret| {
            let previous = stored
                .iter()
                .find(|stored| stored.name.eq_ignore_ascii_case(&secret.name));
            let entries = secret
                .entries
                .iter()
                .map(|entry| {
                    let value = entry.value.clone().or_else(|| {
                        previous
                            .and_then(|previous| {
                                previous
                                    .entries
                                    .iter()
                                    .find(|stored| stored.key.eq_ignore_ascii_case(&entry.key))
                            })
                            .and_then(|stored| stored.value.clone())
                    });
                    SecretEntry {
                        key: entry.key.clone(),
                        value: Some(value.unwrap_or_default()),
                    }
                })
                .collect();
            SecretHydrated {
                name: secret.name.clone(),
                entries,
            }
        })
        .collect())
}

#[cfg(test)]
mod tests;
