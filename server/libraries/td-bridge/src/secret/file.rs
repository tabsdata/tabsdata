//
// Copyright 2026 Tabsdata Inc.
//

use super::SecretStore;
use async_trait::async_trait;
use td_error::TdError;
use td_objects::dxo::secret::{SecretHydrated, SecretOwner, SecretRead, SecretUpsert};
use td_objects::types::basic::EntityName;

#[derive(Default)]
pub struct FileSecretStore;

#[async_trait]
impl SecretStore for FileSecretStore {
    async fn upsert(
        &self,
        _: &SecretOwner,
        _: &EntityName,
        _: &[SecretUpsert],
    ) -> Result<Vec<SecretRead>, TdError> {
        Ok(Vec::new())
    }

    async fn fetch(&self, _: &SecretOwner) -> Result<Vec<SecretRead>, TdError> {
        Ok(Vec::new())
    }

    async fn fetch_hydrated(&self, _: &SecretOwner) -> Result<Vec<SecretHydrated>, TdError> {
        Ok(Vec::new())
    }

    async fn delete(&self, _: &SecretOwner) -> Result<(), TdError> {
        Ok(())
    }
}
