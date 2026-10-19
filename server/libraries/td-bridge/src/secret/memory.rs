//
// Copyright 2026 Tabsdata Inc.
//

use super::{SecretStore, merge_kept, read_of};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;
use td_error::TdError;
use td_objects::dxo::secret::{SecretHydrated, SecretOwner, SecretRead, SecretUpsert};
use td_objects::types::basic::EntityName;

#[derive(Default)]
pub struct InMemorySecretStore {
    owners: Mutex<HashMap<SecretOwner, Vec<SecretHydrated>>>,
}

#[async_trait]
impl SecretStore for InMemorySecretStore {
    async fn upsert(
        &self,
        owner: &SecretOwner,
        _entity: &EntityName,
        secrets: &[SecretUpsert],
    ) -> Result<Vec<SecretRead>, TdError> {
        let mut owners = self.owners.lock().unwrap();
        let stored = owners.get(owner).map(Vec::as_slice).unwrap_or(&[]);
        let merged = merge_kept(secrets, stored)?;
        let read = read_of(&merged);
        owners.insert(owner.clone(), merged);
        Ok(read)
    }

    async fn fetch(&self, owner: &SecretOwner) -> Result<Vec<SecretRead>, TdError> {
        Ok(self
            .owners
            .lock()
            .unwrap()
            .get(owner)
            .map(|secrets| read_of(secrets))
            .unwrap_or_default())
    }

    async fn fetch_hydrated(&self, owner: &SecretOwner) -> Result<Vec<SecretHydrated>, TdError> {
        Ok(self
            .owners
            .lock()
            .unwrap()
            .get(owner)
            .cloned()
            .unwrap_or_default())
    }

    async fn delete(&self, owner: &SecretOwner) -> Result<(), TdError> {
        self.owners.lock().unwrap().remove(owner);
        Ok(())
    }
}
