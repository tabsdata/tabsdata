//
// Copyright 2026 Tabsdata Inc.
//

use super::SecretStore;
use async_trait::async_trait;
use http::StatusCode;
use td_error::{TdError, api_error};
use td_objects::dxo::secret::{SecretHydrated, SecretOwner, SecretRead, SecretUpsert};
use td_objects::types::basic::EntityName;

pub struct NoopSecretStore;

#[async_trait]
impl SecretStore for NoopSecretStore {
    async fn upsert(
        &self,
        _: &SecretOwner,
        _: &EntityName,
        _: &[SecretUpsert],
    ) -> Result<Vec<SecretRead>, TdError> {
        Err(unconfigured())
    }

    async fn fetch(&self, _: &SecretOwner) -> Result<Vec<SecretRead>, TdError> {
        Err(unconfigured())
    }

    async fn fetch_hydrated(&self, _: &SecretOwner) -> Result<Vec<SecretHydrated>, TdError> {
        Err(unconfigured())
    }

    async fn delete(&self, _: &SecretOwner) -> Result<(), TdError> {
        Err(unconfigured())
    }
}

fn unconfigured() -> TdError {
    api_error!(
        status = StatusCode::SERVICE_UNAVAILABLE,
        "the secret store is not configured"
    )
}
