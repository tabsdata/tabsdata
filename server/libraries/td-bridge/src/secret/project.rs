//
// Copyright 2026 Tabsdata Inc.
//

use super::SecretStore;
use td_error::TdError;
use td_objects::dxo::secret::{SecretEntry, SecretHydrated, SecretOwner, SecretRead, SecretUpsert};
use td_objects::types::basic::{EntityName, ProjectId, ProjectName};

pub struct ProjectSecrets<'a> {
    store: &'a dyn SecretStore,
}

impl<'a> ProjectSecrets<'a> {
    pub fn new(store: &'a dyn SecretStore) -> Self {
        Self { store }
    }

    pub async fn upsert(
        &self,
        project_id: &ProjectId,
        project: &ProjectName,
        secrets: &[SecretUpsert],
    ) -> Result<Vec<SecretRead>, TdError> {
        let entity = EntityName::try_from(project.to_string())?;
        self.store
            .upsert(&SecretOwner::project(project_id)?, &entity, secrets)
            .await
    }

    pub async fn rename(
        &self,
        project_id: &ProjectId,
        project: &ProjectName,
    ) -> Result<Vec<SecretRead>, TdError> {
        let kept: Vec<SecretUpsert> = self
            .fetch(project_id)
            .await?
            .into_iter()
            .map(|secret| SecretUpsert {
                name: secret.name,
                entries: secret
                    .keys
                    .into_iter()
                    .map(|key| SecretEntry { key, value: None })
                    .collect(),
            })
            .collect();
        self.upsert(project_id, project, &kept).await
    }

    pub async fn fetch(&self, project_id: &ProjectId) -> Result<Vec<SecretRead>, TdError> {
        self.store.fetch(&SecretOwner::project(project_id)?).await
    }

    pub async fn fetch_hydrated(
        &self,
        project_id: &ProjectId,
    ) -> Result<Vec<SecretHydrated>, TdError> {
        self.store
            .fetch_hydrated(&SecretOwner::project(project_id)?)
            .await
    }

    pub async fn delete(&self, project_id: &ProjectId) -> Result<(), TdError> {
        self.store.delete(&SecretOwner::project(project_id)?).await
    }
}
