//
// Copyright 2026 Tabsdata Inc.
//

use crate::types::basic::{EntityId, ProjectId, SecretKey, SecretName};
use std::collections::HashSet;
use td_error::{ApiError, TdError, api_error};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::AsRefStr, strum::Display)]
pub enum SecretScope {
    #[strum(serialize = "prj")]
    Project,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SecretOwner {
    pub scope: SecretScope,
    pub entity_id: EntityId,
}

impl SecretOwner {
    pub fn project(project_id: &ProjectId) -> Result<Self, TdError> {
        Ok(Self {
            scope: SecretScope::Project,
            entity_id: EntityId::try_from(project_id)?,
        })
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct SecretEntry {
    pub key: SecretKey,
    pub value: Option<String>,
}

impl std::fmt::Debug for SecretEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretEntry")
            .field("key", &self.key)
            .field("value", &self.value.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

#[ta_objects::Dto]
pub struct SecretUpsert {
    pub name: SecretName,
    pub entries: Vec<SecretEntry>,
}

#[ta_objects::Dto]
pub struct SecretRead {
    pub name: SecretName,
    pub keys: Vec<SecretKey>,
}

#[ta_objects::Dto]
pub struct SecretsRead {
    pub secrets: Vec<SecretRead>,
}

#[ta_objects::Dto]
pub struct SecretHydrated {
    pub name: SecretName,
    pub entries: Vec<SecretEntry>,
}

pub fn validate_secrets(secrets: &[SecretUpsert]) -> Result<(), TdError> {
    let mut names = HashSet::new();
    for secret in secrets {
        if !names.insert(secret.name.to_lowercase()) {
            return Err(api_error!(
                ApiError::InputError,
                "the secret '{}' is given more than once (names ignore case)",
                secret.name
            ));
        }
        if secret.entries.is_empty() {
            return Err(api_error!(
                ApiError::InputError,
                "the secret '{}' has no keys",
                secret.name
            ));
        }
        let mut keys = HashSet::new();
        for entry in &secret.entries {
            if !keys.insert(entry.key.to_lowercase()) {
                return Err(api_error!(
                    ApiError::InputError,
                    "the key '{}' is given more than once in the secret '{}' (keys ignore case)",
                    entry.key,
                    secret.name
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
