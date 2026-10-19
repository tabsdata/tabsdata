//
// Copyright 2026 Tabsdata Inc.
//

use crate::tdmanager::commons::active_projects;
use async_trait::async_trait;
use std::collections::HashSet;
use td_database::access::{DbPool, Global};
use td_error::{TdError, td_error};
use td_kubernetes::kube::context::KubeContext;
use td_kubernetes::runtime::gateway::datasource::KubernetesSecretStore;
use td_objects::dxo::secret::SecretOwner;
use td_objects::types::basic::EntityName;
use tracing::info;

pub struct RuleContext {
    pub database: DbPool<Global>,
    pub kubernetes: KubeContext,
}

#[async_trait]
pub trait UpgradeRule: Send + Sync {
    fn id(&self) -> &'static str;

    async fn apply(&self, context: &RuleContext) -> Result<(), TdError>;
}

pub struct RuleRegistration(pub &'static dyn UpgradeRule);

inventory::collect!(RuleRegistration);

#[td_error]
pub enum UpgradeRuleError {
    #[error("Upgrade rule '{id}' is requested more than once.")]
    DuplicateRule { id: String } = 0,
    #[error("Upgrade rule '{id}' is not registered in this build.")]
    UnknownRule { id: String } = 1000,
    #[error("Upgrade rule '{id}' is registered more than once.")]
    AmbiguousRule { id: String } = 5000,
}

pub fn resolve(ids: &[String]) -> Result<Vec<&'static dyn UpgradeRule>, TdError> {
    resolve_in(
        inventory::iter::<RuleRegistration>
            .into_iter()
            .map(|registration| registration.0),
        ids,
    )
}

fn resolve_in(
    registered: impl Iterator<Item = &'static dyn UpgradeRule>,
    ids: &[String],
) -> Result<Vec<&'static dyn UpgradeRule>, TdError> {
    let registered: Vec<_> = registered.collect();
    let mut requested = HashSet::new();
    ids.iter()
        .map(|id| {
            if !requested.insert(id.as_str()) {
                return Err(UpgradeRuleError::DuplicateRule { id: id.clone() }.into());
            }
            let mut matching = registered.iter().filter(|rule| rule.id() == id);
            match (matching.next(), matching.next()) {
                (Some(rule), None) => Ok(*rule),
                (Some(_), Some(_)) => {
                    Err(UpgradeRuleError::AmbiguousRule { id: id.clone() }.into())
                }
                (None, _) => Err(UpgradeRuleError::UnknownRule { id: id.clone() }.into()),
            }
        })
        .collect()
}

pub async fn apply(
    context: &RuleContext,
    rules: &[&'static dyn UpgradeRule],
) -> Result<(), TdError> {
    for rule in rules {
        info!(rule = rule.id(), "Applying upgrade rule");
        rule.apply(context).await?;
        info!(rule = rule.id(), "Upgrade rule applied");
    }
    Ok(())
}

pub struct EnsureProjectSecret;

inventory::submit!(RuleRegistration(&EnsureProjectSecret));

#[async_trait]
impl UpgradeRule for EnsureProjectSecret {
    fn id(&self) -> &'static str {
        "ensure-project-secret"
    }

    async fn apply(&self, context: &RuleContext) -> Result<(), TdError> {
        let projects = active_projects(&context.database).await?;
        let store = KubernetesSecretStore::new(context.kubernetes.clone());
        let mut created = 0;
        for project in &projects {
            let owner = SecretOwner::project(&project.id)?;
            let entity = EntityName::try_from(project.name.to_string())?;
            if store.ensure(&owner, &entity).await? {
                info!(project_id = %project.id, project = %project.name, "Created the project secret");
                created += 1;
            }
        }
        info!(projects = projects.len(), created, "Project secret ensured");
        Ok(())
    }
}

#[cfg(test)]
mod tests;
