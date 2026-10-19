//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project};
use td_bridge::BridgeProviderHandle;
use td_database::access::{DbPool, InProject};
use td_error::TdError;
use td_objects::dxo::crudl::{ReadRequest, WithinProject};
use td_objects::dxo::secret::SecretsRead;

#[service_factory]
async fn read_project_secrets(
    authz: Ctx<AuthzContext>,
    bridge: Ctx<BridgeProviderHandle>,
    db: DbPool<InProject>,
    request: WithinProject<ReadRequest<()>>,
) -> Result<SecretsRead, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, _) = request.into_parts();
    {
        let mut connection = db.read().await?;
        let _grant = authz
            .require::<Project, ManageProject>(&mut connection, &ctx, &project)
            .await?;
    }
    let secrets = bridge.project_secrets().fetch(&project).await?;
    Ok(SecretsRead { secrets })
}

#[cfg(test)]
mod tests;
