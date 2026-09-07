//
// Copyright 2026 Tabsdata Inc.
//

use crate::group::assert_not_system;
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project};
use td_database::access::{GatedConnection, InProject, RW};
use td_error::TdError;
use td_objects::dxo::crudl::{DeleteRequest, WithinProject};
use td_objects::dxo::group::{GroupDB, GroupDBWithNames, GroupParam};
use td_queries::LiveEntry;

#[service_factory]
async fn delete_group(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW, InProject>,
    request: WithinProject<DeleteRequest<GroupParam>>,
) -> Result<(), TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param) = request.into_parts();
    let grant = authz
        .require::<Project, ManageProject>(gated_trx, &ctx, &project)
        .await?;
    let conn = gated_trx.get(grant);

    let current: GroupDBWithNames = conn.live().fetch_one(&param.group).await?;
    // Reject deletion of the seeded system groups via the `system`
    // flag — `default` / `source` / `destination` carry it and
    // therefore cannot be deleted.
    assert_not_system(&current)?;
    conn.live().delete::<GroupDB>(current.base.id).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::group::GroupError;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::TdError;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, Description, GroupIdName, GroupName, UserId};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_security::SOURCE_GROUP;

    #[td_test::test(db)]
    async fn test_delete_group(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let group = db
            .scoped(p.id)
            .seed()
            .group(
                GroupName::try_from("analytics")?,
                Description::try_from("Analytics tier group")?,
            )
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete(GroupParam {
                group: GroupIdName::from_str(&format!("~{}", group.id))?,
            })
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        service.delete_group(request).await?.data;

        // The user-created group is gone; the seeded system rows remain.
        let gone = db
            .live()
            .fetch_all::<GroupDB>(&GroupName::try_from("analytics")?)
            .await?;
        assert!(gone.is_empty());
        let remaining = db.live().fetch_all::<GroupDB>(()).await?;
        assert_eq!(remaining.len(), 3);
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_system_group_rejected(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete(GroupParam {
                group: GroupIdName::from_str(SOURCE_GROUP)?,
            })
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let res = service.delete_group(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<GroupError>();
        assert!(matches!(err, GroupError::SystemGroup(_)));
        Ok(())
    }
}
