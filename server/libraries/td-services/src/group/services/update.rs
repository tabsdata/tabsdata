//
// Copyright 2026 Tabsdata Inc.
//

use crate::group::{assert_group_rename_available, assert_not_system};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project};
use td_database::access::{GatedConnection, InProject, RW};
use td_error::TdError;
use td_objects::FromDao;
use td_objects::dxo::crudl::{UpdateRequest, WithinProject};
use td_objects::dxo::group::{
    Group, GroupDB, GroupDBUpdate, GroupDBWithNames, GroupParam, GroupUpdate,
};
use td_queries::LiveEntry;

#[service_factory]
async fn update_group(
    authz: Ctx<AuthzContext>,
    gated_trx: &mut GatedConnection<RW, InProject>,
    request: WithinProject<UpdateRequest<GroupUpdate, GroupParam>>,
) -> Result<Group, TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param, update) = request.into_parts();
    let grant = authz
        .require::<Project, ManageProject>(gated_trx, &ctx, &project)
        .await?;
    let conn = gated_trx.get(grant);

    let update_db = GroupDBUpdate {
        name: update.name.clone(),
        description: update.description.clone(),
        modified_on: ctx.time,
        modified_by_id: ctx.user_id,
    };

    let current: GroupDBWithNames = conn.live().fetch_one(&param.group).await?;
    // Reject mutation of the seeded system groups via the `system`
    // flag — `default` / `source` / `destination` carry it.
    assert_not_system(&current)?;
    // A rename to a name already taken by another group in the project
    // reports a clean "already exists" error instead of the raw
    // unique-constraint violation from the update write.
    assert_group_rename_available(conn, &update, &current).await?;

    conn.live()
        .update::<GroupDBUpdate, GroupDB>(&update_db, current.base.id)
        .await?;
    let with_names: GroupDBWithNames = conn.live().fetch_one(&current.base.id).await?;
    let read = Group::from_dao(with_names)?;
    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use crate::group::GroupError;
    use std::str::FromStr;
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt as _;
    use td_error::{TdError, assert_service_error};
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::types::basic::{AccessTokenId, Description, GroupIdName, GroupName, UserId};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_update_group(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let _ = db
            .scoped(p.id)
            .seed()
            .group(
                GroupName::try_from("analytics")?,
                Description::try_from("Analytics tier group")?,
            )
            .await?;

        let update = GroupUpdate {
            name: Some(GroupName::try_from("ml")?),
            description: Some(Description::try_from("ML platform group")?),
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                GroupParam {
                    group: GroupIdName::from_str("analytics")?,
                },
                update,
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let response = service.update_group(request).await?.data;

        let found = db
            .live()
            .fetch_one::<GroupDB>(&GroupName::try_from("ml")?)
            .await?;
        assert_eq!(response.id, found.id);
        assert_eq!(response.name, found.name);
        assert_eq!(response.description, found.description);
        assert!(!*found.system);
        Ok(())
    }

    /// Renaming a group to a name already taken by another group in the project
    /// is rejected up front with a clean `SqlError::EntityAlreadyExists` (via
    /// the `assert_group_rename_available` pre-check) rather than the raw
    /// `UniqueViolation` from the unique index.
    #[td_test::test(db)]
    async fn test_update_rename_to_existing_name_rejected(db: TdPool) {
        let p = db.seed().test_project().await.unwrap();
        let _ = db
            .scoped(p.id)
            .seed()
            .group(
                GroupName::try_from("analytics").unwrap(),
                Description::try_from("Analytics tier group").unwrap(),
            )
            .await;
        let _ = db
            .scoped(p.id)
            .seed()
            .group(
                GroupName::try_from("ml").unwrap(),
                Description::try_from("ML platform group").unwrap(),
            )
            .await;

        let update = GroupUpdate {
            name: Some(GroupName::try_from("ml").unwrap()),
            description: None,
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                GroupParam {
                    group: GroupIdName::from_str("analytics").unwrap(),
                },
                update,
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        assert_service_error(service.update_group(request).await, |err| match err {
            td_objects::errors::SqlError::EntityAlreadyExists(_, _) => {}
            other => panic!("Expected 'EntityAlreadyExists', got {other:?}"),
        });
    }

    #[td_test::test(db)]
    async fn test_update_system_group_rejected(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let update = GroupUpdate {
            name: None,
            description: Some(Description::try_from("trying to mutate a system row")?),
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                GroupParam {
                    group: GroupIdName::from_str("default")?,
                },
                update,
            )
            .in_project(p.id);

        let service = Services::with_defaults(db.clone());
        let res = service.update_group(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<GroupError>();
        assert!(matches!(err, GroupError::SystemGroup(_)));
        Ok(())
    }
}
