//
// Copyright 2026 Tabsdata Inc.
//

use crate::group::{assert_group_unused, assert_not_system};
use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_authz::AuthzContext;
use td_authz::rbac::{ManageProject, Project};
use td_database::access::{DbPool, InProject, retrying};
use td_error::TdError;
use td_objects::dxo::crudl::{DeleteRequest, WithinProject};
use td_objects::dxo::group::{GroupDB, GroupParam};
use td_queries::Queries;

#[service_factory]
async fn delete_group(
    authz: Ctx<AuthzContext>,
    db: DbPool<InProject>,
    request: WithinProject<DeleteRequest<GroupParam>>,
) -> Result<(), TdError> {
    let (project, request) = request.into_parts();
    let (ctx, param) = request.into_parts();
    retrying(|attempt| async {
        let mut gated_trx = db.begin(attempt).await?;
        let grant = authz
            .require::<Project, ManageProject>(&mut gated_trx, &ctx, &project)
            .await?;
        let at = gated_trx.locked_at();
        let conn = gated_trx.get(grant);

        let current: GroupDB = conn.scd2().fetch_one(&param.group).await?;
        assert_not_system(&current)?;
        assert_group_unused(conn, &current).await?;
        conn.scd2()
            .close::<GroupDB>(&current.base.group_id, at)
            .await?;
        gated_trx.commit().await?;
        Ok(())
    })
    .await
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
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::group::{GroupCreate, GroupDB};
    use td_objects::types::basic::{
        AccessTokenId, CollectionIdName, CollectionName, Description, GroupIdName, GroupName,
        UserId,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;
    use td_security::SOURCE_GROUP;

    #[td_test::test(db)]
    async fn test_delete_group(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let group = db
            .scoped(p.base.project_id)
            .seed()
            .group(
                GroupName::try_from("analytics")?,
                Description::try_from("Analytics tier group")?,
            )
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete(GroupParam {
                group: GroupIdName::from_str(&format!("~{}", group.base.group_id))?,
            })
            .in_project(p.base.project_id);

        let service = Services::with_defaults(db.clone());
        service.delete_group(request).await?;

        // The user-created group is gone; the seeded system rows remain.
        let gone = db
            .scd2()
            .fetch_all::<GroupDB>(&GroupName::try_from("analytics")?)
            .await?;
        assert!(gone.is_empty());
        let remaining = db.scd2().fetch_all::<GroupDB>(()).await?;
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
            .in_project(p.base.project_id);

        let service = Services::with_defaults(db.clone());
        let res = service.delete_group(request).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let err = err.domain_err::<GroupError>();
        assert!(matches!(err, GroupError::SystemGroup(_)));
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_delete_group_with_collections_rejected(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let scoped = db.scoped(p.base.project_id);
        let group = scoped
            .seed()
            .group(
                GroupName::try_from("analytics")?,
                Description::try_from("Analytics tier group")?,
            )
            .await?;
        scoped
            .seed()
            .collection_in_group(
                &CollectionName::try_from("marts")?,
                &UserId::admin(),
                &group,
            )
            .await?;

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .delete(GroupParam {
                group: GroupIdName::from_str("analytics")?,
            })
            .in_project(p.base.project_id);

        let service = Services::with_defaults(db.clone());
        let err = service.delete_group(request).await.err().unwrap();
        assert!(matches!(
            err.domain_err::<GroupError>(),
            GroupError::GroupHasCollections(_)
        ));
        let kept = db
            .scd2()
            .fetch_all::<GroupDB>(&GroupName::try_from("analytics")?)
            .await?;
        assert_eq!(kept.len(), 1);
        Ok(())
    }

    /// Deleted collections keep pointing at their group, yet only a live one
    /// pins it: once its last collection is deleted the group can go, keeps
    /// its last name as a label, and frees the name for a new group.
    #[td_test::test(db)]
    async fn test_delete_group_of_deleted_collections_frees_its_name(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let scoped = db.scoped(p.base.project_id);
        let name = GroupName::try_from("analytics")?;
        let group = scoped
            .seed()
            .group(name.clone(), Description::try_from("Analytics tier group")?)
            .await?;
        scoped
            .seed()
            .collection_in_group(
                &CollectionName::try_from("marts")?,
                &UserId::admin(),
                &group,
            )
            .await?;
        let admin = RequestContext::with(AccessTokenId::default(), UserId::admin());
        let service = Services::with_defaults(db.clone());
        service
            .delete_collection(
                admin
                    .delete(CollectionParam {
                        collection: CollectionIdName::from_str("marts")?,
                    })
                    .in_project(p.base.project_id),
            )
            .await?;

        service
            .delete_group(
                admin
                    .delete(GroupParam {
                        group: GroupIdName::from_str("analytics")?,
                    })
                    .in_project(p.base.project_id),
            )
            .await?;
        let live: Option<GroupDB> = scoped.scd2().fetch_optional(&group.base.group_id).await?;
        assert!(live.is_none());

        let recreated = service
            .create_group(
                admin
                    .create(
                        (),
                        GroupCreate {
                            name: name.clone(),
                            description: Description::try_from("again")?,
                        },
                    )
                    .in_project(p.base.project_id),
            )
            .await?
            .data;
        assert_ne!(recreated.group_id, group.base.group_id);
        let live: Vec<GroupDB> = scoped.scd2().fetch_all(&name).await?;
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].base.group_id, recreated.group_id);
        Ok(())
    }
}
