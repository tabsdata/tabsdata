//
// Copyright 2025 Tabs Data Inc.
//

use crate::permission::layers::is_permission_on_collection;
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::DeleteRequest;
use td_objects::rest_urls::RolePermissionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin, System};
use td_objects::tower_service::extractor::{extract_req_context, extract_req_name};
use td_objects::tower_service::from::{ExtractService, TryIntoService, UnwrapService, With};
use td_objects::tower_service::sql::{By, SqlDeleteService, SqlSelectIdOrNameService};
use td_objects::types::basic::{CollectionId, EntityId, PermissionId, PermissionIdName};
use td_objects::types::permission::PermissionDB;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{conditional, Do, Else, If, SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service, service_provider};

pub struct DeletePermissionService {
    provider: ServiceProvider<DeleteRequest<RolePermissionParam>, (), TdError>,
}

impl DeletePermissionService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                TransactionProvider::new(db),
                SrvCtxProvider::new(authz_context),
                from_fn(extract_req_context::<DeleteRequest<RolePermissionParam>>),
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SecAdmin, CollAdmin>::check),

                from_fn(extract_req_name::<DeleteRequest<RolePermissionParam>, _>),

                // TODO check RoleParam exists
                from_fn(With::<RolePermissionParam>::extract::<PermissionIdName>),

                from_fn(By::<PermissionIdName>::select::<DaoQueries, PermissionDB>),

                conditional(
                    If(service!(layers!(
                        from_fn(is_permission_on_collection),
                    ))),
                    Do(service!(layers!(
                        from_fn(With::<PermissionDB>::extract::<Option<EntityId>>),
                        from_fn(With::<EntityId>::unwrap_option),
                        from_fn(With::<EntityId>::convert_to::<CollectionId, _>),
                        from_fn(AuthzOn::<CollectionId>::set),
                        from_fn(Authz::<SecAdmin, CollAdmin>::check),
                    ))),
                    Else(service!(layers!()))
                ),

                from_fn(With::<PermissionDB>::extract::<PermissionId>),
                from_fn(By::<PermissionId>::delete::<DaoQueries, PermissionDB>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<DeleteRequest<RolePermissionParam>, (), TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_error::assert_service_error;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_permission::{get_permission, seed_permission};
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::tower_service::authz::AuthzError;
    use td_objects::types::basic::{
        AccessTokenId, Description, EntityName, PermissionType, RoleId, RoleIdName, RoleName,
        UserId,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_delete_permission() {
        use td_authz::Authz;
        use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
        use td_objects::tower_service::extractor::extract_req_context;
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(DaoQueries::default());
        let provider =
            DeletePermissionService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<DeleteRequest<RolePermissionParam>, ()>(&[
            type_of_val(&extract_req_context::<DeleteRequest<RolePermissionParam>>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
            type_of_val(&extract_req_name::<DeleteRequest<RolePermissionParam>, _>),
            type_of_val(&With::<RolePermissionParam>::extract::<PermissionIdName>),
            type_of_val(&By::<PermissionIdName>::select::<DaoQueries, PermissionDB>),
            type_of_val(&is_permission_on_collection),
            type_of_val(&With::<PermissionDB>::extract::<Option<EntityId>>),
            type_of_val(&With::<EntityId>::unwrap_option),
            type_of_val(&With::<EntityId>::convert_to::<CollectionId, _>),
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
            type_of_val(&With::<PermissionDB>::extract::<PermissionId>),
            type_of_val(&By::<PermissionId>::delete::<DaoQueries, PermissionDB>),
        ]);
    }

    #[tokio::test]
    async fn test_delete_permission() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;

        let role = seed_role(
            &db,
            RoleName::try_from("king")?,
            Description::try_from("super user")?,
        )
        .await;
        let seeded = seed_permission(&db, PermissionType::SysAdmin, None, None, &role).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            true,
        )
        .delete(
            RolePermissionParam::builder()
                .role(RoleIdName::try_from("king")?)
                .permission(PermissionIdName::try_from(seeded.id().to_string())?)
                .build()?,
        );

        let service = DeletePermissionService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;
        service.raw_oneshot(request).await?;

        let not_found = get_permission(&db, seeded.id()).await;
        assert!(not_found.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_permission_on_collection_by_coll_admin_ok() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let coll0: CollectionId = seed_collection(&db, None, "c0").await.into();
        let role = seed_role(&db, RoleName::try_from("r0")?, Description::try_from("d")?).await;
        let entity_id: EntityId = (*coll0).into();
        seed_permission(
            &db,
            PermissionType::CollectionAdmin,
            Some(EntityName::try_from("c0")?),
            Some(entity_id),
            &role,
        )
        .await;
        let coll_dev_perm = seed_permission(
            &db,
            PermissionType::CollectionDev,
            Some(EntityName::try_from("c0")?),
            Some(entity_id),
            &role,
        )
        .await;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), role.id(), true)
                .delete(
                    RolePermissionParam::builder()
                        .role(RoleIdName::try_from("r0")?)
                        .permission(PermissionIdName::try_from(coll_dev_perm.id().to_string())?)
                        .build()?,
                );

        let service = DeletePermissionService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;
        assert!(service.raw_oneshot(request).await.is_ok());

        let not_found = get_permission(&db, coll_dev_perm.id()).await;
        assert!(not_found.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_permission_on_collection_by_coll_admin_unauthz() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let coll0: CollectionId = seed_collection(&db, None, "c0").await.into();
        let role = seed_role(&db, RoleName::try_from("r0")?, Description::try_from("d")?).await;
        let entity_id: EntityId = (*coll0).into();
        let coll_dev_perm = seed_permission(
            &db,
            PermissionType::CollectionDev,
            Some(EntityName::try_from("c0")?),
            Some(entity_id),
            &role,
        )
        .await;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), role.id(), true)
                .delete(
                    RolePermissionParam::builder()
                        .role(RoleIdName::try_from("r0")?)
                        .permission(PermissionIdName::try_from(coll_dev_perm.id().to_string())?)
                        .build()?,
                );

        let service = DeletePermissionService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;

        assert_service_error(service, request, |err| match err {
            AuthzError::UnAuthorized(_) => {}
            other => panic!("Expected 'Unauthorized', got {:?}", other),
        })
        .await;
        Ok(())
    }
}
