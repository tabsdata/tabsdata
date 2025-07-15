//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::rest_urls::RoleParam;
use td_objects::sql::{DaoQueries, NoListFilter};
use td_objects::tower_service::authz::{AuthzOn, Requester, SecAdmin, SystemOrRoleId};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlListService, SqlSelectService};
use td_objects::types::basic::{RoleId, RoleIdName};
use td_objects::types::permission::Permission;
use td_objects::types::role::RoleDB;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ListPermissionService {
    provider: ServiceProvider<ListRequest<RoleParam>, ListResponse<Permission>, TdError>,
}

impl ListPermissionService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                ConnectionProvider::new(db),
                SrvCtxProvider::new(authz_context),
                from_fn(With::<ListRequest<RoleParam>>::extract::<RequestContext>),

                from_fn(With::<ListRequest<RoleParam>>::extract_name::<RoleParam>),
                from_fn(With::<RoleParam>::extract::<RoleIdName>),

                from_fn(By::<RoleIdName>::select::<DaoQueries, RoleDB>),
                from_fn(With::<RoleDB>::extract::<RoleId>),
                from_fn(AuthzOn::<SystemOrRoleId>::set),
                from_fn(Authz::<SecAdmin, Requester>::check),
                from_fn(By::<RoleId>::list::<RoleParam, NoListFilter, DaoQueries, Permission>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<ListRequest<RoleParam>, ListResponse<Permission>, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::{ListParams, RequestContext};
    use td_objects::test_utils::seed_permission::seed_permission;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::types::basic::{
        AccessTokenId, Description, EntityId, PermissionType, RoleName, UserId,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_list_permission(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider =
            ListPermissionService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ListRequest<RoleParam>, ListResponse<Permission>>(&[
            type_of_val(&With::<ListRequest<RoleParam>>::extract::<RequestContext>),
            type_of_val(&With::<ListRequest<RoleParam>>::extract_name::<RoleParam>),
            type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
            type_of_val(&By::<RoleIdName>::select::<DaoQueries, RoleDB>),
            type_of_val(&With::<RoleDB>::extract::<RoleId>),
            type_of_val(&AuthzOn::<SystemOrRoleId>::set),
            type_of_val(&Authz::<SecAdmin, Requester>::check),
            type_of_val(&By::<RoleId>::list::<RoleParam, NoListFilter, DaoQueries, Permission>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_list_permissions(db: DbPool) -> Result<(), TdError> {
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
        .list(
            RoleParam::builder()
                .role(RoleIdName::try_from("king")?)
                .build()?,
            ListParams::default(),
        );

        let service = ListPermissionService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        assert_eq!(*response.len(), 1);
        let permission = response.data().first().unwrap();
        assert_eq!(permission.id(), seeded.id());
        assert_eq!(permission.role_id(), seeded.role_id());
        assert_eq!(permission.permission_type(), seeded.permission_type());
        assert_eq!(permission.entity_type(), seeded.entity_type());
        assert_eq!(
            permission.entity_id().unwrap_or(EntityId::all_entities()),
            *seeded.entity_id()
        );
        assert_eq!(permission.granted_by_id(), seeded.granted_by_id());
        assert_eq!(permission.granted_on(), seeded.granted_on());
        assert_eq!(permission.fixed(), seeded.fixed());
        Ok(())
    }
}
