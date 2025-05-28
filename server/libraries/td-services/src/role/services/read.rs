//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::RoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, CollAdmin, SecAdmin, System};
use td_objects::tower_service::from::{
    BuildService, ExtractNameService, ExtractService, TryIntoService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::RoleIdName;
use td_objects::types::role::{Role, RoleBuilder, RoleDBWithNames};
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ReadRoleService {
    provider: ServiceProvider<ReadRequest<RoleParam>, Role, TdError>,
}

impl ReadRoleService {
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
                from_fn(With::<ReadRequest<RoleParam>>::extract::<RequestContext>),
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SecAdmin, CollAdmin>::check),

                from_fn(With::<ReadRequest<RoleParam>>::extract_name::<RoleParam>),
                from_fn(With::<RoleParam>::extract::<RoleIdName>),

                from_fn(By::<RoleIdName>::select::<DaoQueries, RoleDBWithNames>),

                from_fn(With::<RoleDBWithNames>::convert_to::<RoleBuilder, _>),
                from_fn(With::<RoleBuilder>::build::<Role, _>),
            ))
        }
    }

    pub async fn service(&self) -> TdBoxService<ReadRequest<RoleParam>, Role, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_role::{get_role, seed_role};
    use td_objects::types::basic::{
        AccessTokenId, Description, RoleId, RoleIdName, RoleName, UserId,
    };
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_read_role(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider = ReadRoleService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ReadRequest<RoleParam>, Role>(&[
            type_of_val(&With::<ReadRequest<RoleParam>>::extract::<RequestContext>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin, CollAdmin>::check),
            type_of_val(&With::<ReadRequest<RoleParam>>::extract_name::<RoleParam>),
            type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
            type_of_val(&By::<RoleIdName>::select::<DaoQueries, RoleDBWithNames>),
            type_of_val(&With::<RoleDBWithNames>::convert_to::<RoleBuilder, _>),
            type_of_val(&With::<RoleBuilder>::build::<Role, _>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_read_role_with_id(db: DbPool) -> Result<(), TdError> {
        let role = seed_role(
            &db,
            RoleName::try_from("joaquin")?,
            Description::try_from("super user")?,
        )
        .await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            false,
        )
        .read(
            RoleParam::builder()
                .role(RoleIdName::try_from(format!("~{}", role.id()))?)
                .build()?,
        );

        let service = ReadRoleService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let found = get_role(&db, &RoleName::try_from("joaquin").unwrap()).await?;
        assert_eq!(response.id(), found.id());
        assert_eq!(response.name(), found.name());
        assert_eq!(response.description(), found.description());
        assert_eq!(response.created_on(), found.created_on());
        assert_eq!(response.created_by_id(), found.created_by_id());
        assert_eq!(response.modified_on(), found.modified_on());
        assert_eq!(response.modified_by_id(), found.modified_by_id());
        assert_eq!(response.fixed(), found.fixed());
        Ok(())
    }

    #[td_test::test(sqlx)]
    async fn test_read_role_with_name(db: DbPool) -> Result<(), TdError> {
        let _role = seed_role(
            &db,
            RoleName::try_from("joaquin")?,
            Description::try_from("super user")?,
        )
        .await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            false,
        )
        .read(
            RoleParam::builder()
                .role(RoleIdName::try_from("joaquin")?)
                .build()?,
        );

        let service = ReadRoleService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let found = get_role(&db, &RoleName::try_from("joaquin").unwrap()).await?;
        assert_eq!(response.id(), found.id());
        assert_eq!(response.name(), found.name());
        assert_eq!(response.description(), found.description());
        assert_eq!(response.created_on(), found.created_on());
        assert_eq!(response.created_by_id(), found.created_by_id());
        assert_eq!(response.modified_on(), found.modified_on());
        assert_eq!(response.modified_by_id(), found.modified_by_id());
        assert_eq!(response.fixed(), found.fixed());
        Ok(())
    }
}
