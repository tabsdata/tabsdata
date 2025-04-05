//
// Copyright 2025 Tabs Data Inc.
//

use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::rest_urls::RoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::extractor::extract_req_name;
use td_objects::tower_service::from::{BuildService, ExtractService, TryIntoService, With};
use td_objects::tower_service::sql::{By, SqlSelectIdOrNameService};
use td_objects::types::basic::RoleIdName;
use td_objects::types::role::{Role, RoleBuilder, RoleDBWithNames};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{ConnectionProvider, SrvCtxProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct ReadRoleService {
    provider: ServiceProvider<ReadRequest<RoleParam>, Role, TdError>,
}

impl ReadRoleService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(DaoQueries::default());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                from_fn(extract_req_name::<ReadRequest<RoleParam>, _>),
                from_fn(With::<RoleParam>::extract::<RoleIdName>),

                ConnectionProvider::new(db),
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
    #[tokio::test]
    async fn test_tower_metadata_read_role() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(DaoQueries::default());
        let provider = ReadRoleService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ReadRequest<RoleParam>, Role>(&[
            type_of_val(&extract_req_name::<ReadRequest<RoleParam>, _>),
            type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
            type_of_val(&By::<RoleIdName>::select::<DaoQueries, RoleDBWithNames>),
            type_of_val(&With::<RoleDBWithNames>::convert_to::<RoleBuilder, _>),
            type_of_val(&With::<RoleBuilder>::build::<Role, _>),
        ]);
    }

    #[tokio::test]
    async fn test_read_role_with_id() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;

        let role = seed_role(
            &db,
            RoleName::try_from("joaquin")?,
            Description::try_from("super user")?,
        )
        .await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .read(
            RoleParam::builder()
                .role(RoleIdName::try_from(format!("~{}", role.id()))?)
                .build()?,
        );

        let service = ReadRoleService::new(db.clone()).service().await;
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

    #[tokio::test]
    async fn test_read_role_with_name() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;

        let _role = seed_role(
            &db,
            RoleName::try_from("joaquin")?,
            Description::try_from("super user")?,
        )
        .await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .read(
            RoleParam::builder()
                .role(RoleIdName::try_from("joaquin")?)
                .build()?,
        );

        let service = ReadRoleService::new(db.clone()).service().await;
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
