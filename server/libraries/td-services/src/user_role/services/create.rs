//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::extractor::extract_req_dto;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::rest_urls::{RoleParam, UserParam};
use td_objects::sql::roles::RoleQueries;
use td_objects::tower_service::extractor::{extract_req_context, extract_req_name};
use td_objects::tower_service::from::{
    builder, BuildService, ExtractService, SetService, TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::SqlSelectIdOrNameService;
use td_objects::tower_service::sql::{insert, By, SqlSelectService};
use td_objects::types::basic::RoleId;
use td_objects::types::basic::UserId;
use td_objects::types::basic::UserRoleId;
use td_objects::types::role::{
    RoleDB, UserRole, UserRoleBuilder, UserRoleCreate, UserRoleDB, UserRoleDBBuilder,
    UserRoleDBWithNames,
};
use td_objects::types::user::UserDB;
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct CreateUserRoleService {
    provider: ServiceProvider<CreateRequest<RoleParam, UserRoleCreate>, UserRole, TdError>,
}

impl CreateUserRoleService {
    pub fn new(db: DbPool) -> Self {
        let queries = Arc::new(RoleQueries::new());
        Self {
            provider: Self::provider(db, queries),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<RoleQueries>) -> TdError {
            service_provider!(layers!(
                SrvCtxProvider::new(queries),
                from_fn(extract_req_context::<CreateRequest<RoleParam, UserRoleCreate>>),
                from_fn(extract_req_dto::<CreateRequest<RoleParam, UserRoleCreate>, _>),
                from_fn(extract_req_name::<CreateRequest<RoleParam, UserRoleCreate>, _>),

                from_fn(With::<UserRoleCreate>::extract::<UserParam>),

                TransactionProvider::new(db),

                from_fn(builder::<UserRoleDBBuilder>),

                from_fn(By::<RoleParam>::select::<RoleQueries, RoleDB>),
                from_fn(With::<RoleDB>::extract::<RoleId>),
                from_fn(With::<RoleId>::set::<UserRoleDBBuilder>),

                from_fn(By::<UserParam>::select::<RoleQueries, UserDB>),
                from_fn(With::<UserDB>::extract::<UserId>),
                from_fn(With::<UserId>::set::<UserRoleDBBuilder>),

                from_fn(With::<RequestContext>::update::<UserRoleDBBuilder, _>),
                from_fn(With::<UserRoleDBBuilder>::build::<UserRoleDB, _>),

                from_fn(insert::<RoleQueries, UserRoleDB>),

                from_fn(With::<UserRoleDB>::extract::<UserRoleId>),
                from_fn(By::<UserRoleId>::select::<RoleQueries, UserRoleDBWithNames>),
                from_fn(With::<UserRoleDBWithNames>::convert_to::<UserRoleBuilder, _>),
                from_fn(With::<UserRoleBuilder>::build::<UserRole, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<CreateRequest<RoleParam, UserRoleCreate>, UserRole, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::test_utils::seed_role::seed_role;
    use td_objects::test_utils::seed_user::{admin_user, seed_user};
    use td_objects::test_utils::seed_user_role::get_user_role;
    use td_objects::types::basic::{Description, RoleName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_create_user_role() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(RoleQueries::new());
        let provider = CreateUserRoleService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<CreateRequest<RoleParam, UserRoleCreate>, UserRole>(&[
            type_of_val(&extract_req_context::<CreateRequest<RoleParam, UserRoleCreate>>),
            type_of_val(&extract_req_dto::<CreateRequest<RoleParam, UserRoleCreate>, _>),
            type_of_val(&extract_req_name::<CreateRequest<RoleParam, UserRoleCreate>, _>),
            type_of_val(&With::<UserRoleCreate>::extract::<UserParam>),
            type_of_val(&builder::<UserRoleDBBuilder>),
            type_of_val(&By::<RoleParam>::select::<RoleQueries, RoleDB>),
            type_of_val(&With::<RoleDB>::extract::<RoleId>),
            type_of_val(&With::<RoleId>::set::<UserRoleDBBuilder>),
            type_of_val(&By::<UserParam>::select::<RoleQueries, UserDB>),
            type_of_val(&With::<UserDB>::extract::<UserId>),
            type_of_val(&With::<UserId>::set::<UserRoleDBBuilder>),
            type_of_val(&With::<RequestContext>::update::<UserRoleDBBuilder, _>),
            type_of_val(&With::<UserRoleDBBuilder>::build::<UserRoleDB, _>),
            type_of_val(&insert::<RoleQueries, UserRoleDB>),
            type_of_val(&With::<UserRoleDB>::extract::<UserRoleId>),
            type_of_val(&By::<UserRoleId>::select::<RoleQueries, UserRoleDBWithNames>),
            type_of_val(&With::<UserRoleDBWithNames>::convert_to::<UserRoleBuilder, _>),
            type_of_val(&With::<UserRoleBuilder>::build::<UserRole, _>),
        ]);
    }

    #[tokio::test]
    async fn test_create_user_role() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let admin_id = admin_user(&db).await;

        let _user_id = seed_user(&db, None, "joaquin", false).await;
        let _role = seed_role(
            &db,
            RoleName::try_from("king")?,
            Description::try_from("super user")?,
        )
        .await;

        let create = UserRoleCreate::builder()
            .user(UserParam::try_from("joaquin")?)
            .build()?;

        let request = RequestContext::with(&admin_id, "r", true)
            .await
            .create(RoleParam::try_from("king")?, create);

        let service = CreateUserRoleService::new(db.clone()).service().await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let found = get_user_role(&db, response.id()).await?;
        assert_eq!(response.id(), found.id());
        assert_eq!(response.user_id(), found.user_id());
        assert_eq!(response.role_id(), found.role_id());
        assert_eq!(response.added_on(), found.added_on());
        assert_eq!(response.added_by_id(), found.added_by_id());
        assert_eq!(response.fixed(), found.fixed());
        Ok(())
    }
}
