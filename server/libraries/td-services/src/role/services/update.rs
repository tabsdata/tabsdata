//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::extractor::extract_req_dto;
use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::rest_urls::RoleParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
use td_objects::tower_service::extractor::{extract_req_context, extract_req_name};
use td_objects::tower_service::from::{
    BuildService, ExtractService, TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::{
    By, SqlSelectIdOrNameService, SqlSelectService, SqlUpdateService,
};
use td_objects::types::basic::{RoleId, RoleIdName};
use td_objects::types::role::{
    Role, RoleBuilder, RoleDB, RoleDBUpdate, RoleDBUpdateBuilder, RoleDBWithNames, RoleUpdate,
};
use td_tower::box_sync_clone_layer::BoxedSyncCloneServiceLayer;
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct UpdateRoleService {
    provider: ServiceProvider<UpdateRequest<RoleParam, RoleUpdate>, Role, TdError>,
}

impl UpdateRoleService {
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

                from_fn(extract_req_context::<UpdateRequest<RoleParam, RoleUpdate>>),
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SecAdmin>::check),
                from_fn(extract_req_name::<UpdateRequest<RoleParam, RoleUpdate>, _>),
                from_fn(extract_req_dto::<UpdateRequest<RoleParam, RoleUpdate>, _>),

                from_fn(With::<RoleUpdate>::convert_to::<RoleDBUpdateBuilder, _>),
                from_fn(With::<RequestContext>::update::<RoleDBUpdateBuilder, _>),
                from_fn(With::<RoleDBUpdateBuilder>::build::<RoleDBUpdate, _>),

                from_fn(With::<RoleParam>::extract::<RoleIdName>),

                from_fn(By::<RoleIdName>::select::<DaoQueries, RoleDBWithNames>),
                from_fn(With::<RoleDBWithNames>::extract::<RoleId>),
                from_fn(By::<RoleId>::update::<DaoQueries, RoleDBUpdate, RoleDB>),

                from_fn(By::<RoleId>::select::<DaoQueries, RoleDBWithNames>),
                from_fn(With::<RoleDBWithNames>::convert_to::<RoleBuilder, _>),
                from_fn(With::<RoleBuilder>::build::<Role, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<UpdateRequest<RoleParam, RoleUpdate>, Role, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_objects::crudl::RequestContext;
    use td_objects::test_utils::seed_role::{get_role, seed_role};
    use td_objects::types::basic::{AccessTokenId, Description, RoleName, UserId};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_update_role() {
        use td_objects::tower_service::authz::{AuthzOn, SecAdmin, System};
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(DaoQueries::default());
        let provider = UpdateRoleService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<UpdateRequest<RoleParam, RoleUpdate>, Role>(&[
            type_of_val(&extract_req_context::<UpdateRequest<RoleParam, RoleUpdate>>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SecAdmin>::check),
            type_of_val(&extract_req_name::<UpdateRequest<RoleParam, RoleUpdate>, _>),
            type_of_val(&extract_req_dto::<UpdateRequest<RoleParam, RoleUpdate>, _>),
            type_of_val(&With::<RoleUpdate>::convert_to::<RoleDBUpdateBuilder, _>),
            type_of_val(&With::<RequestContext>::update::<RoleDBUpdateBuilder, _>),
            type_of_val(&With::<RoleDBUpdateBuilder>::build::<RoleDBUpdate, _>),
            type_of_val(&With::<RoleParam>::extract::<RoleIdName>),
            type_of_val(&By::<RoleIdName>::select::<DaoQueries, RoleDBWithNames>),
            type_of_val(&With::<RoleDBWithNames>::extract::<RoleId>),
            type_of_val(&By::<RoleId>::update::<DaoQueries, RoleDBUpdate, RoleDB>),
            type_of_val(&By::<RoleId>::select::<DaoQueries, RoleDBWithNames>),
            type_of_val(&With::<RoleDBWithNames>::convert_to::<RoleBuilder, _>),
            type_of_val(&With::<RoleBuilder>::build::<Role, _>),
        ]);
    }

    #[tokio::test]
    async fn test_update_role() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;

        let _role = seed_role(
            &db,
            RoleName::try_from("joaquin")?,
            Description::try_from("super user")?,
        )
        .await;

        let update = RoleUpdate::builder()
            .try_name("not_joaquin_anymore")?
            .try_description("new desc")?
            .build()?;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sec_admin(),
            false,
        )
        .update(
            RoleParam::builder()
                .role(RoleIdName::try_from("joaquin")?)
                .build()?,
            update,
        );

        let service = UpdateRoleService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let not_found = get_role(&db, &RoleName::try_from("joaquin")?).await;
        assert!(not_found.is_err());

        let found = get_role(&db, &RoleName::try_from("not_joaquin_anymore")?).await?;
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