//
// Copyright 2025 Tabs Data Inc.
//

use crate::common::layers::extractor::extract_req_dto;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::sql::roles::RoleQueries;
use td_objects::tower_service::extractor::{extract_req_context, extract_req_name};
use td_objects::tower_service::from::{
    BuildService, ExtractService, TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::{
    By, SqlSelectIdOrNameService, SqlSelectService, SqlUpdateService,
};
use td_objects::types::basic::RoleId;
use td_objects::types::role::{
    Role, RoleBuilder, RoleDB, RoleDBUpdate, RoleDBUpdateBuilder, RoleDBWithNames, RoleParam,
    RoleUpdate,
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
                from_fn(extract_req_context::<UpdateRequest<RoleParam, RoleUpdate>>),
                from_fn(extract_req_name::<UpdateRequest<RoleParam, RoleUpdate>, _>),
                from_fn(extract_req_dto::<UpdateRequest<RoleParam, RoleUpdate>, _>),

                from_fn(With::<RoleUpdate>::convert_to::<RoleDBUpdateBuilder, _>),
                from_fn(With::<RequestContext>::update::<RoleDBUpdateBuilder, _>),
                from_fn(With::<RoleDBUpdateBuilder>::build::<RoleDBUpdate, _>),

                TransactionProvider::new(db),
                from_fn(By::<RoleParam>::select::<RoleQueries, RoleDBWithNames>),
                from_fn(With::<RoleDBWithNames>::extract::<RoleId>),
                from_fn(By::<RoleId>::update::<RoleQueries, RoleDBUpdate, RoleDB>),

                from_fn(By::<RoleId>::select::<RoleQueries, RoleDBWithNames>),
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
    use td_objects::test_utils::seed_user::admin_user;
    use td_objects::types::basic::{Description, RoleName};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_update_role() {
        use td_tower::metadata::{type_of_val, Metadata};

        let db = td_database::test_utils::db().await.unwrap();
        let queries = Arc::new(RoleQueries::new());
        let provider = UpdateRoleService::provider(db, queries);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<UpdateRequest<RoleParam, RoleUpdate>, Role>(&[
            type_of_val(&extract_req_context::<UpdateRequest<RoleParam, RoleUpdate>>),
            type_of_val(&extract_req_name::<UpdateRequest<RoleParam, RoleUpdate>, _>),
            type_of_val(&extract_req_dto::<UpdateRequest<RoleParam, RoleUpdate>, _>),
            type_of_val(&With::<RoleUpdate>::convert_to::<RoleDBUpdateBuilder, _>),
            type_of_val(&With::<RequestContext>::update::<RoleDBUpdateBuilder, _>),
            type_of_val(&With::<RoleDBUpdateBuilder>::build::<RoleDBUpdate, _>),
            type_of_val(&By::<RoleParam>::select::<RoleQueries, RoleDBWithNames>),
            type_of_val(&With::<RoleDBWithNames>::extract::<RoleId>),
            type_of_val(&By::<RoleId>::update::<RoleQueries, RoleDBUpdate, RoleDB>),
            type_of_val(&By::<RoleId>::select::<RoleQueries, RoleDBWithNames>),
            type_of_val(&With::<RoleDBWithNames>::convert_to::<RoleBuilder, _>),
            type_of_val(&With::<RoleBuilder>::build::<Role, _>),
        ]);
    }

    #[tokio::test]
    async fn test_update_role() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;
        let admin_id = admin_user(&db).await;

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

        let request = RequestContext::with(&admin_id, "r", true)
            .await
            .update(RoleParam::try_from("joaquin")?, update);

        let service = UpdateRoleService::new(db.clone()).service().await;
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
