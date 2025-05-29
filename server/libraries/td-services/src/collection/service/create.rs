//
// Copyright 2024 Tabs Data Inc.
//

use std::sync::Arc;
use td_authz::{Authz, AuthzContext};
use td_database::sql::DbPool;
use td_error::TdError;
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SysAdmin, System};
use td_objects::tower_service::from::{
    BuildService, ExtractDataService, ExtractService, TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::{insert, By, SqlSelectService};
use td_objects::types::basic::CollectionId;
use td_objects::types::collection::{
    CollectionCreate, CollectionCreateDB, CollectionCreateDBBuilder, CollectionDBWithNames,
    CollectionRead, CollectionReadBuilder,
};
use td_tower::default_services::{SrvCtxProvider, TransactionProvider};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use td_tower::{layers, p, service_provider};

pub struct CreateCollectionService {
    provider: ServiceProvider<CreateRequest<(), CollectionCreate>, CollectionRead, TdError>,
}

impl CreateCollectionService {
    pub fn new(db: DbPool, authz_context: Arc<AuthzContext>) -> Self {
        let queries = Arc::new(DaoQueries::default());
        CreateCollectionService {
            provider: Self::provider(db, queries, authz_context),
        }
    }

    p! {
        provider(db: DbPool, queries: Arc<DaoQueries>, authz_context: Arc<AuthzContext>) {
            service_provider!(layers!(
                TransactionProvider::new(db),
                SrvCtxProvider::new(queries),
                SrvCtxProvider::new(authz_context),
                from_fn(With::<CreateRequest<(), CollectionCreate>>::extract::<RequestContext>),
                from_fn(AuthzOn::<System>::set),
                from_fn(Authz::<SysAdmin>::check),

                from_fn(With::<CreateRequest<(), CollectionCreate>>::extract_data::<CollectionCreate>),
                from_fn(With::<CollectionCreate>::convert_to::<CollectionCreateDBBuilder, _>),
                from_fn(With::<RequestContext>::update::<CollectionCreateDBBuilder, _>),
                from_fn(With::<CollectionCreateDBBuilder>::build::<CollectionCreateDB, _>),
                from_fn(insert::<DaoQueries, CollectionCreateDB>),

                from_fn(With::<CollectionCreateDB>::extract::<CollectionId>),
                from_fn(By::<CollectionId>::select::<DaoQueries, CollectionDBWithNames>),
                from_fn(With::<CollectionDBWithNames>::convert_to::<CollectionReadBuilder, _>),
                from_fn(With::<CollectionReadBuilder>::build::<CollectionRead, _>),
            ))
        }
    }

    pub async fn service(
        &self,
    ) -> TdBoxService<CreateRequest<(), CollectionCreate>, CollectionRead, TdError> {
        self.provider.make().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use td_authz::AuthzContext;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::types::basic::{
        AccessTokenId, AtTime, CollectionName, Description, RoleId, UserId, UserName,
    };
    use td_objects::types::collection::{CollectionCreate, CollectionCreateDB};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_create_service(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider =
            CreateCollectionService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<CreateRequest<(), CollectionCreate>, CollectionRead>(&[
            type_of_val(&With::<CreateRequest<(), CollectionCreate>>::extract::<RequestContext>),
            type_of_val(&AuthzOn::<System>::set),
            type_of_val(&Authz::<SysAdmin>::check),
            type_of_val(
                &With::<CreateRequest<(), CollectionCreate>>::extract_data::<CollectionCreate>,
            ),
            type_of_val(&With::<CollectionCreate>::convert_to::<CollectionCreateDBBuilder, _>),
            type_of_val(&With::<RequestContext>::update::<CollectionCreateDBBuilder, _>),
            type_of_val(&With::<CollectionCreateDBBuilder>::build::<CollectionCreateDB, _>),
            type_of_val(&insert::<DaoQueries, CollectionCreateDB>),
            type_of_val(&With::<CollectionCreateDB>::extract::<CollectionId>),
            type_of_val(&By::<CollectionId>::select::<DaoQueries, CollectionDBWithNames>),
            type_of_val(&With::<CollectionDBWithNames>::convert_to::<CollectionReadBuilder, _>),
            type_of_val(&With::<CollectionReadBuilder>::build::<CollectionRead, _>),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_create_collection(db: DbPool) {
        let name = CollectionName::try_from("ds0").unwrap();
        let description = Description::try_from("DS0").unwrap();

        let create = CollectionCreate::builder()
            .name(&name)
            .description(&description)
            .build()
            .unwrap();

        let before = AtTime::now().await;
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
            false,
        )
        .create((), create);

        let service = CreateCollectionService::new(db.clone(), Arc::new(AuthzContext::default()))
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let created = response.unwrap();

        assert_eq!(*created.name(), name);
        assert_eq!(*created.description(), description);
        assert!(*created.created_on() >= before);
        assert_eq!(*created.created_by_id(), UserId::admin());
        assert_eq!(*created.created_by(), UserName::admin());
        assert_eq!(created.modified_on(), created.created_on());
        assert_eq!(*created.modified_by_id(), UserId::admin());
        assert_eq!(*created.modified_by(), UserName::admin());

        let found: Vec<CollectionCreateDB> = DaoQueries::default()
            .select_by::<CollectionCreateDB>(&(&name))
            .unwrap()
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
    }
}
