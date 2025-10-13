//
// Copyright 2024 Tabs Data Inc.
//

use ta_services::factory::service_factory;
use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{CreateRequest, RequestContext};
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SysAdmin, System};
use td_objects::tower_service::from::{
    BuildService, ExtractDataService, ExtractService, TryIntoService, UpdateService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService, insert};
use td_objects::types::basic::CollectionId;
use td_objects::types::collection::{
    CollectionCreate, CollectionCreateDB, CollectionCreateDBBuilder, CollectionDBWithNames,
    CollectionRead, CollectionReadBuilder,
};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::layers;

#[service_factory(
    name = CreateCollectionService,
    request = CreateRequest<(), CollectionCreate>,
    response = CollectionRead,
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<CreateRequest<(), CollectionCreate>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SysAdmin>::check),
        from_fn(With::<CreateRequest<(), CollectionCreate>>::extract_data::<CollectionCreate>),
        from_fn(With::<CollectionCreate>::convert_to::<CollectionCreateDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<CollectionCreateDBBuilder, _>),
        from_fn(With::<CollectionCreateDBBuilder>::build::<CollectionCreateDB, _>),
        from_fn(insert::<CollectionCreateDB>),
        from_fn(With::<CollectionCreateDB>::extract::<CollectionId>),
        from_fn(By::<CollectionId>::select::<CollectionDBWithNames>),
        from_fn(With::<CollectionDBWithNames>::convert_to::<CollectionReadBuilder, _>),
        from_fn(With::<CollectionReadBuilder>::build::<CollectionRead, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_services::service::TdService;
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
    #[tokio::test]
    async fn test_tower_metadata_create_service(db: DbPool) {
        use td_tower::metadata::type_of_val;

        CreateCollectionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<CreateRequest<(), CollectionCreate>, CollectionRead>(&[
                type_of_val(
                    &With::<CreateRequest<(), CollectionCreate>>::extract::<RequestContext>,
                ),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SysAdmin>::check),
                type_of_val(
                    &With::<CreateRequest<(), CollectionCreate>>::extract_data::<CollectionCreate>,
                ),
                type_of_val(&With::<CollectionCreate>::convert_to::<CollectionCreateDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<CollectionCreateDBBuilder, _>),
                type_of_val(&With::<CollectionCreateDBBuilder>::build::<CollectionCreateDB, _>),
                type_of_val(&insert::<CollectionCreateDB>),
                type_of_val(&With::<CollectionCreateDB>::extract::<CollectionId>),
                type_of_val(&By::<CollectionId>::select::<CollectionDBWithNames>),
                type_of_val(&With::<CollectionDBWithNames>::convert_to::<CollectionReadBuilder, _>),
                type_of_val(&With::<CollectionReadBuilder>::build::<CollectionRead, _>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_create_collection(db: DbPool) {
        let name = CollectionName::try_from("ds0").unwrap();
        let description = Description::try_from("DS0").unwrap();

        let create = CollectionCreate::builder()
            .name(&name)
            .description(&description)
            .build()
            .unwrap();

        let before = AtTime::now();
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .create((), create);

        let service = CreateCollectionService::with_defaults(db.clone())
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
