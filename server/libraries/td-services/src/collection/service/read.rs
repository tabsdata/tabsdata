//
// Copyright 2024 Tabs Data Inc.
//

use td_authz::{Authz, AuthzContext};
use td_objects::crudl::{ReadRequest, RequestContext};
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, NoPermissions, System};
use td_objects::tower_service::from::{
    BuildService, ExtractNameService, ExtractService, TryIntoService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService};
use td_objects::types::basic::CollectionIdName;
use td_objects::types::collection::{CollectionDBWithNames, CollectionRead, CollectionReadBuilder};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::{layers, service_factory};

#[service_factory(
    name = ReadCollectionService,
    request = ReadRequest<CollectionParam>,
    response = CollectionRead,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn service() {
    layers!(
        from_fn(With::<ReadRequest<CollectionParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<NoPermissions>::check), // no permission required
        from_fn(With::<ReadRequest<CollectionParam>>::extract_name::<CollectionParam>),
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::select::<CollectionDBWithNames>),
        from_fn(With::<CollectionDBWithNames>::convert_to::<CollectionReadBuilder, _>),
        from_fn(With::<CollectionReadBuilder>::build::<CollectionRead, _>),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, CollectionName, Description, RoleId, UserId, UserName,
    };
    use td_tower::ctx_service::RawOneshot;
    use td_tower::td_service::TdService;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_tower_metadata_read_provider(db: DbPool) {
        use td_tower::metadata::type_of_val;

        ReadCollectionService::with_defaults(db)
            .metadata()
            .await
            .assert_service::<ReadRequest<CollectionParam>, CollectionRead>(&[
                type_of_val(&With::<ReadRequest<CollectionParam>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<NoPermissions>::check), // no permission required
                type_of_val(&With::<ReadRequest<CollectionParam>>::extract_name::<CollectionParam>),
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::select::<CollectionDBWithNames>),
                type_of_val(&With::<CollectionDBWithNames>::convert_to::<CollectionReadBuilder, _>),
                type_of_val(&With::<CollectionReadBuilder>::build::<CollectionRead, _>),
            ]);
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_read_collection(db: DbPool) {
        let before = AtTime::now().await;
        let name = CollectionName::try_from("ds0").unwrap();
        let _ = seed_collection(&db, &name, &UserId::admin()).await;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).read(
                CollectionParam::builder()
                    .try_collection(name.to_string())
                    .unwrap()
                    .build()
                    .unwrap(),
            );

        let service = ReadCollectionService::with_defaults(db).service().await;
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let created = response.unwrap();

        assert_eq!(*created.name(), name);
        assert_eq!(*created.description(), Description::default());
        assert!(*created.created_on() >= before);
        assert_eq!(*created.created_by_id(), UserId::admin());
        assert_eq!(*created.created_by(), UserName::admin());
        assert_eq!(created.modified_on(), created.created_on());
        assert_eq!(*created.modified_by_id(), UserId::admin());
        assert_eq!(*created.modified_by(), UserName::admin());
    }
}
