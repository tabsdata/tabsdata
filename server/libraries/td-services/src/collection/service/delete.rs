//
// Copyright 2024 Tabs Data Inc.
//

use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{DeleteRequest, RequestContext};
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SysAdmin, System};
use td_objects::tower_service::from::{
    builder, BuildService, ExtractNameService, ExtractService, UpdateService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService, SqlUpdateService};
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_objects::types::collection::{CollectionDB, CollectionDeleteDB, CollectionDeleteDBBuilder};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = DeleteCollectionService,
    request = DeleteRequest<CollectionParam>,
    response = (),
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        from_fn(With::<DeleteRequest<CollectionParam>>::extract::<RequestContext>),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SysAdmin>::check),
        from_fn(With::<DeleteRequest<CollectionParam>>::extract_name::<CollectionParam>),
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::select::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        from_fn(builder::<CollectionDeleteDBBuilder>),
        from_fn(With::<RequestContext>::update::<CollectionDeleteDBBuilder, _>),
        from_fn(With::<CollectionDB>::update::<CollectionDeleteDBBuilder, _>),
        from_fn(With::<CollectionDeleteDBBuilder>::build::<CollectionDeleteDB, _>),
        from_fn(By::<CollectionId>::update::<CollectionDeleteDB, CollectionDB>),
        // TODO logic delete collection functions (freezing all their tables)
        // TODO logic delete collection tables (freezing all the functions that use those tables)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::CollectionParam;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::types::basic::{AccessTokenId, CollectionName, RoleId, UserId};
    use td_objects::types::collection::{CollectionCreateDB, CollectionDB};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_delete_service(db: DbPool) {
        use td_tower::metadata::type_of_val;

        DeleteCollectionService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<DeleteRequest<CollectionParam>, ()>(&[
                type_of_val(&With::<DeleteRequest<CollectionParam>>::extract::<RequestContext>),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SysAdmin>::check),
                type_of_val(
                    &With::<DeleteRequest<CollectionParam>>::extract_name::<CollectionParam>,
                ),
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::select::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(&builder::<CollectionDeleteDBBuilder>),
                type_of_val(&With::<RequestContext>::update::<CollectionDeleteDBBuilder, _>),
                type_of_val(&With::<CollectionDB>::update::<CollectionDeleteDBBuilder, _>),
                type_of_val(&With::<CollectionDeleteDBBuilder>::build::<CollectionDeleteDB, _>),
                type_of_val(&By::<CollectionId>::update::<CollectionDeleteDB, CollectionDB>),
            ]);
    }

    #[td_test::test(sqlx)]
    async fn test_delete_collection(db: DbPool) {
        let name = CollectionName::try_from("ds0").unwrap();
        let collection = seed_collection(&db, &name, &UserId::admin()).await;

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .delete(
            CollectionParam::builder()
                .try_collection(name.to_string())
                .unwrap()
                .build()
                .unwrap(),
        );

        let service = DeleteCollectionService::with_defaults(db.clone())
            .await
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());

        let found: Vec<CollectionDB> = DaoQueries::default()
            .select_by::<CollectionDB>(&())
            .unwrap()
            .build_query_as()
            .fetch_all(&db)
            .await
            .unwrap();
        assert_eq!(found.len(), 0);

        let res: CollectionCreateDB = DaoQueries::default()
            .select_by::<CollectionCreateDB>(&(collection.id()))
            .unwrap()
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(res.name_when_deleted().as_ref().unwrap(), collection.name());
    }
}
