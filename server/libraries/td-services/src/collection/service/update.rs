//
// Copyright 2024 Tabs Data Inc.
//

use crate::collection::service::layer::update::{
    update_collection_validate, UpdateCollectionDBBuilderUpdate,
};
use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{RequestContext, UpdateRequest};
use td_objects::rest_urls::CollectionParam;
use td_objects::sql::DaoQueries;
use td_objects::tower_service::authz::{AuthzOn, SysAdmin, System};
use td_objects::tower_service::from::{
    BuildService, ExtractDataService, ExtractNameService, ExtractService, TryIntoService,
    UpdateService, With,
};
use td_objects::tower_service::sql::{By, SqlSelectService, SqlUpdateService};
use td_objects::types::basic::{CollectionId, CollectionIdName};
use td_objects::types::collection::{
    CollectionDB, CollectionDBWithNames, CollectionRead, CollectionReadBuilder, CollectionUpdate,
    CollectionUpdateDB, CollectionUpdateDBBuilder,
};
use td_tower::default_services::TransactionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = UpdateCollectionService,
    request = UpdateRequest<CollectionParam, CollectionUpdate>,
    response = CollectionRead,
    connection = TransactionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        from_fn(
            With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract::<RequestContext>
        ),
        from_fn(AuthzOn::<System>::set),
        from_fn(Authz::<SysAdmin>::check),
        from_fn(
            With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract_name::<CollectionParam>
        ),
        from_fn(With::<CollectionParam>::extract::<CollectionIdName>),
        from_fn(By::<CollectionIdName>::select::<CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        from_fn(
            With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract_data::<
                CollectionUpdate,
            >
        ),
        from_fn(update_collection_validate),
        from_fn(With::<CollectionDB>::convert_to::<CollectionUpdateDBBuilder, _>),
        from_fn(With::<RequestContext>::update::<CollectionUpdateDBBuilder, _>),
        from_fn(With::<CollectionUpdate>::update_collection_update_db_builder),
        from_fn(With::<CollectionUpdateDBBuilder>::build::<CollectionUpdateDB, _>),
        from_fn(By::<CollectionId>::update::<CollectionUpdateDB, CollectionDB>),
        from_fn(By::<CollectionId>::select::<CollectionDBWithNames>),
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
    use td_objects::types::collection::CollectionUpdate;
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_update_provider(db: DbPool) {
        use td_tower::metadata::type_of_val;

        UpdateCollectionService::with_defaults(db)
            .await
            .metadata()
            .await
            .assert_service::<UpdateRequest<CollectionParam, CollectionUpdate>, CollectionRead>(&[
                type_of_val(
                    &With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract::<
                        RequestContext,
                    >,
                ),
                type_of_val(&AuthzOn::<System>::set),
                type_of_val(&Authz::<SysAdmin>::check),
                type_of_val(
                    &With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract_name::<
                        CollectionParam,
                    >,
                ),
                type_of_val(&With::<CollectionParam>::extract::<CollectionIdName>),
                type_of_val(&By::<CollectionIdName>::select::<CollectionDB>),
                type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
                type_of_val(
                    &With::<UpdateRequest<CollectionParam, CollectionUpdate>>::extract_data::<
                        CollectionUpdate,
                    >,
                ),
                type_of_val(&update_collection_validate),
                type_of_val(&With::<CollectionDB>::convert_to::<CollectionUpdateDBBuilder, _>),
                type_of_val(&With::<RequestContext>::update::<CollectionUpdateDBBuilder, _>),
                type_of_val(&With::<CollectionUpdate>::update_collection_update_db_builder),
                type_of_val(&With::<CollectionUpdateDBBuilder>::build::<CollectionUpdateDB, _>),
                type_of_val(&By::<CollectionId>::update::<CollectionUpdateDB, CollectionDB>),
                type_of_val(&By::<CollectionId>::select::<CollectionDBWithNames>),
                type_of_val(&With::<CollectionDBWithNames>::convert_to::<CollectionReadBuilder, _>),
                type_of_val(&With::<CollectionReadBuilder>::build::<CollectionRead, _>),
            ]);
    }

    #[td_test::test(sqlx)]
    async fn test_update_collection(db: DbPool) {
        let create_name = CollectionName::try_from("ds0").unwrap();
        let _ = seed_collection(&db, &create_name, &UserId::admin()).await;

        let before_update = AtTime::now().await;

        let name = CollectionName::try_from("ds1").unwrap();
        let description = Description::try_from("DS1").unwrap();

        let update = CollectionUpdate::builder()
            .name(Some(name.clone()))
            .description(Some(description.clone()))
            .build()
            .unwrap();

        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
        )
        .update(
            CollectionParam::builder()
                .try_collection(create_name.to_string())
                .unwrap()
                .build()
                .unwrap(),
            update,
        );

        let service = UpdateCollectionService::with_defaults(db)
            .await
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        assert!(response.is_ok());
        let updated = response.unwrap();

        assert_eq!(*updated.name(), name);
        assert_eq!(*updated.description(), description);
        assert!(*updated.created_on() < before_update);
        assert_eq!(*updated.created_by_id(), UserId::admin());
        assert_eq!(*updated.created_by(), UserName::admin());
        assert!(*updated.modified_on() > before_update);
        assert_eq!(*updated.modified_by_id(), UserId::admin());
        assert_eq!(*updated.modified_by(), UserName::admin());
    }
}
