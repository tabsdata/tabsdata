//
// Copyright 2025. Tabs Data Inc.
//

use td_authz::{Authz, AuthzContext};
use td_error::TdError;
use td_objects::crudl::{ListRequest, ListResponse, RequestContext};
use td_objects::sql::{DaoQueries, NoListFilter};
use td_objects::tower_service::authz::{
    AuthzOn, CollAdmin, CollDev, CollExec, CollRead, InterCollRead,
};
use td_objects::tower_service::from::{ExtractNameService, ExtractService, With};
use td_objects::tower_service::sql::{By, SqlListService, SqlSelectService};
use td_objects::types::basic::{AtTime, CollectionId, CollectionIdName, TableStatus};
use td_objects::types::collection::CollectionDB;
use td_objects::types::table::{CollectionAtName, Table};
use td_tower::default_services::ConnectionProvider;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = TableListByCollectionService,
    request = ListRequest<CollectionAtName>,
    response = ListResponse<Table>,
    connection = ConnectionProvider,
    context = DaoQueries,
    context = AuthzContext,
)]
fn provider() {
    layers!(
        from_fn(With::<ListRequest<CollectionAtName>>::extract::<RequestContext>),
        from_fn(With::<ListRequest<CollectionAtName>>::extract_name::<CollectionAtName>),
        from_fn(With::<CollectionAtName>::extract::<CollectionIdName>),
        // find collection ID
        from_fn(By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
        from_fn(With::<CollectionDB>::extract::<CollectionId>),
        // check requester has collection permissions
        from_fn(AuthzOn::<CollectionId>::set),
        from_fn(Authz::<CollAdmin, CollDev, CollExec, CollRead, InterCollRead>::check),
        // extract attime (natural order)
        from_fn(With::<CollectionAtName>::extract::<AtTime>),
        // list
        from_fn(TableStatus::active_or_frozen),
        from_fn(
            By::<CollectionId>::list_versions_at::<CollectionAtName, NoListFilter, DaoQueries, Table>
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::update::UpdateFunctionService;
    use crate::table::services::delete::TableDeleteService;
    use std::sync::Arc;
    use td_database::sql::DbPool;
    use td_objects::crudl::{ListParams, ListParamsBuilder};
    use td_objects::rest_urls::{FunctionParam, TableParam};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::types::basic::{
        AccessTokenId, AtTime, BundleId, CollectionName, Decorator, RoleId, TableName,
        TableNameDto, UserId,
    };
    use td_objects::types::function::{FunctionRegister, FunctionUpdate};
    use td_tower::ctx_service::RawOneshot;

    #[cfg(feature = "test_tower_metadata")]
    #[td_test::test(sqlx)]
    async fn test_tower_metadata_list_table_versions(db: DbPool) {
        use td_tower::metadata::{type_of_val, Metadata};

        let queries = Arc::new(DaoQueries::default());
        let provider =
            TableListByCollectionService::provider(db, queries, Arc::new(AuthzContext::default()));
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ListRequest<CollectionAtName>, ListResponse<Table>>(&[
            type_of_val(&With::<ListRequest<CollectionAtName>>::extract::<RequestContext>),
            type_of_val(&With::<ListRequest<CollectionAtName>>::extract_name::<CollectionAtName>),
            type_of_val(&With::<CollectionAtName>::extract::<CollectionIdName>),
            // find collection ID
            type_of_val(&By::<CollectionIdName>::select::<DaoQueries, CollectionDB>),
            type_of_val(&With::<CollectionDB>::extract::<CollectionId>),
            // check requester has collection permissions
            type_of_val(&AuthzOn::<CollectionId>::set),
            type_of_val(&Authz::<CollAdmin, CollDev, CollExec, CollRead, InterCollRead>::check),
            // extract attime (natural order)
            type_of_val(&With::<CollectionAtName>::extract::<AtTime>),
            // list
            type_of_val(&TableStatus::active_or_frozen),
            type_of_val(
                &By::<CollectionId>::list_versions_at::<
                    CollectionAtName,
                    NoListFilter,
                    DaoQueries,
                    Table,
                >,
            ),
        ]);
    }

    #[td_test::test(sqlx)]
    async fn test_list_table_versions(db: DbPool) -> Result<(), TdError> {
        let queries = Arc::new(DaoQueries::default());
        let authz_context = Arc::new(AuthzContext::default());
        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection")?,
            &UserId::admin(),
        )
        .await;

        let t0 = AtTime::now().await;

        // Create function with table_1 and table_2 tables
        let tables = vec![
            TableNameDto::try_from("table_1")?,
            TableNameDto::try_from("table_2")?,
        ];
        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(tables)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let _ = seed_function(&db, &collection, &create).await;

        let t1 = AtTime::now().await;

        // Update function with table_1 table (remove table_2)
        let tables = vec![TableNameDto::try_from("table_1")?];
        let update = FunctionUpdate::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(tables)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .update(
            FunctionParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_function("joaquin")?
                .build()?,
            update.clone(),
        );

        let service =
            UpdateFunctionService::new(db.clone(), queries.clone(), authz_context.clone())
                .service()
                .await;
        let response = service.raw_oneshot(request).await;
        let _response = response?;

        // Delete table_2
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .delete(
            TableParam::builder()
                .try_collection(format!("{}", collection.name()))?
                .try_table("table_2")?
                .build()?,
        );

        TableDeleteService::new(db.clone(), authz_context.clone())
            .service()
            .await
            .raw_oneshot(request)
            .await?;

        let t2 = AtTime::now().await;

        // Actual test

        // t0 -> no tables
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .list(
            CollectionAtName::builder()
                .try_collection(format!("~{}", collection.id()))?
                .at(t0)
                .build()?,
            ListParams::default(),
        );

        let service = TableListByCollectionService::new(
            db.clone(),
            queries.clone(),
            Arc::new(AuthzContext::default()),
        )
        .service()
        .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

        assert_eq!(data.len(), 0);

        // t1 -> table_1 and table_2
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .list(
            CollectionAtName::builder()
                .try_collection(format!("~{}", collection.id()))?
                .at(t1)
                .build()?,
            ListParamsBuilder::default()
                .order_by("name".to_string())
                .build()
                .unwrap(),
        );

        let service = TableListByCollectionService::new(
            db.clone(),
            queries.clone(),
            Arc::new(AuthzContext::default()),
        )
        .service()
        .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].name(), &TableName::try_from("table_1")?);
        assert_eq!(data[1].name(), &TableName::try_from("table_2")?);

        // t2 -> table_1
        let request = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::user(),
            true,
        )
        .list(
            CollectionAtName::builder()
                .try_collection(format!("~{}", collection.id()))?
                .at(t2)
                .build()?,
            ListParams::default(),
        );

        let service = TableListByCollectionService::new(
            db.clone(),
            queries.clone(),
            Arc::new(AuthzContext::default()),
        )
        .service()
        .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;
        let data = response.data();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name(), &TableName::try_from("table_1")?);
        Ok(())
    }
}
