//
// Copyright 2025 Tabs Data Inc.
//

use polars::prelude::PolarsError;
use std::borrow::Cow;
use td_error::{TdError, td_error};
use td_objects::crudl::handle_sql_err;
use td_objects::sql::{DaoQueries, SelectBy};
use td_objects::types::execution::TableDataVersionDBWithNames;
use td_storage::SPath;
use td_storage::location::StorageLocation;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

#[td_error]
pub enum StorageServiceError {
    #[error("Could not create storage configs: {0}")]
    CouldNotCreateStorageConfig(#[source] PolarsError) = 5000,
    #[error("Could not create lazy frame to get schema: {0}")]
    CouldNoCreateLazyFrameToGetSchema(#[source] PolarsError) = 5001,
    #[error("Could not get schema: {0}")]
    CouldNotGetSchema(#[source] PolarsError) = 5002,
}

pub async fn resolve_table_location(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Input(data_version): Input<Option<TableDataVersionDBWithNames>>,
) -> Result<Option<SPath>, TdError> {
    if let Some(data_version) = &*data_version {
        if let Some(with_data_table_data_version_id) =
            data_version.with_data_table_data_version_id()
        {
            let data_version_with_data = if data_version.id() == with_data_table_data_version_id {
                Cow::Borrowed(data_version)
            } else {
                let mut conn = connection.lock().await;
                let conn = conn.get_mut_connection()?;

                let found = queries
                    .select_by::<TableDataVersionDBWithNames>(&with_data_table_data_version_id)?
                    .build_query_as()
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;
                Cow::Owned(found)
            };

            let (path, _) = get_spath(&data_version_with_data);
            Ok(Some(path))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

fn get_spath(data_version: &TableDataVersionDBWithNames) -> (SPath, StorageLocation) {
    let storage_location = data_version.storage_version();
    StorageLocation::try_from(storage_location)
        .unwrap()
        .builder(data_version.data_location())
        .collection(data_version.collection_id())
        .data(data_version.id())
        .table(data_version.table_id(), data_version.table_version_id())
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::update::UpdateFunctionService;
    use std::sync::Arc;
    use ta_services::service::TdService;
    use td_database::sql::DbPool;
    use td_objects::crudl::RequestContext;
    use td_objects::rest_urls::FunctionParam;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_table_data_version::{
        seed_table_data_version, seed_table_data_version_with_data,
    };
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionName, Decorator, FunctionRunStatus, RoleId, TableName,
        TableNameDto, TransactionKey, UserId,
    };
    use td_objects::types::collection::CollectionDB;
    use td_objects::types::execution::{
        ExecutionDB, FunctionRunDB, TableDataVersionDB, TransactionDB,
    };
    use td_objects::types::function::{FunctionDB, FunctionRegister};
    use td_objects::types::table::TableDB;
    use td_tower::ctx_service::RawOneshot;
    use td_tower::extractors::ConnectionType;

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_table_location_non_existing(db: DbPool) -> Result<(), TdError> {
        _run_test(db, None, None).await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_table_location_existing_with_data(db: DbPool) -> Result<(), TdError> {
        let (collection, execution, transaction, function_run, table) =
            _setup_table(db.clone()).await?;

        let dv = seed_table_data_version_with_data(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table,
        )
        .await;
        _run_test(db, Some(dv.clone()), Some(dv.clone())).await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_table_location_existing_without_data(db: DbPool) -> Result<(), TdError> {
        let (collection, execution, transaction, function_run, table) =
            _setup_table(db.clone()).await?;

        let dv = seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table,
        )
        .await;
        _run_test(db, Some(dv), None).await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_table_location_existing_previous_with_data(
        db: DbPool,
    ) -> Result<(), TdError> {
        let (collection, execution, transaction, function_run, table) =
            _setup_table(db.clone()).await?;

        let previous_with_data = seed_table_data_version_with_data(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table,
        )
        .await;
        let current_without_data = seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table,
        )
        .await;
        _run_test(db, Some(current_without_data), Some(previous_with_data)).await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_table_location_existing_previous_with_data_different_table_version(
        db: DbPool,
    ) -> Result<(), TdError> {
        let (collection, execution, transaction, function_run, table) =
            _setup_table(db.clone()).await?;

        let previous_with_data = seed_table_data_version_with_data(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table,
        )
        .await;
        let (execution, transaction, function_run, table) =
            _update_table_version(db.clone(), &collection).await?;
        let current_without_data = seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table,
        )
        .await;
        assert_ne!(
            previous_with_data.table_version_id(),
            current_without_data.table_version_id()
        );
        _run_test(db, Some(current_without_data), Some(previous_with_data)).await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_table_location_existing_previous_without_data(
        db: DbPool,
    ) -> Result<(), TdError> {
        let (collection, execution, transaction, function_run, table) =
            _setup_table(db.clone()).await?;

        let _previous_with_data = seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table,
        )
        .await;
        let current_without_data = seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table,
        )
        .await;
        _run_test(db, Some(current_without_data), None).await
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_table_location_existing_previous_without_data_different_table_version(
        db: DbPool,
    ) -> Result<(), TdError> {
        let (collection, execution, transaction, function_run, table) =
            _setup_table(db.clone()).await?;

        let previous_with_data = seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table,
        )
        .await;
        let (execution, transaction, function_run, table) =
            _update_table_version(db.clone(), &collection).await?;
        let current_without_data = seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table,
        )
        .await;
        assert_ne!(
            previous_with_data.table_version_id(),
            current_without_data.table_version_id()
        );
        _run_test(db, Some(current_without_data), None).await
    }

    async fn _setup_table(
        db: DbPool,
    ) -> Result<
        (
            CollectionDB,
            ExecutionDB,
            TransactionDB,
            FunctionRunDB,
            TableDB,
        ),
        TdError,
    > {
        let queries = DaoQueries::default();

        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection")?,
            &UserId::admin(),
        )
        .await;

        let create = FunctionRegister::builder()
            .try_name("joaquin")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_1")?])
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;

        let function = seed_function(&db, &collection, &create).await;
        let execution = seed_execution(&db, &function).await;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;

        let function_run = seed_function_run(
            &db,
            &collection,
            &function,
            &execution,
            &transaction,
            &FunctionRunStatus::Scheduled,
        )
        .await;

        let table = queries
            .select_by::<TableDB>(&(function.id()))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        Ok((collection, execution, transaction, function_run, table))
    }

    async fn _update_table_version(
        db: DbPool,
        collection: &CollectionDB,
    ) -> Result<(ExecutionDB, TransactionDB, FunctionRunDB, TableDB), TdError> {
        // Update the function to change the table version (TODO make this a seed function)
        let update = FunctionRegister::builder()
            .try_name("joaquin_2")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![TableNameDto::try_from("table_1")?])
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user()).update(
                FunctionParam::builder()
                    .try_collection(format!("{}", collection.name()))?
                    .try_function("joaquin")?
                    .build()?,
                update.clone(),
            );
        let service = UpdateFunctionService::with_defaults(db.clone())
            .service()
            .await;
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let function = DaoQueries::default()
            .select_by::<FunctionDB>(&(response.id()))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();
        let execution = seed_execution(&db, &function).await;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = seed_transaction(&db, &execution, &transaction_key).await;

        let function_run = seed_function_run(
            &db,
            collection,
            &function,
            &execution,
            &transaction,
            &FunctionRunStatus::Scheduled,
        )
        .await;

        let table = DaoQueries::default()
            .select_by::<TableDB>(&(function.id(), &TableName::try_from("table_1")?))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();

        Ok((execution, transaction, function_run, table))
    }

    async fn _run_test(
        db: DbPool,
        lookup_data_version: Option<TableDataVersionDB>,
        expected_data_version: Option<TableDataVersionDB>,
    ) -> Result<(), TdError> {
        let queries = Arc::new(DaoQueries::default());

        let lookup_data_version = if let Some(lookup_data_version) = lookup_data_version {
            let d = queries
                .select_by::<TableDataVersionDBWithNames>(&(lookup_data_version.id()))?
                .build_query_as()
                .fetch_one(&db)
                .await
                .unwrap();
            Some(d)
        } else {
            None
        };
        let lookup_data_version = Arc::new(lookup_data_version);

        let connection = db.acquire().await.unwrap();
        let connection = ConnectionType::PoolConnection(connection).into();
        let found = resolve_table_location(
            Connection(connection),
            SrvCtx(queries.clone()),
            Input(lookup_data_version),
        )
        .await?;

        let expected = if let Some(expected_data_version) = expected_data_version {
            let expected_data_version: TableDataVersionDBWithNames = queries
                .select_by::<TableDataVersionDBWithNames>(&(expected_data_version.id()))?
                .build_query_as()
                .fetch_one(&db)
                .await
                .unwrap();
            let (expected, _) = get_spath(&expected_data_version);
            Some(expected)
        } else {
            None
        };
        assert_eq!(found, expected);
        Ok(())
    }
}
