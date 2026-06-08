//
// Copyright 2025 Tabs Data Inc.
//

use polars::prelude::PolarsError;
use td_database::dynamic::DynamicConnection;
use td_error::{TdError, td_error};
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBWithNames;
use td_objects::types::basic::{TableId, TriggeredOn, UncommittedStatus};
use td_queries::LtdvEntry;
use td_storage::SPath;
use td_storage::location::StorageLocation;
use td_tower::extractors::{MutRef, Ref};

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
    mut conn: MutRef<DynamicConnection<'static>>,
    triggered_on: Ref<TriggeredOn>,
    table_id: Ref<TableId>,
    uncommitted: Ref<UncommittedStatus>,
) -> Result<Option<SPath>, TdError> {
    let committed_only = !**uncommitted;
    let data_version = conn
        .ltdv()
        .latest_bound_tdv_with_data_for_table(
            table_id.as_ref(),
            triggered_on.as_ref(),
            committed_only,
        )
        .await?;

    if let Some(data_version) = data_version {
        let (path, _) = get_spath(&data_version)?;
        Ok(Some(path))
    } else {
        Ok(None)
    }
}

fn get_spath(
    data_version: &PhysicalTableDataVersionDBWithNames,
) -> Result<(SPath, StorageLocation), TdError> {
    Ok(StorageLocation::from(&data_version.storage_version)
        .builder(&data_version.data_location)
        .collection(&data_version.collection_id)
        .data(&data_version.id)
        .table(&data_version.table_id, &data_version.table_version_id)
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::services::update::UpdateFunctionService;
    use std::str::FromStr;
    use ta_services::service::TdService;
    use td_database::pool::TdPool;
    use td_objects::dxo::collection::CollectionDB;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionDB;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister, FunctionUpdate};
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::transaction::TransactionDB;
    use td_objects::rest_urls::FunctionParam;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, FunctionIdName,
        LogicalFunctionRunStatus, ProjectId, RoleId, TableName, TableNameDto, TransactionKey,
        UserId,
    };
    use td_objects::{Buildable, and};
    use td_queries::LiveEntry;
    use td_queries::test_utils::SeedEntry;
    use td_tower::ctx_service::RawOneshot;
    use td_tower::project_scope::InProject;

    #[td_test::test(db)]
    async fn test_resolve_table_location_non_existing(db: TdPool) -> Result<(), TdError> {
        _run_test(db, None, None).await
    }

    #[td_test::test(db)]
    async fn test_resolve_table_location_existing_with_data(db: TdPool) -> Result<(), TdError> {
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone()).await?;

        let dv = db
            .seed()
            .physical_table_data_version_with_data(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;
        _run_test(db, Some(dv.clone()), Some(dv.clone())).await
    }

    #[td_test::test(db)]
    async fn test_resolve_table_location_existing_without_data(db: TdPool) -> Result<(), TdError> {
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone()).await?;

        let dv = db
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;
        _run_test(db, Some(dv), None).await
    }

    #[td_test::test(db)]
    async fn test_resolve_table_location_existing_previous_with_data(
        db: TdPool,
    ) -> Result<(), TdError> {
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone()).await?;

        let previous_with_data = db
            .seed()
            .physical_table_data_version_with_data(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;
        let current_without_data = db
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;
        _run_test(db, Some(current_without_data), Some(previous_with_data)).await
    }

    #[td_test::test(db)]
    async fn test_resolve_table_location_existing_previous_with_data_different_table_version(
        db: TdPool,
    ) -> Result<(), TdError> {
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone()).await?;

        let previous_with_data = db
            .seed()
            .physical_table_data_version_with_data(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;
        let (execution, transaction, physical_function_run, table) =
            _update_table_version(db.clone(), &collection).await?;
        let current_without_data = db
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;
        assert_ne!(
            previous_with_data.table_version_id,
            current_without_data.table_version_id
        );
        _run_test(db, Some(current_without_data), Some(previous_with_data)).await
    }

    #[td_test::test(db)]
    async fn test_resolve_table_location_existing_previous_without_data(
        db: TdPool,
    ) -> Result<(), TdError> {
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone()).await?;

        let _previous_with_data = db
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await;
        let current_without_data = db
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;
        _run_test(db, Some(current_without_data), None).await
    }

    #[td_test::test(db)]
    async fn test_resolve_table_location_existing_previous_without_data_different_table_version(
        db: TdPool,
    ) -> Result<(), TdError> {
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone()).await?;

        let previous_with_data = db
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;
        let (execution, transaction, physical_function_run, table) =
            _update_table_version(db.clone(), &collection).await?;
        let current_without_data = db
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;
        assert_ne!(
            previous_with_data.table_version_id,
            current_without_data.table_version_id
        );
        _run_test(db, Some(current_without_data), None).await
    }

    async fn _setup_table(
        db: TdPool,
    ) -> Result<
        (
            CollectionDB,
            ExecutionDB,
            TransactionDB,
            PhysicalFunctionRunDB,
            TableDB,
        ),
        TdError,
    > {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

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

        let function = db.seed().function(&collection, &create).await?;
        let execution = db.seed().execution(&function).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = db.seed().transaction(&execution, &transaction_key).await?;

        let physical_function_run = db
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;

        let table: TableDB = db.live().fetch_one(&function.id).await?;
        Ok((
            collection,
            execution,
            transaction,
            physical_function_run,
            table,
        ))
    }

    async fn _update_table_version(
        db: TdPool,
        collection: &CollectionDB,
    ) -> Result<(ExecutionDB, TransactionDB, PhysicalFunctionRunDB, TableDB), TdError> {
        // Update the function to change the table version (TODO make this a seed function)
        let update = FunctionUpdate::builder()
            .name(Some("joaquin_2".try_into()?))
            .description(Some("function_foo description".try_into()?))
            .snippet(Some("function_foo snippet".try_into()?))
            .decorator(Decorator::Publisher)
            .tables(vec![TableNameDto::try_from("table_1")?])
            .build()?;

        let request =
            RequestContext::with(AccessTokenId::default(), UserId::admin(), RoleId::user())
                .update(
                    FunctionParam::builder()
                        .collection(CollectionIdName::from_str(&format!("{}", collection.name))?)
                        .function(FunctionIdName::from_str("joaquin")?)
                        .build()?,
                    update.clone(),
                )
                .in_project(ProjectId::sandbox());
        let service = UpdateFunctionService::with_defaults(db.clone()).service();
        let response = service.raw_oneshot(request).await;
        let response = response?;

        let function: FunctionDB = db.live().fetch_one(&(response.id)).await?;
        let execution = db.seed().execution(&function).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = db.seed().transaction(&execution, &transaction_key).await?;

        let physical_function_run = db
            .seed()
            .physical_function_run(
                collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;

        let table = db
            .live()
            .fetch_one(and!(function.id, &TableName::try_from("table_1")?))
            .await?;

        Ok((execution, transaction, physical_function_run, table))
    }

    async fn _resolve(
        db: &TdPool,
        table_id: TableId,
        uncommitted: bool,
    ) -> Result<Option<SPath>, TdError> {
        let connection = db.acquire().await.unwrap();
        resolve_table_location(
            MutRef::new(connection),
            Ref::new(TriggeredOn::now()),
            Ref::new(table_id),
            Ref::new(if uncommitted {
                UncommittedStatus::TRUE
            } else {
                UncommittedStatus::FALSE
            }),
        )
        .await
    }

    async fn _run_test(
        db: TdPool,
        lookup_data_version: Option<PhysicalTableDataVersionDB>,
        expected_data_version: Option<PhysicalTableDataVersionDB>,
    ) -> Result<(), TdError> {
        let found = if let Some(lookup_data_version) = lookup_data_version {
            _resolve(&db, lookup_data_version.table_id, true).await?
        } else {
            None
        };

        let expected = if let Some(expected_data_version) = expected_data_version {
            let expected_data_version: PhysicalTableDataVersionDBWithNames =
                db.live().fetch_one(&expected_data_version.id).await?;
            let (expected, _) = get_spath(&expected_data_version)?;
            Some(expected)
        } else {
            None
        };
        assert_eq!(found, expected);
        Ok(())
    }

    async fn _setup_committed_table(
        db: TdPool,
    ) -> Result<
        (
            CollectionDB,
            ExecutionDB,
            TransactionDB,
            PhysicalFunctionRunDB,
            TableDB,
        ),
        TdError,
    > {
        let collection = db
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

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

        let function = db.seed().function(&collection, &create).await?;
        let execution = db.seed().execution(&function).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = db.seed().transaction(&execution, &transaction_key).await?;

        let physical_function_run = db
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Committed,
            )
            .await?;

        let table: TableDB = db.live().fetch_one(&function.id).await?;
        Ok((
            collection,
            execution,
            transaction,
            physical_function_run,
            table,
        ))
    }

    // Tests for committed vs uncommitted behavior

    #[td_test::test(db)]
    async fn test_resolve_committed_data_version_with_uncommitted_true(
        db: TdPool,
    ) -> Result<(), TdError> {
        // Committed data version with data should be found when uncommitted=true
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_committed_table(db.clone()).await?;

        let dv = db
            .seed()
            .physical_table_data_version_with_data(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;

        let found = _resolve(&db, dv.table_id, true).await?;
        assert!(found.is_some());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_resolve_committed_data_version_with_uncommitted_false(
        db: TdPool,
    ) -> Result<(), TdError> {
        // Committed data version with data should be found when uncommitted=false
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_committed_table(db.clone()).await?;

        let dv = db
            .seed()
            .physical_table_data_version_with_data(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;

        let found = _resolve(&db, dv.table_id, false).await?;
        assert!(found.is_some());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_resolve_uncommitted_data_version_with_uncommitted_true(
        db: TdPool,
    ) -> Result<(), TdError> {
        // Non-committed data version with data should be found when uncommitted=true
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone()).await?;

        let dv = db
            .seed()
            .physical_table_data_version_with_data(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;

        let found = _resolve(&db, dv.table_id, true).await?;
        assert!(found.is_some());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_resolve_uncommitted_data_version_with_uncommitted_false(
        db: TdPool,
    ) -> Result<(), TdError> {
        // Non-committed data version with data should NOT be found when uncommitted=false
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone()).await?;

        let dv = db
            .seed()
            .physical_table_data_version_with_data(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;

        let found = _resolve(&db, dv.table_id, false).await?;
        assert!(found.is_none());
        Ok(())
    }
}
