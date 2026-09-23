//
// Copyright 2025 Tabsdata Inc.
//

use polars::prelude::PolarsError;
use td_database::access::{AccessMode, DbScope};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBWithNames;
use td_objects::types::basic::{AtTime, TableId, UncommittedStatus};
use td_queries::Queries;
use td_storage::SPath;
use td_storage::location::StorageLocation;

#[td_error]
pub enum StorageServiceError {
    #[error("Could not create storage configs: {0}")]
    CouldNotCreateStorageConfig(#[source] PolarsError) = 5000,
    #[error("Could not create lazy frame to get schema: {0}")]
    CouldNoCreateLazyFrameToGetSchema(#[source] PolarsError) = 5001,
    #[error("Could not get schema: {0}")]
    CouldNotGetSchema(#[source] PolarsError) = 5002,
}

pub async fn resolve_table_location<S: DbScope, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    triggered_on: &AtTime,
    table_id: &TableId,
    uncommitted: &UncommittedStatus,
) -> Result<Option<SPath>, TdError> {
    let committed_only = !**uncommitted;
    let data_version = conn
        .table_data_versions()
        .latest_bound_tdv_with_data_for_table(table_id, triggered_on, committed_only)
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
    Ok(StorageLocation::from(&data_version.base.storage_version)
        .builder(&data_version.base.base.base.base_path)
        .collection(&data_version.base.base.base.collection_id)
        .data(&data_version.base.base.base.id)
        .table(
            &data_version.base.base.base.table_id,
            &data_version.base.base.base.table_version_id,
        )
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Services;
    use std::collections::HashMap;
    use std::str::FromStr;
    use td_database::access::{InProject, RW};
    use td_database::pool::TdPool;
    use td_database::pool::scoped::ScopedExt;
    use td_objects::and;
    use td_objects::dxo::collection::ActiveCollectionDB;
    use td_objects::dxo::collection::CollectionParam;
    use td_objects::dxo::crudl::RequestContext;
    use td_objects::dxo::execution::ExecutionDB;
    use td_objects::dxo::function::FunctionParam;
    use td_objects::dxo::function::{FunctionDB, FunctionRegister, FunctionUpdate};
    use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
    use td_objects::dxo::project::ActiveProjectDB;
    use td_objects::dxo::table::{TableDB, TableDBWithNames};
    use td_objects::dxo::transaction::TransactionDB;
    use td_objects::types::basic::AtTime;
    use td_objects::types::basic::ReuseFrozen;
    use td_objects::types::basic::TableType;
    use td_objects::types::basic::{
        AccessTokenId, BundleId, CollectionIdName, CollectionName, Decorator, FunctionIdName,
        LogicalFunctionRunStatus, TableIdName, TableNameDto, TransactionKey, UserId,
    };
    use td_queries::Queries;
    use td_queries::test_utils::SeedEntry;

    #[td_test::test(db)]
    async fn test_resolve_table_location_non_existing(db: TdPool) -> Result<(), TdError> {
        _run_test(db, None, None).await
    }

    #[td_test::test(db)]
    async fn test_resolve_table_location_existing_with_data(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone(), &p).await?;

        let dv = db
            .scoped(p.id)
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
        let p = db.seed().test_project().await?;
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone(), &p).await?;

        let dv = db
            .scoped(p.id)
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
        let p = db.seed().test_project().await?;
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone(), &p).await?;

        let previous_with_data = db
            .scoped(p.id)
            .seed()
            .physical_table_data_version_with_data(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;
        // The no-data current version must come from its own execution: within
        // one execution a second seed re-materializes the same LTDV, superseding the
        // with-data version instead of leaving it as a prior logical version.
        let function: FunctionDB = db
            .timeline()
            .fetch_one(&table.base.function_version_id)
            .await?;
        let execution = db.scoped(p.id).seed().execution(&function).await?;
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &TransactionKey::try_from("ANY")?)
            .await?;
        let physical_function_run = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;
        let current_without_data = db
            .scoped(p.id)
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;
        assert_eq!(
            previous_with_data.base.table_version_id,
            current_without_data.base.table_version_id
        );
        _run_test(db, Some(current_without_data), Some(previous_with_data)).await
    }

    #[td_test::test(db)]
    async fn test_resolve_table_location_existing_previous_with_data_different_table_version(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone(), &p).await?;

        let previous_with_data = db
            .scoped(p.id)
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
            _update_table_version(db.clone(), &collection, &p).await?;
        let current_without_data = db
            .scoped(p.id)
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
            previous_with_data.base.table_version_id,
            current_without_data.base.table_version_id
        );
        _run_test(db, Some(current_without_data), Some(previous_with_data)).await
    }

    #[td_test::test(db)]
    async fn test_resolve_table_location_existing_previous_without_data(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone(), &p).await?;

        let _previous_with_data = db
            .scoped(p.id)
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await;
        // Its own run: a run writes each of its tables exactly once.
        let function: FunctionDB = db
            .timeline()
            .fetch_one(&table.base.function_version_id)
            .await?;
        let physical_function_run = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;
        let current_without_data = db
            .scoped(p.id)
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
        let p = db.seed().test_project().await?;
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone(), &p).await?;

        let previous_with_data = db
            .scoped(p.id)
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
            _update_table_version(db.clone(), &collection, &p).await?;
        let current_without_data = db
            .scoped(p.id)
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
            previous_with_data.base.table_version_id,
            current_without_data.base.table_version_id
        );
        _run_test(db, Some(current_without_data), None).await
    }

    async fn _setup_table(
        db: TdPool,
        p: &ActiveProjectDB,
    ) -> Result<
        (
            ActiveCollectionDB,
            ExecutionDB,
            TransactionDB,
            PhysicalFunctionRunDB,
            TableDB,
        ),
        TdError,
    > {
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            name: "joaquin".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_1")?].into(),
            table_types: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db.scoped(p.id).seed().execution(&function).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;

        let physical_function_run = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Scheduled,
            )
            .await?;

        let table: TableDB = db
            .scd2()
            .fetch_first(&function.base.id)
            .await?
            .expect("row must exist");
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
        collection: &ActiveCollectionDB,
        p: &ActiveProjectDB,
    ) -> Result<(ExecutionDB, TransactionDB, PhysicalFunctionRunDB, TableDB), TdError> {
        // Change the table's definition, so the update versions it.
        let table_1 = TableNameDto::try_from("table_1")?;
        let update = FunctionUpdate {
            name: Some("joaquin_2".try_into()?),
            description: Some("function_foo description".try_into()?),
            snippet: Some("function_foo snippet".try_into()?),
            decorator: Some(Decorator::Transformer),
            tables: vec![table_1.clone()].into(),
            table_types: Some(HashMap::from([(table_1, TableType::DQData)])),
            ..Default::default()
        };

        let request = RequestContext::with(AccessTokenId::default(), UserId::admin())
            .update(
                (
                    CollectionParam {
                        collection: CollectionIdName::from_str(&format!("{}", collection.name))?,
                    },
                    FunctionParam {
                        function: FunctionIdName::from_str("joaquin")?,
                    },
                ),
                update.clone(),
            )
            .in_project(p.id);
        let service = Services::with_defaults(db.clone());
        let response = service.update_function(request).await;
        let response = response?;

        let function: FunctionDB = db.timeline().fetch_one(&(response.id)).await?;
        let execution = db.scoped(p.id).seed().execution(&function).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;

        let physical_function_run = db
            .scoped(p.id)
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
            .scd2()
            .fetch_one::<TableDBWithNames>(and!(
                &function.base.collection_id,
                &TableIdName::from_str("table_1")?
            ))
            .await?
            .base;

        Ok((execution, transaction, physical_function_run, table))
    }

    async fn _resolve(
        db: &TdPool,
        table_id: TableId,
        uncommitted: bool,
    ) -> Result<Option<SPath>, TdError> {
        let mut connection =
            DomainConnection::<Apiserver, InProject, RW>::new(db.begin().await.unwrap());
        resolve_table_location(
            &mut connection,
            &AtTime::now(),
            &table_id,
            &if uncommitted {
                UncommittedStatus::TRUE
            } else {
                UncommittedStatus::FALSE
            },
        )
        .await
    }

    async fn _run_test(
        db: TdPool,
        lookup_data_version: Option<PhysicalTableDataVersionDB>,
        expected_data_version: Option<PhysicalTableDataVersionDB>,
    ) -> Result<(), TdError> {
        let found = if let Some(lookup_data_version) = lookup_data_version {
            _resolve(&db, lookup_data_version.base.table_id, true).await?
        } else {
            None
        };

        let expected = if let Some(expected_data_version) = expected_data_version {
            let expected_data_version: PhysicalTableDataVersionDBWithNames = db
                .timeline()
                .fetch_one(&expected_data_version.base.id)
                .await?;
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
        p: &ActiveProjectDB,
    ) -> Result<
        (
            ActiveCollectionDB,
            ExecutionDB,
            TransactionDB,
            PhysicalFunctionRunDB,
            TableDB,
        ),
        TdError,
    > {
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(&CollectionName::try_from("collection")?, &UserId::admin())
            .await?;

        let create = FunctionRegister {
            name: "joaquin".try_into()?,
            description: "function_foo description".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "function_foo snippet".try_into()?,
            decorator: Decorator::Transformer,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("table_1")?].into(),
            table_types: None,
            runtime_values: "mock runtime values".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let execution = db.scoped(p.id).seed().execution(&function).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = db
            .scoped(p.id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;

        let physical_function_run = db
            .scoped(p.id)
            .seed()
            .physical_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &LogicalFunctionRunStatus::Committed,
            )
            .await?;

        let table: TableDB = db
            .scd2()
            .fetch_first(&function.base.id)
            .await?
            .expect("row must exist");
        Ok((
            collection,
            execution,
            transaction,
            physical_function_run,
            table,
        ))
    }

    #[td_test::test(db)]
    async fn test_resolve_committed_data_version_with_uncommitted_true(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_committed_table(db.clone(), &p).await?;

        let dv = db
            .scoped(p.id)
            .seed()
            .physical_table_data_version_with_data(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;

        let found = _resolve(&db, dv.base.table_id, true).await?;
        assert!(found.is_some());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_resolve_committed_data_version_with_uncommitted_false(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_committed_table(db.clone(), &p).await?;

        let dv = db
            .scoped(p.id)
            .seed()
            .physical_table_data_version_with_data(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;

        let found = _resolve(&db, dv.base.table_id, false).await?;
        assert!(found.is_some());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_resolve_uncommitted_data_version_with_uncommitted_true(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone(), &p).await?;

        let dv = db
            .scoped(p.id)
            .seed()
            .physical_table_data_version_with_data(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;

        let found = _resolve(&db, dv.base.table_id, true).await?;
        assert!(found.is_some());
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_resolve_uncommitted_data_version_with_uncommitted_false(
        db: TdPool,
    ) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let (collection, execution, transaction, physical_function_run, table) =
            _setup_table(db.clone(), &p).await?;

        let dv = db
            .scoped(p.id)
            .seed()
            .physical_table_data_version_with_data(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
                &table,
            )
            .await?;

        let found = _resolve(&db, dv.base.table_id, false).await?;
        assert!(found.is_none());
        Ok(())
    }
}
