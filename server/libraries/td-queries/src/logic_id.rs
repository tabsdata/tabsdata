//
// Copyright 2026 Tabs Data Inc.
//

//! The main function, `initialize_logical_ids`, fetches the last used ID for several key
//! entities (executions, transactions, function versions, and table data versions) and
//! initializes their respective ID generators. This is crucial for maintaining the uniqueness
//! and chronological order of new IDs created during the application's lifecycle.

use sea_query::{Order, Query};
use std::fmt::Debug;
use ta_objects::DaoCore;
use td_common::logic_id::KeyId;
use td_database::dynamic::{DynamicDecode, DynamicType};
use td_database::pool::TdPool;
use td_database::sea_query::builder::SeaQueryExt;
use td_error::{TdError, td_error};
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::table_data_version::TableDataVersionDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::types::basic::{
    DV, EP, ExecutionId, FV, FunctionVersionId, TX, TableDataVersionId, TransactionId,
};

/// Errors that can occur during logical ID initialization.
#[td_error]
enum LogicIdsError {
    /// Indicates that the last ID for a given key could not be found.
    #[error("Could not find id column for '{0}'")]
    UnknownIdColumn(String) = 5000,
    #[error("Could not find last id for '{0}': {1}")]
    CouldNotFindLastId(String, Box<dyn std::error::Error + Send + Sync>) = 5001,
}

/// Fetches the last logical ID for a given D from the database.
///
/// This generic function queries the database for the last entry in the table associated with
/// the `D` type, ordered by ID in descending order. It then extracts and returns the ID.
async fn find_last_idl<D, ID, K>(db: &TdPool) -> Result<Option<ID>, TdError>
where
    D: DaoCore,
    ID: DynamicType + DynamicDecode + Send + Unpin + 'static,
    K: KeyId,
{
    let id_col = D::col_for_type(std::any::TypeId::of::<ID>())
        .ok_or_else(|| LogicIdsError::UnknownIdColumn(K::key().to_string()))?;
    let res = Query::select()
        .columns([id_col])
        .from(D::table())
        .order_by(id_col, Order::Desc)
        .limit(1)
        .build_query_scalar()
        .fetch_optional(db)
        .await
        .map_err(|e| LogicIdsError::CouldNotFindLastId(K::key().to_string(), Box::new(e)))?;
    Ok(res)
}

/// Initializes the logical ID generators for all relevant entities.
///
/// This function should be called at application startup to ensure that the ID generators
/// are properly seeded with the last used IDs from the database. This prevents ID collisions
/// and maintains the integrity of the logical ID system.
pub async fn initialize_logical_ids(db: &TdPool) -> Result<(), TdError> {
    let id = find_last_idl::<ExecutionDB, ExecutionId, EP>(db).await?;
    ExecutionId::init(id)?;

    let id = find_last_idl::<TransactionDB, TransactionId, TX>(db).await?;
    TransactionId::init(id)?;

    let id = find_last_idl::<FunctionDB, FunctionVersionId, FV>(db).await?;
    FunctionVersionId::init(id)?;

    let id = find_last_idl::<TableDataVersionDB, TableDataVersionId, DV>(db).await?;
    TableDataVersionId::init(id)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VersionedQueries;
    use crate::test_utils::SeedQueries;
    use sea_query::ExprTrait;
    use sea_query::{Expr, Query};
    use ta_objects::Buildable;
    use ta_objects::and;
    use td_database::sea_query::builder::SeaQueryExt;
    use td_error::TdError;
    use td_objects::dxo::execution::ExecutionDBIden;
    use td_objects::dxo::function::{FunctionDBIden, FunctionRegister};
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::table_data_version::TableDataVersionDBIden;
    use td_objects::dxo::transaction::TransactionDBIden;
    use td_objects::types::basic::TableName;
    use td_objects::types::basic::TransactionKey;
    use td_objects::types::basic::{AtTime, FunctionRunStatus};
    use td_objects::types::basic::{
        BundleId, CollectionName, DV, Decorator, EP, ExecutionId, FV, FunctionVersionId, TX,
        TableDataVersionId, TableNameDto, TransactionId, UserId,
    };
    use td_security::ENCODED_ID_SYSTEM;

    #[tokio::test]
    async fn test_no_logical_ids() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;

        let id = find_last_idl::<ExecutionDB, ExecutionId, EP>(&db).await?;
        assert!(id.is_none());

        let id = find_last_idl::<TransactionDB, TransactionId, TX>(&db).await?;
        assert!(id.is_none());

        let id = find_last_idl::<FunctionDB, FunctionVersionId, FV>(&db).await?;
        assert!(id.is_none());

        let id = find_last_idl::<TableDataVersionDB, TableDataVersionId, DV>(&db).await?;
        assert!(id.is_none());
        //
        Ok(())
    }

    #[tokio::test]
    async fn test_initial_logical_ids() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;

        let collection = db
            .seed_collection(
                &CollectionName::try_from("collection")?,
                &UserId::try_from(ENCODED_ID_SYSTEM)?,
            )
            .await?;

        let table_name = TableNameDto::try_from("t")?;
        let create = FunctionRegister::builder()
            .try_name("f")?
            .try_description("F")?
            .bundle_id(BundleId::default())
            .try_snippet("S")?
            .decorator(Decorator::Publisher)
            .dependencies(None)
            .triggers(None)
            .tables(vec![table_name.clone()])
            .try_runtime_values("V")?
            .reuse_frozen_tables(false)
            .build()?;

        let function = db.seed_function(&collection, &create).await?;
        db.seed_function(&collection, &create).await?;
        db.seed_function(&collection, &create).await?;

        let execution = db.seed_execution(&function).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        db.seed_transaction(&execution, &transaction_key).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        let transaction = db.seed_transaction(&execution, &transaction_key).await?;

        let function_run = db
            .seed_function_run(
                &collection,
                &function,
                &execution,
                &transaction,
                &FunctionRunStatus::Scheduled,
            )
            .await?;

        let table_version = db
            .fetch_one_at::<{ TableDB::All }, TableDB>(
                and!(&collection.id, TableName::try_from(table_name)?),
                &AtTime::now(),
            )
            .await?;

        db.seed_table_data_version(
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table_version,
        )
        .await?;

        let expected = Query::select()
            .expr(Expr::col(FunctionDBIden::Id).max())
            .from(FunctionDBIden::Table)
            .build_query_scalar()
            .fetch_optional(&db)
            .await
            .unwrap();
        let id = find_last_idl::<FunctionDB, FunctionVersionId, FV>(&db).await?;
        assert_eq!(id, expected);

        let expected = Query::select()
            .expr(Expr::col(ExecutionDBIden::Id).max())
            .from(ExecutionDBIden::Table)
            .build_query_scalar()
            .fetch_optional(&db)
            .await
            .unwrap();
        let id = find_last_idl::<ExecutionDB, ExecutionId, EP>(&db).await?;
        assert_eq!(id, expected);

        let expected = Query::select()
            .expr(Expr::col(TransactionDBIden::Id).max())
            .from(TransactionDBIden::Table)
            .build_query_scalar()
            .fetch_optional(&db)
            .await
            .unwrap();
        let id = find_last_idl::<TransactionDB, TransactionId, TX>(&db).await?;
        assert_eq!(id, expected);

        let expected = Query::select()
            .expr(Expr::col(TableDataVersionDBIden::Id).max())
            .from(TableDataVersionDBIden::Table)
            .build_query_scalar()
            .fetch_optional(&db)
            .await
            .unwrap();
        let id = find_last_idl::<TableDataVersionDB, TableDataVersionId, DV>(&db).await?;
        assert_eq!(id, expected);

        Ok(())
    }
}
