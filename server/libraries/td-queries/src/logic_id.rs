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
use td_database::sea_query::executor::SeaExecutor;
use td_error::{TdError, td_error};
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::FunctionDB;
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalTableDataVersionDB};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    EP, ExecutionId, FV, FunctionVersionId, LDV, LFR, LogicalFunctionRunId,
    LogicalTableDataVersionId, PDV, PFR, PhysicalFunctionRunId, PhysicalTableDataVersionId, TX,
    TransactionId, WKR, WorkerId,
};
use td_tower::error::ConnectionError;

/// Errors that can occur during logical ID initialization.
#[td_error]
pub(crate) enum LogicIdsError {
    /// Indicates that the last ID for a given key could not be found.
    #[error("Could not find id column for '{0}'")]
    UnknownIdColumn(String) = 5000,
    #[error("Could not find last id for '{0}': {1}")]
    CouldNotFindLastId(String, Box<dyn std::error::Error + Send + Sync>) = 5001,
}

/// Build the SELECT query for finding the last logical ID.
pub(crate) fn find_last_idl_query<D, ID>() -> Result<sea_query::SelectStatement, LogicIdsError>
where
    D: DaoCore,
    ID: 'static,
{
    let id_col = D::col_for_type(std::any::TypeId::of::<ID>())
        .ok_or_else(|| LogicIdsError::UnknownIdColumn(std::any::type_name::<ID>().to_string()))?;
    Ok(Query::select()
        .columns([id_col])
        .from(D::table())
        .order_by(id_col, Order::Desc)
        .limit(1)
        .take())
}

/// Read a single entity's last-used logical id (the max id in its table).
///
/// Logical ids are global high-water marks, not per-project, so this must read
/// across every project: callers run it on a **bypass-scoped** connection
/// (never a project scope) — see [`initialize_logical_ids`].
async fn find_last_idl<D, ID, K>(executor: impl SeaExecutor) -> Result<Option<ID>, TdError>
where
    D: DaoCore,
    ID: DynamicType + DynamicDecode + Send + Unpin + 'static,
    K: KeyId,
{
    let res = find_last_idl_query::<D, ID>()
        .map_err(|_| LogicIdsError::UnknownIdColumn(K::key().to_string()))?
        .build_query_scalar()
        .fetch_optional(executor)
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
    // Logical IDs are global high-water marks spanning every project, so all
    // reads share one bypass-scoped transaction. The transaction is
    // load-bearing: the bypass GUC is `SET LOCAL`-scoped and would not
    // outlive its own statement on an autocommit connection (fail-closed,
    // empty reads).
    let mut conn = db
        .begin()
        .await
        .map_err(ConnectionError::CannotBeginTransaction)?
        .into_connection();
    conn.bypass_project_scope()
        .await
        .map_err(ConnectionError::CannotBypassProjectScope)?;

    let id = find_last_idl::<ExecutionDB, ExecutionId, EP>(&mut conn).await?;
    ExecutionId::init(id)?;

    let id = find_last_idl::<TransactionDB, TransactionId, TX>(&mut conn).await?;
    TransactionId::init(id)?;

    let id = find_last_idl::<FunctionDB, FunctionVersionId, FV>(&mut conn).await?;
    FunctionVersionId::init(id)?;

    let id = find_last_idl::<PhysicalFunctionRunDB, PhysicalFunctionRunId, PFR>(&mut conn).await?;
    PhysicalFunctionRunId::init(id)?;

    let id =
        find_last_idl::<PhysicalTableDataVersionDB, PhysicalTableDataVersionId, PDV>(&mut conn)
            .await?;
    PhysicalTableDataVersionId::init(id)?;

    let id = find_last_idl::<LogicalFunctionRunDB, LogicalFunctionRunId, LFR>(&mut conn).await?;
    LogicalFunctionRunId::init(id)?;

    let id = find_last_idl::<LogicalTableDataVersionDB, LogicalTableDataVersionId, LDV>(&mut conn)
        .await?;
    LogicalTableDataVersionId::init(id)?;

    let id = find_last_idl::<WorkerDB, WorkerId, WKR>(&mut conn).await?;
    WorkerId::init(id)?;

    if let Some(tx) = conn.into_transaction() {
        tx.commit()
            .await
            .map_err(ConnectionError::CannotCommitTransaction)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CurrentEntry;
    use crate::test_utils::SeedEntry;
    use sea_query::ExprTrait;
    use sea_query::{Expr, Query};
    use ta_objects::Buildable;
    use ta_objects::and;
    use td_database::sea_query::builder::SeaQueryExt;
    use td_error::TdError;
    use td_objects::dxo::execution::ExecutionDBIden;
    use td_objects::dxo::function::{FunctionDBIden, FunctionRegister};
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBIden;
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::transaction::TransactionDBIden;
    use td_objects::types::basic::LogicalFunctionRunStatus;
    use td_objects::types::basic::TableName;
    use td_objects::types::basic::TransactionKey;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, EP, ExecutionId, FV, FunctionVersionId, PDV,
        PhysicalTableDataVersionId, TX, TableNameDto, TransactionId, UserId,
    };
    use td_security::ENCODED_ID_SYSTEM;

    #[td_test::test(db)]
    async fn test_no_logical_ids(db: TdPool) -> Result<(), TdError> {
        let id = find_last_idl::<ExecutionDB, ExecutionId, EP>(&db).await?;
        assert!(id.is_none());

        let id = find_last_idl::<TransactionDB, TransactionId, TX>(&db).await?;
        assert!(id.is_none());

        let id = find_last_idl::<FunctionDB, FunctionVersionId, FV>(&db).await?;
        assert!(id.is_none());

        let id = find_last_idl::<PhysicalTableDataVersionDB, PhysicalTableDataVersionId, PDV>(&db)
            .await?;
        assert!(id.is_none());
        //
        Ok(())
    }

    #[td_test::test(db)]
    async fn test_initial_logical_ids(db: TdPool) -> Result<(), TdError> {
        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(
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

        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        db.scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        db.scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;

        let execution = db.scoped(p.id).seed().execution(&function).await?;
        let transaction_key = TransactionKey::try_from("ANY")?;
        db.scoped(p.id)
            .seed()
            .transaction(&execution, &transaction_key)
            .await?;
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

        let table_version = db
            .current()
            .fetch_one::<TableDB>(and!(&collection.id, TableName::try_from(table_name)?))
            .await?;

        db.scoped(p.id)
            .seed()
            .physical_table_data_version(
                &collection,
                &execution,
                &transaction,
                &physical_function_run,
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
            .expr(Expr::col(PhysicalTableDataVersionDBIden::Id).max())
            .from(PhysicalTableDataVersionDBIden::Table)
            .build_query_scalar()
            .fetch_optional(&db)
            .await
            .unwrap();
        let id = find_last_idl::<PhysicalTableDataVersionDB, PhysicalTableDataVersionId, PDV>(&db)
            .await?;
        assert_eq!(id, expected);

        Ok(())
    }

    // --- SQL generation tests ---

    mod sql {
        use super::*;
        use sea_query::SqliteQueryBuilder;

        #[test]
        fn test_find_last_idl_execution() {
            let sql = find_last_idl_query::<ExecutionDB, ExecutionId>()
                .unwrap()
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains(r#"FROM "executions""#),
                "should query executions: {sql}"
            );
            assert!(sql.contains("ORDER BY"), "should order: {sql}");
            assert!(sql.contains("DESC"), "should order desc: {sql}");
            assert!(sql.contains("LIMIT 1"), "should limit 1: {sql}");
        }

        #[test]
        fn test_find_last_idl_function() {
            let sql = find_last_idl_query::<FunctionDB, FunctionVersionId>()
                .unwrap()
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains(r#"FROM "functions""#),
                "should query functions: {sql}"
            );
            assert!(sql.contains("LIMIT 1"), "should limit 1: {sql}");
        }

        #[test]
        fn test_find_last_idl_transaction() {
            let sql = find_last_idl_query::<TransactionDB, TransactionId>()
                .unwrap()
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains(r#"FROM "transactions""#),
                "should query transactions: {sql}"
            );
        }

        #[test]
        fn test_find_last_idl_table_data_version() {
            let sql =
                find_last_idl_query::<PhysicalTableDataVersionDB, PhysicalTableDataVersionId>()
                    .unwrap()
                    .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains("physical_table_data_version"),
                "should query physical_table_data_versions: {sql}"
            );
        }
    }
}
