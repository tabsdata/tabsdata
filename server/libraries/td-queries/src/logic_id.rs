//
// Copyright 2026 Tabsdata Inc.
//

//! The main function, `initialize_logical_ids`, fetches the last used ID for several key
//! entities (executions, transactions, function versions, and table data versions) and
//! initializes their respective ID generators. This is crucial for maintaining the uniqueness
//! and chronological order of new IDs created during the application's lifecycle.

use sea_query::{Order, Query};
use sqlx::{Decode, Postgres, Type};
use std::fmt::Debug;
use ta_objects::HasColumn;
use td_common::logic_id::KeyId;
use td_database::access::ServiceDbError;
use td_database::connection::PgConnectionExt;
use td_database::pool::TdPool;
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use td_error::{TdError, td_error};
use td_objects::dxo::execution::ExecutionDB;
use td_objects::dxo::function::{FunctionDB, FunctionPatchDB};
use td_objects::dxo::logical::{LogicalFunctionRunDB, LogicalTableDataVersionDB};
use td_objects::dxo::physical_function_run::PhysicalFunctionRunDB;
use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDB;
use td_objects::dxo::transaction::TransactionDB;
use td_objects::dxo::worker::WorkerDB;
use td_objects::types::basic::{
    DV, EP, ExecutionId, FR, FV, FunctionVersionId, LogicalFunctionRunId,
    LogicalTableDataVersionId, PDV, PFR, PhysicalFunctionRunId, PhysicalTableDataVersionId, TX,
    TransactionId, WKR, WorkerId,
};

/// Errors that can occur during logical ID initialization.
#[td_error]
pub(crate) enum LogicIdsError {
    #[error("Could not find last id for '{0}': {1}")]
    CouldNotFindLastId(String, Box<dyn std::error::Error + Send + Sync>) = 5000,
}

/// Build the SELECT query for finding the last logical ID. The id column is
/// the DAO's `ID`-typed column, resolved at compile time.
pub(crate) fn find_last_idl_query<D, ID>() -> sea_query::SelectStatement
where
    D: HasColumn<ID>,
    ID: 'static,
{
    let id_col = D::COL;
    Query::select()
        .columns([id_col])
        .from(D::TABLE)
        .order_by(id_col, Order::Desc)
        .limit(1)
        .take()
}

/// Read a single entity's last-used logical id (the max id in its table).
///
/// Logical ids are global high-water marks, not per-project, so this must read
/// across every project: callers run it on a **bypass-scoped** connection
/// (never a project scope) — see [`initialize_logical_ids`].
async fn find_last_idl<D, ID, K>(executor: impl SeaExecutor) -> Result<Option<ID>, TdError>
where
    D: HasColumn<ID>,
    ID: for<'r> Decode<'r, Postgres> + Type<Postgres> + Send + Unpin + 'static,
    K: KeyId,
{
    let res = find_last_idl_query::<D, ID>()
        .build_query_scalar()
        .fetch_optional(executor)
        .await
        .map_err(|e| LogicIdsError::CouldNotFindLastId(K::key().to_string(), Box::new(e)))?;
    Ok(res)
}

/// Function versions and patches draw from the same `FunctionVersionId`
/// generator but live in separate tables: the high-water mark is the max
/// across both, else a restart re-issues ids already taken by patches.
async fn last_function_version_id<C>(conn: &mut C) -> Result<Option<FunctionVersionId>, TdError>
where
    for<'a> &'a mut C: SeaExecutor,
{
    let head = find_last_idl::<FunctionDB, FunctionVersionId, FV>(&mut *conn).await?;
    let patch = find_last_idl::<FunctionPatchDB, FunctionVersionId, FV>(&mut *conn).await?;
    Ok(head.max(patch))
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
        .map_err(ServiceDbError::CannotBeginTransaction)?;
    conn.scope_across_projects()
        .await
        .map_err(ServiceDbError::CannotBypassProjectScope)?;

    let id = find_last_idl::<ExecutionDB, ExecutionId, EP>(&mut *conn).await?;
    ExecutionId::init(id)?;

    let id = find_last_idl::<TransactionDB, TransactionId, TX>(&mut *conn).await?;
    TransactionId::init(id)?;

    let id = last_function_version_id(&mut *conn).await?;
    FunctionVersionId::init(id)?;

    let id = find_last_idl::<PhysicalFunctionRunDB, PhysicalFunctionRunId, PFR>(&mut *conn).await?;
    PhysicalFunctionRunId::init(id)?;

    let id =
        find_last_idl::<PhysicalTableDataVersionDB, PhysicalTableDataVersionId, PDV>(&mut *conn)
            .await?;
    PhysicalTableDataVersionId::init(id)?;

    let id = find_last_idl::<LogicalFunctionRunDB, LogicalFunctionRunId, FR>(&mut *conn).await?;
    LogicalFunctionRunId::init(id)?;

    let id = find_last_idl::<LogicalTableDataVersionDB, LogicalTableDataVersionId, DV>(&mut *conn)
        .await?;
    LogicalTableDataVersionId::init(id)?;

    let id = find_last_idl::<WorkerDB, WorkerId, WKR>(&mut *conn).await?;
    WorkerId::init(id)?;

    conn.commit()
        .await
        .map_err(ServiceDbError::CannotCommitTransaction)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Queries;
    use crate::test_utils::SeedEntry;
    use sea_query::ExprTrait;
    use sea_query::{Expr, Query};
    use ta_objects::and;
    use td_database::access::LockInstant;
    use td_database::sea_query::builder::SeaQueryExt;
    use td_error::TdError;
    use td_objects::dxo::execution::ExecutionDBIden;
    use td_objects::dxo::function::FunctionPatchInsertDB;
    use td_objects::dxo::function::{FunctionDBIden, FunctionRegister};
    use td_objects::dxo::physical_table_data_version::PhysicalTableDataVersionDBIden;
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::transaction::TransactionDBIden;
    use td_objects::types::basic::LogicalFunctionRunStatus;

    use td_objects::types::basic::ReuseFrozen;
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

    /// Patches take ids from the `FunctionVersionId` generator but land in
    /// `function_patches`, so the boot-time high-water mark must consider
    /// both tables — seeding from `functions` alone would re-issue the
    /// patch's id after a restart.
    #[td_test::test(db)]
    async fn test_last_function_version_id_covers_patches(db: TdPool) -> Result<(), TdError> {
        use crate::Queries;

        use td_objects::types::basic::{
            Description, FunctionRuntimeValues, FunctionStatus, Snippet,
        };

        let p = db.seed().test_project().await?;
        let collection = db
            .scoped(p.id)
            .seed()
            .collection(
                &CollectionName::try_from("collection")?,
                &UserId::try_from(ENCODED_ID_SYSTEM)?,
            )
            .await?;
        let create = FunctionRegister {
            child_functions: None,
            name: "f".try_into()?,
            description: "F".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "S".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![TableNameDto::try_from("t")?].into(),
            table_types: None,
            runtime_values: "V".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };
        let function = db
            .scoped(p.id)
            .seed()
            .function(&collection, &create)
            .await?;
        let patch = FunctionPatchInsertDB {
            id: FunctionVersionId::new(),
            parent_function_version_id: function.base.id.clone().into(),
            function_id: function.base.function_id,
            collection_id: function.base.collection_id,
            snippet: Snippet::try_from("S")?,
            bundle_id: BundleId::default(),
            runtime_values: FunctionRuntimeValues::try_from("V")?,
            description: Description::try_from("F")?,
            defined_by_id: UserId::try_from(ENCODED_ID_SYSTEM)?,
            status: FunctionStatus::Active,
        };
        db.scoped(p.id)
            .scd2()
            .open(&patch, LockInstant::asserted(*function.interval.valid_from))
            .await?;

        let head = find_last_idl::<FunctionDB, FunctionVersionId, FV>(&db).await?;
        assert_eq!(head, Some(function.base.id));
        assert!(Some(&patch.id) > head.as_ref());

        let mut conn = db.begin().await.unwrap();
        conn.scope_across_projects().await.unwrap();
        let last = last_function_version_id(&mut *conn).await?;
        assert_eq!(last, Some(patch.id));
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
        let create = FunctionRegister {
            child_functions: None,
            name: "f".try_into()?,
            description: "F".try_into()?,
            bundle_id: BundleId::default(),
            snippet: "S".try_into()?,
            decorator: Decorator::Publisher,
            connector: None,
            connection: None,
            base_path: None,
            dependencies: None,
            triggers: None,
            tables: vec![table_name.clone()].into(),
            table_types: None,
            runtime_values: "V".try_into()?,
            reuse_frozen_tables: ReuseFrozen::FALSE,
            enabled: None,
            workers: None,
        };

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
            .scd2()
            .fetch_first::<TableDB>(and!(&collection.id, TableName::try_from(table_name)?))
            .await?
            .expect("seeded table must exist");

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
        use sea_query::PostgresQueryBuilder;

        #[test]
        fn test_find_last_idl_execution() {
            let sql =
                find_last_idl_query::<ExecutionDB, ExecutionId>().to_string(PostgresQueryBuilder);
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
                .to_string(PostgresQueryBuilder);
            assert!(
                sql.contains(r#"FROM "functions""#),
                "should query functions: {sql}"
            );
            assert!(sql.contains("LIMIT 1"), "should limit 1: {sql}");
        }

        #[test]
        fn test_find_last_idl_transaction() {
            let sql = find_last_idl_query::<TransactionDB, TransactionId>()
                .to_string(PostgresQueryBuilder);
            assert!(
                sql.contains(r#"FROM "transactions""#),
                "should query transactions: {sql}"
            );
        }

        #[test]
        fn test_find_last_idl_table_data_version() {
            let sql =
                find_last_idl_query::<PhysicalTableDataVersionDB, PhysicalTableDataVersionId>()
                    .to_string(PostgresQueryBuilder);
            assert!(
                sql.contains("physical_table_data_version"),
                "should query physical_table_data_versions: {sql}"
            );
        }
    }
}
