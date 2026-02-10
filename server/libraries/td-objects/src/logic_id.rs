//
// Copyright 2026. Tabs Data Inc.
//

use crate::dxo::DataAccessObject;
use crate::sql::{DaoQueries, SelectBy};
use crate::types::basic::{
    DV, EP, ExecutionId, FV, FunctionVersionId, TX, TableDataVersionId, TransactionId,
};
use std::fmt::Debug;
use td_common::logic_id::KeyId;
use td_database::sql::DbPool;
use td_error::TdError;

#[td_type::Dao]
#[dao(sql_table = "executions", order_by = "id DESC")]
struct ExecutionIdDao {
    id: ExecutionId,
}

#[td_type::Dao]
#[dao(sql_table = "transactions", order_by = "id DESC")]
struct TransactionIdDao {
    id: TransactionId,
}

#[td_type::Dao]
#[dao(sql_table = "functions", order_by = "id DESC")]
struct FunctionVersionIdDao {
    id: FunctionVersionId,
}

#[td_type::Dao]
#[dao(sql_table = "table_data_versions", order_by = "id DESC")]
struct TableDataVersionIdDao {
    id: TableDataVersionId,
}

#[td_error::td_error]
enum LogicIdsError {
    #[error("Could not find last id for '{0}': {1}")]
    CouldNotFindLastId(String, Box<dyn std::error::Error + Send + Sync>),
}

async fn find_last_idl<DAO: Debug + DataAccessObject, ID, K: KeyId>(
    db: &DbPool,
    mapping: impl Fn(DAO) -> ID,
) -> Result<Option<ID>, TdError> {
    let res: Option<DAO> = DaoQueries::default()
        .select_by::<DAO>(&())?
        .push(" LIMIT ")
        .push_bind(1)
        .build_query_as()
        .fetch_optional(db)
        .await
        .map_err(|e| LogicIdsError::CouldNotFindLastId(K::key().to_string(), Box::new(e)))?;
    Ok(res.map(mapping))
}

pub async fn initialialize_logical_ids(db: &DbPool) -> Result<(), TdError> {
    let id = find_last_idl::<ExecutionIdDao, ExecutionId, EP>(db, |dao| dao.id).await?;
    ExecutionId::init(id);

    let id = find_last_idl::<TransactionIdDao, TransactionId, TX>(db, |dao| dao.id).await?;
    TransactionId::init(id);

    let id = find_last_idl::<FunctionVersionIdDao, FunctionVersionId, FV>(db, |dao| dao.id).await?;
    FunctionVersionId::init(id);

    let id =
        find_last_idl::<TableDataVersionIdDao, TableDataVersionId, DV>(db, |dao| dao.id).await?;
    TableDataVersionId::init(id);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dxo::function::FunctionRegister;
    use crate::dxo::table::TableDB;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_execution::seed_execution;
    use crate::test_utils::seed_function::seed_function;
    use crate::test_utils::seed_function_run::seed_function_run;
    use crate::test_utils::seed_table_data_version::seed_table_data_version;
    use crate::test_utils::seed_transaction::seed_transaction;
    use crate::types::basic::FunctionRunStatus;
    use crate::types::basic::TableName;
    use crate::types::basic::TransactionKey;

    use crate::types::basic::{
        BundleId, CollectionName, DV, Decorator, EP, ExecutionId, FV, FunctionVersionId, TX,
        TableDataVersionId, TableNameDto, TransactionId, UserId,
    };
    use sqlx;
    use td_error::TdError;
    use td_security::ENCODED_ID_SYSTEM;

    #[tokio::test]
    async fn test_no_logical_ids() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;

        let id = find_last_idl::<ExecutionIdDao, ExecutionId, EP>(&db, |dao| dao.id).await?;
        assert_eq!(id, None);

        let id = find_last_idl::<TransactionIdDao, TransactionId, TX>(&db, |dao| dao.id).await?;
        assert_eq!(id, None);

        let id =
            find_last_idl::<FunctionVersionIdDao, FunctionVersionId, FV>(&db, |dao| dao.id).await?;
        assert_eq!(id, None);

        let id = find_last_idl::<TableDataVersionIdDao, TableDataVersionId, DV>(&db, |dao| dao.id)
            .await?;
        assert_eq!(id, None);
        //
        Ok(())
    }

    #[tokio::test]
    async fn test_initial_logical_ids() -> Result<(), TdError> {
        let db = td_database::test_utils::db().await?;

        let collection = seed_collection(
            &db,
            &CollectionName::try_from("collection")?,
            &UserId::try_from(ENCODED_ID_SYSTEM)?,
        )
        .await;

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

        let function = seed_function(&db, &collection, &create).await;
        seed_function(&db, &collection, &create).await;
        seed_function(&db, &collection, &create).await;

        let execution = seed_execution(&db, &function).await;
        let transaction_key = TransactionKey::try_from("ANY")?;
        seed_transaction(&db, &execution, &transaction_key).await;
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

        let table_version = DaoQueries::default()
            .select_by::<TableDB>(&(collection.id, TableName::try_from(table_name).unwrap()))?
            .build_query_as()
            .fetch_one(&db)
            .await
            .unwrap();

        seed_table_data_version(
            &db,
            &collection,
            &execution,
            &transaction,
            &function_run,
            &table_version,
        )
        .await;

        let expected: Option<FunctionVersionId> =
            sqlx::query_scalar("SELECT MAX(id) FROM functions")
                .fetch_one(&db)
                .await
                .unwrap();
        let id =
            find_last_idl::<FunctionVersionIdDao, FunctionVersionId, FV>(&db, |dao| dao.id).await?;
        assert_eq!(id, expected);

        let expected: Option<ExecutionId> = sqlx::query_scalar("SELECT MAX(id) FROM executions")
            .fetch_one(&db)
            .await
            .unwrap();
        let id = find_last_idl::<ExecutionIdDao, ExecutionId, EP>(&db, |dao| dao.id).await?;
        assert_eq!(id, expected);

        let expected: Option<TransactionId> =
            sqlx::query_scalar("SELECT MAX(id) FROM transactions")
                .fetch_one(&db)
                .await
                .unwrap();
        let id = find_last_idl::<TransactionIdDao, TransactionId, TX>(&db, |dao| dao.id).await?;
        assert_eq!(id, expected);

        let expected: Option<TableDataVersionId> =
            sqlx::query_scalar("SELECT MAX(id) FROM table_data_versions")
                .fetch_one(&db)
                .await
                .unwrap();
        let id = find_last_idl::<TableDataVersionIdDao, TableDataVersionId, DV>(&db, |dao| dao.id)
            .await?;
        assert_eq!(id, expected);

        Ok(())
    }
}
