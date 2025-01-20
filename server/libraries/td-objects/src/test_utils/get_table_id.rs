//
// Copyright 2025 Tabs Data Inc.
//

use crate::datasets::dao::DsTable;
use td_common::id::Id;
use td_database::sql::DbPool;

pub async fn get_table_id(db: &DbPool, function_id: &Id, table_name: &str) -> Id {
    let mut conn = db.begin().await.unwrap();

    const SELECT_TABLE_ID: &str = "SELECT * FROM ds_tables WHERE function_id = ?1 AND name = ?2";

    let table: DsTable = sqlx::query_as(SELECT_TABLE_ID)
        .bind(function_id.to_string())
        .bind(table_name)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    Id::try_from(table.id()).unwrap()
}

#[cfg(test)]
mod tests {
    use crate::crudl::select_by;
    use crate::datasets::dao::DsTable;
    use crate::test_utils::get_table_id::get_table_id;
    use crate::test_utils::seed_collection::seed_collection;
    use crate::test_utils::seed_dataset::seed_dataset;
    use td_common::id::Id;

    #[tokio::test]
    async fn test_get_table_id() {
        let db = td_database::test_utils::db().await.unwrap();
        let collection_id = seed_collection(&db, None, "collection").await;

        let (_dataset_id, function_id) = seed_dataset(
            &db,
            None,
            &collection_id,
            "dataset",
            &["table_name"],
            &[],
            &[],
            "hash",
        )
        .await;

        let table_id = get_table_id(&db, &function_id, "table_name").await;
        let expected_table: DsTable = select_by(
            &mut db.acquire().await.unwrap(),
            "SELECT * FROM ds_tables WHERE id = ?1",
            &table_id.to_string(),
        )
        .await
        .unwrap();

        assert_eq!(table_id, Id::try_from(expected_table.id()).unwrap());
    }
}
