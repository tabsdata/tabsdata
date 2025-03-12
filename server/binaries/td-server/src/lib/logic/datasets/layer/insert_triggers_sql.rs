//
// Copyright 2024 Tabs Data Inc.
//

use td_error::TdError;
use td_objects::crudl::handle_sql_err;
use td_objects::datasets::dao::DsTrigger;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn insert_triggers_sql(
    Connection(connection): Connection,
    Input(triggers): Input<Vec<DsTrigger>>,
) -> Result<(), TdError> {
    const INSERT_SQL: &str = r#"
        INSERT INTO ds_triggers (
            id,
            collection_id,
            dataset_id,
            function_id,

            trigger_collection_id,
            trigger_dataset_id
        )
        VALUES
            (?1, ?2, ?3, ?4, ?5, ?6)
    "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    for dep in triggers.iter() {
        sqlx::query(INSERT_SQL)
            .bind(dep.id())
            .bind(dep.collection_id())
            .bind(dep.dataset_id())
            .bind(dep.function_id())
            .bind(dep.trigger_collection_id())
            .bind(dep.trigger_dataset_id())
            .execute(&mut *conn)
            .await
            .map_err(handle_sql_err)?;
    }
    Ok(())
}
