//
//  Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_execution::link::{DataGraph, DataLink, Graph};
use td_objects::crudl::handle_sql_err;
use td_objects::dlo::DatasetId;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

// upstream and downstream
pub async fn data_dependencies_graph_sql(
    Connection(connection): Connection,
    Input(dataset): Input<DatasetId>,
) -> Result<DataGraph, TdError> {
    const DATA_DEPENDENCIES_GRAPH_SQL: &str = r#"
        WITH RECURSIVE whole_deps_graph(dataset_id) AS (
            -- direct
            SELECT ? AS dataset_id
            UNION
            -- upstream
            SELECT d.table_dataset_id
                FROM ds_current_dependencies d
                INNER JOIN whole_deps_graph r ON d.dataset_id = r.dataset_id
            UNION
            -- downstream
            SELECT d.dataset_id
                FROM ds_current_dependencies d
                INNER JOIN whole_deps_graph r ON d.table_dataset_id = r.dataset_id
        )
        SELECT
            table_collection_id as source_collection_id,
            table_dataset_id as source_dataset_id,
            table_name as source_table,
            pos as source_pos,
            table_versions as source_versions,
            collection_id as target_collection_id,
            dataset_id as target_dataset_id
        FROM ds_current_dependencies
        WHERE table_dataset_id IN (SELECT dataset_id FROM whole_deps_graph)
           OR dataset_id IN (SELECT dataset_id FROM whole_deps_graph)
    "#;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let data_links: Vec<DataLink> = sqlx::query_as(DATA_DEPENDENCIES_GRAPH_SQL)
        .bind(dataset.as_str())
        .fetch_all(conn)
        .await
        .map_err(handle_sql_err)?;
    Ok(DataGraph(Graph(data_links)))
}

#[cfg(test)]
mod tests {
    use crate::logic::collections::service::tests::create_test_collections;
    use crate::logic::datasets::layer::data_dependencies_graph_sql::data_dependencies_graph_sql;
    use crate::logic::datasets::service::create_dataset::CreateDatasetService;
    use crate::logic::users::service::create_user::tests::create_test_users;
    use td_objects::crudl::RequestContext;
    use td_objects::datasets::dto::DatasetWrite;
    use td_objects::dlo::{CollectionName, DatasetId};
    use td_tower::ctx_service::RawOneshot;
    use td_tower::extractors::*;

    #[tokio::test]
    async fn test_data_dependencies_graph_sql() {
        let db = td_database::test_utils::db().await.unwrap();
        let users = create_test_users(&db, None, "u", 1, true).await;
        let collection = create_test_collections(&db, None, "ds", 1).await;

        let request = RequestContext::with(users[0].id(), "r", false)
            .await
            .create(
                CollectionName::new(collection[0].name()),
                DatasetWrite {
                    name: "d0".to_string(),
                    description: "D0".to_string(),
                    data_location: None,
                    bundle_hash: "hash".to_string(),
                    tables: vec!["t0".to_string()],
                    dependencies: vec![],
                    trigger_by: None,
                    function_snippet: None,
                },
            );

        let service = CreateDatasetService::new(db.clone()).service().await;
        let d0 = service.raw_oneshot(request).await.unwrap();
        println!("{:#?}", d0);

        let request = RequestContext::with(users[0].id(), "r", false)
            .await
            .create(
                CollectionName::new(collection[0].name()),
                DatasetWrite {
                    name: "d1".to_string(),
                    description: "D1".to_string(),
                    data_location: None,
                    bundle_hash: "hash".to_string(),
                    tables: vec!["t1".to_string()],
                    dependencies: vec!["t0@HEAD".to_string()],
                    trigger_by: None,
                    function_snippet: None,
                },
            );

        let service = CreateDatasetService::new(db.clone()).service().await;
        let d1 = service.raw_oneshot(request).await.unwrap();
        println!("{:#?}", d1);

        let request = RequestContext::with(users[0].id(), "r", false)
            .await
            .create(
                CollectionName::new(collection[0].name()),
                DatasetWrite {
                    name: "d2".to_string(),
                    description: "D2".to_string(),
                    data_location: None,
                    bundle_hash: "hash".to_string(),
                    tables: vec!["t2".to_string()],
                    dependencies: vec!["t0@HEAD".to_string()],
                    trigger_by: Some(vec!["t1".to_string()]),
                    function_snippet: Some("def fn():\n".to_string()),
                },
            );

        let service = CreateDatasetService::new(db.clone()).service().await;
        let d2 = service.raw_oneshot(request).await.unwrap();
        println!("{:#?}", d2);

        let request = RequestContext::with(users[0].id(), "r", false)
            .await
            .create(
                CollectionName::new(collection[0].name()),
                DatasetWrite {
                    name: "d3".to_string(),
                    description: "D3".to_string(),
                    data_location: None,
                    bundle_hash: "hash".to_string(),
                    tables: vec!["t3".to_string()],
                    dependencies: vec![
                        "t1@HEAD~10..HEAD".to_string(),
                        "t2@HEAD~10..HEAD".to_string(),
                    ],
                    trigger_by: Some(vec!["ds0/t2".to_string()]),
                    function_snippet: Some("def fn():\n".to_string()),
                },
            );

        let service = CreateDatasetService::new(db.clone()).service().await;
        let d3 = service.raw_oneshot(request).await.unwrap();
        println!("{:#?}", d3);

        let connection = db.acquire().await.unwrap();
        let connection = ConnectionType::PoolConnection(connection).into();
        let connection = Connection::new(connection);

        let dataset = Input::new(DatasetId(d0.id().to_string()));
        let response = data_dependencies_graph_sql(connection.clone(), dataset)
            .await
            .unwrap();
        println!("d0 - {:#?}", response);

        let dataset = Input::new(DatasetId(d1.id().to_string()));
        let response = data_dependencies_graph_sql(connection.clone(), dataset)
            .await
            .unwrap();
        println!("d1 - {:#?}", response);

        let dataset = Input::new(DatasetId(d2.id().to_string()));
        let response = data_dependencies_graph_sql(connection.clone(), dataset)
            .await
            .unwrap();
        println!("d2 - {:#?}", response);

        let dataset = Input::new(DatasetId(d3.id().to_string()));
        let response = data_dependencies_graph_sql(connection.clone(), dataset)
            .await
            .unwrap();
        println!("d3 - {:#?}", response);
    }
}
