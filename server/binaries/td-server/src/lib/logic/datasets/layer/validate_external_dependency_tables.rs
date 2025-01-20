//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use getset::Getters;
use sqlx::FromRow;
use std::collections::{HashMap, HashSet};
use td_common::error::TdError;
use td_common::str::comma_separated;
use td_common::uri::TdUriNameId;
use td_database::sql::create_bindings_literal;
use td_objects::crudl::handle_sql_err;
use td_objects::datasets::dlo::*;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

#[derive(Debug, FromRow, Getters)]
#[getset(get = "pub")]
struct Table {
    name: String,
    dataset_id: String,
}

fn assert_tables(
    uris_map: &HashMap<String, Vec<&TdUriNameId>>,
    dataset_tables_map: &HashMap<String, Vec<Table>>,
) -> Result<(), TdError> {
    let mut not_found = Vec::with_capacity(16);
    for (dataset_id, uris) in uris_map {
        if let Some(tables) = dataset_tables_map.get(dataset_id) {
            let table_names: HashSet<_> = tables.iter().map(|t| t.name().as_str()).collect();
            for uri in uris {
                if !table_names.contains(uri.with_ids().table().unwrap()) {
                    not_found.push(uri.with_names().to_string());
                }
            }
        } else {
            not_found.push(uris[0].with_names().to_string());
        }
    }
    if !not_found.is_empty() {
        Err(DatasetError::CouldNotFindTables(comma_separated(
            &not_found,
        )))?
    } else {
        Ok(())
    }
}

fn group_by_dataset(deps: &FunctionDependencies) -> HashMap<String, Vec<&TdUriNameId>> {
    let mut map: HashMap<String, Vec<&TdUriNameId>> = HashMap::new();
    deps.external().iter().for_each(|uri_name_id| {
        map.entry(uri_name_id.with_ids().dataset().to_string())
            .or_default()
            .push(uri_name_id);
    });
    map
}

pub async fn validate_external_dependency_tables(
    Connection(connection): Connection,
    Input(deps): Input<FunctionDependencies>,
) -> Result<(), TdError> {
    let by_dataset = group_by_dataset(&deps);
    let dataset_ids: Vec<_> = by_dataset.keys().collect();

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_TABLES_IN_DATASETS: &str = r#"
            SELECT
                name,
                dataset_id
            FROM ds_current_tables
            WHERE dataset_id IN ({})
        "#;

    let query =
        SELECT_TABLES_IN_DATASETS.replace("{}", &create_bindings_literal(0, dataset_ids.len()));

    let mut query_as = sqlx::query_as(&query);
    for key in dataset_ids.iter() {
        query_as = query_as.bind(key);
    }

    let res = query_as.fetch_all(conn).await.map_err(handle_sql_err)?;

    let mut map: HashMap<String, Vec<Table>> = HashMap::new();
    res.into_iter().for_each(|table: Table| {
        map.entry(table.dataset_id.to_string())
            .or_default()
            .push(table);
    });

    assert_tables(&by_dataset, &map)
}
