//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::datasets::dao::*;
use td_objects::dlo::{CollectionId, DatasetName};
use td_objects::entity_finder::datasets::DatasetFinder;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn select_dataset_with_names(
    Connection(connection): Connection,
    Input(dataset_name): Input<DatasetName>,
    Input(collection_id): Input<CollectionId>,
) -> Result<DatasetWithNames, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    Ok(DatasetFinder::default()
        .find_by_name(&mut *conn, &collection_id, &dataset_name)
        .await?)
}
