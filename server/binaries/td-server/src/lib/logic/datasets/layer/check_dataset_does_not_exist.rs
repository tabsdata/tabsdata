//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use td_error::TdError;
use td_objects::dlo::{CollectionId, CollectionName, DatasetName};
use td_objects::entity_finder::datasets::DatasetFinder;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn check_dataset_does_not_exist(
    Connection(connection): Connection,
    Input(collection_name): Input<CollectionName>,
    Input(collection_id): Input<CollectionId>,
    Input(dataset_name): Input<DatasetName>,
) -> Result<(), TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let datasets = DatasetFinder::default()
        .find_by_names(&mut *conn, &collection_id, &[&dataset_name])
        .await?;

    match datasets[0] {
        None => Ok(()),
        Some(_) => Err(DatasetError::DatasetAlreadyExists(
            (&collection_name as &str).to_string(),
            (&dataset_name as &str).to_string(),
        ))?,
    }
}
