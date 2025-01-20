//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_objects::dlo::{CollectionId, DatasetId, DatasetName};
use td_objects::entity_finder::EntityFinderError;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

use crate::logic::datasets::error::DatasetError;
use td_objects::entity_finder::datasets::DatasetFinder;

pub async fn find_dataset_id(
    Connection(connection): Connection,
    Input(collection_id): Input<CollectionId>,
    Input(dataset_name): Input<DatasetName>,
) -> Result<DatasetId, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;
    match DatasetFinder::default()
        .find_by_name(&mut *conn, &collection_id, &dataset_name)
        .await
    {
        Ok(dataset) => Ok(DatasetId::new(dataset.id())),
        Err(EntityFinderError::NameNotFound(name)) => Err(DatasetError::DatasetNotFound(name))?,
        Err(err) => Err(err)?,
    }
}
