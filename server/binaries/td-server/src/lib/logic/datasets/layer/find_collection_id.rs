//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use td_error::TdError;
use td_objects::dlo::{CollectionId, CollectionName};
use td_objects::entity_finder::collections::CollectionWithNamesFinder;
use td_objects::entity_finder::EntityFinderError;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn find_collection_id(
    Connection(connection): Connection,
    Input(collection_name): Input<CollectionName>,
) -> Result<CollectionId, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;
    match CollectionWithNamesFinder::default()
        .find_by_name(&mut *conn, &collection_name)
        .await
    {
        Ok(collection) => Ok(CollectionId::new(collection.id())),
        Err(EntityFinderError::NameNotFound(name)) => Err(DatasetError::CollectionNotFound(name))?,
        Err(err) => Err(err)?,
    }
}
