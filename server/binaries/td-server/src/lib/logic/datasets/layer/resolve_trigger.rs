//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use crate::logic::datasets::layer::common_functions::uri_names_to_uri_ids;
use std::ops::Deref;
use td_common::error::TdError;
use td_common::uri::TdUri;
use td_objects::datasets::dlo::*;
use td_objects::datasets::dto::*;
use td_objects::dlo::{CollectionName, DatasetName};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn resolve_trigger(
    Connection(connection): Connection,
    Input(collection_name): Input<CollectionName>,
    Input(dataset_name): Input<DatasetName>,
    Input(dataset): Input<DatasetWrite>,
) -> Result<FunctionTriggers, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let mut trigger_uris = vec![];
    for trigger_uri in dataset.trigger_by().as_ref().unwrap_or(&vec![]).iter() {
        let trigger_uri = TdUri::parse(&collection_name, trigger_uri.as_str())
            .map_err(DatasetError::InvalidTriggerUri)?;
        if !trigger_uri.is_dataset() {
            Err(DatasetError::TriggerUriMustBeADatasetUri(
                trigger_uri.to_string(),
            ))?;
        }
        if trigger_uri.is_versioned() {
            Err(DatasetError::TriggerUriCannotHaveVersions(
                trigger_uri.to_string(),
            ))?;
        }
        let dataset_name = dataset_name.deref().as_str();
        let dataset_uri =
            TdUri::parse(&collection_name, format!("td://{}", dataset_name).as_str())?;
        if trigger_uri == dataset_uri {
            Err(DatasetError::TriggerCannotBeSelf)?;
        }
        let uri = uri_names_to_uri_ids(conn, &[trigger_uri])
            .await?
            .pop()
            .unwrap();
        trigger_uris.push(uri);
    }

    Ok(FunctionTriggers::new(trigger_uris))
}
