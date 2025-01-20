//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use crate::logic::datasets::layer::common_functions;
use std::collections::BTreeMap;
use td_common::error::TdError;
use td_common::str::comma_separated;
use td_common::uri::{TdUri, TdUriNameId};
use td_objects::datasets::dlo::*;
use td_objects::datasets::dto::*;
use td_objects::dlo::{CollectionId, CollectionName, DatasetId, DatasetName};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

pub async fn resolve_dependencies(
    Connection(connection): Connection,
    Input(collection_name): Input<CollectionName>,
    Input(collection_id): Input<CollectionId>,
    Input(dataset_name): Input<DatasetName>,
    Input(dataset_id): Input<DatasetId>,
    Input(dataset): Input<DatasetWrite>,
) -> Result<FunctionDependencies, TdError> {
    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    let self_uri = TdUri::parse(
        &collection_name,
        format!("td://{}", dataset_name.as_str()).as_str(),
    )?;

    // Using BTreeMap to keep order of dependencies
    let mut self_uris = BTreeMap::new();
    let mut external_uris = BTreeMap::new();
    let mut invalid_uris = Vec::with_capacity(external_uris.len());
    let mut non_table_uris = Vec::with_capacity(external_uris.len());
    for (index, dep) in dataset.dependencies().iter().enumerate() {
        match TdUri::parse(&collection_name, dep.as_str()) {
            Ok(uri) => {
                if !uri.is_table() {
                    non_table_uris.push(uri.to_string());
                } else if uri.dataset_uri().without_versions() == self_uri {
                    self_uris.insert(index as i64, uri.versioned());
                } else {
                    external_uris.insert(index as i64, uri.versioned());
                }
            }
            Err(_) => {
                invalid_uris.push(dep.to_string());
            }
        }
    }
    if !invalid_uris.is_empty() {
        Err(DatasetError::InvalidDependencyUris(comma_separated(
            &invalid_uris,
        )))?;
    }
    if !non_table_uris.is_empty() {
        Err(DatasetError::NonTableDependencyUris(comma_separated(
            &non_table_uris,
        )))?;
    }

    let self_uris = self_uris
        .into_iter()
        .map(|(i, uri)| (i, TdUriNameId::from(&uri, &collection_id, &dataset_id)))
        .collect();

    let (indexes, external_uris): (Vec<_>, Vec<_>) = external_uris.into_iter().unzip();
    let external_uris =
        common_functions::uri_names_to_uri_ids(conn, external_uris.as_slice()).await?;
    let external_uris = indexes.into_iter().zip(external_uris).collect();

    Ok(FunctionDependencies::new(self_uris, external_uris))
}
