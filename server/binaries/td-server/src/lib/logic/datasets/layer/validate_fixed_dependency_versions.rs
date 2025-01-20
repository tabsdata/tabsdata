//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use itertools::Itertools;
use sqlx::FromRow;
use std::collections::{HashMap, HashSet};
use td_common::error::TdError;
use td_common::id::Id;
use td_common::uri::{TdUri, TdUriNameId, Version, Versions};
use td_database::sql::create_bindings_literal;
use td_objects::crudl::handle_sql_err;
use td_objects::datasets::dlo::*;
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection};

// all URIs that have fixed versions are converted to single version URIs with just the fixed
// versions. If a URI has multiple fixed versions a single version URI is created for each fixed
// version
fn extract_fixed_version_deps(deps: &FunctionDependencies) -> HashSet<TdUriNameId> {
    deps.external()
        .iter()
        .chain(deps.to_self().iter())
        .flat_map(|uri_name_id| {
            let ids = uri_name_id.with_ids().versions().fixed();
            ids.into_iter()
                .map(|id| uri_name_id.replace_versions(Versions::Single(Version::Fixed(id))))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn as_dataset_id_name_map(uris: &HashSet<TdUriNameId>) -> HashMap<TdUri, &TdUri> {
    uris.iter()
        .map(|uri_name_id| {
            (
                uri_name_id.with_ids().dataset_uri(),
                uri_name_id.with_names(),
            )
        })
        .collect()
}

fn extract_versions(uris: &HashSet<TdUriNameId>) -> Result<Vec<String>, DatasetError> {
    let mut versions = Vec::with_capacity(uris.len());
    for uri in uris.iter() {
        match uri.with_ids().versions() {
            Versions::Single(version) => versions.push(version.to_string()),
            _ => {
                // this cannot happen as received URIs are all single version
                return Err(DatasetError::UriShouldHaveASingleVersion(
                    uri.with_names().to_string(),
                ));
            }
        }
    }
    Ok(versions)
}

#[derive(Debug, Clone, FromRow)]
struct CollectionIdDatasetIdVersion {
    collection_id: String,
    dataset_id: String,
    id: String,
}

impl CollectionIdDatasetIdVersion {
    fn to_uri(&self) -> TdUri {
        TdUri::new_with_ids(
            Id::try_from(&self.collection_id).unwrap(),
            Id::try_from(&self.dataset_id).unwrap(),
            None,
            Some(Versions::Single(Version::Fixed(
                Id::try_from(&self.id).unwrap(),
            ))),
        )
    }
}

pub async fn validate_fixed_dependency_versions(
    Connection(connection): Connection,
    Input(deps): Input<FunctionDependencies>,
) -> Result<(), TdError> {
    let fixed_version_deps = extract_fixed_version_deps(&deps);
    let fixed_version_ids = extract_versions(&fixed_version_deps)?;

    let mut conn = connection.lock().await;
    let conn = conn.get_mut_connection()?;

    const SELECT_FIXED_VERSIONS_DS_DATA: &str = r#"
            SELECT
                collection_id, dataset_id, id
            FROM ds_data_versions
            WHERE id IN ({})
        "#;

    let query = SELECT_FIXED_VERSIONS_DS_DATA
        .replace("{}", &create_bindings_literal(0, fixed_version_ids.len()));

    let mut query_as = sqlx::query_as(&query);
    for key in fixed_version_ids {
        query_as = query_as.bind(key);
    }

    let found_versions: Vec<CollectionIdDatasetIdVersion> =
        query_as.fetch_all(conn).await.map_err(handle_sql_err)?;

    let mut id_names_map = as_dataset_id_name_map(&fixed_version_deps);

    found_versions
        .into_iter()
        .map(|v| v.to_uri())
        .for_each(|uri| {
            id_names_map.remove(&uri);
        });
    if !id_names_map.is_empty() {
        let fix_versions_not_found = id_names_map.values().map(|uri| uri.to_string()).join(", ");
        return Err(DatasetError::FixedVersionDependenciesNotFound(
            fix_versions_not_found,
        ))?;
    }
    Ok(())
}
