//
// Copyright 2024 Tabs Data Inc.
//

use crate::logic::datasets::error::DatasetError;
use sqlx::SqliteConnection;
use std::collections::HashMap;
use td_common::str::comma_separated;
use td_common::uri::{TdUri, TdUriNameId};
use td_error::TdError;
use td_objects::entity_finder::collections::CollectionWithNamesFinder;
use td_objects::entity_finder::datasets::DatasetFinder;

fn group_by_collection(uris: &[TdUri]) -> HashMap<String, Vec<&TdUri>> {
    let mut map: HashMap<String, Vec<&TdUri>> = HashMap::new();
    uris.iter().for_each(|uri| {
        map.entry(uri.collection().to_string())
            .or_default()
            .push(uri);
    });
    map
}

pub async fn uri_names_to_uri_ids_for_collection(
    conn: &mut SqliteConnection,
    collection_id: &str,
    uris: &[&TdUri],
) -> Result<Vec<TdUriNameId>, TdError> {
    let dataset_finder = DatasetFinder::default();
    let dataset_names: Vec<_> = uris.iter().map(|uri| uri.dataset()).collect();
    let dataset_ids = dataset_finder
        .find_ids(&mut *conn, collection_id, dataset_names.as_slice())
        .await?;
    let mut uri_ids = Vec::with_capacity(uris.len());
    let mut not_found = Vec::with_capacity(uris.len());
    for (idx, id) in dataset_ids.iter().enumerate() {
        match id {
            Some(dataset_id) => uri_ids.push(TdUriNameId::new(
                uris[idx].clone(),
                uris[idx].replace(collection_id, dataset_id),
            )),
            None => {
                not_found.push(uris[idx].to_string());
            }
        }
    }
    if !not_found.is_empty() {
        Err(DatasetError::CouldNotFindDatasets(comma_separated(
            &not_found,
        )))?;
    }
    Ok(uri_ids)
}

pub async fn uri_names_to_uri_ids(
    conn: &mut SqliteConnection,
    uris: &[TdUri],
) -> Result<Vec<TdUriNameId>, TdError> {
    let by_collections = group_by_collection(uris);
    let collections: Vec<_> = by_collections.keys().map(|k| k.as_str()).collect();
    let collection_ids = CollectionWithNamesFinder::default()
        .find_ids(&mut *conn, collections.as_slice())
        .await?;
    let mut not_found = Vec::with_capacity(collections.len());
    let mut map = HashMap::new();
    for (idx, name) in collections.into_iter().enumerate() {
        if collection_ids[idx].is_none() {
            not_found.push(by_collections[name][0].to_string());
        } else {
            map.insert(name, collection_ids[idx].as_ref().unwrap());
        }
    }
    if !not_found.is_empty() {
        Err(DatasetError::CouldNotFindCollections(comma_separated(
            &not_found,
        )))?;
    }
    let mut uri_ids = Vec::with_capacity(uris.len());
    for (collection_name, uris) in &by_collections {
        let collection_id: &String = map[collection_name.as_str()];
        let collection_uri_ids =
            uri_names_to_uri_ids_for_collection(conn, collection_id, uris.as_slice()).await?;
        uri_ids.extend(collection_uri_ids);
    }
    Ok(uri_ids)
}

pub fn resolve_location(location: &Option<String>) -> String {
    //A more sophisticated mapping could be done here. based on some config logical names.
    location
        .as_ref()
        .map(String::as_str)
        .unwrap_or_else(|| "/default")
        .to_string()
}

#[cfg(test)]
mod tests {
    use td_common::uri::TdUri;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;

    #[test]
    fn test_group_by_collection() {
        let uris = vec![
            TdUri::parse("ds", "td:///ds0/d0/t0@HEAD").unwrap(),
            TdUri::parse("ds", "td:///ds1/d1/t0@HEAD").unwrap(),
            TdUri::parse("ds", "td:///ds1/d1/t1@HEAD").unwrap(),
        ];
        let map = super::group_by_collection(&uris);
        assert_eq!(map.len(), 2);
        assert_eq!(map["ds0"], vec![&uris[0]]);
        assert_eq!(map["ds1"], vec![&uris[1], &uris[2]]);
    }

    #[tokio::test]
    async fn test_uri_names_to_uri_ids() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (dataset_id, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0"],
            &[],
            &[],
            "hash",
        )
        .await;

        let mut conn = db.acquire().await.unwrap();

        let uris = vec![TdUri::parse("ds", "td:///ds0/d0/t0@HEAD").unwrap()];
        let uri_ids = super::uri_names_to_uri_ids(&mut conn, &uris).await.unwrap();
        assert_eq!(uri_ids.len(), 1);
        assert_eq!(
            uri_ids[0].with_ids(),
            &uris[0].replace(&collection_id.to_string(), &dataset_id.to_string())
        );
    }

    #[test]
    fn test_resolve_location() {
        assert_eq!(super::resolve_location(&None), "/default");
        assert_eq!(super::resolve_location(&Some("/".to_string())), "/");
        assert_eq!(super::resolve_location(&Some("/a".to_string())), "/a");
    }
}
