//
// Copyright 2025 Tabs Data Inc.
//

use itertools::{Either, Itertools};
use sqlx::SqliteConnection;
use std::collections::HashMap;
use std::fmt::Debug;
use td_error::display_vec::DisplayVec;
use td_error::{TdError, display_vec, td_error};
use td_objects::crudl::handle_sql_err;
use td_objects::sql::DaoQueries;
use td_objects::sql::cte::TableQueries;
use td_objects::types::basic::{TableDataVersionId, TableId, TriggeredOn};
use td_objects::types::execution::ActiveTableDataVersionDB;
use td_objects::types::table_ref::{Version, Versions};

#[td_error]
enum VersionResolverError {
    #[error("Could not find the following table data versions: [{0}]")]
    FixedTableDataVersionsNotFound(DisplayVec<TableDataVersionId>) = 0,
}

/// Struct to resolve table data versions. It will resolve relative and fixed versions, using
/// `TableDataVersionDB` Dao, for any triggered_on.
pub struct VersionResolver<'a> {
    table_id: &'a TableId,
    versions: &'a Versions,
    triggered_on: &'a TriggeredOn,
}

impl<'a> VersionResolver<'a> {
    pub fn new(
        table_id: &'a TableId,
        versions: &'a Versions,
        triggered_on: &'a TriggeredOn,
    ) -> Self {
        Self {
            table_id,
            versions,
            triggered_on,
        }
    }

    /// Main resolve function. Note that the return type is a `Vec<Option<TableDataVersionDB>>`,
    /// because versions not existing is not necessarily an error, and a single `Versions` can
    /// resolve to multiple versions (i.e. List).
    pub async fn resolve(
        &self,
        queries: &DaoQueries,
        conn: &mut SqliteConnection,
    ) -> Result<Vec<Option<ActiveTableDataVersionDB>>, TdError> {
        let (table_id, versions, triggered_on) = (self.table_id, self.versions, self.triggered_on);
        let versions = match versions {
            Versions::None => {
                let v = queries
                    .select_table_data_versions_at::<ActiveTableDataVersionDB>(
                        Some(triggered_on),
                        None,
                        table_id,
                        versions,
                    )?
                    .build_query_as()
                    .fetch_optional(&mut *conn)
                    .await
                    .map_err(handle_sql_err)?;
                vec![v]
            }
            Versions::Single(version) => match version {
                Version::Fixed(version) => {
                    // We fail if fixed not found.
                    let v = queries
                        .select_table_data_versions_at::<ActiveTableDataVersionDB>(
                            Some(triggered_on),
                            None,
                            table_id,
                            versions,
                        )?
                        .build_query_as()
                        .fetch_one(&mut *conn)
                        .await
                        .map_err(|_| {
                            VersionResolverError::FixedTableDataVersionsNotFound(display_vec![
                                *version,
                            ])
                        })?;
                    vec![Some(v)]
                }
                Version::Head(_) => {
                    let v = queries
                        .select_table_data_versions_at::<ActiveTableDataVersionDB>(
                            Some(triggered_on),
                            None,
                            table_id,
                            versions,
                        )?
                        .build_query_as()
                        .fetch_optional(&mut *conn)
                        .await
                        .map_err(handle_sql_err)?;
                    vec![v]
                }
            },
            Versions::List(versions) => {
                // Split versions into fixed and head versions so we only do 2 queries.
                let (fixed_versions, head_versions) = versions.iter().cloned().enumerate().fold(
                    (HashMap::new(), HashMap::new()),
                    |(mut fixed, mut head), (index, version)| {
                        match version {
                            Version::Fixed(_) => fixed.insert(index, version),
                            Version::Head(_) => head.insert(index, version),
                        };
                        (fixed, head)
                    },
                );

                // Each fixed versions is queried by its id to find if it exists or not.
                let fixed_versions = if !fixed_versions.is_empty() {
                    let version_list = fixed_versions.values().cloned().collect();
                    let found: Vec<ActiveTableDataVersionDB> = queries
                        .select_table_data_versions_at::<ActiveTableDataVersionDB>(
                            Some(triggered_on),
                            None,
                            table_id,
                            &Versions::List(version_list),
                        )?
                        .build_query_as()
                        .fetch_all(&mut *conn)
                        .await
                        .map_err(handle_sql_err)?;
                    let found: HashMap<_, _> = found.into_iter().map(|v| (*v.id(), v)).collect();

                    let (absolute_versions, not_found): (HashMap<_, _>, Vec<_>) =
                        fixed_versions.iter().partition_map(|(i, v)| {
                            let id = match v {
                                Version::Fixed(id) => id,
                                _ => unreachable!(),
                            };
                            match found.get(id) {
                                Some(v) => Either::Left((i, Some(v.clone()))),
                                None => Either::Right(*id),
                            }
                        });

                    if !not_found.is_empty() {
                        Err(VersionResolverError::FixedTableDataVersionsNotFound(
                            not_found.into(),
                        ))?
                    }

                    absolute_versions
                } else {
                    HashMap::new()
                };

                // Head versions are queried as a range, so we can map them back by position.
                let minmax = head_versions
                    .values()
                    .minmax()
                    .into_option()
                    .map(|(min, max)| (min.clone(), max.clone()));
                let head_versions = if let Some((min, max)) = minmax {
                    let mut found = queries
                        .select_table_data_versions_at::<ActiveTableDataVersionDB>(
                            Some(triggered_on),
                            None,
                            table_id,
                            &Versions::Range(min, max), // range always older to newer
                        )?
                        .build_query_as()
                        .fetch_all(&mut *conn)
                        .await
                        .map_err(handle_sql_err)?;

                    // We know that this vec has all the versions in the range at most, so we can
                    // get the position by the index of the versions (or None if range is too short).
                    found.reverse(); // reverse to sort newer to older
                    let absolute_versions: HashMap<_, _> = head_versions
                        .iter()
                        .map(|(i, v)| {
                            let back = match v {
                                Version::Head(back) => back,
                                _ => unreachable!(),
                            };

                            (i, found.get(-back as usize).cloned())
                        })
                        .collect();

                    absolute_versions
                } else {
                    HashMap::new()
                };

                // Merge fixed and head versions and convert back to Vec<_> sorting it by position.
                let sorted_versions: Vec<_> = fixed_versions
                    .into_iter()
                    .chain(head_versions)
                    .collect::<HashMap<_, _>>()
                    .into_iter()
                    .sorted_by_key(|&(index, _)| index)
                    .map(|(_, version)| version.clone())
                    .collect();

                sorted_versions
            }
            Versions::Range(from, to) => {
                // Check ranges with ids exist and get them as relative.
                let mut not_found = vec![];
                let relative_from = match from {
                    Version::Head(back) => Some(*back as i32),
                    Version::Fixed(id) => {
                        let relative: Option<i32> = queries
                            .find_relative_offset::<ActiveTableDataVersionDB>(
                                Some(triggered_on),
                                None,
                                table_id,
                                &Versions::Single(from.clone()),
                            )?
                            .build_query_scalar()
                            .fetch_optional(&mut *conn)
                            .await
                            .map_err(handle_sql_err)?;

                        match relative {
                            Some(relative) => Some(-relative + 1),
                            None => {
                                not_found.push(*id);
                                None
                            }
                        }
                    }
                };
                let relative_to = match to {
                    Version::Head(back) => Some(*back as i32),
                    Version::Fixed(id) => {
                        let relative: Option<i32> = queries
                            .find_relative_offset::<ActiveTableDataVersionDB>(
                                Some(triggered_on),
                                None,
                                table_id,
                                &Versions::Single(to.clone()),
                            )?
                            .build_query_scalar()
                            .fetch_optional(&mut *conn)
                            .await
                            .map_err(handle_sql_err)?;

                        match relative {
                            Some(relative) => Some(-relative + 1),
                            None => {
                                not_found.push(*id);
                                None
                            }
                        }
                    }
                };
                if !not_found.is_empty() {
                    Err(VersionResolverError::FixedTableDataVersionsNotFound(
                        not_found.into(),
                    ))?
                }

                // Extract relative versions.
                let (relative_from, relative_to) = match (relative_from, relative_to) {
                    (Some(relative_from), Some(relative_to)) => (relative_from, relative_to),
                    _ => unreachable!(),
                };

                // Check relative versions are always older to newer.
                let found = if relative_from > relative_to {
                    vec![None; 0]
                } else {
                    // And fetch the versions.
                    let mut found: Vec<_> = queries
                        .select_table_data_versions_at::<ActiveTableDataVersionDB>(
                            Some(triggered_on),
                            None,
                            table_id,
                            versions,
                        )?
                        .build_query_as()
                        .fetch_all(&mut *conn)
                        .await
                        .map_err(handle_sql_err)?
                        .into_iter()
                        .map(Some)
                        .collect();

                    // In we didn't find enough versions, we fill with None the empty spots.
                    let found_size = found.len();
                    let range_size = (relative_to - relative_from).abs() as usize + 1;
                    found.resize(range_size, None);
                    found.rotate_right(range_size - found_size);

                    found
                };

                found
            }
        };

        Ok(versions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::sql::DbPool;
    use td_error::TdError;
    use td_objects::sql::{DaoQueries, SelectBy};
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_execution::seed_execution;
    use td_objects::test_utils::seed_function::seed_function;
    use td_objects::test_utils::seed_function_run::seed_function_run;
    use td_objects::test_utils::seed_table_data_version::seed_table_data_version;
    use td_objects::test_utils::seed_transaction::seed_transaction;
    use td_objects::types::basic::{
        BundleId, CollectionName, Decorator, FunctionRunStatus, TableDataVersionId, TableName,
        TableNameDto, TransactionKey, UserId,
    };
    use td_objects::types::function::FunctionRegister;
    use td_objects::types::table::TableDB;
    use td_security::ENCODED_ID_SYSTEM;

    // Tables create N times, where N is the number of versions. Returning a map of table name to
    // the table data versions created, also ordered in ASC order.
    async fn seed_table_data_versions<'a>(
        db: &DbPool,
        tables: HashMap<&'a TableNameDto, usize>,
    ) -> HashMap<&'a TableNameDto, Vec<ActiveTableDataVersionDB>> {
        let collection = seed_collection(
            db,
            &CollectionName::try_from("collection").unwrap(),
            &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
        )
        .await;

        let created_tables = tables.keys().map(|t| (*t).clone()).collect();
        let dependencies = None;
        let triggers = None;

        let create = FunctionRegister::builder()
            .try_name("joaquin")
            .unwrap()
            .try_description("function_foo description")
            .unwrap()
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")
            .unwrap()
            .decorator(Decorator::Publisher)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(Some(created_tables))
            .try_runtime_values("mock runtime values")
            .unwrap()
            .reuse_frozen_tables(false)
            .build()
            .unwrap();

        let function_version = seed_function(db, &collection, &create).await;

        let mut table_versions_map = HashMap::new();
        for (table_name_dto, number_of_versions) in tables {
            let table_name = TableName::try_from(table_name_dto).unwrap();
            let mut table_versions = vec![];
            for _ in 0..number_of_versions {
                let execution = seed_execution(db, &function_version).await;

                let transaction_key = TransactionKey::try_from("ANY").unwrap();
                let transaction = seed_transaction(db, &execution, &transaction_key).await;

                let function_run = seed_function_run(
                    db,
                    &collection,
                    &function_version,
                    &execution,
                    &transaction,
                    &FunctionRunStatus::Scheduled,
                )
                .await;

                let table_version = DaoQueries::default()
                    .select_by::<TableDB>(&(collection.id(), &table_name))
                    .unwrap()
                    .build_query_as()
                    .fetch_one(db)
                    .await
                    .unwrap();

                let table_data_version = seed_table_data_version(
                    db,
                    &collection,
                    &execution,
                    &transaction,
                    &function_run,
                    &table_version,
                )
                .await;
                let table_data_version = DaoQueries::default()
                    .select_by::<ActiveTableDataVersionDB>(&(table_data_version.id()))
                    .unwrap()
                    .build_query_as()
                    .fetch_one(db)
                    .await
                    .unwrap();
                table_versions.push(table_data_version);
            }
            table_versions_map.insert(table_name_dto, table_versions);
        }
        table_versions_map
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_none(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 1)])).await;

        let table_id = table_data_versions.get(&table_name).unwrap()[0].table_id();
        let versions = Versions::None;
        let triggered_on = TriggeredOn::now().await;

        let mut conn = db.acquire().await.unwrap();
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.table_id(), table_id);
        assert_eq!(*version_found.has_data(), None);
        assert!(*version_found.triggered_on() < triggered_on);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_none_multiple_tables(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let extra_table_name_1 = TableNameDto::try_from("should_not_be_found_1")?;
        let extra_table_name_2 = TableNameDto::try_from("should_not_be_found_2")?;
        let table_data_versions = seed_table_data_versions(
            &db,
            HashMap::from([
                (&extra_table_name_1, 1),
                (&table_name, 1),
                (&extra_table_name_2, 1),
            ]),
        )
        .await;

        let table_id = table_data_versions.get(&table_name).unwrap()[0].table_id();
        let versions = Versions::None;
        let triggered_on = TriggeredOn::now().await;

        let mut conn = db.acquire().await.unwrap();
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.table_id(), table_id);
        assert_eq!(*version_found.has_data(), None);
        assert!(*version_found.triggered_on() < triggered_on);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_none_triggered_on(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();
        let versions = Versions::None;

        // Assert both versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check the first version is found if its trigger_on is used.
        let triggered_on = version_1.triggered_on();
        let versions_found = VersionResolver::new(table_id, &versions, triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());

        // And that the second version is found if its trigger_on is used.
        let triggered_on = version_2.triggered_on();
        let versions_found = VersionResolver::new(table_id, &versions, triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_2.id());

        // And the second one if current triggered_on is used.
        let triggered_on = TriggeredOn::now().await;
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_2.id());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_single_head(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 3)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        let version_3 = &table_data_versions.get(&table_name).unwrap()[2];
        assert_ne!(version_1.id(), version_2.id());
        assert_ne!(version_1.id(), version_3.id());
        assert_ne!(version_2.id(), version_3.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert_eq!(version_1.table_id(), version_3.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());
        assert!(version_2.triggered_on() < version_3.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check the latest version is found with HEAD.
        let (versions, triggered_on) =
            (Versions::Single(Version::Head(0)), TriggeredOn::now().await);
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_3.id());

        // But not if using a previous triggered_on
        let (versions, triggered_on) =
            (Versions::Single(Version::Head(0)), version_2.triggered_on());
        let versions_found = VersionResolver::new(table_id, &versions, triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_2.id());

        // And then get HEAD~1
        let (versions, triggered_on) = (
            Versions::Single(Version::Head(-1)),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_2.id());

        // And then get HEAD~2
        let (versions, triggered_on) = (
            Versions::Single(Version::Head(-2)),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());

        // And then get HEAD~3 (which should be None)
        let (versions, triggered_on) = (
            Versions::Single(Version::Head(-3)),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        let version_found = versions_found[0].as_ref();
        assert!(version_found.is_none());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_single_fixed(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::Single(Version::Fixed(*version_1.id())),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());

        let (versions, triggered_on) = (
            Versions::Single(Version::Fixed(*version_2.id())),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_2.id());

        // But an error is found if the id is not found.
        let not_found_id = TableDataVersionId::default();
        let (versions, triggered_on) = (
            Versions::Single(Version::Fixed(not_found_id)),
            TriggeredOn::now().await,
        );
        let res = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await;
        assert!(res.is_err());
        let version_error = res.unwrap_err();
        let version_error = version_error.domain_err::<VersionResolverError>();
        assert!(matches!(
            version_error,
            VersionResolverError::FixedTableDataVersionsNotFound(_),
        ));
        let not_found = match version_error {
            VersionResolverError::FixedTableDataVersionsNotFound(not_found) => not_found,
        };
        assert_eq!(not_found.len(), 1);
        assert_eq!(not_found[0], not_found_id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_list_head(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check versions found.
        let (versions, triggered_on) = (
            Versions::List(vec![Version::Head(0), Version::Head(-1), Version::Head(-2)]),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 3);
        // HEAD
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_2.id());
        // HEAD~1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());
        // HEAD~2 (None, only 2 versions)
        assert!(versions_found[2].is_none());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_list_fixed(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::List(vec![
                Version::Fixed(*version_1.id()),
                Version::Fixed(*version_2.id()),
            ]),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 2);
        // Version 1
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());
        // Version 2
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id(), version_2.id());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_list_mixed(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check all versions are found.
        let (versions, triggered_on) = (
            Versions::List(vec![
                Version::Head(-1),
                Version::Fixed(*version_1.id()),
                Version::Head(0),
            ]),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 3);
        // HEAD~1
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());
        // Fixed 1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());
        // HEAD
        assert!(versions_found[2].is_some());
        let version_found = versions_found[2].as_ref().unwrap();
        assert_eq!(version_found.id(), version_2.id());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_list_fixed_not_found(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check fixed versions are not found.
        let not_found_id_1 = TableDataVersionId::default();
        let not_found_id_2 = TableDataVersionId::default();
        let (versions, triggered_on) = (
            Versions::List(vec![
                Version::Fixed(not_found_id_1),
                Version::Fixed(not_found_id_2),
            ]),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await;
        assert!(versions_found.is_err());
        let version_error = versions_found.unwrap_err();
        let version_error = version_error.domain_err::<VersionResolverError>();
        assert!(matches!(
            version_error,
            VersionResolverError::FixedTableDataVersionsNotFound(_),
        ));
        let not_found = match version_error {
            VersionResolverError::FixedTableDataVersionsNotFound(not_found) => not_found,
        };
        assert_eq!(not_found.len(), 2);
        let not_found_ids = [not_found_id_1, not_found_id_2];
        assert!(not_found_ids.contains(&not_found[0]));
        assert!(not_found_ids.contains(&not_found[1]));
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_head(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 3)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        let version_3 = &table_data_versions.get(&table_name).unwrap()[2];
        assert_ne!(version_1.id(), version_2.id());
        assert_ne!(version_1.id(), version_3.id());
        assert_ne!(version_2.id(), version_3.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert_eq!(version_1.table_id(), version_3.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());
        assert!(version_2.triggered_on() < version_3.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(-2), Version::Head(0)),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 3);
        // HEAD~2
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());
        // HEAD~1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id(), version_2.id());
        // HEAD
        assert!(versions_found[2].is_some());
        let version_found = versions_found[2].as_ref().unwrap();
        assert_eq!(version_found.id(), version_3.id());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_head_incomplete(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(-2), Version::Head(0)),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 3);
        // HEAD~2
        assert!(versions_found[0].is_none());
        // HEAD~1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());
        // HEAD
        assert!(versions_found[2].is_some());
        let version_found = versions_found[2].as_ref().unwrap();
        assert_eq!(version_found.id(), version_2.id());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_inverse_head(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(0), Version::Head(-1)),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert!(versions_found.is_empty());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_fixed(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 3)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        let version_3 = &table_data_versions.get(&table_name).unwrap()[2];
        assert_ne!(version_1.id(), version_2.id());
        assert_ne!(version_1.id(), version_3.id());
        assert_ne!(version_2.id(), version_3.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert_eq!(version_1.table_id(), version_3.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());
        assert!(version_2.triggered_on() < version_3.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::Range(
                Version::Fixed(*version_1.id()),
                Version::Fixed(*version_3.id()),
            ),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 3);
        // HEAD~2
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());
        // HEAD~1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id(), version_2.id());
        // HEAD
        assert!(versions_found[2].is_some());
        let version_found = versions_found[2].as_ref().unwrap();
        assert_eq!(version_found.id(), version_3.id());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_inverse_fixed(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 3)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        let version_3 = &table_data_versions.get(&table_name).unwrap()[2];
        assert_ne!(version_1.id(), version_2.id());
        assert_ne!(version_1.id(), version_3.id());
        assert_ne!(version_2.id(), version_3.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert_eq!(version_1.table_id(), version_3.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());
        assert!(version_2.triggered_on() < version_3.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check versions found is empty: range is inverted.
        let (versions, triggered_on) = (
            Versions::Range(
                Version::Fixed(*version_3.id()),
                Version::Fixed(*version_1.id()),
            ),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert!(versions_found.is_empty());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_same_head(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check version is found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(-1), Version::Head(-1)),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_same_head_not_found(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check version is found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(-3), Version::Head(-3)),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_none());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_same_fixed(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check version is found.
        let (versions, triggered_on) = (
            Versions::Range(
                Version::Fixed(*version_1.id()),
                Version::Fixed(*version_1.id()),
            ),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_mixed(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check versions are found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Fixed(*version_1.id()), Version::Head(0)),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 2);
        // Fixed
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());
        // HEAD
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id(), version_2.id());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_mixed_same(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check versions are found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(-1), Version::Fixed(*version_1.id())),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        // HEAD and fixed
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_mixed_head_bound(db: DbPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id(), version_2.id());
        assert_eq!(version_1.table_id(), version_2.table_id());
        assert!(version_1.triggered_on() < version_2.triggered_on());

        // Get table_id
        let table_id = version_1.table_id();

        // Check versions are found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(-5), Version::Fixed(*version_1.id())),
            TriggeredOn::now().await,
        );
        let versions_found = VersionResolver::new(table_id, &versions, &triggered_on)
            .resolve(&DaoQueries::default(), &mut conn)
            .await?;
        assert_eq!(versions_found.len(), 5);
        // Head bound as None (nothing else found)
        assert!(versions_found[0].is_none());
        assert!(versions_found[1].is_none());
        assert!(versions_found[2].is_none());
        assert!(versions_found[3].is_none());
        // Fixed
        assert!(versions_found[4].is_some());
        let version_found = versions_found[4].as_ref().unwrap();
        assert_eq!(version_found.id(), version_1.id());
        Ok(())
    }
}
