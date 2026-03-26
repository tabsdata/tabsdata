//
// Copyright 2025 Tabs Data Inc.
//

use derive_builder::Builder;
use itertools::{Either, Itertools};
use std::collections::HashMap;
use std::fmt::Debug;
use td_database::dynamic::DynamicConnection;
use td_error::DisplayVec;
use td_error::{TdError, display_vec, td_error};
use td_objects::dxo::execution::VersionedExecutionDB;
use td_objects::dxo::table_data_version::ActiveTableDataVersionDB;
use td_objects::types::basic::{FunctionId, TableDataVersionId, TableId, TriggeredOn};
use td_objects::types::table_ref::{Version, Versions};
use td_objects::{and, lte};
use td_queries::{BasicQueries, DataVersionsQueries};

#[td_error]
pub enum VersionResolverError {
    #[error("Could not find the following table data versions: [{0}]")]
    FixedTableDataVersionsNotFound(DisplayVec<TableDataVersionId>) = 0,
    #[error("In a range, the left version must be older than the right version.")]
    InvalidRange(Versions),
}

/// Struct to resolve table data versions. It will resolve relative and fixed versions, using
/// `TableDataVersionDB` Dao, for any triggered_on.
#[derive(Debug, Builder)]
#[builder(build_fn(error = "TdError"))]
pub struct VersionResolver<'a> {
    // This function is the one requesting the table versions. Some version resolving
    // might need this version to find the versions required (like NEW, which needs all
    // versions since the function requesting last executed).
    function_id: &'a FunctionId,
    table_id: &'a TableId,
    versions: &'a Versions,
    triggered_on: &'a TriggeredOn,
    #[builder(default = "false")]
    error_on_desc_range: bool,
}

impl<'a> VersionResolver<'a> {
    pub fn builder() -> VersionResolverBuilder<'a> {
        VersionResolverBuilder::default()
    }

    /// Main resolve function. Note that the return type is a `Vec<Option<TableDataVersionDB>>`,
    /// because versions not existing is not necessarily an error, and a single `Versions` can
    /// resolve to multiple versions (i.e. List).
    pub async fn resolve(
        &self,
        conn: &mut DynamicConnection<'_>,
    ) -> Result<Vec<Option<ActiveTableDataVersionDB>>, TdError> {
        let (table_id, versions, triggered_on) = (self.table_id, self.versions, self.triggered_on);
        let versions = match versions {
            Versions::None => {
                let v = conn
                    .fetch_optional_data_version_at::<{ ActiveTableDataVersionDB::All }, ActiveTableDataVersionDB>(
                        table_id, triggered_on, versions,
                    )
                    .await?;
                vec![v]
            }
            Versions::Single(version) => match version {
                Version::Fixed(version) => {
                    // We fail if fixed not found.
                    let v = conn
                        .fetch_one_data_version_at::<{ ActiveTableDataVersionDB::All }, ActiveTableDataVersionDB>(
                            table_id, triggered_on, versions,
                        )
                        .await
                        .map_err(|_| {
                            VersionResolverError::FixedTableDataVersionsNotFound(display_vec![
                                version.clone(),
                            ])
                        })?;
                    vec![Some(v)]
                }
                Version::Head(_) | Version::Initial(_) => {
                    let v = conn
                        .fetch_optional_data_version_at::<{ ActiveTableDataVersionDB::All }, ActiveTableDataVersionDB>(
                            table_id, triggered_on, versions,
                        )
                        .await?;
                    vec![v]
                }
            },
            Versions::List(versions) => {
                // Split versions into fixed, head and initial versions so we only do 3 queries.
                let (fixed_versions, head_versions, initial_versions) =
                    versions.iter().cloned().enumerate().fold(
                        (HashMap::new(), HashMap::new(), HashMap::new()),
                        |(mut fixed, mut head, mut initial), (index, version)| {
                            match version {
                                Version::Fixed(_) => fixed.insert(index, version),
                                Version::Head(_) => head.insert(index, version),
                                Version::Initial(_) => initial.insert(index, version),
                            };
                            (fixed, head, initial)
                        },
                    );

                // Each fixed versions is queried by its id to find if it exists or not.
                let fixed_versions = if !fixed_versions.is_empty() {
                    let version_list = fixed_versions.values().cloned().collect();
                    let found = conn
                        .fetch_all_data_versions_at::<{ ActiveTableDataVersionDB::All }, ActiveTableDataVersionDB>(
                            table_id, triggered_on, &Versions::List(version_list),
                        )
                        .await?;
                    let found: HashMap<_, _> =
                        found.into_iter().map(|v| (v.id.clone(), v)).collect();

                    let (absolute_versions, not_found): (HashMap<_, _>, Vec<_>) =
                        fixed_versions.iter().partition_map(|(i, v)| {
                            let id = match v {
                                Version::Fixed(id) => id,
                                _ => unreachable!(),
                            };
                            match found.get(id) {
                                Some(v) => Either::Left((*i, Some(v.clone()))),
                                None => Either::Right(id.clone()),
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

                // Head and initial versions are queried as a range, so we can map them back by position.
                let mut find_min_max_range =
                    async |versions: HashMap<usize, Version>, reverse: bool| {
                        let minmax = versions
                            .values()
                            .minmax()
                            .into_option()
                            .map(|(min, max)| (min.clone(), max.clone()));

                        let versions = if let Some((min, max)) = minmax {
                            let mut found = conn
                                .fetch_all_data_versions_at::<{ ActiveTableDataVersionDB::All }, ActiveTableDataVersionDB>(
                                    table_id, triggered_on, &Versions::Range(min, max), // range always older to newer
                                )
                                .await?;

                            // We know that this vec has all the versions in the range at most, so we can
                            // get the position by the index of the versions (or None if range is too short).
                            // We reverse it if needed, so HEAD is newer to older, but INITIAL is older to newer,
                            // to make indexing easier.
                            if reverse {
                                found.reverse();
                            }
                            let absolute_versions: HashMap<_, _> = versions
                                .iter()
                                .map(|(i, v)| {
                                    let v = match v {
                                        Version::Head(back) => found.get(-back as usize).cloned(),
                                        Version::Initial(forward) => {
                                            found.get(*forward as usize).cloned()
                                        }
                                        _ => unreachable!(),
                                    };

                                    (*i, v)
                                })
                                .collect();

                            absolute_versions
                        } else {
                            HashMap::new()
                        };
                        Ok::<_, TdError>(versions)
                    };
                let head_versions = find_min_max_range(head_versions, true).await?;
                let initial_versions = find_min_max_range(initial_versions, false).await?;

                // Merge fixed and head versions and convert back to Vec<_> sorting it by position.
                let sorted_versions: Vec<_> = fixed_versions
                    .into_iter()
                    .chain(head_versions)
                    .chain(initial_versions)
                    .collect::<HashMap<_, _>>()
                    .into_iter()
                    .sorted_by_key(|&(index, _)| index)
                    .map(|(_, version)| version)
                    .collect();

                sorted_versions
            }
            Versions::Range(from, to) => {
                // Map id ranges to HEAD ranges.
                let mut not_found = vec![];
                let relative_from = match from {
                    Version::Head(_) | Version::Initial(_) => Some(from),
                    Version::Fixed(id) => {
                        let relative =
                            conn.fetch_relative_offset_at::<{ ActiveTableDataVersionDB::All }, ActiveTableDataVersionDB>(
                                table_id,
                                triggered_on,
                                from
                            ).await?;

                        match relative {
                            Some(relative) => Some(&Version::Head((-relative + 1) as isize)),
                            None => {
                                not_found.push(id.clone());
                                None
                            }
                        }
                    }
                };
                let relative_to = match to {
                    Version::Head(_) | Version::Initial(_) => Some(to),
                    Version::Fixed(id) => {
                        let relative =
                            conn.fetch_relative_offset_at::<{ ActiveTableDataVersionDB::All }, ActiveTableDataVersionDB>(
                                table_id,
                                triggered_on,
                                to,
                            ).await?;

                        match relative {
                            Some(relative) => Some(&Version::Head((-relative + 1) as isize)),
                            None => {
                                not_found.push(id.clone());
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

                // Check relative versions are always older to newer (if we can).
                let is_desc_range = match (relative_from, relative_to) {
                    (Version::Head(from), Version::Head(to)) => from > to,
                    (Version::Initial(from), Version::Initial(to)) => from > to,
                    _ => false,
                };
                if is_desc_range {
                    if self.error_on_desc_range {
                        Err(VersionResolverError::InvalidRange(self.versions.clone()))?
                    } else {
                        vec![]
                    }
                } else {
                    // And fetch the versions.
                    let mut found = conn
                        .fetch_all_data_versions_at::<{ ActiveTableDataVersionDB::All }, ActiveTableDataVersionDB>(
                            table_id, triggered_on, versions,
                        )
                        .await?
                        .into_iter()
                        .map(Some)
                        .collect::<Vec<_>>();

                    // If we didn't find enough versions, we fill with None the empty spots, if possible.
                    let found_size = found.len();
                    match (relative_from, relative_to) {
                        (Version::Head(from), Version::Head(to)) => {
                            let range_size = (to - from).unsigned_abs() + 1;
                            found.resize(range_size, None);
                            found.rotate_right(range_size - found_size);
                        }
                        (Version::Initial(from), Version::Initial(to)) => {
                            let range_size = (to - from).unsigned_abs() + 1;
                            found.resize(range_size, None);
                        }
                        _ => {
                            // HEAD to INITIAL or INITIAL to HEAD ranges do not define a size, so we
                            // cannot fill with None the missing versions.
                        }
                    }

                    found
                }
            }
            Versions::New => {
                // NEW refers to versions that started existing after the last execution of the
                // current function. Previous versions to that time (even if they were never used
                // by the function) are not considered new.
                // This is useful to maintain consistency, even if removing/adding the table as
                // a dependency, or depending on one table on NEW and (for example) HEAD.
                // We use offset=1 because the current ExecutionDB is already in place, and we
                // need to ignore it (offset=0 is the latest, offset=1 is the previous one).
                let last_execution = conn
                    .fetch_latest_before::<{ VersionedExecutionDB::All }, VersionedExecutionDB>(
                        and!(&self.function_id, lte!(triggered_on)),
                        1,
                    )
                    .await?;

                // If there is a previous execution, find the latest data version at that time
                // to use as the lower bound (exclusive) for NEW versions.
                let last_table_data_version = match last_execution {
                    Some(last_execution) => conn
                        .fetch_optional_data_version_at::<{ ActiveTableDataVersionDB::All }, ActiveTableDataVersionDB>(
                            table_id, &last_execution.triggered_on, &Versions::None,
                        )
                        .await?,
                    None => None,
                };

                match last_table_data_version {
                    // From that version to HEAD, skipping the lower bound itself.
                    Some(v) => conn
                        .fetch_all_data_versions_at::<{ ActiveTableDataVersionDB::All }, ActiveTableDataVersionDB>(
                            table_id, triggered_on,
                            &Versions::Range(Version::Fixed(v.id), Version::Head(0)),
                        )
                        .await?
                        .into_iter()
                        .skip(1)
                        .map(Some)
                        .collect::<Vec<_>>(),
                    // No previous execution or no data version at that time: all versions are NEW.
                    None => conn
                        .fetch_all_data_versions_at::<{ ActiveTableDataVersionDB::All }, ActiveTableDataVersionDB>(
                            table_id, triggered_on,
                            &Versions::Range(Version::Initial(0), Version::Head(0)),
                        )
                        .await?
                        .into_iter()
                        .map(Some)
                        .collect(),
                }
            }
        };

        Ok(versions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::dxo::function::FunctionRegister;
    use td_objects::dxo::table::TableDB;
    use td_objects::dxo::table_data_version::ActiveTableDataVersionDB;
    use td_objects::types::basic::{
        AtTime, BundleId, CollectionName, Decorator, FunctionRunStatus, TableName, TableNameDto,
        TransactionKey, UserId,
    };
    use td_objects::{Buildable, and};
    use td_queries::test_utils::SeedQueries;
    use td_queries::{BasicQueries, VersionedQueries};
    use td_security::ENCODED_ID_SYSTEM;

    // Tables create N times, where N is the number of versions. Returning a map of table name to
    // the table data versions created, also ordered in ASC order.
    async fn seed_table_data_versions<'a>(
        db: &TdPool,
        tables: HashMap<&'a TableNameDto, usize>,
    ) -> HashMap<&'a TableNameDto, Vec<ActiveTableDataVersionDB>> {
        let collection = db
            .seed_collection(
                &CollectionName::try_from("collection").unwrap(),
                &UserId::try_from(ENCODED_ID_SYSTEM).unwrap(),
            )
            .await
            .unwrap();

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
            .connector(None)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(Some(created_tables))
            .try_runtime_values("mock runtime values")
            .unwrap()
            .reuse_frozen_tables(false)
            .build()
            .unwrap();

        let function_version = db.seed_function(&collection, &create).await.unwrap();

        let mut table_versions_map = HashMap::new();
        for (table_name_dto, number_of_versions) in tables {
            let table_name = TableName::try_from(table_name_dto).unwrap();
            let mut table_versions = vec![];
            for _ in 0..number_of_versions {
                let execution = db.seed_execution(&function_version).await.unwrap();

                let transaction_key = TransactionKey::try_from("ANY").unwrap();
                let transaction = db
                    .seed_transaction(&execution, &transaction_key)
                    .await
                    .unwrap();

                let function_run = db
                    .seed_function_run(
                        &collection,
                        &function_version,
                        &execution,
                        &transaction,
                        &FunctionRunStatus::Scheduled,
                    )
                    .await
                    .unwrap();

                let table_version = db
                    .fetch_one_at::<{ TableDB::All }, TableDB>(
                        and!(&collection.id, &table_name),
                        &AtTime::now(),
                    )
                    .await
                    .unwrap();

                let table_data_version = db
                    .seed_table_data_version(
                        &collection,
                        &execution,
                        &transaction,
                        &function_run,
                        &table_version,
                    )
                    .await
                    .unwrap();
                let table_data_version = db
                    .fetch_one::<ActiveTableDataVersionDB>(&table_data_version.id)
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
    async fn test_resolve_none(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 1)])).await;

        let table_id = table_data_versions.get(&table_name).unwrap()[0].table_id;
        let versions = Versions::None;
        let triggered_on = TriggeredOn::now();

        let mut conn = db.acquire().await.unwrap();
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.table_id, table_id);
        assert_eq!(version_found.has_data, None);
        assert!(version_found.triggered_on < triggered_on);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_none_multiple_tables(db: TdPool) -> Result<(), TdError> {
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

        let table_id = table_data_versions.get(&table_name).unwrap()[0].table_id;
        let versions = Versions::None;
        let triggered_on = TriggeredOn::now();

        let mut conn = db.acquire().await.unwrap();
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.table_id, table_id);
        assert_eq!(version_found.has_data, None);
        assert!(version_found.triggered_on < triggered_on);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_none_triggered_on(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();
        let versions = Versions::None;

        // Assert both versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check the first version is found if its trigger_on is used.
        let triggered_on = &version_1.triggered_on;
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);

        // And that the second version is found if its trigger_on is used.
        let triggered_on = &version_2.triggered_on;
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);

        // And the second one if current triggered_on is used.
        let triggered_on = TriggeredOn::now();
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_single_head(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 3)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        let version_3 = &table_data_versions.get(&table_name).unwrap()[2];
        assert_ne!(version_1.id, version_2.id);
        assert_ne!(version_1.id, version_3.id);
        assert_ne!(version_2.id, version_3.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert_eq!(version_1.table_id, version_3.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);
        assert!(version_2.triggered_on < version_3.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check the latest version is found with HEAD.
        let (versions, triggered_on) = (Versions::Single(Version::Head(0)), TriggeredOn::now());
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_3.id);

        // But not if using a previous triggered_on
        let (versions, triggered_on) =
            (Versions::Single(Version::Head(0)), &version_2.triggered_on);
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);

        // And then get HEAD~1
        let (versions, triggered_on) = (Versions::Single(Version::Head(-1)), TriggeredOn::now());
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);

        // And then get HEAD~2
        let (versions, triggered_on) = (Versions::Single(Version::Head(-2)), TriggeredOn::now());
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);

        // And then get HEAD~3 (which should be None)
        let (versions, triggered_on) = (Versions::Single(Version::Head(-3)), TriggeredOn::now());
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        let version_found = versions_found[0].as_ref();
        assert!(version_found.is_none());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_single_initial(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 3)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        let version_3 = &table_data_versions.get(&table_name).unwrap()[2];
        assert_ne!(version_1.id, version_2.id);
        assert_ne!(version_1.id, version_3.id);
        assert_ne!(version_2.id, version_3.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert_eq!(version_1.table_id, version_3.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);
        assert!(version_2.triggered_on < version_3.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check the latest version is found with INITIAL.
        let (versions, triggered_on) = (Versions::Single(Version::Initial(0)), TriggeredOn::now());
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);

        // But not if using a previous triggered_on
        let (versions, triggered_on) = (
            Versions::Single(Version::Initial(1)),
            &version_2.triggered_on,
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);

        // And then get INITIAL~1
        let (versions, triggered_on) = (Versions::Single(Version::Initial(1)), TriggeredOn::now());
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);

        // And then get INITIAL~2
        let (versions, triggered_on) = (Versions::Single(Version::Initial(2)), TriggeredOn::now());
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_3.id);

        // And then get INITIAL~3 (which should be None)
        let (versions, triggered_on) = (Versions::Single(Version::Initial(3)), TriggeredOn::now());
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        let version_found = versions_found[0].as_ref();
        assert!(version_found.is_none());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_single_fixed(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::Single(Version::Fixed(version_1.id.clone())),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);

        let (versions, triggered_on) = (
            Versions::Single(Version::Fixed(version_2.id.clone())),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);

        // But an error is found if the id is not found.
        let not_found_id = TableDataVersionId::new();
        let (versions, triggered_on) = (
            Versions::Single(Version::Fixed(not_found_id.clone())),
            TriggeredOn::now(),
        );
        let res = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await;
        assert!(res.is_err());
        let version_error = res.unwrap_err();
        let version_error = version_error.domain_err::<VersionResolverError>();
        assert!(matches!(
            version_error,
            VersionResolverError::FixedTableDataVersionsNotFound(_),
        ));
        let VersionResolverError::FixedTableDataVersionsNotFound(not_found) = version_error else {
            unreachable!()
        };
        assert_eq!(not_found.len(), 1);
        assert_eq!(not_found[0], not_found_id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_list_relative(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check versions found.
        let (versions, triggered_on) = (
            Versions::List(vec![
                Version::Head(0),
                Version::Head(-1),
                Version::Head(-2),
                Version::Initial(0),
                Version::Initial(1),
                Version::Initial(2),
            ]),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 6);
        // HEAD
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        // HEAD~1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // HEAD~2 (None, only 2 versions)
        assert!(versions_found[2].is_none());
        // INITIAL
        assert!(versions_found[3].is_some());
        let version_found = versions_found[3].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // INITIAL~1
        assert!(versions_found[4].is_some());
        let version_found = versions_found[4].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        // INITIAL~2 (None, only 2 versions)
        assert!(versions_found[5].is_none());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_list_fixed(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::List(vec![
                Version::Fixed(version_1.id.clone()),
                Version::Fixed(version_2.id.clone()),
            ]),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 2);
        // Version 1
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // Version 2
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_list_mixed(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check all versions are found.
        let (versions, triggered_on) = (
            Versions::List(vec![
                Version::Head(-1),
                Version::Fixed(version_1.id.clone()),
                Version::Head(0),
                Version::Initial(0),
            ]),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 4);
        // HEAD~1
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // Fixed 1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // HEAD
        assert!(versions_found[2].is_some());
        let version_found = versions_found[2].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        // INITIAL
        assert!(versions_found[3].is_some());
        let version_found = versions_found[3].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_list_fixed_not_found(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check fixed versions are not found.
        let not_found_id_1 = TableDataVersionId::new();
        let not_found_id_2 = TableDataVersionId::new();
        let (versions, triggered_on) = (
            Versions::List(vec![
                Version::Fixed(not_found_id_1.clone()),
                Version::Fixed(not_found_id_2.clone()),
            ]),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await;
        assert!(versions_found.is_err());
        let version_error = versions_found.unwrap_err();
        let version_error = version_error.domain_err::<VersionResolverError>();
        assert!(matches!(
            version_error,
            VersionResolverError::FixedTableDataVersionsNotFound(_),
        ));
        let VersionResolverError::FixedTableDataVersionsNotFound(not_found) = version_error else {
            unreachable!()
        };
        assert_eq!(not_found.len(), 2);
        let not_found_ids = [not_found_id_1, not_found_id_2];
        assert!(not_found_ids.contains(&not_found[0]));
        assert!(not_found_ids.contains(&not_found[1]));
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_head(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 3)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        let version_3 = &table_data_versions.get(&table_name).unwrap()[2];
        assert_ne!(version_1.id, version_2.id);
        assert_ne!(version_1.id, version_3.id);
        assert_ne!(version_2.id, version_3.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert_eq!(version_1.table_id, version_3.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);
        assert!(version_2.triggered_on < version_3.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(-2), Version::Head(0)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 3);
        // HEAD~2
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // HEAD~1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        // HEAD
        assert!(versions_found[2].is_some());
        let version_found = versions_found[2].as_ref().unwrap();
        assert_eq!(version_found.id, version_3.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_initial(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 3)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        let version_3 = &table_data_versions.get(&table_name).unwrap()[2];
        assert_ne!(version_1.id, version_2.id);
        assert_ne!(version_1.id, version_3.id);
        assert_ne!(version_2.id, version_3.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert_eq!(version_1.table_id, version_3.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);
        assert!(version_2.triggered_on < version_3.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::Range(Version::Initial(0), Version::Initial(2)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 3);
        // INITIAL
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // INITIAL~1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        // INITIAL~2
        assert!(versions_found[2].is_some());
        let version_found = versions_found[2].as_ref().unwrap();
        assert_eq!(version_found.id, version_3.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_initial_head(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 3)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        let version_3 = &table_data_versions.get(&table_name).unwrap()[2];
        assert_ne!(version_1.id, version_2.id);
        assert_ne!(version_1.id, version_3.id);
        assert_ne!(version_2.id, version_3.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert_eq!(version_1.table_id, version_3.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);
        assert!(version_2.triggered_on < version_3.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::Range(Version::Initial(0), Version::Head(0)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 3);
        // INITIAL
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // INITIAL~1 or HEAD~1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        // HEAD
        assert!(versions_found[2].is_some());
        let version_found = versions_found[2].as_ref().unwrap();
        assert_eq!(version_found.id, version_3.id);

        // And the inverse returns nothing
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(0), Version::Initial(0)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 0);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_head_initial(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 3)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        let version_3 = &table_data_versions.get(&table_name).unwrap()[2];
        assert_ne!(version_1.id, version_2.id);
        assert_ne!(version_1.id, version_3.id);
        assert_ne!(version_2.id, version_3.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert_eq!(version_1.table_id, version_3.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);
        assert!(version_2.triggered_on < version_3.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(-2), Version::Initial(2)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 3);
        // INITIAL
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // INITIAL~1 or HEAD~1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        // HEAD
        assert!(versions_found[2].is_some());
        let version_found = versions_found[2].as_ref().unwrap();
        assert_eq!(version_found.id, version_3.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_head_incomplete(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(-2), Version::Head(0)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 3);
        // HEAD~2
        assert!(versions_found[0].is_none());
        // HEAD~1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // HEAD
        assert!(versions_found[2].is_some());
        let version_found = versions_found[2].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_inverse_head(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // And the inverse returns nothing
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(0), Version::Head(-1)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert!(versions_found.is_empty());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_inverse_initial(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // And the inverse returns nothing
        let (versions, triggered_on) = (
            Versions::Range(Version::Initial(1), Version::Initial(0)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert!(versions_found.is_empty());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_fixed(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 3)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        let version_3 = &table_data_versions.get(&table_name).unwrap()[2];
        assert_ne!(version_1.id, version_2.id);
        assert_ne!(version_1.id, version_3.id);
        assert_ne!(version_2.id, version_3.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert_eq!(version_1.table_id, version_3.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);
        assert!(version_2.triggered_on < version_3.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check fixed versions are found with its ids.
        let (versions, triggered_on) = (
            Versions::Range(
                Version::Fixed(version_1.id.clone()),
                Version::Fixed(version_3.id.clone()),
            ),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 3);
        // HEAD~2
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // HEAD~1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        // HEAD
        assert!(versions_found[2].is_some());
        let version_found = versions_found[2].as_ref().unwrap();
        assert_eq!(version_found.id, version_3.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_inverse_fixed(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 3)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        let version_3 = &table_data_versions.get(&table_name).unwrap()[2];
        assert_ne!(version_1.id, version_2.id);
        assert_ne!(version_1.id, version_3.id);
        assert_ne!(version_2.id, version_3.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert_eq!(version_1.table_id, version_3.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);
        assert!(version_2.triggered_on < version_3.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check versions found is empty: range is inverted.
        let (versions, triggered_on) = (
            Versions::Range(
                Version::Fixed(version_3.id.clone()),
                Version::Fixed(version_1.id.clone()),
            ),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert!(versions_found.is_empty());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_same_head(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check version is found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(-1), Version::Head(-1)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_same_initial(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check version is found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Initial(1), Version::Initial(1)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_same_head_not_found(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check version is found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(-3), Version::Head(-3)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_none());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_same_initial_not_found(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check version is found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Initial(-3), Version::Initial(-3)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_none());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_same_fixed(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check version is found.
        let (versions, triggered_on) = (
            Versions::Range(
                Version::Fixed(version_1.id.clone()),
                Version::Fixed(version_1.id.clone()),
            ),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_fixed_head(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check versions are found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Fixed(version_1.id.clone()), Version::Head(0)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 2);
        // Fixed
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // HEAD
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_fixed_initial(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check versions are found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Fixed(version_1.id.clone()), Version::Initial(1)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 2);
        // Fixed
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // INITIAL
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_matching_head_fixed(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check versions are found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(-1), Version::Fixed(version_1.id.clone())),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        // HEAD and fixed
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_matching_head_initial(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check version is found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Initial(0), Version::Head(-1)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_matching_initial_fixed(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check versions are found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Initial(0), Version::Fixed(version_1.id.clone())),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 1);
        // HEAD and fixed
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_mixed_head_bound(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // Check versions are found.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(-5), Version::Fixed(version_1.id.clone())),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
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
        assert_eq!(version_found.id, version_1.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_empty_list(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let requester_collection = db
            .seed_collection(
                &CollectionName::try_from("requester_collection")?,
                &UserId::try_from(ENCODED_ID_SYSTEM)?,
            )
            .await?;
        let create = FunctionRegister::builder()
            .try_name("requester")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .connector(None)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        // This function version is the one requesting the table data versions.
        let requester_function = db.seed_function(&requester_collection, &create).await?;
        // Create execution for function (meaning all previously created versions were already taken by this execution).
        let _ = db.seed_execution(&requester_function).await;
        // Create 2nd execution for function (which would be, the current execution)
        let _ = db.seed_execution(&requester_function).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get table_id
        let table_id = version_1.table_id;

        // These are all versions that can resolve to an empty list, in this case, with 2
        // version available.
        let range_empty_cases = [
            Versions::New,
            Versions::Range(Version::Initial(3), Version::Head(0)),
            Versions::Range(Version::Initial(3), Version::Head(-1)),
            Versions::Range(Version::Fixed(version_1.id.clone()), Version::Head(-3)),
            Versions::Range(Version::Head(0), Version::Fixed(version_1.id.clone())),
            Versions::Range(Version::Fixed(version_2.id.clone()), Version::Initial(0)),
            Versions::Range(Version::Initial(3), Version::Fixed(version_1.id.clone())),
        ];
        let triggered_on = TriggeredOn::now();

        for versions in range_empty_cases {
            let versions_found = VersionResolver::builder()
                .function_id(&requester_function.function_id)
                .table_id(&table_id)
                .versions(&versions)
                .triggered_on(&triggered_on)
                .build()?
                .resolve(&mut conn)
                .await?;
            assert_eq!(versions_found.len(), 0, "versions: {:?}", versions);
        }
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_validate(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 1)])).await;

        let table_id = table_data_versions.get(&table_name).unwrap()[0].table_id;

        // Assert invalid range errors are returned.
        let (versions, triggered_on) = (
            Versions::Range(Version::Head(0), Version::Head(-1)),
            TriggeredOn::now(),
        );
        let mut conn = db.acquire().await.unwrap();
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .error_on_desc_range(true)
            .build()?
            .resolve(&mut conn)
            .await;
        assert!(versions_found.is_err());
        let version_error = versions_found.unwrap_err();
        let version_error = version_error.domain_err::<VersionResolverError>();
        assert!(matches!(
            version_error,
            VersionResolverError::InvalidRange(_)
        ));
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_new_no_execution(db: TdPool) -> Result<(), TdError> {
        // checks that all versions are resolved by NEW when there is no execution
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let requester_collection = db
            .seed_collection(
                &CollectionName::try_from("requester_collection")?,
                &UserId::try_from(ENCODED_ID_SYSTEM)?,
            )
            .await?;
        let create = FunctionRegister::builder()
            .try_name("requester")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .connector(None)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        // This function version is the one requesting the table data versions
        let requester_function = db.seed_function(&requester_collection, &create).await?;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get function_id and table_id
        let function_id = requester_function.function_id;
        let table_id = version_1.table_id;

        // Check new versions are found for that function (no execution means all of them).
        let (versions, triggered_on) = (Versions::New, TriggeredOn::now());
        let versions_found = VersionResolver::builder()
            .function_id(&function_id)
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;

        // No execution, gets all table versions
        assert_eq!(versions_found.len(), 2);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_new_with_actual_execution(db: TdPool) -> Result<(), TdError> {
        // checks that no versions are resolved by NEW when there is an execution
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let requester_collection = db
            .seed_collection(
                &CollectionName::try_from("requester_collection")?,
                &UserId::try_from(ENCODED_ID_SYSTEM)?,
            )
            .await?;
        let create = FunctionRegister::builder()
            .try_name("requester")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .connector(None)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        // This function version is the one requesting the table data versions.
        let requester_function = db.seed_function(&requester_collection, &create).await?;
        // Create execution for function (meaning all previously created versions were already taken by this execution).
        let _ = db.seed_execution(&requester_function).await;
        // Create 2nd execution for function (which would be, the current execution)
        let _ = db.seed_execution(&requester_function).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get function_id and table_id
        let function_id = requester_function.function_id;
        let table_id = version_1.table_id;

        // Check versions are found.
        let (versions, triggered_on) = (Versions::New, TriggeredOn::now());
        let versions_found = VersionResolver::builder()
            .function_id(&function_id)
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;

        // With execution, get no table versions
        assert_eq!(versions_found.len(), 0);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_new_with_previous_execution(db: TdPool) -> Result<(), TdError> {
        // checks that versions are resolved by NEW when there is an execution prior to them
        let requester_collection = db
            .seed_collection(
                &CollectionName::try_from("requester_collection")?,
                &UserId::try_from(ENCODED_ID_SYSTEM)?,
            )
            .await?;
        let create = FunctionRegister::builder()
            .try_name("requester")?
            .try_description("function_foo description")?
            .bundle_id(BundleId::default())
            .try_snippet("function_foo snippet")?
            .decorator(Decorator::Publisher)
            .connector(None)
            .dependencies(None)
            .triggers(None)
            .tables(None)
            .try_runtime_values("mock runtime values")?
            .reuse_frozen_tables(false)
            .build()?;
        // This function version is the one requesting the table data versions.
        let requester_function = db.seed_function(&requester_collection, &create).await?;
        // Create execution for function (meaning all previously created versions were already taken by this execution).
        let _ = db.seed_execution(&requester_function).await?;

        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        // Assert all versions are for the same table_id but are different versions.
        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        // Get function_id and table_id
        let function_id = requester_function.function_id;
        let table_id = version_1.table_id;

        // Check versions are found.
        let (versions, triggered_on) = (Versions::New, TriggeredOn::now());
        let versions_found = VersionResolver::builder()
            .function_id(&function_id)
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;

        // With previous execution, get all table versions (all are new)
        assert_eq!(versions_found.len(), 2);
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_list_empty_vec(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let table_id = table_data_versions.get(&table_name).unwrap()[0].table_id;
        let versions = Versions::List(vec![]);
        let triggered_on = TriggeredOn::now();
        let mut conn = db.acquire().await.unwrap();

        // Empty list resolves to empty result, even when versions exist.
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert!(versions_found.is_empty());
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_validate_initial(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 1)])).await;

        let table_id = table_data_versions.get(&table_name).unwrap()[0].table_id;
        let triggered_on = TriggeredOn::now();
        let mut conn = db.acquire().await.unwrap();

        // Inversed INITIAL range with error_on_desc_range returns error.
        let versions = Versions::Range(Version::Initial(1), Version::Initial(0));
        let result = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .error_on_desc_range(true)
            .build()?
            .resolve(&mut conn)
            .await;
        assert!(result.is_err());
        let result_err = result.unwrap_err();
        let version_error = result_err.domain_err::<VersionResolverError>();
        assert!(matches!(
            version_error,
            VersionResolverError::InvalidRange(_)
        ));
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_fixed_not_found(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let table_id = version_1.table_id;
        let triggered_on = TriggeredOn::now();
        let mut conn = db.acquire().await.unwrap();

        // Range with a non-existent fixed "from" returns FixedTableDataVersionsNotFound.
        let bad_id = TableDataVersionId::new();
        let versions = Versions::Range(Version::Fixed(bad_id.clone()), Version::Head(0));
        let result = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await;
        assert!(result.is_err());
        let result_err = result.unwrap_err();
        let err = result_err.domain_err::<VersionResolverError>();
        let VersionResolverError::FixedTableDataVersionsNotFound(not_found) = err else {
            panic!("Expected FixedTableDataVersionsNotFound, got {:?}", err);
        };
        assert_eq!(not_found.len(), 1);
        assert_eq!(not_found[0], bad_id);

        // Same for non-existent fixed "to".
        let versions = Versions::Range(Version::Head(-1), Version::Fixed(bad_id.clone()));
        let result = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await;
        assert!(result.is_err());
        let result_err = result.unwrap_err();
        let err = result_err.domain_err::<VersionResolverError>();
        let VersionResolverError::FixedTableDataVersionsNotFound(not_found) = err else {
            panic!("Expected FixedTableDataVersionsNotFound, got {:?}", err);
        };
        assert_eq!(not_found.len(), 1);
        assert_eq!(not_found[0], bad_id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_head_with_triggered_on(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 3)])).await;

        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        let table_id = version_1.table_id;
        let mut conn = db.acquire().await.unwrap();

        // Using version_2's triggered_on as cutoff: only v1 and v2 are visible.
        // Range HEAD~1..HEAD should return v1 and v2.
        let versions = Versions::Range(Version::Head(-1), Version::Head(0));
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&version_2.triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 2);
        assert_eq!(versions_found[0].as_ref().unwrap().id, version_1.id);
        assert_eq!(versions_found[1].as_ref().unwrap().id, version_2.id);
        Ok(())
    }

    #[td_test::test(sqlx)]
    #[tokio::test]
    async fn test_resolve_range_initial_incomplete(db: TdPool) -> Result<(), TdError> {
        let table_name = TableNameDto::try_from("joaquin")?;
        let table_data_versions =
            seed_table_data_versions(&db, HashMap::from([(&table_name, 2)])).await;

        let mut conn = db.acquire().await.unwrap();

        let version_1 = &table_data_versions.get(&table_name).unwrap()[0];
        let version_2 = &table_data_versions.get(&table_name).unwrap()[1];
        assert_ne!(version_1.id, version_2.id);
        assert_eq!(version_1.table_id, version_2.table_id);
        assert!(version_1.triggered_on < version_2.triggered_on);

        let table_id = version_1.table_id;

        // INITIAL..INITIAL~2 requests 3 versions but only 2 exist.
        // None should be appended at the newer (to) end.
        let (versions, triggered_on) = (
            Versions::Range(Version::Initial(0), Version::Initial(2)),
            TriggeredOn::now(),
        );
        let versions_found = VersionResolver::builder()
            .function_id(&FunctionId::default())
            .table_id(&table_id)
            .versions(&versions)
            .triggered_on(&triggered_on)
            .build()?
            .resolve(&mut conn)
            .await?;
        assert_eq!(versions_found.len(), 3);
        // INITIAL
        assert!(versions_found[0].is_some());
        let version_found = versions_found[0].as_ref().unwrap();
        assert_eq!(version_found.id, version_1.id);
        // INITIAL~1
        assert!(versions_found[1].is_some());
        let version_found = versions_found[1].as_ref().unwrap();
        assert_eq!(version_found.id, version_2.id);
        // INITIAL~2 (doesn't exist, padded at the end)
        assert!(versions_found[2].is_none());
        Ok(())
    }
}
