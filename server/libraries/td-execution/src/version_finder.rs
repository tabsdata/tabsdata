//
// Copyright 2024 Tabs Data Inc.
//

use crate::dataset::Dataset;
use crate::error::ExecutionPlannerError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use getset::Getters;
use sqlx::{FromRow, SqliteConnection};
use td_common::dataset::DatasetRef;
use td_common::id::Id;

#[derive(Debug, Getters, FromRow)]
#[getset(get = "pub")]
pub struct DsDataVersion {
    id: String,
}

impl DsDataVersion {
    pub fn new(id: &str) -> Self {
        Self { id: id.to_string() }
    }
}

#[derive(Debug, Getters, FromRow)]
#[getset(get = "pub")]
pub struct DsFunction {
    id: String,
}

impl DsFunction {
    pub fn new(id: &str) -> Self {
        Self { id: id.to_string() }
    }
}

#[derive(Debug, Getters, FromRow)]
#[getset(get = "pub")]
pub struct DsTable {
    id: String,
}

impl DsTable {
    pub fn new(id: &str) -> Self {
        Self { id: id.to_string() }
    }
}

pub type Limit = isize;
pub type Offset = isize;

pub trait IntoLimitAndOffset {
    fn into_limit_and_offset(self) -> Result<(Limit, Offset), ExecutionPlannerError>;
}

impl IntoLimitAndOffset for (isize, isize) {
    /// Converts a tuple of two isize into a tuple of limit and offset.
    /// The limit is the absolute difference between the two values, and the offset is the minimum.
    /// The sign of the limit is the direction of the range, and the offset is the starting point.
    /// [offset, offset + limit] is the range. Offset the newest, offset-limit the oldest.
    /// Note that from and to should be always zero or negative, given that [`Version::Head`] is.
    /// For example:
    /// Head(0),Head(0) -> (1, 0)
    /// Head(-2),Head(-3) -> (-2, 2)
    /// Head(-3),Head(-2) -> (2, 2)
    fn into_limit_and_offset(self) -> Result<(Limit, Offset), ExecutionPlannerError> {
        let (from, to) = (self.0, self.1);
        if from > 0 || to > 0 {
            // Should never reach this point.
            return Err(ExecutionPlannerError::InvalidVersionRange(from, to));
        }

        if from > to {
            let limit = (to - from) - 1; // -1 because the range is inclusive
            let offset = from; // Greatest as offset
            Ok((limit, offset))
        } else {
            let limit = (to - from) + 1; // +1 because the range is inclusive
            let offset = to; // Greatest as offset
            Ok((limit, offset))
        }
    }
}

#[async_trait]
pub trait VersionFinder: Send + Sync {
    /// Returns the function id of the dataset.
    async fn function_id(&mut self) -> Result<&DsFunction, ExecutionPlannerError>;

    /// Returns the table id of the table, if it exists.
    async fn table_id(
        &mut self,
        table_name: Option<&String>,
    ) -> Result<Option<&DsTable>, ExecutionPlannerError>;

    /// Returns the offset of a fixed version in the dataset. It should always be zero or negative,
    /// as it is relative to the latest, same as in [`Version::Head`].
    async fn offset_for_fixed(
        &mut self,
        fixed_id: &DsDataVersion,
    ) -> Result<isize, ExecutionPlannerError>;

    /// Returns the fixed version of the given data_version, if it exists.
    async fn fixed(&mut self, id: &Id) -> Result<DsDataVersion, ExecutionPlannerError>;

    /// Returns a range of versions of the dataset. The vector will only contain
    /// the versions that exist in the range. Offset is the newest version, and offset+limit is the oldest.
    /// The vector should be always ordered from newest to oldest (offset and back). The range is inclusive.
    async fn head_range(
        &mut self,
        limit: Limit,
        offset: Offset,
    ) -> Result<Vec<DsDataVersion>, ExecutionPlannerError>;
}

#[derive(Debug, Default)]
enum CacheState<T> {
    Cached(T),
    #[default]
    Missing,
    Skip,
}

impl<T> CacheState<T> {
    fn is_missing(&self) -> bool {
        matches!(self, CacheState::Missing)
    }

    fn get(&self) -> Option<&T> {
        match self {
            CacheState::Cached(value) => Some(value),
            _ => None,
        }
    }
}

pub struct SqlVersionFinder<'a> {
    connection: &'a mut SqliteConnection,
    dataset: &'a Dataset,
    trigger_time: &'a DateTime<Utc>,
    function_id: CacheState<DsFunction>,
    table_id: CacheState<DsTable>,
}

impl<'a> SqlVersionFinder<'a> {
    pub fn new(
        connection: &'a mut SqliteConnection,
        dataset: &'a Dataset,
        trigger_time: &'a DateTime<Utc>,
    ) -> Self {
        Self {
            connection,
            dataset,
            trigger_time,
            function_id: CacheState::default(),
            table_id: CacheState::default(),
        }
    }
}

#[async_trait]
impl VersionFinder for SqlVersionFinder<'_> {
    async fn function_id(&mut self) -> Result<&DsFunction, ExecutionPlannerError> {
        if self.function_id.is_missing() {
            // We can select from current because we always use the latest dataset version to execute.
            const SELECT_DS_FUNCTION_ID_SQL: &str = r#"
                SELECT
                    id
                FROM ds_current_functions
                WHERE dataset_id = ?1
            "#;

            let function_id = sqlx::query_as(SELECT_DS_FUNCTION_ID_SQL)
                .bind(self.dataset.dataset())
                .fetch_one(&mut *self.connection)
                .await
                .map_err(ExecutionPlannerError::CouldNotFetchFunction)?;
            self.function_id = CacheState::Cached(function_id);
        }

        match self.function_id.get() {
            Some(function_id) => Ok(function_id),
            _ => Err(ExecutionPlannerError::CouldNotFindFunctionId), // This should never happen.
        }
    }

    async fn table_id(
        &mut self,
        table_name: Option<&String>,
    ) -> Result<Option<&DsTable>, ExecutionPlannerError> {
        if self.table_id.is_missing() {
            self.table_id = match table_name {
                Some(table_name) => {
                    // We can select from current because we always use the latest dataset version to execute.
                    const SELECT_DS_TABLE_SQL: &str = r#"
                        SELECT
                            id
                        FROM ds_current_tables
                        WHERE name = ?1 AND dataset_id = ?2
                    "#;

                    let table = sqlx::query_as(SELECT_DS_TABLE_SQL)
                        .bind(table_name)
                        .bind(self.dataset.dataset())
                        .fetch_one(&mut *self.connection)
                        .await
                        .map_err(ExecutionPlannerError::CouldNotFetchTable)?;

                    CacheState::Cached(table)
                }
                // We do not want to query the table if we found out it doesn't exist.
                None => CacheState::Skip,
            };
        }
        Ok(self.table_id.get())
    }

    async fn offset_for_fixed(
        &mut self,
        fixed_id: &DsDataVersion,
    ) -> Result<isize, ExecutionPlannerError> {
        // We can do this because id is sortable by date. Looking for greater than the given id in
        // the trigger range, it looks for the most recent data_version at the trigger time.
        const GET_OFFSET_SQL: &str = r#"
            SELECT COUNT(*)
            FROM ds_data_versions_available
            WHERE dataset_id = ?1 AND triggered_on <= ?2 AND id > ?3
        "#;

        let offset: i64 = sqlx::query_scalar(GET_OFFSET_SQL)
            .bind(self.dataset.dataset())
            .bind(self.trigger_time)
            .bind(fixed_id.id())
            .fetch_one(&mut *self.connection)
            .await
            .map_err(ExecutionPlannerError::CouldNotFetchTable)?;
        let offset = -offset as isize;
        Ok(offset)
    }

    async fn fixed(&mut self, id: &Id) -> Result<DsDataVersion, ExecutionPlannerError> {
        // This is used to assert that the fixed version exists for the dataset in the trigger range.
        const SELECT_FIXED_DS_DATA_VERSION: &str = r#"
            SELECT
                id
            FROM ds_data_versions_available
            WHERE dataset_id = ?1 AND triggered_on <= ?2 AND id = ?3
        "#;

        let version = sqlx::query_as(SELECT_FIXED_DS_DATA_VERSION)
            .bind(self.dataset.dataset())
            .bind(self.trigger_time)
            .bind(id.to_string())
            .fetch_one(&mut *self.connection)
            .await
            .map_err(ExecutionPlannerError::CouldNotFetchDataVersion)?;
        Ok(version)
    }

    async fn head_range(
        &mut self,
        limit: Limit,
        offset: Offset,
    ) -> Result<Vec<DsDataVersion>, ExecutionPlannerError> {
        // By using <= we can get the head/plan version that was previously generated automatically,
        // because trigger_time is unique across execution plans.
        const SELECT_HEAD_RANGE_DS_DATA_VERSION: &str = r#"
            SELECT
                id
            FROM ds_data_versions_available
            WHERE dataset_id = ?1 AND triggered_on <= ?2
            LIMIT ?3 OFFSET ?4
        "#;

        let versions: Vec<_> = sqlx::query_as(SELECT_HEAD_RANGE_DS_DATA_VERSION)
            .bind(self.dataset.dataset())
            .bind(self.trigger_time)
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(&mut *self.connection)
            .await
            .map_err(ExecutionPlannerError::CouldNotFetchDataVersion)?;
        Ok(versions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_common::id;
    use td_common::time::UniqueUtc;
    use td_objects::test_utils::get_table_id::get_table_id;
    use td_objects::test_utils::seed_collection::seed_collection;
    use td_objects::test_utils::seed_data_version::seed_data_version;
    use td_objects::test_utils::seed_dataset::seed_dataset;
    use td_objects::test_utils::seed_user::seed_user;

    #[test]
    fn test_into_limit_and_offset() {
        // from, to -> limit, offset
        assert_eq!((0, 0).into_limit_and_offset().unwrap(), (1, 0));
        assert_eq!((0, -1).into_limit_and_offset().unwrap(), (-2, 0));
        assert_eq!((-1, 0).into_limit_and_offset().unwrap(), (2, 0));
        assert_eq!((-1, -1).into_limit_and_offset().unwrap(), (1, -1));
        assert_eq!((-1, -2).into_limit_and_offset().unwrap(), (-2, -1));
        assert_eq!((-2, -1).into_limit_and_offset().unwrap(), (2, -1));
        assert_eq!((-5, 0).into_limit_and_offset().unwrap(), (6, 0));
        assert_eq!((0, -5).into_limit_and_offset().unwrap(), (-6, 0));
    }

    #[test]
    fn test_into_limit_and_offset_error() {
        assert!(matches!(
            (1, 0).into_limit_and_offset(),
            Err(ExecutionPlannerError::InvalidVersionRange(1, 0))
        ));
        assert!(matches!(
            (0, 1).into_limit_and_offset(),
            Err(ExecutionPlannerError::InvalidVersionRange(0, 1))
        ));
        assert!(matches!(
            (1, 1).into_limit_and_offset(),
            Err(ExecutionPlannerError::InvalidVersionRange(1, 1))
        ));
    }

    #[test]
    fn test_cache_state() {
        let mut cache = CacheState::default();
        assert!(cache.is_missing());
        assert!(cache.get().is_none());

        cache = CacheState::Cached(42);
        assert!(!cache.is_missing());
        assert_eq!(cache.get(), Some(&42));

        cache = CacheState::Skip;
        assert!(!cache.is_missing());
        assert!(cache.get().is_none());
    }

    #[tokio::test]
    async fn test_function_id() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (dataset_id, function_id) = seed_dataset(
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
        let dataset = Dataset::new(&collection_id.to_string(), &dataset_id.to_string());
        let trigger_time = UniqueUtc::now_millis().await;

        let mut finder = SqlVersionFinder::new(&mut conn, &dataset, &trigger_time);
        let resolved_id = finder.function_id().await.unwrap();

        assert_eq!(resolved_id.id, function_id.to_string());
        match &finder.function_id {
            CacheState::Cached(resolved_id) => assert_eq!(resolved_id.id, function_id.to_string()),
            _ => panic!("Expected function_id to be cached"),
        }

        let resolved_id = finder.function_id().await.unwrap();
        assert_eq!(resolved_id.id, function_id.to_string());
    }

    #[tokio::test]
    async fn test_function_id_multiple_functions() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (dataset_id, function_id) = seed_dataset(
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

        let (_dataset_id, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d1",
            &["t1"],
            &[],
            &[],
            "hash",
        )
        .await;

        let mut conn = db.acquire().await.unwrap();
        let dataset = Dataset::new(&collection_id.to_string(), &dataset_id.to_string());
        let trigger_time = UniqueUtc::now_millis().await;

        let mut finder = SqlVersionFinder::new(&mut conn, &dataset, &trigger_time);
        let resolved_id = finder.function_id().await.unwrap();

        assert_eq!(resolved_id.id, function_id.to_string());
        match &finder.function_id {
            CacheState::Cached(resolved_id) => assert_eq!(resolved_id.id, function_id.to_string()),
            _ => panic!("Expected function_id to be cached"),
        }

        let resolved_id = finder.function_id().await.unwrap();
        assert_eq!(resolved_id.id, function_id.to_string());
    }

    #[tokio::test]
    async fn test_table_id_some() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (dataset_id, function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0", "t1", "t2"],
            &[],
            &[],
            "hash",
        )
        .await;
        let table_id = get_table_id(&db, &function_id, "t0").await;

        let mut conn = db.acquire().await.unwrap();
        let dataset = Dataset::new(&collection_id.to_string(), &dataset_id.to_string());
        let trigger_time = UniqueUtc::now_millis().await;

        let mut finder = SqlVersionFinder::new(&mut conn, &dataset, &trigger_time);
        let resolved_id = finder.table_id(Some(&"t0".to_string())).await.unwrap();

        assert!(resolved_id.is_some());
        let resolved_id = resolved_id.unwrap();
        assert_eq!(resolved_id.id, table_id.to_string());
        match &finder.table_id {
            CacheState::Cached(resolved_id) => assert_eq!(resolved_id.id, table_id.to_string()),
            _ => panic!("Expected table_id to be cached"),
        }

        let resolved_id = finder.table_id(Some(&"t0".to_string())).await.unwrap();
        assert!(resolved_id.is_some());
        let resolved_id = resolved_id.unwrap();
        assert_eq!(resolved_id.id, table_id.to_string());
    }

    #[tokio::test]
    async fn test_table_id_none() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (dataset_id, _function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0", "t1", "t2"],
            &[],
            &[],
            "hash",
        )
        .await;

        let mut conn = db.acquire().await.unwrap();
        let dataset = Dataset::new(&collection_id.to_string(), &dataset_id.to_string());
        let trigger_time = UniqueUtc::now_millis().await;

        let mut finder = SqlVersionFinder::new(&mut conn, &dataset, &trigger_time);
        let resolved_id = finder.table_id(None).await.unwrap();

        assert!(resolved_id.is_none());
        assert!(matches!(&finder.table_id, CacheState::Skip));

        let resolved_id = finder.table_id(None).await.unwrap();
        assert!(resolved_id.is_none());
        assert!(matches!(&finder.table_id, CacheState::Skip));
    }

    #[tokio::test]
    async fn test_offset_for_fixed() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (dataset_id, function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0", "t1", "t2"],
            &[],
            &[],
            "hash",
        )
        .await;

        // We create 2 data versions to test the offset.
        let head_1 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &id::id(),
            &id::id(),
            "M",
            "D",
        )
        .await;
        let ds_head_1 = DsDataVersion::new(&head_1.to_string());
        let head_0 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &id::id(),
            &id::id(),
            "M",
            "D",
        )
        .await;
        let ds_head = DsDataVersion::new(&head_0.to_string());

        let mut conn = db.acquire().await.unwrap();
        let dataset = Dataset::new(&collection_id.to_string(), &dataset_id.to_string());
        let trigger_time = UniqueUtc::now_millis().await;

        let mut finder = SqlVersionFinder::new(&mut conn, &dataset, &trigger_time);

        let offset = finder.offset_for_fixed(&ds_head).await.unwrap();
        assert_eq!(offset, 0);

        let offset_1 = finder.offset_for_fixed(&ds_head_1).await.unwrap();
        assert_eq!(offset_1, -1);
    }

    #[tokio::test]
    async fn test_fixed() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (dataset_id, function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0", "t1", "t2"],
            &[],
            &[],
            "hash",
        )
        .await;

        // We create 2 data versions to test the fixed versions are properly found.
        let head_1 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &id::id(),
            &id::id(),
            "M",
            "D",
        )
        .await;
        let head_0 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &id::id(),
            &id::id(),
            "M",
            "D",
        )
        .await;

        let mut conn = db.acquire().await.unwrap();
        let dataset = Dataset::new(&collection_id.to_string(), &dataset_id.to_string());
        let trigger_time = UniqueUtc::now_millis().await;

        let mut finder = SqlVersionFinder::new(&mut conn, &dataset, &trigger_time);

        let ds_data_version = finder.fixed(&head_1).await.unwrap();
        assert_eq!(ds_data_version.id(), &head_1.to_string());

        let ds_data_version = finder.fixed(&head_0).await.unwrap();
        assert_eq!(ds_data_version.id(), &head_0.to_string());
    }

    #[tokio::test]
    async fn test_head_range() {
        let db = td_database::test_utils::db().await.unwrap();
        let user_id = seed_user(&db, None, "u0", true).await;
        let collection_id = seed_collection(&db, None, "ds0").await;
        let (dataset_id, function_id) = seed_dataset(
            &db,
            Some(user_id.to_string()),
            &collection_id,
            "d0",
            &["t0", "t1", "t2"],
            &[],
            &[],
            "hash",
        )
        .await;

        // We create 2 data versions to test the range versions are properly found.
        let head_1 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &id::id(),
            &id::id(),
            "M",
            "D",
        )
        .await;
        let head_0 = seed_data_version(
            &db,
            &collection_id,
            &dataset_id,
            &function_id,
            &id::id(),
            &id::id(),
            "M",
            "D",
        )
        .await;

        let mut conn = db.acquire().await.unwrap();
        let dataset = Dataset::new(&collection_id.to_string(), &dataset_id.to_string());
        let trigger_time = UniqueUtc::now_millis().await;

        let mut finder = SqlVersionFinder::new(&mut conn, &dataset, &trigger_time);

        let ds_data_version = finder.head_range(1, 0).await.unwrap();
        assert_eq!(ds_data_version.len(), 1);
        assert_eq!(ds_data_version[0].id(), &head_0.to_string());

        let ds_data_version = finder.head_range(2, 0).await.unwrap();
        assert_eq!(ds_data_version.len(), 2);
        assert_eq!(ds_data_version[0].id(), &head_0.to_string());
        assert_eq!(ds_data_version[1].id(), &head_1.to_string());

        let ds_data_version = finder.head_range(3, 0).await.unwrap();
        assert_eq!(ds_data_version.len(), 2);
        assert_eq!(ds_data_version[0].id(), &head_0.to_string());
        assert_eq!(ds_data_version[1].id(), &head_1.to_string());

        let ds_data_version = finder.head_range(2, 1).await.unwrap();
        assert_eq!(ds_data_version.len(), 1);
        assert_eq!(ds_data_version[0].id(), &head_1.to_string());

        let ds_data_version = finder.head_range(1, 2).await.unwrap();
        assert_eq!(ds_data_version.len(), 0);
    }
}
