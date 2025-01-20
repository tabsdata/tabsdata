//
//  Copyright 2024 Tabs Data Inc.
//

use chrono::{DateTime, Utc};
use polars::prelude::PolarsError;
use td_common::uri::TdUriError;
use td_error::td_error;
use td_storage::StorageError;

#[td_error]
pub enum DatasetError {
    #[error("Could not find the following fixed versions: {0}")]
    FixedVersionDependenciesNotFound(String) = 0,
    #[error("Invalid table names, it must be an alphanumeric (plus _ and -) word starting with a character up to  characters long. Invalid names: {0}")]
    InvalidTableNames(String) = 1,
    #[error("Dataset already exits: td://{0}/{1}")]
    DatasetAlreadyExists(String, String) = 2,
    #[error("Could not find datasets: {0}")]
    CouldNotFindDatasets(String) = 3,
    #[error("Could not find collections: {0}")]
    CouldNotFindCollections(String) = 4,
    #[error("Invalid dependency URIs: {0}")]
    InvalidDependencyUris(String) = 5,
    #[error("Non table dependency URIs: {0}")]
    NonTableDependencyUris(String) = 6,
    #[error("Invalid trigger URI: {0}")]
    InvalidTriggerUri(#[from] TdUriError) = 7,
    #[error("Could not find tables: {0}")]
    CouldNotFindTables(String) = 8,
    #[error("Function bundle upload failed")]
    FunctionBundleUploadFailed = 9,
    #[error("Function bundle buffering failed: {0}")]
    FunctionBundleBufferingFailed(#[source] std::io::Error) = 10,
    #[error("Function bundle hash mismatch")]
    FunctionBundleHashMismatch = 11,
    #[error("Function not found")]
    FunctionNotFound = 12,
    #[error("Function cannot set a table created by itself as a trigger")]
    TriggerCannotBeSelf = 13,
    #[error("Function has duplicate table names")]
    DuplicateTableNames = 14,
    #[error("Collection not found {0}")]
    CollectionNotFound(String) = 15,
    #[error("Trigger URI must be a dataset tabsdata URI: {0}")]
    TriggerUriMustBeADatasetUri(String) = 16,
    #[error("Trigger URI cannot have versions: {0}")]
    TriggerUriCannotHaveVersions(String) = 17,
    #[error("Dataset not found {0}")]
    DatasetNotFound(String) = 18,
    #[error("Dependencies with invalid ranges (left boundary must be lower than the right boundary for relative versions): {0}")]
    DependenciesWithInvalidRanges(String) = 20,
    #[error("The following table(s) are already defined in the collection: {0}")]
    TablesAlreadyDefinedInCollection(String) = 21,
    #[error("Table(s) '{0}' not found")]
    TablesNotFound(String) = 22,
    #[error("Commit '{0}' does not exist")]
    CommitIdDoesNotExists(String) = 23,
    #[error("Table has no data at commit '{0}'")]
    TableHasNoDataAtCommit(String) = 24,
    #[error("Table has no data at datetime '{0}'")]
    TableHasNoDataAtTime(DateTime<Utc>) = 25,

    #[error("Fixed version not found")]
    FixedVersionNotFound = 1000,
    #[error("HEAD Relative version not found")]
    HeadRelativeVersionNotFound = 1001,
    #[error("Table not found in the given version")]
    TableNotFound = 1002,
    #[error("Execution plan not found")]
    ExecutionPlanNotFound = 1003,

    #[error("The function bundle has already been uploaded")]
    FunctionBundleAlreadyUploaded = 2000,

    #[error("Function bundle save failed: {0}")]
    FunctionBundleSaveFailed(#[source] StorageError) = 5001,
    #[error("Uri should have a single version: {0}")]
    UriShouldHaveASingleVersion(String) = 5002,
    #[error("Sql error: {0}")]
    SqlError(#[source] sqlx::Error) = 5003,
    #[error("Invalid version found: {0}")]
    InvalidVersionFound(String) = 5004,

    #[error("Could not create storage configs: {0}")]
    CouldNotCreateStorageConfig(#[source] PolarsError) = 5005,
    #[error("Could not create lazy frame to get schema: {0}")]
    CouldNoCreateLazyFrameToGetSchema(#[source] PolarsError) = 5006,
    #[error("Could not get schema: {0}")]
    CouldNotGetSchema(#[source] PolarsError) = 5007,
    #[error("Could not create lazy frame to get sample: {0}")]
    CouldNoCreateLazyFrameToGetSample(#[source] PolarsError) = 5008,
    #[error("Could not get the offset/limit for the table, error: {0}")]
    CouldNotGetOffsetLimit(#[source] PolarsError) = 5009,
    #[error("Could not create Parquet file to get sample, error: {0}")]
    CouldNotCreateParquetToGetSample(#[source] PolarsError) = 5010,
}
