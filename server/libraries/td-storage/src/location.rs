//
// Copyright 2025 Tabs Data Inc.
//

use crate::{SPath, StorageError};
use std::fmt::Debug;
use td_objects::types::basic::{
    BundleId, CollectionId, BasePath, StageId, FunctionVersionId, Partition,
    PhysicalTableDataVersionId, StorageVersion, TableId, TableVersionId, TransactionId,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub enum StorageLocation {
    /// The [`StorageLocation`] creates storage URIS for the different types of data tabsdata stores.
    ///
    /// It is an enum to allow adding URI creation strategies and using them side to side in a
    /// backwards compatible way.
    ///
    /// Variant `V1` produces [`SPath`] in the following format (words in uppercase are placeholders
    /// for IDs):
    ///
    /// * LOCATION: base path (collection / function-version override, or the value
    ///   stamped on the data version / bundle)
    /// * COLLECTION: collection ID
    /// * FUNCTION: function ID
    /// * FUNCTION_VERSION: function_version ID
    /// * TRANSACTION: transaction ID
    /// * DATA_VERSION: data_version ID
    /// * TABLE: table ID
    /// * TABLE_VERSION: table_version ID
    ///
    /// Layouts:
    /// * `/LOCATION`
    /// * `/LOCATION/c/COLLECTION`
    /// * `/LOCATION/c/COLLECTION/x/TRANSACTION/f/FUNCTION_VERSION` (physical_function_run contents)
    /// * `/LOCATION/c/COLLECTION/d/DATA_VERSION/t/TABLE/TABLE_VERSION.t`
    /// * `/LOCATION/c/COLLECTION/d/DATA_VERSION/t/TABLE/TABLE_VERSION/p/PARTITION.p`
    /// * `/LOCATION/bundles/c/COLLECTION/f/BUNDLE.tgz`
    /// * `/LOCATION/stages/c/COLLECTION/f/FUNCTION`
    /// * `/LOCATION/checkpoints/c/COLLECTION/f/FUNCTION`
    #[default]
    V1,
}

impl StorageLocation {
    /// Return the current version of the storage location.
    pub fn current() -> Self {
        Self::default()
    }

    /// Return a builder for the storage location variant
    pub fn builder(&self, location: &BasePath) -> LocationBuilder {
        match self {
            StorageLocation::V1 => {
                LocationBuilder::new(location.clone(), Box::new(V1LocationBuilder))
            }
        }
    }
}

/// 1:1 mapping with [`StorageVersion`]: each variant here has the same-named
/// variant there. Infallible because the variant sets are kept in sync.
impl From<&StorageVersion> for StorageLocation {
    fn from(value: &StorageVersion) -> Self {
        match value {
            StorageVersion::V1 => Self::V1,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct LocationBuilderInfo {
    location: BasePath,
    collection: Option<String>,
    bundle: Option<String>,
    stage: Option<String>,
    checkpoint: Option<String>,
    data_version: Option<String>,
    transaction: Option<String>,
    function_version: Option<String>,
    table: Option<String>,
    table_version: Option<String>,
    partition: Option<String>,
}

/// Builder for the table location.
#[derive(Debug)]
pub struct TableBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl TableBuilder {
    /// Set the table name for the table location.
    pub fn table(&mut self, table: &TableId, table_version: &TableVersionId) -> &mut Self {
        self.info.table = Some(table.to_string());
        self.info.table_version = Some(table_version.to_string());
        self.info.partition = None;
        self
    }

    /// Set the table name and partition for the table location.
    pub fn partition(
        &mut self,
        table: &TableId,
        table_version: &TableVersionId,
        partition: &Partition,
    ) -> &mut Self {
        self.info.table = Some(table.to_string());
        self.info.table_version = Some(table_version.to_string());
        self.info.partition = Some(partition.to_string());
        self
    }

    /// Build the table location.
    pub fn build(&self) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta table location.
    pub fn build_meta(
        &self,
        meta_name: impl Into<String>,
    ) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

/// Builder for the data location.
#[derive(Debug)]
pub struct DataBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl DataBuilder {
    /// Set the version for the data location.
    pub fn data(&mut self, data_version: &PhysicalTableDataVersionId) -> &mut Self {
        self.info.data_version = Some(data_version.to_string());
        self
    }

    /// Return a [`TableBuilder`] based on the [`DataBuilder`]
    /// with the given table name.
    pub fn table(self, table: &TableId, table_version: &TableVersionId) -> TableBuilder {
        let mut builder = TableBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.table(table, table_version);
        builder
    }

    /// Return a [`TableBuilder`] based on the [`DataBuilder`]
    /// with the given table name and partition.
    pub fn partition(
        self,
        table: &TableId,
        table_version: &TableVersionId,
        partition: &Partition,
    ) -> TableBuilder {
        let mut builder = TableBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.partition(table, table_version, partition);
        builder
    }

    /// Build the data location.
    pub fn build(&self) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta data location.
    pub fn build_meta(
        &self,
        meta_name: impl Into<String>,
    ) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

/// Builder for the function location.
#[derive(Debug)]
pub struct FunctionBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl FunctionBuilder {
    /// Set the function and function version.
    pub fn function(&mut self, bundle: &BundleId) -> &mut Self {
        self.info.bundle = Some(bundle.to_string());
        self
    }

    /// Build the function location.
    pub fn build(&self) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta function location.
    pub fn build_meta(
        &self,
        meta_name: impl Into<String>,
    ) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

#[derive(Debug)]
pub struct StageBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl StageBuilder {
    /// Set the function.
    pub fn function(&mut self, function: &StageId) -> &mut Self {
        self.info.stage = Some(function.to_string());
        self
    }

    /// Build the stage location.
    pub fn build(&self) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta stage location.
    pub fn build_meta(
        &self,
        meta_name: impl Into<String>,
    ) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

#[derive(Debug)]
pub struct CheckpointBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl CheckpointBuilder {
    /// Set the function.
    pub fn function(&mut self, function: &StageId) -> &mut Self {
        self.info.checkpoint = Some(function.to_string());
        self
    }

    /// Build the checkpoint location.
    pub fn build(&self) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta checkpoint location.
    pub fn build_meta(
        &self,
        meta_name: impl Into<String>,
    ) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

/// Builder for the collection location.
#[derive(Debug)]
pub struct CollectionBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl CollectionBuilder {
    /// Set the collection name for the collection location.
    pub fn collection(&mut self, collection: &CollectionId) -> &mut Self {
        self.info.collection = Some(collection.to_string());
        self
    }

    /// Return a [`FunctionBuilder`] based on the [`CollectionBuilder`]
    pub fn function(self, bundle: &BundleId) -> FunctionBuilder {
        let mut builder = FunctionBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.function(bundle);
        builder
    }

    /// Return a [`StageBuilder`] based on the [`CollectionBuilder`]
    pub fn stage(self, function: &StageId) -> StageBuilder {
        let mut builder = StageBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.function(function);
        builder
    }

    /// Return a [`CheckpointBuilder`] based on the [`CollectionBuilder`]
    pub fn checkpoint(self, function: &StageId) -> CheckpointBuilder {
        let mut builder = CheckpointBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.function(function);
        builder
    }

    /// Return a [`DataBuilder`] based on the [`CollectionBuilder`]
    pub fn data(self, data_version: &PhysicalTableDataVersionId) -> DataBuilder {
        let mut builder = DataBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.data(data_version);
        builder
    }

    /// Return a [`TransactionBuilder`] based on the [`CollectionBuilder`]
    pub fn transaction(self, transaction: &TransactionId) -> TransactionBuilder {
        let mut builder = TransactionBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.transaction(transaction);
        builder
    }

    /// Build the collection location.
    pub fn build(&self) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta collection location.
    pub fn build_meta(
        &self,
        meta_name: impl Into<String>,
    ) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

/// Builder for the transaction location.
#[derive(Debug)]
pub struct TransactionBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl TransactionBuilder {
    /// Set the transaction name for the transaction location.
    pub fn transaction(&mut self, transaction: &TransactionId) -> &mut Self {
        self.info.transaction = Some(transaction.to_string());
        self
    }

    /// Return a [`FunctionVersionBuilder`] based on the [`CollectionBuilder`]
    pub fn function_version(self, version: &FunctionVersionId) -> FunctionVersionBuilder {
        let mut builder = FunctionVersionBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.function_version(version);
        builder
    }

    /// Build the transaction location.
    pub fn build(&self) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta collection location.
    pub fn build_meta(
        &self,
        meta_name: impl Into<String>,
    ) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

/// Builder for the collection location.
#[derive(Debug)]
pub struct FunctionVersionBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl FunctionVersionBuilder {
    /// Set the function_version name for the function_version location.
    pub fn function_version(&mut self, function_version: &FunctionVersionId) -> &mut Self {
        self.info.function_version = Some(function_version.to_string());
        self
    }

    /// Build the function_version location.
    pub fn build(&self) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta collection location.
    pub fn build_meta(
        &self,
        meta_name: impl Into<String>,
    ) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

/// Builder for the location.
#[derive(Debug)]
pub struct LocationBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl LocationBuilder {
    fn new(location: BasePath, version_builder: Box<dyn VersionLocationBuilder>) -> Self {
        Self {
            info: LocationBuilderInfo {
                location,
                ..Default::default()
            },
            version_builder,
        }
    }

    /// Set the location for the location builder.
    pub fn location(&mut self, location: &BasePath) -> &mut Self {
        self.info.location = location.clone();
        self
    }

    /// Return a [`CollectionBuilder`] based on the [`LocationBuilder`]
    /// for the given collection.
    pub fn collection(self, collection: &CollectionId) -> CollectionBuilder {
        let mut builder = CollectionBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.collection(collection);
        builder
    }

    /// Build the location.
    pub fn build(&self) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta location.
    pub fn build_meta(
        &self,
        meta_name: impl Into<String>,
    ) -> Result<(SPath, StorageLocation), StorageError> {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

/// Trait to be implemented for each variant of the [`LocationStorage`] enum.
///
/// It is used to build the location based on the information provided.
trait VersionLocationBuilder: Debug {
    /// Build the location based on the information provided.
    fn build(
        &self,
        info: &LocationBuilderInfo,
        postfix: Option<&str>,
    ) -> Result<(SPath, StorageLocation), StorageError>;
}

/// Builder for the V1 version.
#[derive(Debug)]
struct V1LocationBuilder;

impl VersionLocationBuilder for V1LocationBuilder {
    fn build(
        &self,
        info: &LocationBuilderInfo,
        postfix: Option<&str>,
    ) -> Result<(SPath, StorageLocation), StorageError> {
        let mut path = SPath::parse(info.location.as_str())?;
        if let Some(collection) = &info.collection {
            path = path.child("c")?.child(collection)?;
            if let Some(bundle) = &info.bundle {
                // Bundles are stored at {LOCATION}/bundles/c/COLLECTION/f/BUNDLE.tgz
                // we need to recalculate the base path accordingly (prefixed by the
                // base path, not the collection segment built above).
                path = SPath::parse(info.location.as_str())?
                    .child("bundles")?
                    .child("c")?
                    .child(collection)?;

                path = path.child("f")?.child(&format!("{bundle}.tgz"))?;
            } else if let Some(stage) = &info.stage {
                // Stages are stored at {LOCATION}/stages/c/COLLECTION/f
                // we need to recalculate the base path accordingly.
                path = SPath::parse(info.location.as_str())?
                    .child("stages")?
                    .child("c")?
                    .child(collection)?;
                path = path.child("f")?.child(stage)?;
            } else if let Some(stage) = &info.checkpoint {
                // Checkpoints are stored at {LOCATION}/checkpoints/c/COLLECTION/f
                path = SPath::parse(info.location.as_str())?
                    .child("checkpoints")?
                    .child("c")?
                    .child(collection)?;
                path = path.child("f")?.child(stage)?;
            } else if let Some(data_version) = &info.data_version {
                // function always is present if data is present
                path = path.child("d")?.child(data_version)?;
                if let Some(table) = &info.table {
                    // `table_version` is set in lockstep with `table` by
                    // `TableBuilder::table` / `partition`.
                    let table_version = info.table_version.as_ref().ok_or_else(|| {
                        StorageError::InvalidPath(
                            path.to_string(),
                            "table set without table_version".to_string(),
                        )
                    })?;
                    path = path.child("t")?.child(table)?;
                    if let Some(partition) = &info.partition {
                        path = path
                            .child(table_version)?
                            .child("p")?
                            .child(&format!("{partition}.p"))?;
                    } else {
                        path = path.child(&format!("{table_version}.t"))?;
                    }
                }
            } else if let Some(transaction) = &info.transaction {
                path = path.child("x")?.child(transaction)?;
                if let Some(function_version) = &info.function_version {
                    path = path.child("f")?.child(function_version)?;
                }
            }
        }
        if let Some(postfix) = postfix {
            let name = path.filename().ok_or_else(|| {
                StorageError::InvalidPath(
                    path.to_string(),
                    "cannot append postfix to root path".to_string(),
                )
            })?;
            path = path
                .parent()
                .ok_or_else(|| {
                    StorageError::InvalidPath(
                        path.to_string(),
                        "cannot take parent of root path".to_string(),
                    )
                })?
                .child(&format!("{name}-{postfix}"))?
        }
        Ok((path, StorageLocation::V1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_error::TdError;
    use td_objects::types::basic::{
        BundleId, CollectionId, BasePath, FunctionVersionId, Partition,
        PhysicalTableDataVersionId, TableId, TableVersionId, TransactionId,
    };

    #[test]
    fn test_data_location_current_version() {
        assert!(matches!(StorageLocation::current(), StorageLocation::V1))
    }

    #[test]
    fn test_location_current_builder_version() -> Result<(), TdError> {
        let data_location = BasePath::try_from("/")?;
        assert!(matches!(
            StorageLocation::current()
                .builder(&data_location)
                .build()?
                .1,
            StorageLocation::V1
        ));
        Ok(())
    }

    #[test]
    fn test_location_builder_v2() -> Result<(), TdError> {
        let data_location = BasePath::try_from("/L")?;
        let mut builder = StorageLocation::V1.builder(&data_location);
        assert_eq!(builder.build()?.0, SPath::parse("/L")?);
        assert_eq!(builder.build_meta("foo")?.0, SPath::parse("/L-foo.meta")?);
        let data_location = BasePath::try_from("/LL")?;
        builder.location(&data_location);
        assert_eq!(builder.build()?.0, SPath::parse("/LL")?);
        Ok(())
    }

    #[test]
    fn test_collection_builder_v2() -> Result<(), TdError> {
        let data_location = BasePath::try_from("/L")?;
        let collection = CollectionId::default();
        let mut builder = StorageLocation::V1
            .builder(&data_location)
            .collection(&collection);
        assert_eq!(
            builder.build()?.0,
            SPath::parse(format!("/L/c/{collection}"))?
        );
        assert_eq!(
            builder.build_meta("foo")?.0,
            SPath::parse(format!("/L/c/{collection}-foo.meta"))?
        );
        let collection = CollectionId::default();
        builder.collection(&collection);
        assert_eq!(
            builder.build()?.0,
            SPath::parse(format!("/L/c/{collection}"))?
        );
        Ok(())
    }

    #[test]
    fn test_function_builder_v2() -> Result<(), TdError> {
        let data_location = BasePath::try_from("/L")?;
        let collection = CollectionId::default();
        let bundle = BundleId::default();
        let mut builder = StorageLocation::V1
            .builder(&data_location)
            .collection(&collection)
            .function(&bundle);
        assert_eq!(
            builder.build()?.0,
            SPath::parse(format!("/L/bundles/c/{collection}/f/{bundle}.tgz"))?
        );
        assert_eq!(
            builder.build_meta("foo")?.0,
            SPath::parse(format!("/L/bundles/c/{collection}/f/{bundle}.tgz-foo.meta"))?
        );

        let bundle = BundleId::default();
        builder.function(&bundle);
        assert_eq!(
            builder.build()?.0,
            SPath::parse(format!("/L/bundles/c/{collection}/f/{bundle}.tgz"))?
        );
        Ok(())
    }

    #[test]
    fn test_stage_builder_v2() -> Result<(), TdError> {
        let data_location = BasePath::try_from("/L")?;
        let collection = CollectionId::default();
        let function = StageId::default();
        let mut builder = StorageLocation::V1
            .builder(&data_location)
            .collection(&collection)
            .stage(&function);
        assert_eq!(
            builder.build()?.0,
            SPath::parse(format!("/L/stages/c/{collection}/f/{function}"))?
        );
        assert_eq!(
            builder.build_meta("foo")?.0,
            SPath::parse(format!("/L/stages/c/{collection}/f/{function}-foo.meta"))?
        );

        let function = StageId::default();
        builder.function(&function);
        assert_eq!(
            builder.build()?.0,
            SPath::parse(format!("/L/stages/c/{collection}/f/{function}"))?
        );
        Ok(())
    }

    #[test]
    fn test_data_builder_v2() -> Result<(), TdError> {
        let data_location = BasePath::try_from("/L")?;
        let collection = CollectionId::default();
        let physical_table_data_version = PhysicalTableDataVersionId::new();
        let mut builder = StorageLocation::V1
            .builder(&data_location)
            .collection(&collection)
            .data(&physical_table_data_version);

        assert_eq!(
            builder.build()?.0,
            SPath::parse(format!("/L/c/{collection}/d/{physical_table_data_version}"))?
        );
        assert_eq!(
            builder.build_meta("foo")?.0,
            SPath::parse(format!(
                "/L/c/{collection}/d/{physical_table_data_version}-foo.meta"
            ))?
        );
        let physical_table_data_version = PhysicalTableDataVersionId::new();
        builder.data(&physical_table_data_version);
        assert_eq!(
            builder.build()?.0,
            SPath::parse(format!("/L/c/{collection}/d/{physical_table_data_version}"))?
        );
        Ok(())
    }

    #[test]
    fn test_table_builder_v2() -> Result<(), TdError> {
        let data_location = BasePath::try_from("/L")?;
        let collection = CollectionId::default();
        let physical_table_data_version = PhysicalTableDataVersionId::new();
        let table = TableId::default();
        let table_version = TableVersionId::default();
        let mut builder = StorageLocation::V1
            .builder(&data_location)
            .collection(&collection)
            .data(&physical_table_data_version)
            .table(&table, &table_version);

        assert_eq!(
            builder.build()?.0,
            SPath::parse(format!(
                "/L/c/{collection}/d/{physical_table_data_version}/t/{table}/{table_version}.t"
            ))?
        );
        assert_eq!(
            builder.build_meta("foo")?.0,
            SPath::parse(format!(
                "/L/c/{collection}/d/{physical_table_data_version}/t/{table}/{table_version}.t-foo.meta"
            ))?
        );

        let table = TableId::default();
        let table_version = TableVersionId::default();
        builder.table(&table, &table_version);
        assert_eq!(
            builder.build()?.0,
            SPath::parse(format!(
                "/L/c/{collection}/d/{physical_table_data_version}/t/{table}/{table_version}.t"
            ))?
        );

        let partition = Partition::try_from("p")?;
        builder.partition(&table, &table_version, &partition);
        assert_eq!(
            builder.build()?.0,
            SPath::parse(format!(
                "/L/c/{collection}/d/{physical_table_data_version}/t/{table}/{table_version}/p/{partition}.p"
            ))?
        );
        assert_eq!(
            builder.build_meta("foo")?.0,
            SPath::parse(format!(
                "/L/c/{collection}/d/{physical_table_data_version}/t/{table}/{table_version}/p/{partition}.p-foo.meta"
            ))?
        );

        let table = TableId::default();
        let table_version = TableVersionId::default();
        let partition = Partition::try_from("p")?;
        builder.partition(&table, &table_version, &partition);
        assert_eq!(
            builder.build()?.0,
            SPath::parse(format!(
                "/L/c/{collection}/d/{physical_table_data_version}/t/{table}/{table_version}/p/{partition}.p"
            ))?
        );
        Ok(())
    }

    #[test]
    fn test_transaction_function_version_builder_v2() -> Result<(), TdError> {
        let data_location = BasePath::try_from("/L")?;
        let collection = CollectionId::default();
        let transaction = TransactionId::new();
        let function_version = FunctionVersionId::new();
        let builder = StorageLocation::V1
            .builder(&data_location)
            .collection(&collection)
            .transaction(&transaction)
            .function_version(&function_version);
        assert_eq!(
            builder.build()?.0,
            SPath::parse(format!(
                "/L/c/{collection}/x/{transaction}/f/{function_version}"
            ))?
        );
        assert_eq!(
            builder.build_meta("foo")?.0,
            SPath::parse(format!(
                "/L/c/{collection}/x/{transaction}/f/{function_version}-foo.meta"
            ))?
        );
        Ok(())
    }
}
