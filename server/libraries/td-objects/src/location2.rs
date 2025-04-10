//
// Copyright 2025. Tabs Data Inc.
//

use crate::types::basic::{
    BundleId, CollectionId, DataLocation, Partition, TableDataVersionId, TableId, TableVersionId,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::ops::Deref;
use td_storage::SPath;

/// The [`StorageLocation`] creates storage URIS for the different types of data tabsdata stores.
///
/// It is an enum to allow adding URI creation strategies and using them side to side in a
/// backwards compatible way.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, strum_macros::Display)]
pub enum StorageLocation {
    /// Version 2 of the storage location. produces [`SPath`] in the following format
    /// (words in uppercase are placeholders for IDs):
    ///
    /// LOCATION: function datalocation
    /// COLLECTION: collection ID
    /// FUNCTION: function ID
    /// FUNCTION_VERSION: function_version ID
    /// DATA_VERSION: data_version ID
    /// TABLE: table ID
    /// TABLE_VERSION: table_version ID
    ///
    /// * /LOCATION
    /// * /LOCATION/c/COLLECTION
    /// * /LOCATION/c/COLLECTION/f/BUNDLE.tgz
    /// * /LOCATION/c/COLLECTION/d/DATA_VERSION/t/TABLE/TABLE_VERSION.t
    /// * /LOCATION/c/COLLECTION/d/DATA_VERSION/t/TABLE/TABLE_VERSION/p/PARTITION.p
    V2,
}

impl StorageLocation {
    /// Return the current version of the storage location.
    pub fn current() -> Self {
        Self::V2
    }

    /// Return a builder for the storage location variant
    pub fn builder(&self, location: &DataLocation) -> LocationBuilder {
        match self {
            StorageLocation::V2 => LocationBuilder::new(
                SPath::parse(location.deref()).unwrap(),
                Box::new(V2LocationBuilder),
            ),
        }
    }

    pub fn parse<'a>(version: impl Into<&'a str>) -> Result<Self, String> {
        match version.into() {
            "V1" => Ok(Self::V2),
            unknown_version => Err(format!(
                "Unknown StorageLocation version {}",
                unknown_version
            )),
        }
    }
}

impl From<StorageLocation> for String {
    fn from(value: StorageLocation) -> Self {
        value.to_string()
    }
}

impl TryFrom<String> for StorageLocation {
    type Error = String;
    fn try_from(value: String) -> Result<Self, String> {
        StorageLocation::parse(value.as_str())
    }
}

#[derive(Debug, Clone, Default)]
struct LocationBuilderInfo {
    location: SPath,
    collection: Option<String>,
    bundle: Option<String>,
    data_version: Option<String>,
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
    pub fn build(&self) -> (SPath, StorageLocation) {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta table location.
    pub fn build_meta(&self, meta_name: impl Into<String>) -> (SPath, StorageLocation) {
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
    pub fn data(&mut self, data_version: &TableDataVersionId) -> &mut Self {
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
    pub fn build(&self) -> (SPath, StorageLocation) {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta data location.
    pub fn build_meta(&self, meta_name: impl Into<String>) -> (SPath, StorageLocation) {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

/// Builder for the dataset location.
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

    /// Build the dataset location.
    pub fn build(&self) -> (SPath, StorageLocation) {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta dataset location.
    pub fn build_meta(&self, meta_name: impl Into<String>) -> (SPath, StorageLocation) {
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

    /// Return a [`DataBuilder`] based on the [`CollectionBuilder`]
    pub fn data(self, data_version: &TableDataVersionId) -> DataBuilder {
        let mut builder = DataBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.data(data_version);
        builder
    }

    /// Build the collection location.
    pub fn build(&self) -> (SPath, StorageLocation) {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta collection location.
    pub fn build_meta(&self, meta_name: impl Into<String>) -> (SPath, StorageLocation) {
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
    fn new(location: impl Into<SPath>, version_builder: Box<dyn VersionLocationBuilder>) -> Self {
        Self {
            info: LocationBuilderInfo {
                location: location.into(),
                ..Default::default()
            },
            version_builder,
        }
    }

    /// Set the location for the location builder.
    pub fn location(&mut self, location: &DataLocation) -> &mut Self {
        self.info.location = SPath::parse(location.as_str()).unwrap();
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
    pub fn build(&self) -> (SPath, StorageLocation) {
        self.version_builder.build(&self.info, None)
    }

    /// Build the meta location.
    pub fn build_meta(&self, meta_name: impl Into<String>) -> (SPath, StorageLocation) {
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
    fn build(&self, info: &LocationBuilderInfo, postfix: Option<&str>) -> (SPath, StorageLocation);
}

/// Builder for the V1 version.
#[derive(Debug)]
struct V2LocationBuilder;

impl VersionLocationBuilder for V2LocationBuilder {
    fn build(&self, info: &LocationBuilderInfo, postfix: Option<&str>) -> (SPath, StorageLocation) {
        let mut path = info.location.clone();
        if let Some(collection) = &info.collection {
            path = path.child("c").unwrap().child(collection).unwrap();
            if let Some(bundle) = &info.bundle {
                path = path
                    .child("f")
                    .unwrap()
                    .child(&format!("{}.tgz", bundle))
                    .unwrap();
            } else if let Some(data_version) = &info.data_version {
                // function always is present if data is present
                path = path.child("d").unwrap().child(data_version).unwrap();
                if let Some(table) = &info.table {
                    let table_version = info.table_version.as_ref().unwrap();
                    path = path.child("t").unwrap().child(table).unwrap();
                    if let Some(partition) = &info.partition {
                        path = path
                            .child(table_version)
                            .unwrap()
                            .child("p")
                            .unwrap()
                            .child(&format!("{}.p", partition))
                            .unwrap();
                    } else {
                        path = path.child(&format!("{}.t", table_version)).unwrap();
                    }
                }
            }
        }
        if let Some(postfix) = postfix {
            let name = path.filename().unwrap();
            path = path
                .parent()
                .unwrap()
                .child(&format!("{}-{}", name, postfix))
                .unwrap()
        }
        (path, StorageLocation::V2)
    }
}

#[cfg(test)]
mod tests {
    use super::StorageLocation;
    use crate::types::basic::{
        BundleId, CollectionId, DataLocation, Partition, TableDataVersionId, TableId,
        TableVersionId,
    };
    use td_error::TdError;
    use td_storage::SPath;

    #[test]
    fn test_data_location_current_version() {
        assert!(matches!(StorageLocation::current(), StorageLocation::V2))
    }

    #[test]
    fn test_location_current_builder_version() -> Result<(), TdError> {
        let data_location = DataLocation::try_from("/")?;
        assert!(matches!(
            StorageLocation::current().builder(&data_location).build().1,
            StorageLocation::V2
        ));
        Ok(())
    }

    #[test]
    fn test_location_builder_v2() -> Result<(), TdError> {
        let data_location = DataLocation::try_from("/L")?;
        let mut builder = StorageLocation::V2.builder(&data_location);
        assert_eq!(builder.build().0, SPath::parse("/L")?);
        assert_eq!(builder.build_meta("foo").0, SPath::parse("/L-foo.meta")?);
        let data_location = DataLocation::try_from("/LL")?;
        builder.location(&data_location);
        assert_eq!(builder.build().0, SPath::parse("/LL")?);
        Ok(())
    }

    #[test]
    fn test_collection_builder_v2() -> Result<(), TdError> {
        let data_location = DataLocation::try_from("/L")?;
        let collection = CollectionId::default();
        let mut builder = StorageLocation::V2
            .builder(&data_location)
            .collection(&collection);
        assert_eq!(
            builder.build().0,
            SPath::parse(format!("/L/c/{}", collection))?
        );
        assert_eq!(
            builder.build_meta("foo").0,
            SPath::parse(format!("/L/c/{}-foo.meta", collection)).unwrap()
        );
        let collection = CollectionId::default();
        builder.collection(&collection);
        assert_eq!(
            builder.build().0,
            SPath::parse(format!("/L/c/{}", collection))?
        );
        Ok(())
    }

    #[test]
    fn test_function_builder_v2() -> Result<(), TdError> {
        let data_location = DataLocation::try_from("/L")?;
        let collection = CollectionId::default();
        let bundle = BundleId::default();
        let mut builder = StorageLocation::V2
            .builder(&data_location)
            .collection(&collection)
            .function(&bundle);
        assert_eq!(
            builder.build().0,
            SPath::parse(format!("/L/c/{}/f/{}.tgz", collection, bundle))?
        );
        assert_eq!(
            builder.build_meta("foo").0,
            SPath::parse(format!("/L/c/{}/f/{}.tgz-foo.meta", collection, bundle))?
        );

        let bundle = BundleId::default();
        builder.function(&bundle);
        assert_eq!(
            builder.build().0,
            SPath::parse(format!("/L/c/{}/f/{}.tgz", collection, bundle))?
        );
        Ok(())
    }

    #[test]
    fn test_data_builder_v2() -> Result<(), TdError> {
        let data_location = DataLocation::try_from("/L")?;
        let collection = CollectionId::default();
        let table_data_version = TableDataVersionId::default();
        let mut builder = StorageLocation::V2
            .builder(&data_location)
            .collection(&collection)
            .data(&table_data_version);

        assert_eq!(
            builder.build().0,
            SPath::parse(format!("/L/c/{}/d/{}", collection, table_data_version))?
        );
        assert_eq!(
            builder.build_meta("foo").0,
            SPath::parse(format!(
                "/L/c/{}/d/{}-foo.meta",
                collection, table_data_version
            ))?
        );
        let table_data_version = TableDataVersionId::default();
        builder.data(&table_data_version);
        assert_eq!(
            builder.build().0,
            SPath::parse(format!("/L/c/{}/d/{}", collection, table_data_version))?
        );
        Ok(())
    }

    #[test]
    fn test_table_builder_v2() -> Result<(), TdError> {
        let data_location = DataLocation::try_from("/L")?;
        let collection = CollectionId::default();
        let table_data_version = TableDataVersionId::default();
        let table = TableId::default();
        let table_version = TableVersionId::default();
        let mut builder = StorageLocation::V2
            .builder(&data_location)
            .collection(&collection)
            .data(&table_data_version)
            .table(&table, &table_version);

        assert_eq!(
            builder.build().0,
            SPath::parse(format!(
                "/L/c/{}/d/{}/t/{}/{}.t",
                collection, table_data_version, table, table_version
            ))?
        );
        assert_eq!(
            builder.build_meta("foo").0,
            SPath::parse(format!(
                "/L/c/{}/d/{}/t/{}/{}.t-foo.meta",
                collection, table_data_version, table, table_version
            ))?
        );

        let table = TableId::default();
        let table_version = TableVersionId::default();
        builder.table(&table, &table_version);
        assert_eq!(
            builder.build().0,
            SPath::parse(format!(
                "/L/c/{}/d/{}/t/{}/{}.t",
                collection, table_data_version, table, table_version
            ))?
        );

        let partition = Partition::default();
        builder.partition(&table, &table_version, &partition);
        assert_eq!(
            builder.build().0,
            SPath::parse(format!(
                "/L/c/{}/d/{}/t/{}/{}/p/{}.p",
                collection, table_data_version, table, table_version, partition
            ))?
        );
        assert_eq!(
            builder.build_meta("foo").0,
            SPath::parse(format!(
                "/L/c/{}/d/{}/t/{}/{}/p/{}.p-foo.meta",
                collection, table_data_version, table, table_version, partition
            ))?
        );

        let table = TableId::default();
        let table_version = TableVersionId::default();
        let partition = Partition::default();
        builder.partition(&table, &table_version, &partition);
        assert_eq!(
            builder.build().0,
            SPath::parse(format!(
                "/L/c/{}/d/{}/t/{}/{}/p/{}.p",
                collection, table_data_version, table, table_version, partition
            ))?
        );
        Ok(())
    }
}
