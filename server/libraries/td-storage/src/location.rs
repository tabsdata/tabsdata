//
// Copyright 2024 Tabs Data Inc.
//

use crate::SPath;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};

/// The [`StorageLocation`] creates storage URIS for the different types of data tabsdata stores.
///
/// It is an enum to allow adding URI creation strategies and using them side to side in a
/// backwards compatible way.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageLocation {
    /// Version 1 of the storage location. produces [`SPath`] in the following format
    /// (words in uppercase are placeholders for IDs):
    ///
    /// * /LOCATION
    /// * /LOCATION/s/COLLECTION
    /// * /LOCATION/s/COLLECTION/d/DATASET
    /// * /LOCATION/s/COLLECTION/d/DATASET/f/FUNCTION.f
    /// * /LOCATION/s/COLLECTION/d/DATASET/v/FUNCTION/VERSION
    /// * /LOCATION/s/COLLECTION/d/DATASET/v/FUNCTION/VERSION/t/TABLE.t
    /// * /LOCATION/s/COLLECTION/d/DATASET/v/FUNCTION/VERSION/t/TABLE/p/PARTITION.p
    V1,
}

impl Display for StorageLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageLocation::V1 => write!(f, "V1"),
        }
    }
}

impl StorageLocation {
    /// Returns the current version of the storage location.
    pub fn current() -> Self {
        Self::V1
    }

    /// Returns a builder for the storage location variant
    pub fn builder(&self, location: impl Into<SPath>) -> LocationBuilder {
        match self {
            StorageLocation::V1 => LocationBuilder::new(location, Box::new(V1LocationBuilder)),
        }
    }

    pub fn parse<'a>(version: impl Into<&'a str>) -> Result<Self, String> {
        match version.into() {
            "V1" => Ok(Self::V1),
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
    dataset: Option<String>,
    function: Option<String>,
    version: Option<String>,
    table: Option<String>,
    partition: Option<String>,
}

/// Builder for the table location.
#[derive(Debug)]
pub struct TableLocationBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl TableLocationBuilder {
    /// Sets the table name for the table location.
    pub fn table(&mut self, table: impl Into<String>) -> &mut Self {
        self.info.table = Some(table.into());
        self.info.partition = None;
        self
    }

    /// Sets the table name and partition for the table location.
    pub fn partition(
        &mut self,
        table: impl Into<String>,
        partition: impl Into<String>,
    ) -> &mut Self {
        self.info.table = Some(table.into());
        self.info.partition = Some(partition.into());
        self
    }

    /// Builds the table location.
    pub fn build(&self) -> (SPath, StorageLocation) {
        self.version_builder.build(&self.info, None)
    }

    /// Builds the meta table location.
    pub fn build_meta(&self, meta_name: impl Into<String>) -> (SPath, StorageLocation) {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

/// Builder for the data location.
#[derive(Debug)]
pub struct DataVersionLocationBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl DataVersionLocationBuilder {
    /// Sets the version for the data location.
    pub fn version(&mut self, version: impl Into<String>) -> &mut Self {
        self.info.version = Some(version.into());
        self
    }

    /// Returns a [`TableLocationBuilder`] based on the [`DataVersionLocationBuilder`]
    /// with the given table name.
    pub fn table(self, table: impl Into<String>) -> TableLocationBuilder {
        let mut builder = TableLocationBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.table(table);
        builder
    }

    /// Returns a [`TableLocationBuilder`] based on the [`DataVersionLocationBuilder`]
    /// with the given table name and partition.
    pub fn partition(
        self,
        table: impl Into<String>,
        partition: impl Into<String>,
    ) -> TableLocationBuilder {
        let mut builder = TableLocationBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.partition(table, partition);
        builder
    }

    /// Builds the data location.
    pub fn build(&self) -> (SPath, StorageLocation) {
        self.version_builder.build(&self.info, None)
    }

    /// Builds the meta data location.
    pub fn build_meta(&self, meta_name: impl Into<String>) -> (SPath, StorageLocation) {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

/// Builder for the function location.
#[derive(Debug)]
pub struct FunctionLocationBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl FunctionLocationBuilder {
    /// Sets the function name for the function location.
    pub fn function(&mut self, function: impl Into<String>) -> &mut Self {
        self.info.function = Some(function.into());
        self
    }

    /// Returns a [`DataVersionLocationBuilder`] based on the [`FunctionLocationBuilder`]
    /// for the given data version.
    pub fn version(self, version: impl Into<String>) -> DataVersionLocationBuilder {
        let mut builder = DataVersionLocationBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.version(version);
        builder
    }

    /// Builds the function location.
    pub fn build(&self) -> (SPath, StorageLocation) {
        self.version_builder.build(&self.info, None)
    }

    /// Builds the meta function location.
    pub fn build_meta(&self, meta_name: impl Into<String>) -> (SPath, StorageLocation) {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

/// Builder for the dataset location.
#[derive(Debug)]
pub struct DatasetLocationBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl DatasetLocationBuilder {
    /// Sets the dataset name for the dataset location.
    pub fn dataset(&mut self, dataset: impl Into<String>) -> &mut Self {
        self.info.dataset = Some(dataset.into());
        self
    }

    /// Returns a [`FunctionLocationBuilder`] based on the [`DatasetLocationBuilder`] for the given function.
    pub fn function(self, function: impl Into<String>) -> FunctionLocationBuilder {
        let mut builder = FunctionLocationBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.function(function);
        builder
    }

    /// Builds the dataset location.
    pub fn build(&self) -> (SPath, StorageLocation) {
        self.version_builder.build(&self.info, None)
    }

    /// Builds the meta dataset location.
    pub fn build_meta(&self, meta_name: impl Into<String>) -> (SPath, StorageLocation) {
        self.version_builder.build(
            &self.info,
            Some(format!("{}.meta", meta_name.into()).as_str()),
        )
    }
}

/// Builder for the collection location.
#[derive(Debug)]
pub struct CollectionLocationBuilder {
    info: LocationBuilderInfo,
    version_builder: Box<dyn VersionLocationBuilder>,
}

impl CollectionLocationBuilder {
    /// Sets the collection name for the collection location.
    pub fn collection(&mut self, collection: impl Into<String>) -> &mut Self {
        self.info.collection = Some(collection.into());
        self
    }

    /// Returns a [`DatasetLocationBuilder`] based on the [`CollectionLocationBuilder`]
    /// for the given dataset.
    pub fn dataset(self, dataset: impl Into<String>) -> DatasetLocationBuilder {
        let mut builder = DatasetLocationBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.dataset(dataset);
        builder
    }

    /// Builds the collection location.
    pub fn build(&self) -> (SPath, StorageLocation) {
        self.version_builder.build(&self.info, None)
    }

    /// Builds the meta collection location.
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

    /// Sets the location for the location builder.
    pub fn location(&mut self, location: impl Into<SPath>) -> &mut Self {
        self.info.location = location.into();
        self
    }

    /// Returns a [`CollectionLocationBuilder`] based on the [`LocationBuilder`]
    /// for the given collection.
    pub fn collection(self, collection: impl Into<String>) -> CollectionLocationBuilder {
        let mut builder = CollectionLocationBuilder {
            info: self.info,
            version_builder: self.version_builder,
        };
        builder.collection(collection);
        builder
    }

    /// Builds the location.
    pub fn build(&self) -> (SPath, StorageLocation) {
        self.version_builder.build(&self.info, None)
    }

    /// Builds the meta location.
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
    /// Builds the location based on the information provided.
    fn build(&self, info: &LocationBuilderInfo, postfix: Option<&str>) -> (SPath, StorageLocation);
}

/// Builder for the V1 version.
#[derive(Debug)]
struct V1LocationBuilder;

impl VersionLocationBuilder for V1LocationBuilder {
    fn build(&self, info: &LocationBuilderInfo, postfix: Option<&str>) -> (SPath, StorageLocation) {
        let mut path = info.location.clone();
        if let Some(collection) = &info.collection {
            path = path.child("s").unwrap().child(collection).unwrap();
            if let Some(dataset) = &info.dataset {
                path = path.child("d").unwrap().child(dataset).unwrap();
                if let Some(data) = &info.version {
                    // function always is present if data is present
                    path = path
                        .child("v")
                        .unwrap()
                        .child(info.function.as_ref().unwrap())
                        .unwrap()
                        .child(data)
                        .unwrap();
                    if let Some(partition) = &info.partition {
                        let table = info.table.as_ref().unwrap();
                        path = path
                            .child("t")
                            .unwrap()
                            .child(table)
                            .unwrap()
                            .child("p")
                            .unwrap()
                            .child(&format!("{}.p", partition))
                            .unwrap();
                    } else if let Some(table) = &info.table {
                        path = path
                            .child("t")
                            .unwrap()
                            .child(&format!("{}.t", table))
                            .unwrap();
                    }
                } else if let Some(function) = &info.function {
                    path = path
                        .child("f")
                        .unwrap()
                        .child(&format!("{}.f", function))
                        .unwrap();
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
        (path, StorageLocation::V1)
    }
}

#[cfg(test)]
mod tests {
    use crate::location::StorageLocation;
    use crate::SPath;

    #[test]
    fn test_data_location_current_version() {
        assert!(matches!(StorageLocation::current(), StorageLocation::V1))
    }

    #[test]
    fn test_location_current_builder_version() {
        assert!(matches!(
            StorageLocation::current()
                .builder(SPath::default())
                .build()
                .1,
            StorageLocation::V1
        ))
    }

    #[test]
    fn test_location_builder_v1() {
        let mut builder = StorageLocation::V1.builder(SPath::parse("/L").unwrap());
        assert_eq!(builder.build().0, SPath::parse("/L").unwrap());
        assert_eq!(
            builder.build_meta("foo").0,
            SPath::parse("/L-foo.meta").unwrap()
        );
        builder.location(SPath::parse("/LL").unwrap());
        assert_eq!(builder.build().0, SPath::parse("/LL").unwrap());
    }

    #[test]
    fn test_collection_builder_v1() {
        let mut builder = StorageLocation::V1
            .builder(SPath::parse("/L").unwrap())
            .collection("DS");
        assert_eq!(builder.build().0, SPath::parse("/L/s/DS").unwrap());
        assert_eq!(
            builder.build_meta("foo").0,
            SPath::parse("/L/s/DS-foo.meta").unwrap()
        );
        builder.collection("DDSS");
        assert_eq!(builder.build().0, SPath::parse("/L/s/DDSS").unwrap());
    }

    #[test]
    fn test_dataset_builder_v1() {
        let mut builder = StorageLocation::V1
            .builder(SPath::parse("/L").unwrap())
            .collection("DS")
            .dataset("D");
        assert_eq!(builder.build().0, SPath::parse("/L/s/DS/d/D").unwrap());
        assert_eq!(
            builder.build_meta("foo").0,
            SPath::parse("/L/s/DS/d/D-foo.meta").unwrap()
        );
        builder.dataset("DD");
        assert_eq!(builder.build().0, SPath::parse("/L/s/DS/d/DD").unwrap());
    }

    #[test]
    fn test_function_builder_v1() {
        let mut builder = StorageLocation::V1
            .builder(SPath::parse("/L").unwrap())
            .collection("DS")
            .dataset("D")
            .function("F");
        assert_eq!(
            builder.build().0,
            SPath::parse("/L/s/DS/d/D/f/F.f").unwrap()
        );
        assert_eq!(
            builder.build_meta("foo").0,
            SPath::parse("/L/s/DS/d/D/f/F.f-foo.meta").unwrap()
        );
        builder.function("FF");
        assert_eq!(
            builder.build().0,
            SPath::parse("/L/s/DS/d/D/f/FF.f").unwrap()
        );
    }

    #[test]
    fn test_data_builder_v1() {
        let mut builder = StorageLocation::V1
            .builder(SPath::parse("/L").unwrap())
            .collection("DS")
            .dataset("D")
            .function("F")
            .version("V");
        assert_eq!(
            builder.build().0,
            SPath::parse("/L/s/DS/d/D/v/F/V").unwrap()
        );
        assert_eq!(
            builder.build_meta("foo").0,
            SPath::parse("/L/s/DS/d/D/v/F/V-foo.meta").unwrap()
        );
        builder.version("VV");
        assert_eq!(
            builder.build().0,
            SPath::parse("/L/s/DS/d/D/v/F/VV").unwrap()
        );
    }

    #[test]
    fn test_table_builder_v1() {
        let mut builder = StorageLocation::V1
            .builder(SPath::parse("/L").unwrap())
            .collection("DS")
            .dataset("D")
            .function("F")
            .version("V")
            .table("T");
        assert_eq!(
            builder.build().0,
            SPath::parse("/L/s/DS/d/D/v/F/V/t/T.t").unwrap()
        );
        assert_eq!(
            builder.build_meta("foo").0,
            SPath::parse("/L/s/DS/d/D/v/F/V/t/T.t-foo.meta").unwrap()
        );
        builder.table("TT");
        assert_eq!(
            builder.build().0,
            SPath::parse("/L/s/DS/d/D/v/F/V/t/TT.t").unwrap()
        );
        builder.partition("T", "P");
        assert_eq!(
            builder.build().0,
            SPath::parse("/L/s/DS/d/D/v/F/V/t/T/p/P.p").unwrap()
        );
        assert_eq!(
            builder.build_meta("foo").0,
            SPath::parse("/L/s/DS/d/D/v/F/V/t/T/p/P.p-foo.meta").unwrap()
        );
        builder.table("TTT");
        assert_eq!(
            builder.build().0,
            SPath::parse("/L/s/DS/d/D/v/F/V/t/TTT.t").unwrap()
        );
    }
}
