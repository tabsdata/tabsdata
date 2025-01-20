//
// Copyright 2024 Tabs Data Inc.
//

//! This module defines the necessary types and traits to compute graph and execution plans.

use crate::link::Link;
use getset::Getters;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::Deref;
use td_common::dataset::{DatasetRef, TableRef, VersionRef};
use td_common::id::Id;
use td_common::uri::{TdUri, Version, Versions};
use td_transaction::TransactionBy;

/// Represents a dataset to perform graph resolution.
#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Dataset {
    collection_id: String,
    dataset_id: String,
}

impl DatasetRef for Dataset {
    fn collection(&self) -> &str {
        &self.collection_id
    }

    fn dataset(&self) -> &str {
        &self.dataset_id
    }
}

impl Dataset {
    pub fn new(collection_id: &str, dataset_id: &str) -> Self {
        Self {
            collection_id: collection_id.to_string(),
            dataset_id: dataset_id.to_string(),
        }
    }

    pub fn from_link<L: Link>(link: &L) -> (Self, Self) {
        (
            Self {
                collection_id: link.source_collection_id().to_string(),
                dataset_id: link.source_dataset_id().to_string(),
            },
            Self {
                collection_id: link.target_collection_id().to_string(),
                dataset_id: link.target_dataset_id().to_string(),
            },
        )
    }
}

impl Debug for Dataset {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let formatted: String = self.dataset_id.chars().tail(5).collect();
        write!(f, "{}", formatted)
    }
}

impl Display for Dataset {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct DatasetWithUris {
    dataset_id: TdUri,
    dataset_name: TdUri,
}

impl DatasetRef for DatasetWithUris {
    fn collection(&self) -> &str {
        self.dataset_name.collection()
    }

    fn dataset(&self) -> &str {
        self.dataset_name.dataset()
    }
}

impl DatasetWithUris {
    pub fn new(dataset_id: TdUri, dataset_name: TdUri) -> Self {
        Self {
            dataset_id,
            dataset_name,
        }
    }

    pub fn dataset_uri_with_ids(&self) -> &TdUri {
        &self.dataset_id
    }

    pub fn dataset_uri_with_names(&self) -> &TdUri {
        &self.dataset_name
    }
}

impl Debug for DatasetWithUris {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let formatted: String = self.dataset_name.to_string();
        write!(f, "{}", formatted)
    }
}

impl Display for DatasetWithUris {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Represents an executable dataset in the execution graph. Handy to create an execution graph.
#[derive(Clone, Eq, PartialEq, Hash, Getters)]
#[getset(get = "pub")]
pub struct ExecutableDataset<D>
where
    D: DatasetRef,
{
    dataset: D,
    execute: bool,
    transaction_by: TransactionBy,
}

impl<D> DatasetRef for ExecutableDataset<D>
where
    D: DatasetRef,
{
    fn collection(&self) -> &str {
        self.dataset.collection()
    }

    fn dataset(&self) -> &str {
        self.dataset.dataset()
    }
}

impl<D> ExecutableDataset<D>
where
    D: DatasetRef,
{
    pub fn new(dataset: D, execute: bool) -> Self {
        Self {
            dataset,
            execute,
            transaction_by: TransactionBy::default(),
        }
    }

    pub fn with_transaction(dataset: D, execute: bool, transaction_by: TransactionBy) -> Self {
        Self {
            dataset,
            execute,
            transaction_by,
        }
    }
}

impl<D> Debug for ExecutableDataset<D>
where
    D: DatasetRef,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.dataset)
    }
}

impl<D> Display for ExecutableDataset<D>
where
    D: DatasetRef,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.dataset)
    }
}

impl VersionRef for RelativeVersions {
    fn existing_count(&self) -> usize {
        self.versions().existing_count()
    }
}

impl VersionRef for AbsoluteVersion {
    fn existing_count(&self) -> usize {
        self.versions.existing_count()
    }
}

impl VersionRef for AbsoluteVersions {
    fn existing_count(&self) -> usize {
        self.iter().map(|v| v.existing_count()).sum()
    }
}

impl VersionRef for ResolvedVersion {
    fn existing_count(&self) -> usize {
        self.absolute_versions.existing_count()
    }
}

impl<V, T> VersionRef for TdVersions<V, T>
where
    V: VersionRef,
    T: TableRef,
{
    fn existing_count(&self) -> usize {
        self.versions().existing_count()
    }
}

/// Versions, similar to [`TdUri`], but abstracted from collection/dataset, as these do not
/// affect graph resolution.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TdVersions<V = Versions, T = String>
where
    V: VersionRef,
    T: TableRef,
{
    Dataset { versions: V },
    Table { versions: V, table: T, pos: i64 },
}

impl<T: TableRef> TdVersions<Versions, T> {
    /// This is only possible on [`Versions`].
    pub fn trigger() -> Self {
        Self::Dataset {
            versions: Versions::Single(Version::Head(0)),
        }
    }

    pub fn shift(&mut self, shift: isize) {
        match self {
            TdVersions::Dataset { versions } => versions.shift(shift),
            TdVersions::Table { versions, .. } => {
                versions.shift(shift);
            }
        }
    }
}

impl<V, T> TdVersions<V, T>
where
    V: VersionRef,
    T: TableRef,
{
    pub fn from_table(versions: impl Into<V>, table: impl Into<T>, pos: i64) -> Self {
        Self::Table {
            versions: versions.into(),
            table: table.into(),
            pos,
        }
    }

    pub fn from_dataset(versions: impl Into<V>) -> Self {
        Self::Dataset {
            versions: versions.into(),
        }
    }

    pub fn table(&self) -> Option<&T> {
        match self {
            TdVersions::Dataset { .. } => None,
            TdVersions::Table { table, .. } => Some(table),
        }
    }

    pub fn position(&self) -> Option<i64> {
        match self {
            TdVersions::Dataset { .. } => None,
            TdVersions::Table { pos, .. } => Some(*pos),
        }
    }

    pub fn versions(&self) -> &V {
        match self {
            TdVersions::Dataset { versions } => versions,
            TdVersions::Table { versions, .. } => versions,
        }
    }
}

/// Represents a version of a dataset, which can be either a planned version or the current version.
#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum RelativeVersions {
    /// Version of a dataset relative to the plan.
    Plan(TdVersions),
    /// Version of a dataset relative to the existent versions.
    Current(TdVersions),
    /// Version of a dataset relative to itself. Special case, it happens on self dependencies.
    Same(TdVersions),
}

impl Default for RelativeVersions {
    fn default() -> Self {
        RelativeVersions::Plan(TdVersions::trigger())
    }
}

impl Debug for RelativeVersions {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.versions())
    }
}

impl RelativeVersions {
    pub fn versions(&self) -> &TdVersions {
        match self {
            RelativeVersions::Plan(versions) => versions,
            RelativeVersions::Current(versions) => versions,
            RelativeVersions::Same(versions) => versions,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct AbsoluteVersion {
    function_id: Id,
    versions: TdVersions<Option<Id>, Id>,
    position: i64,
}

impl Debug for AbsoluteVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let version: String = self
            .versions
            .versions()
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or("None".to_string())
            .chars()
            .collect();
        let table: String = self
            .versions
            .table()
            .map(|v| v.to_string())
            .unwrap_or("None".to_string())
            .chars()
            .collect();
        write!(f, "{}@{}", table, version)
    }
}

impl Display for AbsoluteVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl AbsoluteVersion {
    pub fn new(function_id: Id, versions: TdVersions<Option<Id>, Id>, position: i64) -> Self {
        Self {
            function_id,
            versions,
            position,
        }
    }

    pub fn function_id(&self) -> &Id {
        &self.function_id
    }

    pub fn id(&self) -> Option<&Id> {
        self.versions.versions().as_ref()
    }

    pub fn position(&self) -> i64 {
        self.position
    }

    pub fn table_id(&self) -> Option<&Id> {
        self.versions.table()
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct AbsoluteVersions(Vec<AbsoluteVersion>);

impl Debug for AbsoluteVersions {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let formatted: String = self.iter().map(|v| v.to_string()).join(", ");
        write!(f, "{}", formatted)
    }
}

impl Display for AbsoluteVersions {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl AbsoluteVersions {
    pub fn new(versions: Vec<AbsoluteVersion>) -> Self {
        Self(versions)
    }
}

impl Deref for AbsoluteVersions {
    type Target = Vec<AbsoluteVersion>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Getters, Serialize, Deserialize)]
#[getset(get = "pub")]
pub struct ResolvedVersion {
    absolute_versions: AbsoluteVersions,
    relative_versions: RelativeVersions,
}

impl ResolvedVersion {
    pub fn new(absolute_versions: AbsoluteVersions, relative_versions: RelativeVersions) -> Self {
        Self {
            absolute_versions,
            relative_versions,
        }
    }
}

impl Debug for ResolvedVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Id: {:?}. Name: {}", self.absolute_versions, self)
    }
}

impl Display for ResolvedVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let td_versions = self.relative_versions.versions();
        let table = td_versions
            .table()
            .map(|t| t.to_string())
            .unwrap_or("".to_string());
        let version = td_versions.versions().to_string();
        write!(f, "{}@{}", table, version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use td_common::id::id;
    use td_common::uri::Version;

    #[test]
    fn test_dataset_new() {
        let dataset = Dataset::new("collection_id", "dataset_id");
        assert_eq!(dataset.collection_id, "collection_id");
        assert_eq!(dataset.dataset_id, "dataset_id");
    }

    #[test]
    fn test_dataset_from_link() {
        struct MockLink;
        impl Link for MockLink {
            fn source_collection_id(&self) -> &str {
                "source_collection_id"
            }
            fn source_dataset_id(&self) -> &str {
                "source_dataset_id"
            }
            fn target_collection_id(&self) -> &str {
                "target_collection_id"
            }
            fn target_dataset_id(&self) -> &str {
                "target_dataset_id"
            }
        }

        let link = MockLink;
        let (source, target) = Dataset::from_link(&link);
        assert_eq!(source.collection(), "source_collection_id");
        assert_eq!(source.dataset(), "source_dataset_id");
        assert_eq!(target.collection(), "target_collection_id");
        assert_eq!(target.dataset(), "target_dataset_id");
    }

    #[test]
    fn test_executable_dataset_new() {
        let dataset = Dataset::new("collection_id", "dataset_id");
        let executable_dataset = ExecutableDataset::new(dataset.clone(), true);
        assert_eq!(executable_dataset.dataset, dataset);
        assert!(executable_dataset.execute);
    }

    #[test]
    fn test_absolute_version_new() {
        let function_id = id();
        let versions = TdVersions::from_table(Some(id()), id(), 0);
        let absolute_version = AbsoluteVersion::new(function_id, versions.clone(), 0);
        assert_eq!(absolute_version.function_id(), &function_id);
        assert_eq!(&absolute_version.versions, &versions);
    }

    #[test]
    fn test_absolute_versions_new() {
        let function_id = id();
        let versions = TdVersions::from_table(Some(id()), id(), 0);
        let absolute_version = AbsoluteVersion::new(function_id, versions, 0);
        let absolute_versions = AbsoluteVersions::new(vec![absolute_version.clone()]);
        assert_eq!(absolute_versions.len(), 1);
        assert_eq!(absolute_versions[0], absolute_version);
    }

    #[test]
    fn test_resolved_version_new() {
        let absolute_versions = AbsoluteVersions::new(vec![]);
        let relative_versions = RelativeVersions::default();
        let resolved_version =
            ResolvedVersion::new(absolute_versions.clone(), relative_versions.clone());
        assert_eq!(resolved_version.absolute_versions(), &absolute_versions);
        assert_eq!(resolved_version.relative_versions(), &relative_versions);
    }

    #[test]
    fn test_relation_version_plan() {
        let created = Versions::Single(Version::Head(-1));
        let link = TdVersions::Dataset {
            versions: created.clone(),
        };
        let relation_version = RelativeVersions::Plan(link.clone());
        if let RelativeVersions::Plan(v) = relation_version {
            assert_eq!(v, link);
        } else {
            panic!("Expected RelativeVersion::Plan variant");
        }
    }

    #[test]
    fn test_relation_version_current() {
        let created = Versions::Single(Version::Head(-1));
        let link = TdVersions::Dataset {
            versions: created.clone(),
        };
        let relation_version = RelativeVersions::Current(link.clone());
        if let RelativeVersions::Current(v) = relation_version {
            assert_eq!(v, link);
        } else {
            panic!("Expected RelativeVersion::Current variant");
        }
    }
}
