//
// Copyright 2026 Tabsdata Inc.
//

use crate::parse::{parse_table_ref, parse_versioned_table_ref, parse_versions};
use crate::types::basic::{CollectionName, LogicalTableDataVersionId};
use derive_new::new;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use ta_objects::ComposedString;
use td_error::TdError;

/// `Fixed` references identify a *logic* table data version (LTDV) —
/// the stable plan-time identity, not the concrete `physical_table_data_versions`
/// row currently materializing it. The concrete TDV reachable from an
/// LTDV changes on replay or redirect; references that pinned the concrete
/// id directly would silently mis-resolve afterwards. Pinning the LTDV id
/// keeps "the same version reference" anchored on the same logical version
/// regardless of later attempts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Version {
    /// A fixed version, identified by its logical identity.
    Fixed(LogicalTableDataVersionId),
    /// A head relative version, it is always zero or negative.
    Head(isize),
    /// An initial relative version, it is always zero or positive.
    Initial(isize),
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Version::Fixed(id) => write!(f, "{id}"),
            Version::Head(back) => {
                if *back == 0 {
                    write!(f, "HEAD")
                } else {
                    write!(f, "HEAD~{}", -back)
                }
            }
            Version::Initial(forward) => {
                if *forward == 0 {
                    write!(f, "INITIAL")
                } else {
                    write!(f, "INITIAL~{}", forward)
                }
            }
        }
    }
}

impl Version {
    pub fn shift_mut(&mut self, pos: isize) {
        // No need to shift INITIAL, as it does not change on new versions.
        if let Version::Head(head) = self {
            *head += pos
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Versions {
    None,
    Single(Version),
    List(Vec<Version>),
    /// A range of versions. Always older to newer.
    Range(Version, Version),
    /// Range of all versions available since last execution.
    New,
}

impl Versions {
    pub fn shift(&self, pos: isize) -> Self {
        match self {
            Versions::None => Versions::Single(Version::Head(pos)),
            Versions::Single(version) => {
                let mut version = version.clone();
                version.shift_mut(pos);
                Versions::Single(version)
            }
            Versions::List(versions) => {
                let mut versions = versions.clone();
                versions.iter_mut().for_each(|v| v.shift_mut(pos));
                Versions::List(versions)
            }
            Versions::Range(from, to) => {
                let mut from = from.clone();
                from.shift_mut(pos);
                let mut to = to.clone();
                to.shift_mut(pos);
                Versions::Range(from, to)
            }
            Versions::New => self.clone(),
        }
    }

    pub fn is_multiple(&self) -> bool {
        matches!(
            self,
            Versions::List(_) | Versions::Range(_, _) | Versions::New
        )
    }
}

impl ComposedString for Versions {
    fn parse(s: impl Into<String>) -> Result<Self, TdError> {
        parse_versions(s)
    }

    fn compose(&self) -> String {
        self.to_string()
    }
}

impl Display for Versions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Versions::None => write!(f, ""),
            Versions::Single(version) => write!(f, "{version}"),
            Versions::List(versions) => {
                let versions = versions
                    .iter()
                    .map(|v| format!("{v}"))
                    .collect::<Vec<_>>()
                    .join(",");
                write!(f, "{versions}")
            }
            Versions::Range(from, to) => write!(f, "{from}..{to}"),
            Versions::New => write!(f, "NEW"),
        }
    }
}

impl From<&Versions> for Versions {
    fn from(v: &Versions) -> Self {
        v.clone()
    }
}

/// A table named from a function: an optional collection, the table and
/// its versions. The collection is a name on a request, a label on a read.
#[derive(Debug, Clone, PartialEq, Eq, Hash, new)]
pub struct VersionedTableRef<T, C = CollectionName> {
    pub collection: Option<C>,
    pub table: T,
    pub versions: Versions,
}

impl<T, C, E> ComposedString for VersionedTableRef<T, C>
where
    T: Display + TryFrom<String, Error = E>,
    C: Display + TryFrom<String, Error = TdError>,
    E: Into<TdError>,
{
    fn parse(s: impl Into<String>) -> Result<Self, TdError> {
        parse_versioned_table_ref(s)
    }

    fn compose(&self) -> String {
        self.to_string()
    }
}

impl<T, C> From<VersionedTableRef<T, C>> for String
where
    T: Display,
    C: Display,
{
    fn from(value: VersionedTableRef<T, C>) -> Self {
        value.to_string()
    }
}

impl<T, C> Display for VersionedTableRef<T, C>
where
    T: Display,
    C: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match (&self.collection, &self.versions) {
            (Some(collection), Versions::None) => write!(f, "{}/{}", collection, self.table),
            (Some(collection), versions) => {
                write!(f, "{}/{}@{}", collection, self.table, versions)
            }
            (None, Versions::None) => write!(f, "{}", self.table),
            (None, versions) => write!(f, "{}@{}", self.table, versions),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, new)]
pub struct TableRef<T, C = CollectionName> {
    pub collection: Option<C>,
    pub table: T,
}

impl<T, C, E> ComposedString for TableRef<T, C>
where
    T: Display + TryFrom<String, Error = E>,
    C: Display + TryFrom<String, Error = TdError>,
    E: Into<TdError>,
{
    fn parse(s: impl Into<String>) -> Result<Self, TdError> {
        parse_table_ref(s)
    }

    fn compose(&self) -> String {
        self.to_string()
    }
}

impl<T, C> From<TableRef<T, C>> for String
where
    T: Display,
    C: Display,
{
    fn from(value: TableRef<T, C>) -> Self {
        value.to_string()
    }
}

impl<T, C> Display for TableRef<T, C>
where
    T: Display,
    C: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(collection) = &self.collection {
            write!(f, "{}/{}", collection, self.table)
        } else {
            write!(f, "{}", self.table)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::basic::{CollectionId, CollectionLabel, TableName};
    use crate::types::label::Label;

    #[test]
    fn test_versioned_table_ref_to_string() {
        let table = VersionedTableRef::<TableName>::parse("table").unwrap();
        assert_eq!(table.to_string(), "table");
        let table = VersionedTableRef::<TableName>::parse("collection/table").unwrap();
        assert_eq!(table.to_string(), "collection/table");
        let table = VersionedTableRef::<TableName>::parse("collection/table@HEAD^^").unwrap();
        assert_eq!(table.to_string(), "collection/table@HEAD~2");
        let table = VersionedTableRef::<TableName>::parse("collection/table@HEAD^^..HEAD").unwrap();
        assert_eq!(table.to_string(), "collection/table@HEAD~2..HEAD");
        let table = VersionedTableRef::<TableName>::parse("collection/table@HEAD^^,HEAD").unwrap();
        assert_eq!(table.to_string(), "collection/table@HEAD~2,HEAD");
        let table = VersionedTableRef::<TableName>::parse("collection/table").unwrap();
        assert_eq!(table.to_string(), "collection/table");
        let table = VersionedTableRef::<TableName>::parse("collection/table@INITIAL^^").unwrap();
        assert_eq!(table.to_string(), "collection/table@INITIAL~2");
        let table =
            VersionedTableRef::<TableName>::parse("collection/table@INITIAL^^..INITIAL").unwrap();
        assert_eq!(table.to_string(), "collection/table@INITIAL~2..INITIAL");
        let table =
            VersionedTableRef::<TableName>::parse("collection/table@INITIAL^^,INITIAL").unwrap();
        assert_eq!(table.to_string(), "collection/table@INITIAL~2,INITIAL");
        let table = VersionedTableRef::<TableName>::parse("collection/table").unwrap();
        assert_eq!(table.to_string(), "collection/table");
        let table =
            VersionedTableRef::<TableName>::parse("collection/table@INITIAL^^..HEAD").unwrap();
        assert_eq!(table.to_string(), "collection/table@INITIAL~2..HEAD");
        let table =
            VersionedTableRef::<TableName>::parse("collection/table@HEAD^^,INITIAL").unwrap();
        assert_eq!(table.to_string(), "collection/table@HEAD~2,INITIAL");
        let table =
            VersionedTableRef::<TableName>::parse("collection/table@dv20240101-123").unwrap();
        assert_eq!(table.to_string(), "collection/table@dv20240101-0000000123");
    }

    #[test]
    fn test_table_ref_to_string() {
        let table = TableRef::<TableName>::parse("table").unwrap();
        assert_eq!(table.to_string(), "table");
        let table = TableRef::<TableName>::parse("collection/table").unwrap();
        assert_eq!(table.to_string(), "collection/table");
    }

    #[test]
    fn test_labeled_refs_round_trip_a_deleted_collection() {
        let id = CollectionId::default();
        let deleted = format!("[{id}]");
        let table = TableRef::<TableName, CollectionLabel>::parse("collection/table").unwrap();
        assert_eq!(table.to_string(), "collection/table");
        let table =
            TableRef::<TableName, CollectionLabel>::parse(format!("{deleted}/table")).unwrap();
        assert_eq!(
            table.collection,
            Some(CollectionLabel::new(Label::Deleted(id)))
        );
        assert_eq!(table.to_string(), format!("{deleted}/table"));
        let table = VersionedTableRef::<TableName, CollectionLabel>::parse(format!(
            "{deleted}/table@HEAD^^"
        ))
        .unwrap();
        assert_eq!(
            table.collection,
            Some(CollectionLabel::new(Label::Deleted(id)))
        );
        assert_eq!(table.to_string(), format!("{deleted}/table@HEAD~2"));
        assert!(TableRef::<TableName>::parse(format!("{deleted}/table")).is_err());
        assert!(VersionedTableRef::<TableName>::parse(format!("{deleted}/table@HEAD")).is_err());
    }

    #[test]
    fn test_versions_to_string() {
        let versions = Versions::parse("HEAD").unwrap();
        assert_eq!(versions.to_string(), "HEAD");
        let versions = Versions::parse("HEAD^^").unwrap();
        assert_eq!(versions.to_string(), "HEAD~2");
        let versions = Versions::parse("HEAD^^,HEAD").unwrap();
        assert_eq!(versions.to_string(), "HEAD~2,HEAD");
        let versions = Versions::parse("HEAD^^..HEAD").unwrap();
        assert_eq!(versions.to_string(), "HEAD~2..HEAD");
        let versions = Versions::parse("INITIAL").unwrap();
        assert_eq!(versions.to_string(), "INITIAL");
        let versions = Versions::parse("INITIAL^^").unwrap();
        assert_eq!(versions.to_string(), "INITIAL~2");
        let versions = Versions::parse("INITIAL^^,INITIAL").unwrap();
        assert_eq!(versions.to_string(), "INITIAL~2,INITIAL");
        let versions = Versions::parse("INITIAL^^..INITIAL").unwrap();
        assert_eq!(versions.to_string(), "INITIAL~2..INITIAL");
        let versions = Versions::parse("INITIAL^^,HEAD").unwrap();
        assert_eq!(versions.to_string(), "INITIAL~2,HEAD");
        let versions = Versions::parse("INITIAL^^..HEAD").unwrap();
        assert_eq!(versions.to_string(), "INITIAL~2..HEAD");
        let versions = Versions::parse("HEAD^^,INITIAL").unwrap();
        assert_eq!(versions.to_string(), "HEAD~2,INITIAL");
        let versions = Versions::parse("HEAD^^..INITIAL").unwrap();
        assert_eq!(versions.to_string(), "HEAD~2..INITIAL");
        // A fixed version renders padded, whichever form it was parsed from.
        let versions = Versions::parse("dv20240101-123").unwrap();
        assert_eq!(versions.to_string(), "dv20240101-0000000123");
        let versions = Versions::parse("dv20240101-0000000123").unwrap();
        assert_eq!(versions.to_string(), "dv20240101-0000000123");
    }
}
