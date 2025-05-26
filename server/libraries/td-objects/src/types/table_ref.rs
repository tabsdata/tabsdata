//
// Copyright 202∞ Tabs Data Inc.
//

use crate::types::basic::{CollectionName, TableDataVersionId};
use crate::types::parse::{parse_table_ref, parse_versioned_table_ref, parse_versions};
use crate::types::ComposedString;
use derive_new::new;
use std::fmt::{Display, Formatter};
use td_error::TdError;

/// It represents a version.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Version {
    /// A fixed version.
    Fixed(TableDataVersionId),
    /// A head relative version, it is always zero or negative.
    Head(isize),
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Version::Fixed(id) => write!(f, "{}", id),
            Version::Head(back) => {
                if *back == 0 {
                    write!(f, "HEAD")
                } else {
                    write!(f, "HEAD~{}", -back)
                }
            }
        }
    }
}

impl Version {
    pub fn shift_mut(&mut self, pos: isize) {
        if let Version::Head(head) = self {
            *head += pos
        }
    }
}

/// It represents a set of versions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Versions {
    /// No versions
    None,
    /// A single version.
    Single(Version),
    /// A list of versions.
    List(Vec<Version>),
    /// A range of versions. Always older to newer.
    Range(Version, Version),
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
        }
    }
}

impl ComposedString for Versions {
    fn parse(s: impl Into<String>) -> Result<Self, TdError>
    where
        Self: Sized,
    {
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
            Versions::Single(version) => write!(f, "{}", version),
            Versions::List(versions) => {
                let versions = versions
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>()
                    .join(",");
                write!(f, "{}", versions)
            }
            Versions::Range(from, to) => write!(f, "{}..{}", from, to),
        }
    }
}

impl From<&Versions> for Versions {
    fn from(v: &Versions) -> Self {
        v.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, new, getset::Getters)]
#[getset(get = "pub")]
pub struct VersionedTableRef<T> {
    collection: Option<CollectionName>,
    table: T,
    versions: Versions,
}

impl<T, E> ComposedString for VersionedTableRef<T>
where
    T: Display + TryFrom<String, Error = E>,
    E: Into<TdError>,
{
    fn parse(s: impl Into<String>) -> Result<Self, TdError>
    where
        Self: Sized,
    {
        parse_versioned_table_ref(s)
    }

    fn compose(&self) -> String {
        self.to_string()
    }
}

impl<T> From<VersionedTableRef<T>> for String
where
    T: Display,
{
    fn from(value: VersionedTableRef<T>) -> Self {
        value.to_string()
    }
}

impl<T> Display for VersionedTableRef<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match (&self.collection, &self.versions) {
            (Some(collection), Versions::None) => write!(f, "{}/{}", collection, &self.table),
            (Some(collection), versions) => {
                write!(f, "{}/{}@{}", collection, &self.table, versions)
            }
            (None, Versions::None) => write!(f, "{}", &self.table),
            (None, versions) => write!(f, "{}@{}", &self.table, versions),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, new, getset::Getters)]
#[getset(get = "pub")]
pub struct TableRef<T> {
    collection: Option<CollectionName>,
    table: T,
}

impl<T, E> ComposedString for TableRef<T>
where
    T: Display + TryFrom<String, Error = E>,
    E: Into<TdError>,
{
    fn parse(s: impl Into<String>) -> Result<Self, TdError>
    where
        Self: Sized,
    {
        parse_table_ref(s)
    }

    fn compose(&self) -> String {
        self.to_string()
    }
}

impl<T> From<TableRef<T>> for String
where
    T: Display,
{
    fn from(value: TableRef<T>) -> Self {
        value.to_string()
    }
}

impl<T> Display for TableRef<T>
where
    T: Display,
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
    use crate::types::basic::TableName;
    use td_common::id;

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
        let table = format!("collection/table@{}", id::id());
        assert_eq!(table.to_string(), table);
    }

    #[test]
    fn test_table_ref_to_string() {
        let table = TableRef::<TableName>::parse("table").unwrap();
        assert_eq!(table.to_string(), "table");
        let table = TableRef::<TableName>::parse("collection/table").unwrap();
        assert_eq!(table.to_string(), "collection/table");
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
        let id = id::id();
        let versions = Versions::parse(format!("{}", id)).unwrap();
        assert_eq!(versions.to_string(), format!("{}", id));
    }
}
