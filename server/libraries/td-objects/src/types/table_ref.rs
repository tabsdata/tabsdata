//
// Copyright 202∞ Tabs Data Inc.
//

use crate::types::basic::{CollectionName, TableName};
use crate::types::parse::{parse_table_ref, parse_versioned_table_ref, parse_versions};
use crate::types::ComposedString;
use derive_new::new;
use std::fmt::{Display, Formatter};
use td_common::id::Id;
use td_error::TdError;

/// It represents a version.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Version {
    /// A fixed version.
    Fixed(Id),
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

/// It represents a set of versions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Versions {
    /// No versions
    None,
    /// A single version.
    Single(Version),
    /// A list of versions.
    List(Vec<Version>),
    /// A range of versions.
    Range(Version, Version),
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
pub struct VersionedTableRef {
    collection: Option<CollectionName>,
    table: TableName,
    versions: Versions,
}

impl ComposedString for VersionedTableRef {
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

impl Display for VersionedTableRef {
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
pub struct TableRef {
    collection: Option<CollectionName>,
    table: TableName,
}

impl ComposedString for TableRef {
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

impl Display for TableRef {
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
    use td_common::id;

    #[test]
    fn test_versioned_table_ref_to_string() {
        let table = VersionedTableRef::parse("table").unwrap();
        assert_eq!(table.to_string(), "table");
        let table = VersionedTableRef::parse("collection/table").unwrap();
        assert_eq!(table.to_string(), "collection/table");
        let table = VersionedTableRef::parse("collection/table@HEAD^^").unwrap();
        assert_eq!(table.to_string(), "collection/table@HEAD~2");
        let table = VersionedTableRef::parse("collection/table@HEAD^^..HEAD").unwrap();
        assert_eq!(table.to_string(), "collection/table@HEAD~2..HEAD");
        let table = VersionedTableRef::parse("collection/table@HEAD^^,HEAD").unwrap();
        assert_eq!(table.to_string(), "collection/table@HEAD~2,HEAD");
        let table = format!("collection/table@{}", id::id());
        assert_eq!(table.to_string(), table);
    }

    #[test]
    fn test_table_ref_to_string() {
        let table = TableRef::parse("table").unwrap();
        assert_eq!(table.to_string(), "table");
        let table = TableRef::parse("collection/table").unwrap();
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
