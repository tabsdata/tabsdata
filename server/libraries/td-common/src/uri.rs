//
// Copyright 2024 Tabs Data Inc.
//

use crate::id::Id;
use crate::name::{name_regex_pattern, name_with_dot_regex_pattern};
use crate::uri::TdUri::{Dataset, Table};
use getset::Getters;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use td_error::td_error;

/// [`TdUri`] parsing errors.
#[td_error]
pub enum TdUriError {
    #[error(
        "URI '{0}', invalid URI syntax, it must be td://[/collection/]dataset[/table][@versions]"
    )]
    InvalidUri(String) = 0,
    #[error(
        "URI '{0}', invalid version '{1}', it must be HEAD, HEAD^..., HEAD~# or a fixed version"
    )]
    InvalidVersionInUri(String, String) = 1,
    #[error("URI '{0}', invalid fixed version '{1}', it is not a valid ID")]
    InvalidFixedVersionInUri(String, String) = 2,
    #[error("URI '{0}', invalid versions '{1}', it must be a single <VERSION>, a list of <VERSION>,<VERSION>,... or a range <VERSION>..<VERSION>")]
    InvalidVersions(String, String) = 3,
    #[error("Invalid version '{0}', it must be HEAD, HEAD^..., HEAD~# or a fixed version")]
    InvalidVersion(String) = 4,
    #[error("Invalid fixed version '{0}', it is not a valid ID")]
    InvalidFixedVersion(String) = 5,
}

/// It represents a dataset version.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Version {
    /// A fixed version.
    Fixed(Id),
    /// A head relative version, it is always zero or negative.
    Head(isize),
}

impl Version {
    /// Parses a [`Version`]. A version can be relative to the head or a fixed version.
    pub fn parse(version: &str) -> Result<Version, TdUriError> {
        const ID_LEN: usize = 26;

        lazy_static! {
            static ref ID_REGEX_: String = format!("(?<id>[A-Z0-9]{{{}}})", ID_LEN);
            static ref VERSION_REGEX_: String = format!(
                "^(HEAD(?<head_back>(\\^)*))$|^(HEAD\\~(?<head_minus>[0-9]+))$|^{}$",
                ID_REGEX_.deref()
            );
            static ref VERSION_REGEX: Regex = Regex::new(&VERSION_REGEX_).unwrap();
        }

        match VERSION_REGEX.captures(version) {
            None => Err(TdUriError::InvalidVersion(version.to_string())),
            Some(captures) => {
                let version = match captures.name("head_back") {
                    Some(back) => Version::Head(-(back.len() as isize)),
                    None => match captures.name("head_minus") {
                        Some(head_minus) => {
                            let minus: isize = head_minus
                                .as_str()
                                .parse()
                                .map_err(|_| TdUriError::InvalidVersion(version.to_string()))?;
                            Version::Head(-minus)
                        }
                        None => {
                            let version = captures.name("id").unwrap().as_str();
                            let id = Id::try_from(version).map_err(|_| {
                                TdUriError::InvalidFixedVersion(version.to_string())
                            })?;
                            Version::Fixed(id)
                        }
                    },
                };
                Ok(version)
            }
        }
    }

    pub fn shift(&mut self, pos: isize) {
        if let Version::Head(head) = self {
            *head += pos
        }
    }

    pub fn is_fixed(&self) -> bool {
        matches!(self, Version::Fixed(_))
    }

    pub fn is_head(&self) -> bool {
        matches!(self, Version::Head(_))
    }

    pub fn head(&self) -> Option<isize> {
        match self {
            Version::Head(back) => Some(*back),
            _ => None,
        }
    }
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

/// It represents a set of dataset versions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

impl Versions {
    /// Gets all the already known versions from the set of versions.
    pub fn fixed(&self) -> Vec<Id> {
        match self {
            Versions::None => vec![],
            Versions::Single(version) => match version {
                Version::Fixed(id) => vec![*id],
                _ => vec![],
            },
            Versions::List(versions) => versions
                .iter()
                .filter_map(|v| match v {
                    Version::Fixed(id) => Some(*id),
                    _ => None,
                })
                .collect(),
            Versions::Range(from, to) => {
                let mut ids = vec![];
                if let Version::Fixed(id) = from {
                    ids.push(*id);
                }
                if let Version::Fixed(id) = to {
                    ids.push(*id);
                }
                ids
            }
        }
    }

    pub fn flatten(&self) -> Vec<Version> {
        match self {
            Versions::None => vec![],
            Versions::Single(version) => vec![version.clone()],
            Versions::List(versions) => versions.clone(),
            Versions::Range(from, to) => vec![from.clone(), to.clone()],
        }
    }

    pub fn shift(&mut self, pos: isize) {
        match self {
            Versions::Single(version) => version.shift(pos),
            Versions::List(versions) => versions.iter_mut().for_each(|v| v.shift(pos)),
            Versions::Range(from, to) => {
                from.shift(pos);
                to.shift(pos);
            }
            _ => {}
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Versions::None)
    }

    pub fn is_single(&self) -> bool {
        matches!(self, Versions::Single(_))
    }

    pub fn is_list(&self) -> bool {
        matches!(self, Versions::List(_))
    }

    pub fn is_range(&self) -> bool {
        matches!(self, Versions::Range(_, _))
    }

    /// If not a range it returns [`None`]
    ///
    /// If it is a range and any of the versions is fixed it returns [`Some(true)`]
    /// If it is a range and both versions are relative and the left one is previous
    /// the right one it returns [`Some(true)`], otherwise it returns[`Some(false)`]
    pub fn is_range_valid(&self) -> Option<bool> {
        match self {
            Versions::Range(from, to) => match (from, to) {
                (Version::Head(from), Version::Head(to)) if from > to => Some(false),
                _ => Some(true),
            },
            _ => None,
        }
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
    fn from(value: &Versions) -> Versions {
        value.clone()
    }
}

/// A tabsdata URI that represents a dataset or a table and a set of versions for it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TdUri {
    /// A dataset. If versions is not specified ([`Versions::None`]) it is assumed it is means [`Versions::Single`]([`Version::Head`](0)).
    Dataset {
        collection: String,
        dataset: String,
        versions: Versions,
    },
    /// A table.  If versions is not specified ([`Versions::None`]) it is assumed it is means [`Versions::Single`]([`Version::Head`](0)).
    Table {
        collection: String,
        dataset: String,
        table: String,
        versions: Versions,
    },
}

pub trait ToUriString {
    fn to_uri_string(&self) -> String;
}

impl ToUriString for TdUri {
    fn to_uri_string(&self) -> String {
        match self {
            Dataset {
                collection,
                dataset,
                versions,
            } => {
                if matches!(versions, Versions::None) {
                    format!("td:///{}/{}", collection, dataset)
                } else {
                    format!("td:///{}/{}@{}", collection, dataset, versions)
                }
            }
            Table {
                collection,
                dataset,
                table,
                versions,
            } => {
                if matches!(versions, Versions::None) {
                    format!("td:///{}/{}/{}", collection, dataset, table)
                } else {
                    format!("td:///{}/{}/{}@{}", collection, dataset, table, versions)
                }
            }
        }
    }
}

impl Display for TdUri {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Dataset {
                collection,
                dataset,
                versions,
            } => {
                if matches!(versions, Versions::None) {
                    write!(f, "{}/{}", collection, dataset)
                } else {
                    write!(f, "{}/{}@{}", collection, dataset, versions)
                }
            }
            Table {
                collection,
                dataset,
                table,
                versions,
            } => {
                if matches!(versions, Versions::None) {
                    write!(f, "{}/{}/{}", collection, dataset, table)
                } else {
                    write!(f, "{}/{}/{}@{}", collection, dataset, table, versions)
                }
            }
        }
    }
}

impl TdUri {
    /// Parses a [`Version`]. A version can be relative to the head or a fixed version.
    fn parse_version<'a>(uri: &'a str, version: &'a str) -> Result<Version, TdUriError> {
        match Version::parse(version) {
            Ok(version) => Ok(version),
            Err(TdUriError::InvalidVersion(version)) => {
                Err(TdUriError::InvalidVersionInUri(uri.to_string(), version))
            }
            Err(TdUriError::InvalidFixedVersion(version)) => Err(
                TdUriError::InvalidFixedVersionInUri(uri.to_string(), version),
            ),
            Err(e) => Err(e),
        }
    }

    /// Parses a [`Versions`]. Versions can be a single version, a list of versions or a range of versions.
    pub fn parse_versions<'a>(uri: &'a str, versions: &'a str) -> Result<Versions, TdUriError> {
        lazy_static! {
            static ref PROTO_VERSION_REGEX_: String = String::from("[a-zA-Z0-9_-~\\^]*");
            static ref VERSIONS_REGEX_: String = format!(
                "^(?<single>{})$|^(?<list>({})(,{})+)$|^(?<range>{}..{})$",
                PROTO_VERSION_REGEX_.deref(),
                PROTO_VERSION_REGEX_.deref(),
                PROTO_VERSION_REGEX_.deref(),
                PROTO_VERSION_REGEX_.deref(),
                PROTO_VERSION_REGEX_.deref()
            );
            static ref VERSIONS_REGEX: Regex = Regex::new(&VERSIONS_REGEX_).unwrap();
        }

        let captures = VERSIONS_REGEX
            .captures(versions)
            .ok_or_else(|| TdUriError::InvalidVersions(uri.to_string(), versions.to_string()))?;

        let result = if let Some(version) = captures.name("single") {
            Ok(Versions::Single(Self::parse_version(
                uri,
                version.as_str(),
            )?))
        } else if let Some(list) = captures.name("list") {
            let parsed_list = list
                .as_str()
                .split(',')
                .map(|v| Self::parse_version(uri, v))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Versions::List(parsed_list))
        } else if let Some(range) = captures.name("range") {
            let mut parsed_range = range
                .as_str()
                .split("..")
                .map(|v| Self::parse_version(uri, v))
                .collect::<Result<Vec<_>, _>>()?;
            let to = parsed_range.pop().unwrap();
            let from = parsed_range.pop().unwrap();
            Ok(Versions::Range(from, to))
        } else {
            Err(TdUriError::InvalidVersions(
                uri.to_string(),
                versions.to_string(),
            ))
        };
        result
    }

    pub fn trigger_uri(collection: impl Into<String>, dataset: impl Into<String>) -> Self {
        Dataset {
            collection: collection.into(),
            dataset: dataset.into(),
            versions: Versions::None,
        }
    }

    pub fn table_uri(
        collection: impl Into<String>,
        dataset: impl Into<String>,
        table: impl Into<String>,
        versions: impl Into<Versions>,
    ) -> Self {
        Table {
            collection: collection.into(),
            dataset: dataset.into(),
            table: table.into(),
            versions: versions.into(),
        }
    }

    pub fn new(
        collection: &str,
        dataset: &str,
        table: Option<&str>,
        versions: Option<&str>,
    ) -> Result<Self, TdUriError> {
        let versions = match versions {
            Some(versions) => Self::parse_versions("", versions)?,
            None => Versions::None,
        };
        let uri = if let Some(table) = table {
            Table {
                collection: collection.to_string(),
                dataset: dataset.to_string(),
                table: table.to_string(),
                versions,
            }
        } else {
            Dataset {
                collection: collection.to_string(),
                dataset: dataset.to_string(),
                versions,
            }
        };
        Ok(uri)
    }

    pub fn new_with_ids(
        collection: Id,
        dataset: Id,
        table: Option<String>,
        versions: Option<Versions>,
    ) -> Self {
        if let Some(table) = table {
            Table {
                collection: collection.to_string(),
                dataset: dataset.to_string(),
                table,
                versions: versions.unwrap_or(Versions::None),
            }
        } else {
            Dataset {
                collection: collection.to_string(),
                dataset: dataset.to_string(),
                versions: versions.unwrap_or(Versions::None),
            }
        }
    }

    /// Parses a tabsdata URI, the collection is context is with URIs that do not specify a collection.
    ///
    /// Tabsdata URI syntax:
    ///
    /// * td://[/collection/]dataset[/table][@<VERSION>|<VERSION>,<VERSION>,...|<VERSION>..<VERSION>]
    ///
    /// <VERSION> syntax:
    ///
    /// * HEAD
    /// * HEAD^...
    /// * HEAD~#
    /// * An [`Id`] string representation.
    pub fn parse<'a>(
        collection_in_context: &str,
        uri: impl Into<&'a str>,
    ) -> Result<Self, TdUriError> {
        let uri = uri.into();

        lazy_static! {
            static ref _URI_REGEX: String = format!(
                "^td://(/(?<collection>{})/)?(?<dataset>{})(/(?<table>{}))?(@(?<versions>.+))?$",
                name_regex_pattern(),
                name_regex_pattern(),
                name_with_dot_regex_pattern()
            );
            static ref URI_REGEX: Regex = Regex::new(&_URI_REGEX).unwrap();
        }

        match URI_REGEX.captures(uri) {
            None => Err(TdUriError::InvalidUri(format!("Invalid URI: {}", uri))),
            Some(captures) => {
                let collection = captures
                    .name("collection")
                    .map(|ds| ds.as_str())
                    .unwrap_or(collection_in_context);
                let dataset = captures.name("dataset").map(|d| d.as_str()).unwrap();
                let table = captures.name("table").map(|t| t.as_str());
                let versions = captures.name("versions").map(|v| v.as_str());
                let versions = match versions {
                    None => Versions::None,
                    Some(versions) => Self::parse_versions(uri, versions)?,
                };
                let uri = match table {
                    None => Dataset {
                        collection: collection.to_string(),
                        dataset: dataset.to_string(),
                        versions,
                    },
                    Some(table) => Table {
                        collection: collection.to_string(),
                        dataset: dataset.to_string(),
                        table: table.to_string(),
                        versions,
                    },
                };
                Ok(uri)
            }
        }
    }

    /// Returns a URI that is versioned.
    ///
    /// For versioned URIs returns the same URI.
    ///
    /// For non-versioned URIs, it converts the URI to a versioned URI with
    /// [`Versions::Single`]([`Version::Head`](0)) version.
    pub fn versioned(&self) -> Self {
        let mut normalized = self.clone();
        let versions = match &mut normalized {
            Dataset { versions, .. } => versions,
            Table { versions, .. } => versions,
        };
        if matches!(versions, Versions::None) {
            *versions = Versions::Single(Version::Head(0));
        }
        normalized
    }

    /// Returns the collection name.
    pub fn collection(&self) -> &str {
        match self {
            Dataset { collection, .. } => collection,
            Table { collection, .. } => collection,
        }
    }

    /// Returns the dataset name.
    pub fn dataset(&self) -> &str {
        match self {
            Dataset { dataset, .. } => dataset,
            Table { dataset, .. } => dataset,
        }
    }

    /// Returns the table name, if it is a table URI. [`None`] otherwise.
    pub fn table(&self) -> Option<&str> {
        match self {
            Dataset { .. } => None,
            Table { table, .. } => Some(table),
        }
    }

    /// Returns the versions.
    pub fn versions(&self) -> &Versions {
        match self {
            Dataset { versions, .. } => versions,
            Table { versions, .. } => versions,
        }
    }

    /// If the URI variant is a dataset.
    pub fn is_dataset(&self) -> bool {
        matches!(self, Dataset { .. })
    }

    /// If the URI variant is a table.
    pub fn is_table(&self) -> bool {
        matches!(self, Table { .. })
    }

    /// If the URI is versioned.
    pub fn is_versioned(&self) -> bool {
        !matches!(self.versions(), Versions::None)
    }

    /// Return a new [`TdUri`] replacing the collection and dataset.
    pub fn replace(&self, collection: &str, dataset: &str) -> Self {
        let mut this = self.clone();
        match &mut this {
            Dataset {
                collection: ref mut uri_collection,
                dataset: ref mut uri_dataset,
                ..
            } => {
                *uri_collection = collection.to_string();
                *uri_dataset = dataset.to_string();
            }
            Table {
                collection: ref mut uri_collection,
                dataset: ref mut uri_dataset,
                ..
            } => {
                *uri_collection = collection.to_string();
                *uri_dataset = dataset.to_string();
            }
        };
        this
    }

    /// If the URI is a dataset URI it returns it,
    /// if it is a table URI it returns the corresponding dataset URI.
    pub fn dataset_uri(&self) -> Self {
        match self {
            Dataset { .. } => self.clone(),
            Table {
                collection,
                dataset,
                ..
            } => Dataset {
                collection: collection.clone(),
                dataset: dataset.clone(),
                versions: self.versions().clone(),
            },
        }
    }

    /// Returns the URI without any versioning information.
    pub fn without_versions(&self) -> Self {
        match self {
            Dataset {
                collection,
                dataset,
                ..
            } => Dataset {
                collection: collection.clone(),
                dataset: dataset.clone(),
                versions: Versions::None,
            },
            Table {
                collection,
                dataset,
                table,
                ..
            } => Table {
                collection: collection.clone(),
                dataset: dataset.clone(),
                table: table.clone(),
                versions: Versions::None,
            },
        }
    }

    pub fn with_versions(&self, versions: Versions) -> Self {
        let mut this = self.clone();
        match &mut this {
            Dataset {
                versions: ref mut uri_versions,
                ..
            } => {
                *uri_versions = versions;
            }
            Table {
                versions: ref mut uri_versions,
                ..
            } => {
                *uri_versions = versions;
            }
        }
        this
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Getters)]
#[getset(get = "pub")]
pub struct TdUriNameId {
    with_names: TdUri,
    with_ids: TdUri,
}

impl TdUriNameId {
    pub fn from(with_names: &TdUri, collection_id: &str, dataset_id: &str) -> Self {
        Self {
            with_names: with_names.clone(),
            with_ids: with_names.replace(collection_id, dataset_id),
        }
    }

    pub fn new(with_names: TdUri, with_ids: TdUri) -> Self {
        Self {
            with_names,
            with_ids,
        }
    }

    pub fn dissolve(self) -> (TdUri, TdUri) {
        (self.with_names, self.with_ids)
    }

    pub fn replace_versions(&self, versions: Versions) -> Self {
        Self {
            with_names: self.with_names.with_versions(versions.clone()),
            with_ids: self.with_ids.with_versions(versions),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id;

    #[test]
    fn test_parse_version() {
        let version = Version::parse("HEAD").unwrap();
        assert!(matches!(version, Version::Head(0)));
        let version = Version::parse("HEAD^").unwrap();
        assert!(matches!(version, Version::Head(-1)));
        let version = Version::parse("HEAD~1").unwrap();
        assert!(matches!(version, Version::Head(-1)));
        let id = id::id();
        let version = Version::parse(&id.to_string()).unwrap();
        assert!(matches!(
            Version::parse("HEAD~a"),
            Err(TdUriError::InvalidVersion(_))
        ));
        assert!(matches!(version, Version::Fixed(_)));
        assert!(matches!(
            Version::parse(&"A".repeat(26)),
            Err(TdUriError::InvalidFixedVersion(_))
        ));
    }

    #[test]
    fn test_parse_valid_uris() {
        let valid_uri_with_id = format!("td:///collection/dataset/table@{}", id::id());
        let valid_uris = vec![
            "td://dataset",
            "td:///collection/dataset",
            "td://dataset@HEAD",
            "td:///collection/dataset@HEAD",
            "td://dataset/table",
            "td:///collection/dataset/table",
            "td://dataset/table@HEAD",
            "td:///collection/dataset/table@HEAD",
            "td:///collection/dataset/table@HEAD^",
            "td:///collection/dataset/table@HEAD~1",
            "td:///collection/dataset/table@HEAD^^^^,HEAD^,HEAD",
            "td:///collection/dataset/table@HEAD^^..HEAD",
            "td:///collection/dataset/.table@HEAD^^..HEAD", // with DOT, system tables
            valid_uri_with_id.as_str(),
        ];
        valid_uris.into_iter().for_each(|uri| {
            let parsed = TdUri::parse("default", uri).unwrap();
            println!("{} - {} - {}", uri, parsed, parsed.versioned());
        });
    }

    #[test]
    fn test_parse_invalid_uris() {
        let invalid_uris = vec![
            "tx://dataset",
            "td:dataset",
            "td:/dataset",
            "td://dataset/",
            "td:///collection//",
            "td:///collection//dataset",
            "td:///collection//dataset/",
            "td:///collection/dataset/table/",
            "td://dataset@head",
            "td:///collection/dataset@HEAD-1",
            "td:///collection/dataset/table@01234567890123456789012",
        ];
        invalid_uris.into_iter().for_each(|uri| {
            assert!(TdUri::parse("default", uri).is_err());
        });
    }

    #[test]
    fn test_to_uri_string() {
        let uri = TdUri::parse("default", "td://dataset").unwrap();
        assert_eq!(uri.to_uri_string(), "td:///default/dataset");
        let uri = TdUri::parse("default", "td://dataset/table").unwrap();
        assert_eq!(uri.to_uri_string(), "td:///default/dataset/table");
        let uri = TdUri::parse("default", "td://dataset/table@HEAD^^").unwrap();
        assert_eq!(uri.to_uri_string(), "td:///default/dataset/table@HEAD~2");
        let uri = TdUri::parse("default", "td://dataset/table@HEAD^^..HEAD").unwrap();
        assert_eq!(
            uri.to_uri_string(),
            "td:///default/dataset/table@HEAD~2..HEAD"
        );
        let uri = TdUri::parse("default", "td://dataset/table@HEAD^^,HEAD").unwrap();
        assert_eq!(
            uri.to_uri_string(),
            "td:///default/dataset/table@HEAD~2,HEAD"
        );
    }

    #[test]
    fn test_to_string() {
        let uri = TdUri::parse("default", "td://dataset").unwrap();
        assert_eq!(uri.to_string(), "default/dataset");
        let uri = TdUri::parse("default", "td://dataset/table").unwrap();
        assert_eq!(uri.to_string(), "default/dataset/table");
        let uri = TdUri::parse("default", "td://dataset/table@HEAD^^").unwrap();
        assert_eq!(uri.to_string(), "default/dataset/table@HEAD~2");
        let uri = TdUri::parse("default", "td://dataset/table@HEAD^^..HEAD").unwrap();
        assert_eq!(uri.to_string(), "default/dataset/table@HEAD~2..HEAD");
        let uri = TdUri::parse("default", "td://dataset/table@HEAD^^,HEAD").unwrap();
        assert_eq!(uri.to_string(), "default/dataset/table@HEAD~2,HEAD");
        let uri = format!("collection/dataset/table@{}", id::id());
        assert_eq!(uri.to_string(), uri);
    }

    #[test]
    fn test_versioned() {
        let uri = TdUri::parse("default", "td://dataset").unwrap();
        assert!(matches!(uri.versions(), &Versions::None));
        let versioned = uri.versioned();
        assert!(matches!(
            versioned.versions(),
            &Versions::Single(Version::Head(0))
        ));
    }

    #[test]
    fn test_getters() {
        let uri = TdUri::parse("default", "td://dataset@HEAD").unwrap();
        assert_eq!(uri.collection(), "default");
        assert_eq!(uri.dataset(), "dataset");
        assert_eq!(uri.table(), None);
        assert_eq!(uri.versions(), &Versions::Single(Version::Head(0)));
    }

    #[test]
    fn test_replace() {
        let uri = TdUri::parse("default", "td:///collection/dataset@HEAD").unwrap();
        let uri = uri.replace("foo", "bar");
        assert_eq!(uri.collection(), "foo");
        assert_eq!(uri.dataset(), "bar");
        assert_eq!(uri.table(), None);
        assert_eq!(uri.versions(), &Versions::Single(Version::Head(0)));
    }

    #[test]
    fn test_is_methods() {
        let uri = TdUri::parse("default", "td:///collection/dataset").unwrap();
        assert!(uri.is_dataset());
        assert!(!uri.is_table());
        assert!(!uri.is_versioned());

        let uri = TdUri::parse("default", "td:///collection/dataset/table@HEAD").unwrap();
        assert!(!uri.is_dataset());
        assert!(uri.is_table());
        assert!(uri.is_versioned());
    }

    #[test]
    fn test_td_name_id() {
        let uri1 = TdUri::parse("default", "td:///ds/d").unwrap();
        let uri2 = TdUri::parse("default", "td:///ds/d").unwrap();
        let td_nam_id = TdUriNameId::new(uri1.clone(), uri2.clone());
        assert_eq!(td_nam_id.with_names(), &uri1);
        assert_eq!(td_nam_id.with_ids(), &uri2);
        assert_eq!(td_nam_id.dissolve(), (uri1, uri2))
    }

    #[test]
    fn test_fixed() {
        let id0 = id::id();
        let id1 = id::id();
        let versions = Versions::Single(Version::Fixed(id0));
        assert_eq!(versions.fixed(), vec![id0]);
        let versions = Versions::List(vec![Version::Fixed(id0), Version::Head(0)]);
        assert_eq!(versions.fixed(), vec![id0]);
        let versions = Versions::Range(Version::Fixed(id0), Version::Head(0));
        assert_eq!(versions.fixed(), vec![id0]);
        let versions = Versions::Range(Version::Fixed(id0), Version::Fixed(id1));
        assert_eq!(versions.fixed(), vec![id0, id1]);
    }

    #[test]
    fn test_flatten() {
        let id0 = id::id();
        let versions = Versions::Single(Version::Fixed(id0));
        assert_eq!(versions.flatten(), vec![Version::Fixed(id0)]);
        let versions = Versions::List(vec![Version::Fixed(id0), Version::Head(0)]);
        assert_eq!(
            versions.flatten(),
            vec![Version::Fixed(id0), Version::Head(0)]
        );
        let versions = Versions::Range(Version::Fixed(id0), Version::Head(0));
        assert_eq!(
            versions.flatten(),
            vec![Version::Fixed(id0), Version::Head(0)]
        );
    }

    #[test]
    fn test_version() {
        let version = Version::Fixed(id::id());
        assert!(version.is_fixed());
        assert!(!version.is_head());
        assert_eq!(version.head(), None);

        let version = Version::Head(-1);
        assert!(!version.is_fixed());
        assert!(version.is_head());
        assert_eq!(version.head(), Some(-1));
    }

    #[test]
    fn test_versions() {
        let id0 = id::id();
        let versions = Versions::Single(Version::Fixed(id0));
        assert!(versions.is_single());
        assert!(!versions.is_list());
        assert!(!versions.is_range());
        assert!(!versions.is_empty());

        let versions = Versions::List(vec![Version::Fixed(id0), Version::Head(0)]);
        assert!(!versions.is_single());
        assert!(versions.is_list());
        assert!(!versions.is_range());
        assert!(!versions.is_empty());

        let versions = Versions::Range(Version::Fixed(id0), Version::Head(0));
        assert!(!versions.is_single());
        assert!(!versions.is_list());
        assert!(versions.is_range());
        assert!(!versions.is_empty());

        let versions = Versions::None;
        assert!(!versions.is_single());
        assert!(!versions.is_list());
        assert!(!versions.is_range());
        assert!(versions.is_empty());
    }

    #[test]
    fn test_versions_is_range_valid() {
        let versions = Versions::Single(Version::Fixed(id::id()));
        assert_eq!(versions.is_range_valid(), None);

        let versions = Versions::List(vec![Version::Fixed(id::id()), Version::Head(0)]);
        assert_eq!(versions.is_range_valid(), None);

        let versions = Versions::Range(Version::Fixed(id::id()), Version::Fixed(id::id()));
        assert_eq!(versions.is_range_valid(), Some(true));

        let versions = Versions::Range(Version::Fixed(id::id()), Version::Head(0));
        assert_eq!(versions.is_range_valid(), Some(true));

        let versions = Versions::Range(Version::Head(0), Version::Fixed(id::id()));
        assert_eq!(versions.is_range_valid(), Some(true));

        let versions = Versions::Range(Version::Head(-1), Version::Head(0));
        assert_eq!(versions.is_range_valid(), Some(true));

        let versions = Versions::Range(Version::Head(0), Version::Head(0));
        assert_eq!(versions.is_range_valid(), Some(true));

        let versions = Versions::Range(Version::Head(-4), Version::Head(-5));
        assert_eq!(versions.is_range_valid(), Some(false));
    }
}
