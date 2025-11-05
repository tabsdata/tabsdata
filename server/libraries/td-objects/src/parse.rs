//
// Copyright 2025 Tabs Data Inc.
//

use crate::table_ref::{TableRef, Version, VersionedTableRef, Versions};
use crate::types::string::CollectionName;
use constcat::concat;
use regex::Regex;
use std::sync::LazyLock;
use td_common::id::Id;
use td_error::{TdError, td_error};

const IDENTIFIER_LEN: &str = "99";
pub const IDENTIFIER_PATTERN: &str = concat!("[a-zA-Z][a-zA-Z0-9_]{0,", IDENTIFIER_LEN, "}");
pub const UNDERSCORE_IDENTIFIER_PATTERN: &str =
    concat!("[a-zA-Z_][a-zA-Z0-9_]{0,", IDENTIFIER_LEN, "}");

pub const DATA_LOCATION_REGEX: &str = concat!("^(/|(/", IDENTIFIER_PATTERN, ")*)$");

#[td_error]
enum ParserError {
    #[error("Could not parse '{0}', expected: {1}")]
    CouldNotParse(String, String) = 0,
}

pub fn parser(
    regex: Regex,
    s: impl Into<String>,
    message: impl Into<String>,
) -> Result<String, TdError> {
    let s = s.into();
    if regex.is_match(&s) {
        Ok(s)
    } else {
        Err(ParserError::CouldNotParse(s, message.into()))?
    }
}

const VERSIONS_MARKER_REGEX: &str = "[^/@]+";
const VERSIONED_TABLE_PATTERN: &str = concat!(
    "^((?P<collection>[^/]+)/)?(?P<table>[^@]+)(@(?P<versions>",
    VERSIONS_MARKER_REGEX,
    "))?$"
);

pub fn parse_versioned_table_ref<T, E>(
    s: impl Into<String>,
) -> Result<VersionedTableRef<T>, TdError>
where
    T: TryFrom<String, Error = E>,
    E: Into<TdError>,
{
    static VERSIONED_TABLE_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(VERSIONED_TABLE_PATTERN).unwrap());

    let s = s.into();
    let (collection, table, versions) = match VERSIONED_TABLE_REGEX.captures(&s) {
        Some(captures) => {
            let collection = captures.name("collection").map(|m| m.as_str());
            let table = captures.name("table").unwrap().as_str();
            let versions = captures.name("versions").map(|m| m.as_str());
            if let Some(versions) = versions {
                let versions = parse_versions(versions)?;
                (collection, table, versions)
            } else {
                (collection, table, Versions::None)
            }
        }
        None => Err(ParserError::CouldNotParse(
            s.clone(),
            "a table dependency, a [<COLLECTION>/]<TABLE>[@<VERSIONS>] with \
<COLLECTION> and <NAME> being a [_A-Za-z0-9] word of up to 100 characters each \
and <VERSIONS> being a single version, a range of versions or a list of versions"
                .to_string(),
        ))?,
    };
    let collection = collection.map(CollectionName::try_from).transpose()?;
    let table = T::try_from(table.to_string()).map_err(Into::into)?;
    Ok(VersionedTableRef::new(collection, table, versions))
}

const TABLE_PATTERN: &str = "^((?P<collection>[^/]+)/)?(?P<table>[^/]+)$";

pub fn parse_table_ref<T, E>(s: impl Into<String>) -> Result<TableRef<T>, TdError>
where
    T: TryFrom<String, Error = E>,
    E: Into<TdError>,
{
    static TABLE_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(TABLE_PATTERN).unwrap());

    let s = s.into();
    let (collection, table) = match TABLE_REGEX.captures(&s) {
        Some(captures) => {
            let collection = captures.name("collection").map(|m| m.as_str());
            let table = captures.name("table").unwrap().as_str();
            (collection, table)
        }
        None => Err(ParserError::CouldNotParse(
            s.clone(),
            "trigger name, a [<COLLECTION>/]<TABLE> with <COLLECTION> and <NAME> \
being a [_A-Za-z0-9] word of up to 100 characters each"
                .to_string(),
        ))?,
    };
    let collection = collection.map(CollectionName::try_from).transpose()?;
    let table = T::try_from(table.to_string()).map_err(Into::into)?;
    Ok(TableRef::new(collection, table))
}

const VERSION_PATTERN: &str = concat!(
    "^(",
    "HEAD(?<head_back>\\^{0,10})",
    "|HEAD~(?<head_minus>[0-9]{1,7})",
    "|INITIAL(?<initial_forward>\\^{0,10})",
    "|INITIAL~(?<initial_plus>[0-9]{1,7})",
    "|(?<id>[A-Z0-9]{26})",
    ")$"
);

pub fn parse_version(s: impl Into<String>) -> Result<Version, TdError> {
    static VERSION_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(VERSION_PATTERN).unwrap());

    let s = s.into();
    match VERSION_REGEX.captures(&s) {
        None => Err(ParserError::CouldNotParse(
            s.clone(),
            "a single version, HEAD, INITIAL or fixed".to_string(),
        ))?,
        Some(captures) => {
            let version = if let Some(back) = captures.name("head_back") {
                Version::Head(-(back.len() as isize))
            } else if let Some(head_minus) = captures.name("head_minus") {
                let minus: isize = head_minus.as_str().parse().map_err(|_| {
                    ParserError::CouldNotParse(s.clone(), "a version HEAD index".to_string())
                })?;
                Version::Head(-minus)
            } else if let Some(forward) = captures.name("initial_forward") {
                Version::Initial(forward.len() as isize)
            } else if let Some(initial_plus) = captures.name("initial_plus") {
                let plus: isize = initial_plus.as_str().parse().map_err(|_| {
                    ParserError::CouldNotParse(s.clone(), "a version INITIAL index".to_string())
                })?;
                Version::Initial(plus)
            } else {
                let version = captures.name("id").unwrap().as_str();
                let id = Id::try_from(version).map_err(|_| {
                    ParserError::CouldNotParse(s.clone(), "a valid version ID".to_string())
                })?;
                Version::Fixed(id.into())
            };
            Ok(version)
        }
    }
}

const UNNAMED_VERSION_PATTERN: &str =
    "(HEAD(\\^{0,10})|HEAD(~[0-9]{1,7})|INITIAL(\\^{0,10})|INITIAL(\\~[0-9]{1,7})|[A-Z0-9]{26})";
const VERSIONS_PATTERN: &str = concat!(
    "^((?<single>",
    UNNAMED_VERSION_PATTERN,
    ")|(?<list>(",
    UNNAMED_VERSION_PATTERN,
    "(,",
    UNNAMED_VERSION_PATTERN,
    ")+))|(?<range>(",
    UNNAMED_VERSION_PATTERN,
    "..",
    UNNAMED_VERSION_PATTERN,
    ")))$"
);

pub fn parse_versions(s: impl Into<String>) -> Result<Versions, TdError> {
    static VERSIONS_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(VERSIONS_PATTERN).unwrap());

    let s = s.into();

    if s.is_empty() {
        return Ok(Versions::None);
    }

    let captures = VERSIONS_REGEX.captures(&s).ok_or_else(|| {
        ParserError::CouldNotParse(
            s.clone(),
            "<VERSIONS> being a single version, a range of versions or a list of versions"
                .to_string(),
        )
    })?;

    if let Some(version) = captures.name("single") {
        Ok(Versions::Single(parse_version(version.as_str())?))
    } else if let Some(list) = captures.name("list") {
        let parsed_list = list
            .as_str()
            .split(',')
            .map(parse_version)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Versions::List(parsed_list))
    } else if let Some(range) = captures.name("range") {
        let mut parsed_range = range
            .as_str()
            .split("..")
            .map(parse_version)
            .collect::<Result<Vec<_>, _>>()?;
        let to = parsed_range.pop().unwrap();
        let from = parsed_range.pop().unwrap();
        Ok(Versions::Range(from, to))
    } else {
        Err(ParserError::CouldNotParse(
            s.clone(),
            "<VERSIONS> being a single version, a range of versions or a list of versions"
                .to_string(),
        ))?
    }
}

const NAME_PATTERN: &str = concat!("^", IDENTIFIER_PATTERN, "$");

pub fn parse_name(s: impl Into<String>, name_type: &str) -> Result<String, TdError> {
    static REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(NAME_PATTERN).unwrap());

    parser(
        REGEX.clone(),
        s,
        format!("{name_type}, a [A-Za-z0-9] word of up to 100 characters"),
    )
}

const UNDERSCORE_NAME_PATTERN: &str = concat!("^", UNDERSCORE_IDENTIFIER_PATTERN, "$");

pub fn parse_underscore_name(s: impl Into<String>, name_type: &str) -> Result<String, TdError> {
    static REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(UNDERSCORE_NAME_PATTERN).unwrap());

    parser(
        REGEX.clone(),
        s,
        format!("{name_type}, a [_A-Za-z0-9] word of up to 100 characters"),
    )
}

pub fn parse_collection(s: impl Into<String>) -> Result<String, TdError> {
    parse_name(s, "Collection name")
}

pub fn parse_entity(s: impl Into<String>) -> Result<String, TdError> {
    parse_name(s, "Entity name")
}

pub fn parse_function(s: impl Into<String>) -> Result<String, TdError> {
    parse_name(s, "Function name")
}

pub fn parse_execution(s: impl Into<String>) -> Result<String, TdError> {
    parse_name(s, "Execution name")
}

pub fn parse_table(s: impl Into<String>) -> Result<String, TdError> {
    parse_underscore_name(s, "Table name")
}

pub fn parse_role(s: impl Into<String>) -> Result<String, TdError> {
    parse_name(s, "Role name")
}

pub fn parse_user(s: impl Into<String>) -> Result<String, TdError> {
    parse_name(s, "User name")
}

pub fn parse_email(s: impl Into<String>) -> Result<String, TdError> {
    const EMAIL_PATTERN: &str = "[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}";

    static REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(EMAIL_PATTERN).unwrap());

    parser(
        REGEX.clone(),
        s.into().to_lowercase(),
        "Invalid email address",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_ref::Version;
    use crate::types::string::TableNameDto;
    use td_common::id;

    #[test]
    fn test_parse_name() {
        let name = parse_name("abc".to_string(), "test").unwrap();
        assert_eq!(name, "abc");

        assert!(parse_name("".to_string(), "test").is_err());
        assert!(parse_name(" a".to_string(), "test").is_err());
        assert!(parse_name("a ".to_string(), "test").is_err());
        assert!(parse_name(" a ".to_string(), "test").is_err());
        assert!(parse_name("a a".to_string(), "test").is_err());
        assert!(parse_name("0a".to_string(), "test").is_err());
        assert!(parse_name("@".to_string(), "test").is_err());
        assert!(parse_name("a".repeat(101), "test").is_err());

        assert!(parse_name("A_".to_string(), "test").is_ok());
        assert!(parse_name("A".to_string(), "test").is_ok());
        assert!(parse_name("A1".to_string(), "test").is_ok());
        assert!(parse_name("a".to_string(), "test").is_ok());
        assert!(parse_name("a1".to_string(), "test").is_ok());
        assert!(parse_name("AZaz09_".to_string(), "test").is_ok());
        assert!(parse_name("a".repeat(100), "test").is_ok());
    }

    #[test]
    fn test_parse_names() {
        assert!(parse_user("abc".to_string()).is_ok());
        assert!(parse_user("_abc".to_string()).is_err());
        assert!(parse_collection("abc".to_string()).is_ok());
        assert!(parse_collection("_abc".to_string()).is_err());
        assert!(parse_function("abc".to_string()).is_ok());
        assert!(parse_function("_abc".to_string()).is_err());
        assert!(parse_table("abc".to_string()).is_ok());
        assert!(parse_table("_abc".to_string()).is_ok());
    }

    #[test]
    fn test_parse_table_ref() {
        assert!(parse_table_ref::<TableNameDto, _>("abc ".to_string()).is_err());
        assert!(parse_table_ref::<TableNameDto, _>(" abc ".to_string()).is_err());
        assert!(parse_table_ref::<TableNameDto, _>("abc/".to_string()).is_err());
        assert!(parse_table_ref::<TableNameDto, _>("/abc".to_string()).is_err());
        assert!(parse_table_ref::<TableNameDto, _>("@/a".to_string()).is_err());

        assert!(parse_table_ref::<TableNameDto, _>("abc".to_string()).is_ok());
        assert!(parse_table_ref::<TableNameDto, _>("xyz/abc".to_string()).is_ok());
    }

    #[test]
    fn test_parse_versioned_table_ref() {
        assert!(parse_versioned_table_ref::<TableNameDto, _>(" abc".to_string()).is_err());
        assert!(parse_versioned_table_ref::<TableNameDto, _>("abc ".to_string()).is_err());
        assert!(parse_versioned_table_ref::<TableNameDto, _>("abc/abc ".to_string()).is_err());
        assert!(parse_versioned_table_ref::<TableNameDto, _>(" abc/abc".to_string()).is_err());
        assert!(parse_versioned_table_ref::<TableNameDto, _>("abc/abc@".to_string()).is_err());
        assert!(parse_versioned_table_ref::<TableNameDto, _>("@abc".to_string()).is_err());
        assert!(
            parse_versioned_table_ref::<TableNameDto, _>("abc/abc@HEAD..HEAD,HEAD".to_string())
                .is_err()
        );
        assert!(
            parse_versioned_table_ref::<TableNameDto, _>("abc/abc@HEAD..HEAD..HEAD".to_string())
                .is_err()
        );
        assert!(parse_versioned_table_ref::<TableNameDto, _>("abc/abc@HEAD~".to_string()).is_err());

        assert!(parse_versioned_table_ref::<TableNameDto, _>("abc".to_string()).is_ok());
        assert!(parse_versioned_table_ref::<TableNameDto, _>("xyz/abc".to_string()).is_ok());
        assert!(parse_versioned_table_ref::<TableNameDto, _>("xyz/abc@HEAD".to_string()).is_ok());
        assert!(parse_versioned_table_ref::<TableNameDto, _>("xyz/abc@HEAD^^".to_string()).is_ok());
        assert!(
            parse_versioned_table_ref::<TableNameDto, _>("xyz/abc@HEAD^,HEAD".to_string()).is_ok()
        );
        assert!(
            parse_versioned_table_ref::<TableNameDto, _>("xyz/abc@HEAD^..HEAD".to_string()).is_ok()
        );
        assert!(parse_versioned_table_ref::<TableNameDto, _>("xyz/abc@HEAD~1".to_string()).is_ok());
        assert!(
            parse_versioned_table_ref::<TableNameDto, _>("xyz/abc@HEAD~10,HEAD".to_string())
                .is_ok()
        );
        assert!(
            parse_versioned_table_ref::<TableNameDto, _>("xyz/abc@HEAD~10..HEAD".to_string())
                .is_ok()
        );
    }

    #[test]
    fn test_parse_version() {
        let version = parse_version("HEAD").unwrap();
        assert_eq!(version, Version::Head(0));
        let version = parse_version("HEAD^").unwrap();
        assert_eq!(version, Version::Head(-1));
        let version = parse_version("HEAD~1").unwrap();
        assert_eq!(version, Version::Head(-1));
        assert!(parse_version("HEAD~a").is_err());
        let version = parse_version("INITIAL").unwrap();
        assert_eq!(version, Version::Initial(0));
        let version = parse_version("INITIAL^").unwrap();
        assert_eq!(version, Version::Initial(1));
        let version = parse_version("INITIAL~1").unwrap();
        assert_eq!(version, Version::Initial(1));
        assert!(parse_version("INITIAL~a").is_err());
        let id = id::id();
        let version = parse_version(id).unwrap();
        assert_eq!(version, Version::Fixed(id.into()));
        assert!(parse_version("A".repeat(26)).is_err());
    }

    #[test]
    fn test_parse_versions() {
        let versions = parse_versions("HEAD").unwrap();
        assert_eq!(versions, Versions::Single(Version::Head(0)));
        let versions = parse_versions("HEAD^").unwrap();
        assert_eq!(versions, Versions::Single(Version::Head(-1)));
        let versions = parse_versions("HEAD~1").unwrap();
        assert_eq!(versions, Versions::Single(Version::Head(-1)));
        let versions = parse_versions("HEAD~1,HEAD").unwrap();
        assert_eq!(
            versions,
            Versions::List(vec![Version::Head(-1), Version::Head(0)])
        );
        let versions = parse_versions("HEAD~1..HEAD").unwrap();
        assert_eq!(
            versions,
            Versions::Range(Version::Head(-1), Version::Head(0))
        );

        let versions = parse_versions("INITIAL").unwrap();
        assert_eq!(versions, Versions::Single(Version::Initial(0)));
        let versions = parse_versions("INITIAL^").unwrap();
        assert_eq!(versions, Versions::Single(Version::Initial(1)));
        let versions = parse_versions("INITIAL~1").unwrap();
        assert_eq!(versions, Versions::Single(Version::Initial(1)));
        let versions = parse_versions("INITIAL~1,INITIAL").unwrap();
        assert_eq!(
            versions,
            Versions::List(vec![Version::Initial(1), Version::Initial(0)])
        );
        let versions = parse_versions("INITIAL~1..INITIAL").unwrap();
        assert_eq!(
            versions,
            Versions::Range(Version::Initial(1), Version::Initial(0))
        );

        let versions = parse_versions("INITIAL~1,HEAD").unwrap();
        assert_eq!(
            versions,
            Versions::List(vec![Version::Initial(1), Version::Head(0)])
        );
        let versions = parse_versions("INITIAL~1..HEAD").unwrap();
        assert_eq!(
            versions,
            Versions::Range(Version::Initial(1), Version::Head(0))
        );
        let versions = parse_versions("HEAD~1,INITIAL").unwrap();
        assert_eq!(
            versions,
            Versions::List(vec![Version::Head(-1), Version::Initial(0)])
        );
        let versions = parse_versions("HEAD~1..INITIAL").unwrap();
        assert_eq!(
            versions,
            Versions::Range(Version::Head(-1), Version::Initial(0))
        );

        let id = id::id();
        let versions = parse_versions(format!("{id}")).unwrap();
        assert_eq!(versions, Versions::Single(Version::Fixed(id.into())));
        let versions = parse_versions(format!("{id},HEAD~2")).unwrap();
        assert_eq!(
            versions,
            Versions::List(vec![Version::Fixed(id.into()), Version::Head(-2)])
        );
        let versions = parse_versions(format!("HEAD~2,{id}")).unwrap();
        assert_eq!(
            versions,
            Versions::List(vec![Version::Head(-2), Version::Fixed(id.into())])
        );
        let versions = parse_versions(format!("{id},INITIAL~2")).unwrap();
        assert_eq!(
            versions,
            Versions::List(vec![Version::Fixed(id.into()), Version::Initial(2)])
        );
        let versions = parse_versions(format!("INITIAL~2,{id}")).unwrap();
        assert_eq!(
            versions,
            Versions::List(vec![Version::Initial(2), Version::Fixed(id.into())])
        );

        assert!(parse_versions("HEAD~a").is_err());
        assert!(parse_version("A".repeat(26)).is_err());
    }

    #[test]
    fn test_parse_valid_versioned_table_refs() {
        let valid_table_with_id = format!("collection/table@{}", id::id());
        let valid_tables = vec![
            "table",
            "collection/table",
            "table@HEAD",
            "collection/table@HEAD",
            "table@HEAD",
            "collection/table@HEAD",
            "collection/table@HEAD^",
            "collection/table@HEAD~1",
            "collection/table@HEAD^^^^,HEAD^,HEAD",
            "collection/table@HEAD^^..HEAD",
            "collection/table@INITIAL",
            "table@INITIAL",
            "collection/table@INITIAL",
            "collection/table@INITIAL^",
            "collection/table@INITIAL~1",
            "collection/table@INITIAL^^^^,INITIAL^,INITIAL",
            "collection/table@INITIAL^^..INITIAL",
            "collection/table@HEAD^^^^,INITIAL^,INITIAL",
            "collection/table@INITIAL^^..HEAD",
            "collection/table@INITIAL^^^^,HEAD^,INITIAL",
            "collection/table@INITIAL^^..HEAD",
            valid_table_with_id.as_str(),
        ];
        valid_tables.into_iter().for_each(|table| {
            let parsed = parse_versioned_table_ref::<TableNameDto, _>(table).unwrap();
            println!("{} -> {} - {}", table, parsed, parsed.versions);
        });
    }

    #[test]
    fn test_parse_valid_table_refs() {
        let valid_tables = vec!["table", "collection/table"];
        valid_tables.into_iter().for_each(|table| {
            let parsed = parse_table_ref::<TableNameDto, _>(table.to_string()).unwrap();
            println!("{table} -> {parsed}");
        });
    }

    #[test]
    fn test_parse_invalid_table_refs() {
        let invalid_table_refs = vec![
            "/table",
            "table/",
            "/table/",
            "collection//",
            "collection//table",
            "collection//table/",
            "collection/table/table/",
            "table@head",
            "table@initial",
            "collection/table@HEAD-1",
            "collection/table@INITIAL-1",
            "collection/table@01234567890123456789012",
            // Valid versioned, invalid refs
            "table@HEAD",
            "collection/table@HEAD",
            "table@HEAD",
            "collection/table@HEAD",
            "collection/table@HEAD^",
            "collection/table@HEAD~1",
            "collection/table@HEAD^^^^,HEAD^,HEAD",
            "collection/table@HEAD^^..HEAD",
            "table@INITIAL",
            "collection/table@INITIAL",
            "table@INITIAL",
            "collection/table@INITIAL",
            "collection/table@INITIAL^",
            "collection/table@INITIAL~1",
            "collection/table@INITIAL^^^^,INITIAL^,INITIAL",
            "collection/table@INITIAL^^..INITIAL",
        ];
        invalid_table_refs.into_iter().for_each(|table| {
            let parsed = parse_table_ref::<TableNameDto, _>(table);
            assert!(parsed.is_err());
        });
    }
}
