//
// Copyright 2025 Tabs Data Inc.
//

use constcat::concat;
use lazy_static::lazy_static;
use regex::Regex;
use td_common::error::TdError;
use td_error::td_error;

const IDENTIFIER_LEN: &str = "99";

const IDENTIFIER_PATTERN: &str = concat!("[a-zA-Z_][a-zA-Z0-9_]{0,", IDENTIFIER_LEN, "}");

const NAME_PATTERN: &str = concat!("^", IDENTIFIER_PATTERN, "$");
const DATA_LOCATION_REGEX: &str = concat!("^(/|(/", IDENTIFIER_PATTERN, ")*)$");
const TRIGGER_PATTERN: &str = concat!("^(", IDENTIFIER_PATTERN, "/)?(", IDENTIFIER_PATTERN, ")$");
const VERSIONS_MARKER_REGEX: &str = "[^/@]*";
const DEPENDENCY_PATTERN: &str = concat!(
    "^(",
    IDENTIFIER_PATTERN,
    "/)?(",
    IDENTIFIER_PATTERN,
    ")(@",
    VERSIONS_MARKER_REGEX,
    ")?$"
);

const VERSION_PATTERN: &str = "(HEAD(\\^{0,10})|HEAD(~[0-9]{1,7})|[A-Z0-9]{26})";
pub const VERSIONS_PATTERN: &str = concat!(
    "^((?<single>",
    VERSION_PATTERN,
    ")|(?<list>(",
    VERSION_PATTERN,
    "(,",
    VERSION_PATTERN,
    ")+))|(?<range>(",
    VERSION_PATTERN,
    "..",
    VERSION_PATTERN,
    ")))$"
);
#[td_error]
enum ParserError {
    #[error("Could not parse '{0}', expected: {1}")]
    CouldNotParse(String, String) = 0,
}

fn parser(regex: Regex, s: String, message: impl Into<String>) -> Result<String, TdError> {
    if regex.is_match(&s) {
        Ok(s)
    } else {
        Err(ParserError::CouldNotParse(s, message.into()))?
    }
}

fn parse_dependency(s: String) -> Result<String, TdError> {
    lazy_static! {
        static ref DEPENDENCY_REGEX: Regex = Regex::new(DEPENDENCY_PATTERN).unwrap();
    }
    lazy_static! {
        static ref VERSIONS_REGEX: Regex = Regex::new(VERSIONS_PATTERN).unwrap();
    }
    let s = parser(
        DEPENDENCY_REGEX.clone(),
        s,
        "a table dependency, a [<COLLECTION>/]<TABLE>[@<VERSIONS>] with \
<COLLECTION> and <NAME> being a [_A-Za-z0-9] word of up to 100 characters each \
and <VERSIONS> being a single version, a range of versions or a list of versions",
    )?;
    if let Some(versions_marker) = s.find('@') {
        let s = s.split_at(versions_marker + 1).1.to_string();
        parser(
            VERSIONS_REGEX.clone(),
            s,
            "a single <VERSION>, a range <VERSION>..<VERSION>, or a list <VERSION>, ....\
Each version being a 'HEAD' relative version (Git notation) or a version ID",
        )?;
    }
    Ok(s)
}

fn parse_name(s: String, name_type: &str) -> Result<String, TdError> {
    lazy_static! {
        static ref REGEX: Regex = Regex::new(NAME_PATTERN).unwrap();
    }
    parser(
        REGEX.clone(),
        s,
        format!("{name_type}, a [_A-Za-z0-9] word of up to 100 characters"),
    )
}

fn parse_collection(s: String) -> Result<String, TdError> {
    parse_name(s, "Collection name")
}

fn parse_entity(s: String) -> Result<String, TdError> {
    parse_name(s, "Entity name")
}

fn parse_function(s: String) -> Result<String, TdError> {
    parse_name(s, "Function name")
}

fn parse_table(s: String) -> Result<String, TdError> {
    parse_name(s, "Table name")
}

fn parse_role(s: String) -> Result<String, TdError> {
    parse_name(s, "Role name")
}

fn parse_trigger(s: String) -> Result<String, TdError> {
    lazy_static! {
        static ref REGEX: Regex = Regex::new(TRIGGER_PATTERN).unwrap();
    }
    parser(
        REGEX.clone(),
        s,
        "trigger name, a [<COLLECTION>/]<TABLE> with <COLLECTION> and <NAME> \
being a [_A-Za-z0-9] word of up to 100 characters each",
    )
}

fn parse_user(s: String) -> Result<String, TdError> {
    parse_name(s, "User name")
}

#[td_type::typed(timestamp)]
pub struct AtTime;

#[td_type::typed(id)]
pub struct BundleId;

#[td_type::typed(id)]
pub struct CollectionId;

#[td_type::typed(string(parser = parse_collection))]
pub struct CollectionName;

#[td_type::typed(string(regex = DATA_LOCATION_REGEX))]
pub struct DataLocation;

#[td_type::typed(string(min_len = 0, max_len = 200))]
pub struct Description;

#[td_type::typed(id)]
pub struct DependencyId;

#[td_type::typed(i16(min = 0, max = 200))]
pub struct DependencyPos;

#[td_type::typed(string(regex = DependencyStatus::REGEX))]
pub struct DependencyStatus;

impl DependencyStatus {
    const REGEX: &'static str = "^[AD]$";

    pub fn active() -> Self {
        Self("A".to_string())
    }

    pub fn deleted() -> Self {
        Self("D".to_string())
    }
}

#[td_type::typed(id)]
pub struct DependencyVersionId;

#[td_type::typed(id)]
pub struct EntityId;

#[td_type::typed(string(parser = parse_entity))]
pub struct EntityName;

#[td_type::typed(id)]
pub struct ExecutionPlanId;

#[td_type::typed(bool(default = false))]
pub struct Fixed;

#[td_type::typed(bool)]
pub struct FixedRole;

#[td_type::typed(bool)]
pub struct Frozen;

#[td_type::typed(id)]
pub struct FunctionId;

#[td_type::typed(string(parser = parse_function))]
pub struct FunctionName;

// JSON blob with `version`, `envs` & `secrets` top entries.
// info used in decorator.
#[td_type::typed(string(max_len = 4096))]
pub struct FunctionRuntimeValues;

#[td_type::typed(string(regex = FunctionStatus::REGEX))]
pub struct FunctionStatus;

impl FunctionStatus {
    const REGEX: &'static str = "^[AFD]$";

    pub fn active() -> Self {
        Self("A".to_string())
    }

    pub fn frozen() -> Self {
        Self("F".to_string())
    }

    pub fn deleted() -> Self {
        Self("D".to_string())
    }
}

#[td_type::typed(id)]
pub struct FunctionVersionId;

#[td_type::typed(string(min_len = 1, max_len = 1024))]
pub struct Partition;

#[td_type::typed(id)]
pub struct PermissionId;

#[td_type::typed(string(regex = PermissionType::REGEX ))]
pub struct PermissionType;

impl PermissionType {
    const REGEX: &'static str = "^(sa|ss|ca|cd|cx|cr|cR)$";

    pub fn sys_admin() -> Self {
        Self("sa".to_string())
    }

    pub fn sec_admin() -> Self {
        Self("ss".to_string())
    }

    pub fn collection_admin() -> Self {
        Self("ca".to_string())
    }

    pub fn collection_dev() -> Self {
        Self("cd".to_string())
    }

    pub fn collection_exec() -> Self {
        Self("cx".to_string())
    }

    pub fn collection_read() -> Self {
        Self("cr".to_string())
    }

    pub fn collection_read_all() -> Self {
        Self("cR".to_string())
    }

    pub fn on_entity_type(&self) -> PermissionEntityType {
        if self.0.starts_with("s") {
            PermissionEntityType::system()
        } else {
            PermissionEntityType::collection()
        }
    }
}

#[td_type::typed(string(regex = PermissionEntityType::REGEX ))]
pub struct PermissionEntityType;

impl PermissionEntityType {
    const REGEX: &'static str = "^(s|c)$";

    pub fn system() -> Self {
        Self("s".to_string())
    }

    pub fn collection() -> Self {
        Self("c".to_string())
    }
}

#[td_type::typed(timestamp)]
pub struct PublishedOn;

#[td_type::typed(id)]
pub struct RoleId;

#[td_type::typed(string(parser = parse_role))]
pub struct RoleName;

#[td_type::typed(string(min_len = 0, max_len = 4096))]
pub struct Snippet;

#[td_type::typed(string(min_len = 1, max_len = 10))]
pub struct StorageVersion;

#[td_type::typed(string(parser = parse_dependency))]
pub struct TableDependency;

#[td_type::typed(id)]
pub struct TableDataId;

#[td_type::typed(string(regex = TableDataVersionStatus::REGEX))]
pub struct TableDataVersionStatus;

impl TableDataVersionStatus {
    const REGEX: &'static str = "^[SRDEFHCP]$";

    pub fn scheduled() -> Self {
        Self("S".to_string())
    }

    pub fn running() -> Self {
        Self("R".to_string())
    }

    pub fn done() -> Self {
        Self("D".to_string())
    }

    pub fn error() -> Self {
        Self("E".to_string())
    }

    pub fn failed() -> Self {
        Self("F".to_string())
    }

    pub fn hold() -> Self {
        Self("H".to_string())
    }

    pub fn canceled() -> Self {
        Self("C".to_string())
    }

    pub fn publish() -> Self {
        Self("P".to_string())
    }
}

#[td_type::typed(id)]
pub struct TableDataVersionId;

#[td_type::typed(id)]
pub struct TableId;

#[td_type::typed(string(parser = parse_table))]
pub struct TableName;

#[td_type::typed(i16)]
pub struct TableFunctionParamPos;

#[td_type::typed(string(regex = TableStatus::REGEX))]
pub struct TableStatus;

impl TableStatus {
    const REGEX: &'static str = "^[AFD]$";

    pub fn active() -> Self {
        Self("A".to_string())
    }

    pub fn frozen() -> Self {
        Self("F".to_string())
    }

    pub fn deleted() -> Self {
        Self("D".to_string())
    }
}

#[td_type::typed(string(parser = parse_trigger))]
pub struct TableTrigger;

#[td_type::typed(id)]
pub struct TableVersionId;

#[td_type::typed(string(parser = parse_dependency))]
pub struct TableVersions;

#[td_type::typed(id)]
pub struct TransactionId;

#[td_type::typed(timestamp)]
pub struct TriggeredOn;

#[td_type::typed(id)]
pub struct TriggerId;

#[td_type::typed(string(regex = TriggerStatus::REGEX))]
pub struct TriggerStatus;

impl TriggerStatus {
    const REGEX: &'static str = "^[AD]$";

    pub fn active() -> Self {
        Self("A".to_string())
    }

    pub fn deleted() -> Self {
        Self("D".to_string())
    }
}

#[td_type::typed(id)]
pub struct TriggerVersionId;

#[td_type::typed(id)]
pub struct UserId;

#[td_type::typed(string(parser = parse_user))]
pub struct UserName;

#[td_type::typed(id)]
pub struct UsersRolesId;

#[cfg(test)]
mod tests {

    #[test]
    fn test_parse_name() {
        let name = super::parse_name("abc".to_string(), "test").unwrap();
        assert_eq!(name, "abc");

        assert!(super::parse_name("".to_string(), "test").is_err());
        assert!(super::parse_name(" a".to_string(), "test").is_err());
        assert!(super::parse_name("a ".to_string(), "test").is_err());
        assert!(super::parse_name(" a ".to_string(), "test").is_err());
        assert!(super::parse_name("a a".to_string(), "test").is_err());
        assert!(super::parse_name("0a".to_string(), "test").is_err());
        assert!(super::parse_name("@".to_string(), "test").is_err());
        assert!(super::parse_name("a".repeat(101), "test").is_err());

        assert!(super::parse_name("A_".to_string(), "test").is_ok());
        assert!(super::parse_name("A".to_string(), "test").is_ok());
        assert!(super::parse_name("A1".to_string(), "test").is_ok());
        assert!(super::parse_name("a".to_string(), "test").is_ok());
        assert!(super::parse_name("a1".to_string(), "test").is_ok());
        assert!(super::parse_name("AZaz09_".to_string(), "test").is_ok());
        assert!(super::parse_name("a".repeat(100), "test").is_ok());
    }

    #[test]
    fn test_parse_names() {
        assert!(super::parse_user("abc".to_string()).is_ok());
        assert!(super::parse_collection("abc".to_string()).is_ok());
        assert!(super::parse_function("abc".to_string()).is_ok());
        assert!(super::parse_table("abc".to_string()).is_ok());
    }

    #[test]
    fn test_parse_trigger() {
        assert!(super::parse_trigger("abc ".to_string()).is_err());
        assert!(super::parse_trigger(" abc ".to_string()).is_err());
        assert!(super::parse_trigger("abc/".to_string()).is_err());
        assert!(super::parse_trigger("/abc".to_string()).is_err());
        assert!(super::parse_trigger("@/a".to_string()).is_err());

        assert!(super::parse_trigger("abc".to_string()).is_ok());
        assert!(super::parse_trigger("xyz/abc".to_string()).is_ok());
    }

    #[test]
    fn test_parse_dependency() {
        assert!(super::parse_dependency(" abc".to_string()).is_err());
        assert!(super::parse_dependency("abc ".to_string()).is_err());
        assert!(super::parse_dependency("abc/abc ".to_string()).is_err());
        assert!(super::parse_dependency(" abc/abc".to_string()).is_err());
        assert!(super::parse_dependency("abc/abc@".to_string()).is_err());
        assert!(super::parse_dependency("abc/abc@HEAD..HEAD,HEAD".to_string()).is_err());
        assert!(super::parse_dependency("abc/abc@HEAD..HEAD..HEAD".to_string()).is_err());
        assert!(super::parse_dependency("abc/abc@HEAD~".to_string()).is_err());

        assert!(super::parse_dependency("abc".to_string()).is_ok());
        assert!(super::parse_dependency("xyz/abc".to_string()).is_ok());
        assert!(super::parse_dependency("xyz/abc@HEAD".to_string()).is_ok());
        assert!(super::parse_dependency("xyz/abc@HEAD^^".to_string()).is_ok());
        assert!(super::parse_dependency("xyz/abc@HEAD^,HEAD".to_string()).is_ok());
        assert!(super::parse_dependency("xyz/abc@HEAD^..HEAD".to_string()).is_ok());
        assert!(super::parse_dependency("xyz/abc@HEAD~1".to_string()).is_ok());
        assert!(super::parse_dependency("xyz/abc@HEAD~10,HEAD".to_string()).is_ok());
        assert!(super::parse_dependency("xyz/abc@HEAD~10..HEAD".to_string()).is_ok());
    }
}
