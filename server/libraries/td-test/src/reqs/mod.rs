//
// Copyright 2025 Tabs Data Inc.
//

use crate::{TestSetup, TestSetupExecution};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use sha2::{Digest, Sha256};
use std::any::type_name;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use testdir::testdir;

pub mod aws_s3;
pub mod azure_storage;

pub mod gcp_storage;
pub mod mysql;
pub mod oracle;
pub mod postgres;

pub use aws_s3::S3WithAccessKeySecretKeyReqs;
pub use azure_storage::AzureStorageWithAccountKeyReqs;
pub use gcp_storage::GcpStorageWithServiceAccountKeyReqs;
pub use mysql::MySqlReqs;
pub use oracle::OracleReqs;
pub use postgres::PostgresReqs;

const TEST_SKIP_IF_NO_REQS: &str = "TD_TEST_SKIP_IF_NO_REQS";

const TEST_DIR_KEY: &str = "TEST_DIR";

const TESTS_USER_KEY: &str = "TESTS_USER";
const TESTS_TIMESTAMP_KEY: &str = "TESTS_TIMESTAMP";

pub(crate) fn user(vars: &HashMap<String, String>) -> String {
    vars.get(TESTS_USER_KEY)
        .cloned()
        .unwrap_or_else(whoami::username)
}

fn format_timestamp(datetime: &DateTime<Utc>) -> String {
    datetime.format("%Y_%m_%d_%H_%M_%S").to_string()
}

pub(crate) fn timestamp(vars: &HashMap<String, String>) -> String {
    lazy_static! {
        static ref TESTS_TIMESTAMP: String = format_timestamp(&Utc::now());
    }
    let _: &str = &TESTS_TIMESTAMP;
    vars.get(TESTS_TIMESTAMP_KEY)
        .unwrap_or(&TESTS_TIMESTAMP)
        .clone()
}

fn test_dir(vars: &HashMap<String, String>) -> String {
    vars.get(TEST_DIR_KEY).unwrap().to_string()
}

fn test_path(vars: &HashMap<String, String>) -> PathBuf {
    let user = user(vars);
    let timestamp = timestamp(vars);
    let test_name = &test_dir(vars)[1..];

    Path::new(&user).join(timestamp).join(test_name)
}

fn test_identifier(vars: &HashMap<String, String>, prefix: Option<u8>) -> String {
    let user = user(vars);
    let timestamp = timestamp(vars);
    let test_dir = &test_dir(vars)[1..];
    let mut hash = hex::encode(Sha256::digest(test_dir));
    if let Some(prefix) = prefix {
        hash = hash.chars().take(prefix as usize).collect()
    };
    format!("{user}__{timestamp}__{hash}")
}

pub struct ReqsTestSetup<'a, R> {
    test_name: &'a str,
    env_prefix: &'a str,
    do_not_fail: bool, // This is only used for unit tests of Requirements implementations.
    _phantom: PhantomData<R>,
}

impl<'a, R> ReqsTestSetup<'a, R> {
    #[allow(dead_code)]
    pub fn new(test_name: &'a str, env_prefix: &'a str, do_not_fail: bool) -> Self {
        Self {
            test_name,
            env_prefix,
            do_not_fail,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<R> TestSetup<R> for ReqsTestSetup<'_, R>
where
    R: TestRequirements + Sync,
{
    /// Creates AWS test playground or skips the test if not available.
    async fn setup(&self) -> TestSetupExecution<R> {
        match TestRequirementsInEnv::get::<R>(
            self.test_name,
            &testdir!(),
            type_name::<R>(),
            self.env_prefix,
            self.do_not_fail,
        ) {
            None => TestSetupExecution::Skip,
            Some(t) => TestSetupExecution::Run(t),
        }
    }
}

/// Defines the required keys to be available for a test as well as a constructor for the
/// [`TestRequirements`] instance when the keys are available.
///
/// The [`TestRequirements::test_path`] and [`TestRequirements::test_identifier`] provide
/// unique values per test that are prefixed with the user running the tests and the timestamp
/// of the test run. Resources created in external systems should be prefixed with these values
/// to enable resource clean up of resources in the external systems. How? By purging resources
/// with a timestamp older than a certain threshold.
///
/// The build user can be overridden via the `TESTS_USER` environment variable.
///
/// The build timestamp can be overridden via the `TESTS_TIMESTAMP` environment variable.
pub trait TestRequirements {
    /// Required keys to create the [`TestRequirements`] instance.
    fn keys() -> &'static [&'static str];

    /// Creates the [`TestRequirements`] instance, all [`TestRequirements::keys`] must be available.
    fn new(vars: impl Into<HashMap<String, String>>) -> Self;

    /// The variables used to create the [`TestRequirements`] instance.
    fn vars(&self) -> &HashMap<String, String>;

    /// A relative path unique for the test execution.
    ///
    /// `${TESTS_USER}/${TESTS_TIMESTAMP}/${TEST_DIR}`
    fn test_path(&self) -> PathBuf {
        test_path(self.vars())
    }

    /// A unique identifier for the test execution.
    ///
    /// `${TESTS_USER}__${TESTS_TIMESTAMP}__SHA256(${TEST_DIR})`
    ///
    /// If `prefix` is `None` a full SHA256 hash is used, otherwise the first `prefix` characters
    /// of the hash are used.
    fn test_identifier(&self, prefix: impl Into<Option<u8>>) -> String {
        test_identifier(self.vars(), prefix.into())
    }
}

/// Obtains [`TestRequirements`] from environment variables.
pub(crate) struct TestRequirementsInEnv {}

impl TestRequirementsInEnv {
    pub(crate) fn namespace_key(name_space: &str, key: &str) -> String {
        format!("{name_space}__{key}")
    }

    fn env_var_name(name: &str) -> String {
        name.to_uppercase()
    }

    fn find_requirements_in_env_vars(
        namespace: &str,
        keys: &[&str],
        vars: &mut HashMap<String, String>,
    ) -> Result<(), String> {
        let mut missing = Vec::new();
        for key in keys {
            let env_var_name = Self::env_var_name(&Self::namespace_key(namespace, key));
            if let Some(value) = vars.get(&env_var_name) {
                vars.insert(key.to_string(), value.to_string());
            } else {
                missing.push(env_var_name.to_string());
            }
        }
        if !missing.is_empty() {
            Err(format!("Missing ENV vars: {}", missing.join(", ")))
        } else {
            Ok(())
        }
    }

    fn requirements<R: TestRequirements>(
        namespace: &str,
        vars: &HashMap<String, String>,
    ) -> Result<R, String> {
        let mut vars = vars.clone();
        Self::find_requirements_in_env_vars(namespace, R::keys(), &mut vars)?;
        Ok(R::new(vars))
    }

    /// Resolve the environment variables available for the test run. This code is effective
    /// only when running tests from the IDE.
    ///
    /// Simulates the behavior of `cargo make` by loading the `~/.tabsdata-dev/make.env` file and
    /// augmenting the current environment variables with the ones defined in the `make.env` file.
    /// If a variable is already redefined as environment variable, the environment variable value
    /// takes precedence.
    ///
    /// The `vars` parameter must be the environment variables. It is a parameter for the purposes
    /// of unit-testing of this function.
    pub(crate) fn resolve_test_run_variables<R: TestRequirements>(
        test_name: &str,
        test_dir: &Path,
        requirements_name: &str,
        namespace: &str,
        vars: &HashMap<String, String>,
        do_not_fail: bool,
    ) -> Option<R> {
        let mut vars = vars.clone();
        vars.insert(
            TEST_DIR_KEY.to_string(),
            test_dir.to_str().unwrap().to_string(),
        );
        match Self::requirements::<R>(namespace, &vars) {
            Ok(reqs) => Some(reqs),
            Err(msg) => {
                let msg =
                    format!("Requirements for {requirements_name}({namespace}) not met: {msg}");
                match vars.get(TEST_SKIP_IF_NO_REQS) {
                    None if !do_not_fail => {
                        panic!("Missing requirements, test: {test_name}. {msg}");
                    }
                    Some(skip) if skip != "true" && !do_not_fail => {
                        panic!("Missing requirements, test: {test_name}. {msg}");
                    }
                    _ => {
                        println!(
                            "Missing requirements, skipping test because env var {TEST_SKIP_IF_NO_REQS}=true, test: {test_name}. {msg}"
                        );
                        None
                    }
                }
            }
        }
    }

    /// Obtains, if available, a [`TestRequirements`] from environment variables.
    ///
    /// The [`TestRequirements::keys`] are prefixed with `<namespace>__` then uppercased.
    /// If those environment variables are available, then the [`TestRequirements`] is
    /// created from it.
    ///
    /// If not available it prints a message indicating what environment variables are missing
    /// to meet the requirements.
    pub fn get<R: TestRequirements>(
        test_name: &str,
        test_dir: &Path,
        requirements_name: &str,
        namespace: &str,
        do_not_fail: bool,
    ) -> Option<R> {
        let envs: HashMap<String, String> = std::env::vars().collect();
        Self::resolve_test_run_variables::<R>(
            test_name,
            test_dir,
            requirements_name,
            namespace,
            &envs,
            do_not_fail,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TEST_DIR_KEY, TEST_SKIP_IF_NO_REQS, TestRequirements, TestRequirementsInEnv, test_dir,
        timestamp, user,
    };
    use path_slash::PathBufExt;
    use sha2::Digest;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use testdir::testdir;

    #[test]
    fn test_namespace_key() {
        assert_eq!(TestRequirementsInEnv::namespace_key("ns", "key"), "ns__key")
    }

    #[test]
    fn test_env_var_name() {
        assert_eq!(TestRequirementsInEnv::env_var_name("foo_var"), "FOO_VAR")
    }

    #[test]
    fn test_find_requirements_env_vars() {
        let mut env_vars: HashMap<String, String> =
            HashMap::from([("NS__KEY".to_string(), "value".to_string())]);
        let keys = ["key"];
        assert!(
            TestRequirementsInEnv::find_requirements_in_env_vars("NS", &keys, &mut env_vars)
                .is_ok()
        );

        let mut env_vars: HashMap<String, String> =
            HashMap::from([("NS__KEY_X".to_string(), "value".to_string())]);
        let keys = ["key"];
        assert!(
            TestRequirementsInEnv::find_requirements_in_env_vars("NS", &keys, &mut env_vars)
                .is_err()
        );
    }

    struct MyReqs {
        key: String,
        vars: HashMap<String, String>,
    }

    impl TestRequirements for MyReqs {
        fn keys() -> &'static [&'static str] {
            &["key"]
        }

        fn new(vars: impl Into<HashMap<String, String>>) -> Self {
            let vars = vars.into();
            Self {
                key: vars["key"].clone(),
                vars: vars.clone(),
            }
        }

        fn vars(&self) -> &HashMap<String, String> {
            &self.vars
        }
    }

    #[test]
    fn test_requirements() {
        let env_vars: HashMap<String, String> = HashMap::from([
            (
                TEST_DIR_KEY.to_string(),
                testdir!().to_str().unwrap().to_string(),
            ),
            ("NS__KEY".to_string(), "value".to_string()),
        ]);

        let reqs = TestRequirementsInEnv::requirements::<MyReqs>("NS", &env_vars).unwrap();
        assert_eq!(reqs.key, "value");

        let user = user(&env_vars);
        let timestamp = timestamp(&env_vars);
        let test_dir = &test_dir(&env_vars)[1..];
        let hash = hex::encode(&sha2::Sha256::digest(test_dir.as_bytes())[..]);

        let test_path_pathbuf = reqs.test_path();
        let test_path_pathbuf_check = test_path_pathbuf
            .to_slash()
            .unwrap_or_else(|| panic!("Invalid characters in test path: {test_path_pathbuf:?}"));

        let test_dir_pathbuf = PathBuf::from(test_dir);
        let test_dir_pathbuf_check = test_dir_pathbuf
            .to_slash()
            .unwrap_or_else(|| panic!("Invalid characters in test dir: {test_path_pathbuf:?}"));

        assert_eq!(
            test_path_pathbuf_check.as_ref(),
            &format!("{user}/{timestamp}/{test_dir_pathbuf_check}")
        );
        assert_eq!(
            reqs.test_identifier(None),
            format!("{user}__{timestamp}__{hash}")
        );
    }

    #[test]
    fn test_get() {
        let vars: HashMap<String, String> =
            HashMap::from([("NS__KEY".to_string(), "value".to_string())]);
        let result = TestRequirementsInEnv::requirements::<MyReqs>("NS", &vars);
        assert_eq!(result.unwrap().key, "value");

        let vars: HashMap<String, String> =
            HashMap::from([(TEST_SKIP_IF_NO_REQS.to_string(), "true".to_string())]);
        assert!(
            TestRequirementsInEnv::resolve_test_run_variables::<MyReqs>(
                "test_get",
                &testdir!(),
                "MyReqs",
                "NS",
                &vars,
                true
            )
            .is_none()
        );
    }

    #[test]
    #[should_panic]
    fn test_fail_on_skip() {
        let vars: HashMap<String, String> =
            HashMap::from([(TEST_SKIP_IF_NO_REQS.to_string(), "true".to_string())]);
        let result = TestRequirementsInEnv::requirements::<MyReqs>("NS", &vars);
        assert_eq!(result.unwrap().key, "value");

        let vars: HashMap<String, String> =
            HashMap::from([("NS__KEYX".to_string(), "value".to_string())]);
        let _ = TestRequirementsInEnv::resolve_test_run_variables::<MyReqs>(
            "test_get",
            &testdir!(),
            "MyReqs",
            "NS",
            &vars,
            false,
        );
    }
}
