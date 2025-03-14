//
// Copyright 2025 Tabs Data Inc.
//

use crate::{TestSetup, TestSetupExecution};
use async_trait::async_trait;
use std::any::type_name;
use std::collections::HashMap;
use std::marker::PhantomData;

pub struct ReqsTestSetup<'a, R> {
    test_name: &'a str,
    env_prefix: &'a str,
    _phantom: PhantomData<R>,
}

impl<'a, R> ReqsTestSetup<'a, R> {
    #[allow(dead_code)]
    pub fn new(test_name: &'a str, env_prefix: &'a str) -> Self {
        Self {
            test_name,
            env_prefix,
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
        match TestRequirementsInEnv::get::<R>(self.test_name, type_name::<R>(), self.env_prefix) {
            None => TestSetupExecution::Skip,
            Some(t) => TestSetupExecution::Run(t),
        }
    }
}

/// Defines the required keys to be available for a test as well as a constructor for the
/// [`TestRequirements`] instance when the keys are available.
pub trait TestRequirements {
    /// Required keys to create the [`TestRequirements`] instance.
    fn keys() -> &'static [&'static str];

    /// Creates the [`TestRequirements`] instance, all [`TestRequirements::keys`] must be available.
    fn new(values: &HashMap<String, String>) -> Self;
}

/// Obtains [`TestRequirements`] from environment variables.
pub struct TestRequirementsInEnv {}

impl TestRequirementsInEnv {
    fn namespace_key(name_space: &str, key: &str) -> String {
        format!("{}__{}", name_space, key)
    }

    fn env_var_name(name: &str) -> String {
        name.to_uppercase()
    }

    fn find_requirements_env_vars(
        env_vars: &HashMap<String, String>,
        keys: &[&str],
        ns: &str,
    ) -> Result<HashMap<String, String>, String> {
        let mut found = HashMap::new();
        let mut missing = Vec::new();
        for key in keys {
            let env_var_name = Self::env_var_name(&Self::namespace_key(ns, key));
            match env_vars.get(&env_var_name) {
                Some(value) => {
                    found.insert(key.to_string(), value.to_string());
                }
                None => missing.push(format!("{} not found", env_var_name)),
            }
        }
        if missing.is_empty() {
            Ok(found)
        } else {
            Err(format!("Missing ENV vars {}", missing.join(", ")))
        }
    }

    fn requirements<R: TestRequirements>(
        env_vars: &HashMap<String, String>,
        namespace: &str,
    ) -> Result<R, String> {
        let reqs = Self::find_requirements_env_vars(env_vars, R::keys(), namespace)?;
        Ok(R::new(&reqs))
    }

    pub fn _get<R: TestRequirements>(
        env_vars: &HashMap<String, String>,
        test_name: &str,
        requirements_name: &str,
        namespace: &str,
    ) -> Option<R> {
        match Self::requirements::<R>(env_vars, namespace) {
            Ok(reqs) => Some(reqs),
            Err(msg) => {
                println!(
                    "SKIPPING {}. Requirements for {}({}) not met: {}",
                    test_name, requirements_name, namespace, msg
                );
                None
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
        requirements_name: &str,
        namespace: &str,
    ) -> Option<R> {
        let envs: HashMap<String, String> = std::env::vars().collect();
        Self::_get::<R>(&envs, test_name, requirements_name, namespace)
    }
}

#[allow(dead_code)]
/// Requirements for a test using AWS Access Key, Secret Key and Region.
pub struct AwsAccessKeySecretKeyReqs {
    access_key: String,
    secret_key: String,
    region: String,
}

impl TestRequirements for AwsAccessKeySecretKeyReqs {
    fn keys() -> &'static [&'static str] {
        &["aws_access_key", "aws_secret_key", "aws_region"]
    }

    fn new(values: &HashMap<String, String>) -> Self {
        Self {
            access_key: values["aws_access_key"].clone(),
            secret_key: values["aws_secret_key"].clone(),
            region: values["aws_region"].clone(),
        }
    }
}

#[allow(dead_code)]
/// Requirements for a test using Azure Account Name and Account Key.
pub struct AzureAccountKeyReqs {
    account_name: String,
    account_key: String,
}

impl TestRequirements for AzureAccountKeyReqs {
    fn keys() -> &'static [&'static str] {
        &["az_account_name", "az_account_key"]
    }

    fn new(values: &HashMap<String, String>) -> Self {
        Self {
            account_name: values["az_account_name"].clone(),
            account_key: values["az_account_key"].clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate as td_test;

    use super::{
        AwsAccessKeySecretKeyReqs, AzureAccountKeyReqs, TestRequirements, TestRequirementsInEnv,
    };
    use std::collections::HashMap;

    #[td_test::test(when(reqs = AwsAccessKeySecretKeyReqs, env_prefix= "s3"))]
    async fn test_test(keys: AwsAccessKeySecretKeyReqs) {
        println!("{:?}", keys.access_key);
        // etc
    }

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
        let env_vars: HashMap<String, String> =
            HashMap::from([("NS__KEY".to_string(), "value".to_string())]);
        let keys = ["key"];
        let result = TestRequirementsInEnv::find_requirements_env_vars(&env_vars, &keys, "NS");
        assert_eq!(
            result.unwrap(),
            [("key".to_string(), "value".to_string())]
                .iter()
                .cloned()
                .collect()
        );

        let env_vars: HashMap<String, String> =
            HashMap::from([("NS__KEY_X".to_string(), "value".to_string())]);
        let keys = ["key"];
        let result = TestRequirementsInEnv::find_requirements_env_vars(&env_vars, &keys, "NS");
        assert!(result.is_err());
    }

    #[test]
    fn test_requirements() {
        struct MyReqs {
            key: String,
        }

        impl TestRequirements for MyReqs {
            fn keys() -> &'static [&'static str] {
                &["key"]
            }

            fn new(values: &HashMap<String, String>) -> Self {
                Self {
                    key: values["key"].clone(),
                }
            }
        }

        let env_vars: HashMap<String, String> =
            HashMap::from([("NS__KEY".to_string(), "value".to_string())]);
        let result = TestRequirementsInEnv::requirements::<MyReqs>(&env_vars, "NS");
        assert_eq!(result.unwrap().key, "value");
    }

    #[test]
    fn test_get() {
        struct MyReqs {
            key: String,
        }

        impl TestRequirements for MyReqs {
            fn keys() -> &'static [&'static str] {
                &["key"]
            }

            fn new(values: &HashMap<String, String>) -> Self {
                Self {
                    key: values["key"].clone(),
                }
            }
        }

        let env_vars: HashMap<String, String> =
            HashMap::from([("NS__KEY".to_string(), "value".to_string())]);
        let result = TestRequirementsInEnv::_get::<MyReqs>(&env_vars, "test", "MyReqs", "NS");
        assert_eq!(result.unwrap().key, "value");

        let env_vars: HashMap<String, String> =
            HashMap::from([("NS__KEYX".to_string(), "value".to_string())]);
        assert!(TestRequirementsInEnv::_get::<MyReqs>(&env_vars, "test", "MyReqs", "NS").is_none());
    }

    #[test]
    fn test_awss_access_key_secret_key_reqs() {
        let env_vars: HashMap<String, String> = HashMap::from([
            ("NS__AWS_ACCESS_KEY".to_string(), "ak".to_string()),
            ("NS__AWS_SECRET_KEY".to_string(), "sk".to_string()),
            ("NS__AWS_REGION".to_string(), "r".to_string()),
        ]);
        let reqs =
            TestRequirementsInEnv::_get::<AwsAccessKeySecretKeyReqs>(&env_vars, "ns", "aws", "NS");
        assert_eq!(reqs.as_ref().unwrap().access_key, "ak");
        assert_eq!(reqs.as_ref().unwrap().secret_key, "sk");
        assert_eq!(reqs.as_ref().unwrap().region, "r");
    }

    #[test]
    fn test_azure_account_key_reqs() {
        let env_vars: HashMap<String, String> = HashMap::from([
            ("NS__AZ_ACCOUNT_NAME".to_string(), "an".to_string()),
            ("NS__AZ_ACCOUNT_KEY".to_string(), "ak".to_string()),
        ]);
        let reqs =
            TestRequirementsInEnv::_get::<AzureAccountKeyReqs>(&env_vars, "ns", "azure", "NS");
        assert_eq!(reqs.as_ref().unwrap().account_name, "an");
        assert_eq!(reqs.as_ref().unwrap().account_key, "ak");
    }
}
