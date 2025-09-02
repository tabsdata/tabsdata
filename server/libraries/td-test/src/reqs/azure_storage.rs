//
// Copyright 2025. Tabs Data Inc.
//

use crate::reqs::TestRequirements;
use std::collections::HashMap;

#[allow(dead_code)]
/// Requirements for a test using Azure storage with Account Name and Account Key.
pub struct AzureStorageWithAccountKeyReqs {
    pub uri: String,
    pub account_name: String,
    pub account_key: String,
    vars: HashMap<String, String>,
}

impl TestRequirements for AzureStorageWithAccountKeyReqs {
    fn keys() -> &'static [&'static str] {
        &["az_uri", "az_account_name", "az_account_key"]
    }

    fn new(vars: impl Into<HashMap<String, String>>) -> Self {
        let vars = vars.into();
        Self {
            uri: vars["az_uri"].clone(),
            account_name: vars["az_account_name"].clone(),
            account_key: vars["az_account_key"].clone(),
            vars,
        }
    }

    fn vars(&self) -> &HashMap<String, String> {
        &self.vars
    }
}

#[cfg(test)]
mod tests {
    use crate as td_test;

    use crate::reqs::{AzureStorageWithAccountKeyReqs, TestRequirementsInEnv};
    use std::collections::HashMap;
    use testdir::testdir;

    #[crate::test(when(reqs = AzureStorageWithAccountKeyReqs, env_prefix= "az_test_not_defined", do_not_fail_reqs= true))]
    async fn test_signature_azure_storage_with_account_key_reqs(
        _az: AzureStorageWithAccountKeyReqs,
    ) {
        panic!()
    }

    #[test]
    fn test_azure_storage_with_account_key_reqs() {
        let vars: HashMap<String, String> = HashMap::from([
            ("NS__AZ_URI".to_string(), "u".to_string()),
            ("NS__AZ_ACCOUNT_NAME".to_string(), "an".to_string()),
            ("NS__AZ_ACCOUNT_KEY".to_string(), "ak".to_string()),
        ]);
        let reqs =
            TestRequirementsInEnv::resolve_test_run_variables::<AzureStorageWithAccountKeyReqs>(
                "test_azure_storage_with_account_key_reqs",
                &testdir!(),
                "AzureStorageWithAccountKeyReqs",
                "ns",
                &vars,
                false,
            );
        assert_eq!(reqs.as_ref().unwrap().uri, "u");
        assert_eq!(reqs.as_ref().unwrap().account_name, "an");
        assert_eq!(reqs.as_ref().unwrap().account_key, "ak");
    }
}
