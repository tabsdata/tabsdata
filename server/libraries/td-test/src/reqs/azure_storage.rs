//
// Copyright 2025 Tabsdata Inc.
//

crate::define_requires! {
    /// Requirements for a test using Azure storage with Account Name and Account Key.
    pub struct AzureStorageWithAccountKeyReqs {
        uri: az_uri,
        account_name: az_account_name,
        account_key: az_account_key,
    }
}

#[cfg(test)]
mod tests {
    use crate::__private::{TestSetup, TestSetupExecution};
    use crate::reqs::{AzureStorageWithAccountKeyReqs, ReqsTestSetup, TestRequirementsInEnv};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_signature_azure_storage_with_account_key_reqs_skips_when_missing() {
        let setup = ReqsTestSetup::<AzureStorageWithAccountKeyReqs>::new_for_self_test(
            "test_signature_azure_storage_with_account_key_reqs_skips_when_missing",
            "az_test_not_defined",
        );
        assert!(matches!(setup.setup().await, TestSetupExecution::Skip));
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
                &td_common::tdtestdir!(),
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
