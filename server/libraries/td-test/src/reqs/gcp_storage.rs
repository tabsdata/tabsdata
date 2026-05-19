//
// Copyright 2025. Tabs Data Inc.
//

crate::define_requires! {
    /// Requirements for a test using Google Cloud Storage with a Service Account Key.
    pub struct GcpStorageWithServiceAccountKeyReqs {
        uri: gcp_storage_uri,
        service_account_key: gcp_service_account_key,
    }
}

#[cfg(test)]
mod tests {
    use crate::__private::{TestSetup, TestSetupExecution};
    use crate::reqs::{
        GcpStorageWithServiceAccountKeyReqs, ReqsTestSetup, TestRequirementsInEnv,
    };
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_signature_gcp_storage_with_service_account_key_reqs_skips_when_missing() {
        let setup = ReqsTestSetup::<GcpStorageWithServiceAccountKeyReqs>::new_for_self_test(
            "test_signature_gcp_storage_with_service_account_key_reqs_skips_when_missing",
            "gcp_test_not_defined",
        );
        assert!(matches!(setup.setup().await, TestSetupExecution::Skip));
    }

    #[test]
    fn test_gcp_storage_with_service_account_key_reqs() {
        let vars: HashMap<String, String> = HashMap::from([
            ("NS__GCP_STORAGE_URI".to_string(), "u".to_string()),
            ("NS__GCP_SERVICE_ACCOUNT_KEY".to_string(), "sak".to_string()),
        ]);
        let reqs = TestRequirementsInEnv::resolve_test_run_variables::<
            GcpStorageWithServiceAccountKeyReqs,
        >(
            "test_gcp_storage_with_service_account_key_reqs",
            &td_common::tdtestdir!(),
            "GcpStorageWithServiceAccountKeyReqs",
            "ns",
            &vars,
            false,
        );
        assert_eq!(reqs.as_ref().unwrap().uri, "u");
        assert_eq!(reqs.as_ref().unwrap().service_account_key, "sak");
    }
}
