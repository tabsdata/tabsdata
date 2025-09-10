//
// Copyright 2025. Tabs Data Inc.
//

use crate::reqs::TestRequirements;
use std::collections::HashMap;

#[allow(dead_code)]
/// Requirements for a test using GCP storage with Service Account Key (JSON).
pub struct GcpStorageWithServiceAccountKeyReqs {
    pub uri: String,
    pub service_account_key: String,
    vars: HashMap<String, String>,
}

impl TestRequirements for GcpStorageWithServiceAccountKeyReqs {
    fn keys() -> &'static [&'static str] {
        &["gcp_storage_uri", "gcp_service_account_key"]
    }

    fn new(vars: impl Into<HashMap<String, String>>) -> Self {
        let vars = vars.into();
        Self {
            uri: vars["gcp_storage_uri"].clone(),
            service_account_key: vars["gcp_service_account_key"].clone(),
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

    use crate::reqs::{GcpStorageWithServiceAccountKeyReqs, TestRequirementsInEnv};
    use std::collections::HashMap;
    use testdir::testdir;

    #[crate::test(when(reqs = GcpStorageWithServiceAccountKeyReqs, env_prefix= "gcp_test_not_defined", do_not_fail_reqs= true))]
    #[tokio::test]
    async fn test_signature_gcp_storage_with_service_account_key_reqs(
        _gcp: GcpStorageWithServiceAccountKeyReqs,
    ) {
        panic!()
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
            &testdir!(),
            "GcpStorageWithServiceAccountKeyReqs",
            "ns",
            &vars,
            false,
        );
        assert_eq!(reqs.as_ref().unwrap().uri, "u");
        assert_eq!(reqs.as_ref().unwrap().service_account_key, "sak");
    }
}
