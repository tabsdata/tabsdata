//
// Copyright 2025. Tabs Data Inc.
//

crate::define_requires! {
    /// Requirements for an S3 test using AWS Access Key, Secret Key and Region.
    pub struct S3WithAccessKeySecretKeyReqs {
        uri: s3_uri,
        region: s3_region,
        access_key: s3_access_key,
        secret_key: s3_secret_key,
    }
}

#[cfg(test)]
mod tests {
    use crate::__private::{TestSetup, TestSetupExecution};
    use crate::reqs::{ReqsTestSetup, S3WithAccessKeySecretKeyReqs, TestRequirementsInEnv};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_signature_s3_with_access_key_secret_key_reqs_skips_when_missing() {
        let setup = ReqsTestSetup::<S3WithAccessKeySecretKeyReqs>::new_for_self_test(
            "test_signature_s3_with_access_key_secret_key_reqs_skips_when_missing",
            "s3_test_not_defined",
        );
        assert!(matches!(setup.setup().await, TestSetupExecution::Skip));
    }

    #[test]
    fn test_s3_with_access_key_secret_key_reqs() {
        let vars: HashMap<String, String> = HashMap::from([
            (
                "TD_TEST_DIR".to_string(),
                td_common::tdtestdir!().to_str().unwrap().to_string(),
            ),
            (
                "TD_TEST_TIMESTAMP".to_string(),
                "0000_00_00_00_00_00".to_string(),
            ),
            ("NS__S3_URI".to_string(), "u".to_string()),
            ("NS__S3_REGION".to_string(), "r".to_string()),
            ("NS__S3_ACCESS_KEY".to_string(), "ak".to_string()),
            ("NS__S3_SECRET_KEY".to_string(), "sk".to_string()),
        ]);
        let reqs = TestRequirementsInEnv::resolve_test_run_variables::<S3WithAccessKeySecretKeyReqs>(
            "test_s3_with_access_key_secret_key_reqs",
            &td_common::tdtestdir!(),
            "S3WithAccessKeySecretKeyReqs",
            "ns",
            &vars,
            false,
        );
        assert_eq!(reqs.as_ref().unwrap().uri, "u");
        assert_eq!(reqs.as_ref().unwrap().region, "r");
        assert_eq!(reqs.as_ref().unwrap().access_key, "ak");
        assert_eq!(reqs.as_ref().unwrap().secret_key, "sk");
    }
}
