//
// Copyright 2025. Tabs Data Inc.
//

use crate::reqs::TestRequirements;
use std::collections::HashMap;

#[allow(dead_code)]
/// Requirements for an S3 test using AWS Access Key, Secret Key and Region.
pub struct S3WithAccessKeySecretKeyReqs {
    pub uri: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    vars: HashMap<String, String>,
}

impl TestRequirements for S3WithAccessKeySecretKeyReqs {
    fn keys() -> &'static [&'static str] {
        &["s3_uri", "s3_region", "s3_access_key", "s3_secret_key"]
    }

    fn new(vars: impl Into<HashMap<String, String>>) -> Self {
        let vars = vars.into();
        Self {
            uri: vars["s3_uri"].clone(),
            region: vars["s3_region"].clone(),
            access_key: vars["s3_access_key"].clone(),
            secret_key: vars["s3_secret_key"].clone(),
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

    use crate::reqs::{S3WithAccessKeySecretKeyReqs, TestRequirementsInEnv};
    use std::collections::HashMap;
    use testdir::testdir;

    #[crate::test(when(reqs = S3WithAccessKeySecretKeyReqs, env_prefix= "s3_test_not_defined"))]
    async fn test_signature_s3_with_access_key_secret_key_reqs(_s3: S3WithAccessKeySecretKeyReqs) {
        panic!()
    }

    #[test]
    fn test_s3_with_access_key_secret_key_reqs() {
        let vars: HashMap<String, String> = HashMap::from([
            (
                "TEST_DIR".to_string(),
                testdir!().to_str().unwrap().to_string(),
            ),
            (
                "TESTS_TIMESTAMP".to_string(),
                "0000_00_00_00_00_00".to_string(),
            ),
            ("NS__S3_URI".to_string(), "u".to_string()),
            ("NS__S3_REGION".to_string(), "r".to_string()),
            ("NS__S3_ACCESS_KEY".to_string(), "ak".to_string()),
            ("NS__S3_SECRET_KEY".to_string(), "sk".to_string()),
        ]);
        let reqs = TestRequirementsInEnv::resolve_test_run_variables::<S3WithAccessKeySecretKeyReqs>(
            "test_s3_with_access_key_secret_key_reqs",
            &testdir!(),
            "S3WithAccessKeySecretKeyReqs",
            "ns",
            &vars,
        );
        assert_eq!(reqs.as_ref().unwrap().uri, "u");
        assert_eq!(reqs.as_ref().unwrap().region, "r");
        assert_eq!(reqs.as_ref().unwrap().access_key, "ak");
        assert_eq!(reqs.as_ref().unwrap().secret_key, "sk");
    }
}
