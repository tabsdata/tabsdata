//
// Copyright 2025. Tabs Data Inc.
//

crate::define_requires! {
    /// Requirements for an Oracle test using URI, user and password.
    pub struct OracleReqs {
        uri: oracle_uri,
        user: oracle_user,
        password: oracle_password,
    }
}

#[cfg(test)]
mod tests {
    use crate::__private::{TestSetup, TestSetupExecution};
    use crate::reqs::{OracleReqs, ReqsTestSetup, TestRequirementsInEnv};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_signature_oracle_reqs_skips_when_missing() {
        let setup = ReqsTestSetup::<OracleReqs>::new_for_self_test(
            "test_signature_oracle_reqs_skips_when_missing",
            "oracle_test_not_defined",
        );
        assert!(matches!(setup.setup().await, TestSetupExecution::Skip));
    }

    #[test]
    fn test_oracle_reqs() {
        let vars: HashMap<String, String> = HashMap::from([
            (
                "TD_TEST_DIR".to_string(),
                td_common::tdtestdir!().to_str().unwrap().to_string(),
            ),
            (
                "TD_TEST_TIMESTAMP".to_string(),
                "0000_00_00_00_00_00".to_string(),
            ),
            ("NS__ORACLE_URI".to_string(), "uri".to_string()),
            ("NS__ORACLE_USER".to_string(), "user".to_string()),
            ("NS__ORACLE_PASSWORD".to_string(), "password".to_string()),
        ]);
        let reqs = TestRequirementsInEnv::resolve_test_run_variables::<OracleReqs>(
            "test_oracle_reqs",
            &td_common::tdtestdir!(),
            "OracleReqs",
            "ns",
            &vars,
            false,
        );
        assert_eq!(reqs.as_ref().unwrap().uri, "uri");
        assert_eq!(reqs.as_ref().unwrap().user, "user");
        assert_eq!(reqs.as_ref().unwrap().password, "password");
    }
}
