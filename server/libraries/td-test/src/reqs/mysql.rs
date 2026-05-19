//
// Copyright 2025. Tabs Data Inc.
//

crate::define_requires! {
    /// Requirements for a MySQL test using URI, user and password.
    pub struct MySqlReqs {
        uri: mysql_uri,
        user: mysql_user,
        password: mysql_password,
    }
}

#[cfg(test)]
mod tests {
    use crate::__private::{TestSetup, TestSetupExecution};
    use crate::reqs::{MySqlReqs, ReqsTestSetup, TestRequirementsInEnv};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_signature_mysql_reqs_skips_when_missing() {
        let setup = ReqsTestSetup::<MySqlReqs>::new_for_self_test(
            "test_signature_mysql_reqs_skips_when_missing",
            "mysql_test_not_defined",
        );
        assert!(matches!(setup.setup().await, TestSetupExecution::Skip));
    }

    #[test]
    fn test_mysql_reqs() {
        let vars: HashMap<String, String> = HashMap::from([
            (
                "TD_TEST_DIR".to_string(),
                td_common::tdtestdir!().to_str().unwrap().to_string(),
            ),
            (
                "TD_TEST_TIMESTAMP".to_string(),
                "0000_00_00_00_00_00".to_string(),
            ),
            ("NS__MYSQL_URI".to_string(), "uri".to_string()),
            ("NS__MYSQL_USER".to_string(), "user".to_string()),
            ("NS__MYSQL_PASSWORD".to_string(), "password".to_string()),
        ]);
        let reqs = TestRequirementsInEnv::resolve_test_run_variables::<MySqlReqs>(
            "test_mysql_reqs",
            &td_common::tdtestdir!(),
            "MySqlReqs",
            "ns",
            &vars,
            false,
        );
        assert_eq!(reqs.as_ref().unwrap().uri, "uri");
        assert_eq!(reqs.as_ref().unwrap().user, "user");
        assert_eq!(reqs.as_ref().unwrap().password, "password");
    }
}
