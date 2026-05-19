//
// Copyright 2025. Tabs Data Inc.
//

crate::define_requires! {
    /// Requirements for a Postgres test using URI, user and password.
    pub struct PostgresReqs {
        uri: postgres_uri,
        user: postgres_user,
        password: postgres_password,
    }
}

#[cfg(test)]
mod tests {
    use crate::__private::{TestSetup, TestSetupExecution};
    use crate::reqs::{PostgresReqs, ReqsTestSetup, TestRequirementsInEnv};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_signature_postgres_reqs_skips_when_missing() {
        let setup = ReqsTestSetup::<PostgresReqs>::new_for_self_test(
            "test_signature_postgres_reqs_skips_when_missing",
            "postgres_test_not_defined",
        );
        assert!(matches!(setup.setup().await, TestSetupExecution::Skip));
    }

    #[test]
    fn test_postgres_reqs() {
        let vars: HashMap<String, String> = HashMap::from([
            (
                "TD_TEST_DIR".to_string(),
                td_common::tdtestdir!().to_str().unwrap().to_string(),
            ),
            (
                "TD_TEST_TIMESTAMP".to_string(),
                "0000_00_00_00_00_00".to_string(),
            ),
            ("NS__POSTGRES_URI".to_string(), "uri".to_string()),
            ("NS__POSTGRES_USER".to_string(), "user".to_string()),
            ("NS__POSTGRES_PASSWORD".to_string(), "password".to_string()),
        ]);
        let reqs = TestRequirementsInEnv::resolve_test_run_variables::<PostgresReqs>(
            "test_postgres_reqs",
            &td_common::tdtestdir!(),
            "PostgresReqs",
            "ns",
            &vars,
            false,
        );
        assert_eq!(reqs.as_ref().unwrap().uri, "uri");
        assert_eq!(reqs.as_ref().unwrap().user, "user");
        assert_eq!(reqs.as_ref().unwrap().password, "password");
    }
}
