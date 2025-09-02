//
// Copyright 2025. Tabs Data Inc.
//

use crate::reqs::TestRequirements;
use std::collections::HashMap;

#[allow(dead_code)]
/// Requirements for a Postgres test using URI, user and password.
pub struct PostgresReqs {
    pub uri: String,
    pub user: String,
    pub password: String,
    vars: HashMap<String, String>,
}

impl TestRequirements for PostgresReqs {
    fn keys() -> &'static [&'static str] {
        &["postgres_uri", "postgres_user", "postgres_password"]
    }

    fn new(vars: impl Into<HashMap<String, String>>) -> Self {
        let vars = vars.into();
        Self {
            uri: vars["postgres_uri"].clone(),
            user: vars["postgres_user"].clone(),
            password: vars["postgres_password"].clone(),
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

    use crate::reqs::{PostgresReqs, TestRequirementsInEnv};
    use std::collections::HashMap;
    use testdir::testdir;

    #[crate::test(when(reqs = PostgresReqs, env_prefix= "postgres_test_not_defined", do_not_fail_reqs= true))]
    #[tokio::test]
    async fn test_signature_postgres_reqs(_s3: PostgresReqs) {
        panic!()
    }

    #[test]
    fn test_postgres_reqs() {
        let vars: HashMap<String, String> = HashMap::from([
            (
                "TEST_DIR".to_string(),
                testdir!().to_str().unwrap().to_string(),
            ),
            (
                "TESTS_TIMESTAMP".to_string(),
                "0000_00_00_00_00_00".to_string(),
            ),
            ("NS__POSTGRES_URI".to_string(), "uri".to_string()),
            ("NS__POSTGRES_USER".to_string(), "user".to_string()),
            ("NS__POSTGRES_PASSWORD".to_string(), "password".to_string()),
        ]);
        let reqs = TestRequirementsInEnv::resolve_test_run_variables::<PostgresReqs>(
            "test_postgres_reqs",
            &testdir!(),
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
