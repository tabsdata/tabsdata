//
// Copyright 2025. Tabs Data Inc.
//

use crate::reqs::TestRequirements;
use std::collections::HashMap;

#[allow(dead_code)]
/// Requirements for a MySQL test using URI, user and password.
pub struct MySqlReqs {
    pub uri: String,
    pub user: String,
    pub password: String,
    vars: HashMap<String, String>,
}

impl TestRequirements for MySqlReqs {
    fn keys() -> &'static [&'static str] {
        &["mysql_uri", "mysql_user", "mysql_password"]
    }

    fn new(vars: impl Into<HashMap<String, String>>) -> Self {
        let vars = vars.into();
        Self {
            uri: vars["mysql_uri"].clone(),
            user: vars["mysql_user"].clone(),
            password: vars["mysql_password"].clone(),
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

    use crate::reqs::{MySqlReqs, TestRequirementsInEnv};
    use std::collections::HashMap;
    use testdir::testdir;

    #[crate::test(when(reqs = MySqlReqs, env_prefix= "mysql_test_not_defined", do_not_fail_reqs= true))]
    async fn test_signature_mysql_reqs(_s3: MySqlReqs) {
        panic!()
    }

    #[test]
    fn test_mysql_reqs() {
        let vars: HashMap<String, String> = HashMap::from([
            (
                "TEST_DIR".to_string(),
                testdir!().to_str().unwrap().to_string(),
            ),
            (
                "TESTS_TIMESTAMP".to_string(),
                "0000_00_00_00_00_00".to_string(),
            ),
            ("NS__MYSQL_URI".to_string(), "uri".to_string()),
            ("NS__MYSQL_USER".to_string(), "user".to_string()),
            ("NS__MYSQL_PASSWORD".to_string(), "password".to_string()),
        ]);
        let reqs = TestRequirementsInEnv::resolve_test_run_variables::<MySqlReqs>(
            "test_mysql_reqs",
            &testdir!(),
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
