//
// Copyright 2024 Tabs Data Inc.
//

use crate::entity_finder::{EntityFinder, IdName};
use crate::users::dao::{User, UserWithNames};

impl IdName for UserWithNames {
    fn id(&self) -> &str {
        self.id()
    }

    fn name(&self) -> &str {
        self.name()
    }
}
pub type UserWithNamesFinder = EntityFinder<UserWithNames>;

impl Default for UserWithNamesFinder {
    fn default() -> Self {
        const SELECT_BY_IDS_TEMPLATE: &str = r#"
            SELECT
                id,
                name,
                full_name,
                email,
                created_on,
                created_by_id,
                created_by,
                modified_on,
                modified_by_id,
                modified_by,
                password_set_on,
                password_must_change,
                enabled
            FROM users_with_names
            WHERE
                id IN ({})
        "#;

        const SELECT_BY_NAMES_TEMPLATE: &str = r#"
            SELECT
                id,
                name,
                full_name,
                email,
                created_on,
                created_by_id,
                created_by,
                modified_on,
                modified_by_id,
                modified_by,
                password_set_on,
                password_must_change,
                enabled
            FROM users_with_names
            WHERE
                name IN ({})
        "#;

        UserWithNamesFinder::new(SELECT_BY_IDS_TEMPLATE, SELECT_BY_NAMES_TEMPLATE)
    }
}

impl IdName for User {
    fn id(&self) -> &str {
        self.id()
    }

    fn name(&self) -> &str {
        self.name()
    }
}
pub type UserFinder = EntityFinder<User>;

impl Default for UserFinder {
    fn default() -> Self {
        const SELECT_BY_IDS_TEMPLATE: &str = r#"
            SELECT
                id,
                name,
                full_name,
                email,
                created_on,
                created_by_id,
                modified_on,
                modified_by_id,
                password_hash,
                password_set_on,
                password_must_change,
                enabled
            FROM users
            WHERE
                id IN ({})
        "#;

        const SELECT_BY_NAMES_TEMPLATE: &str = r#"
            SELECT
                id,
                name,
                full_name,
                email,
                created_on,
                created_by_id,
                modified_on,
                modified_by_id,
                password_hash,
                password_set_on,
                password_must_change,
                enabled
            FROM users
            WHERE
                name IN ({})
        "#;

        UserFinder::new(SELECT_BY_IDS_TEMPLATE, SELECT_BY_NAMES_TEMPLATE)
    }
}
