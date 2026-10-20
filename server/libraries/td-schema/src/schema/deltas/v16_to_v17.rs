//
// Copyright 2026 Tabsdata Inc.
//

//! v16 → v17: an execution no longer records how its plan was walked; its
//! runs and their inputs record the plan itself.

use crate::schema::render::update_db_version;
use crate::schema::version::SchemaDelta;

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 16;
    const TO_VERSION: usize = 17;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        vec![
            "ALTER TABLE executions DROP COLUMN recursion_options".to_string(),
            update_db_version(Self::TO_VERSION),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v16_to_v17() {
        validate_delta::<Migration>();
    }
}
