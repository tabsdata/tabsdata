//
// Copyright 2025 Tabsdata Inc.
//

use td_database::access::SeesProjectRows;
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::and;
use td_objects::dxo::function::{FunctionDBWithNames, FunctionUpdate};
use td_objects::types::basic::CollectionId;
use td_queries::Queries;

pub async fn assert_function_name_not_exists<S: SeesProjectRows>(
    conn: &mut DomainConnection<Apiserver, S>,
    collection_id: &CollectionId,
    function: &FunctionDBWithNames,
    function_update: &FunctionUpdate,
) -> Result<(), TdError> {
    if let Some(name) = &function_update.name
        && *name != function.base.base.name
    {
        conn.latest()
            .assert_not_exists::<FunctionDBWithNames>(and!(collection_id, name))
            .await?;
    }
    Ok(())
}
