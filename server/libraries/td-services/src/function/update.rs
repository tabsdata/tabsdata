//
// Copyright 2025 Tabsdata Inc.
//

use td_database::domain::{Apiserver, DomainConnection};
use td_error::{TdError, td_error};
use td_objects::and;
use td_objects::dxo::function::{FunctionDBWithNames, FunctionUpdate};
use td_objects::types::basic::{CollectionId, CollectionName, FunctionName};
use td_queries::CurrentEntry;

#[td_error]
enum UpdateFunctionError {
    #[error("Function '{0}' already exists in collection '{1}'")]
    FunctionAlreadyExists(FunctionName, CollectionName) = 0,
}

pub async fn assert_function_name_not_exists(
    conn: &mut DomainConnection<Apiserver>,
    collection_id: &CollectionId,
    collection_name: &CollectionName,
    function: &FunctionDBWithNames,
    function_update: &FunctionUpdate,
) -> Result<(), TdError> {
    if let Some(name) = &function_update.name
        && *name != function.base.name
    {
        let found = conn
            .current()
            .state(FunctionDBWithNames::available())
            .fetch_optional(and!(*collection_id, name))
            .await?;

        if found.is_some() {
            Err(UpdateFunctionError::FunctionAlreadyExists(
                name.clone(),
                collection_name.clone(),
            ))?
        }
    }

    Ok(())
}
