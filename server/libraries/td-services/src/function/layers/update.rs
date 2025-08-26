//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::{TdError, td_error};
use td_objects::crudl::handle_sql_err;
use td_objects::sql::DaoQueries;
use td_objects::sql::cte::CteQueries;
use td_objects::types::basic::{
    AtTime, CollectionId, CollectionName, FunctionName, FunctionStatus,
};
use td_objects::types::function::{FunctionDBWithNames, FunctionUpdate};
use td_tower::extractors::{Connection, Input, IntoMutSqlConnection, SrvCtx};

#[td_error]
enum UpdateFunctionError {
    #[error("Function '{0}' already exists in collection '{1}'")]
    FunctionAlreadyExists(FunctionName, CollectionName) = 0,
}

pub async fn assert_function_name_not_exists(
    Connection(connection): Connection,
    SrvCtx(queries): SrvCtx<DaoQueries>,
    Input(at_time): Input<AtTime>,
    Input(collection_id): Input<CollectionId>,
    Input(collection_name): Input<CollectionName>,
    Input(function): Input<FunctionDBWithNames>,
    Input(function_update): Input<FunctionUpdate>,
) -> Result<(), TdError> {
    if function_update.name() != function.name() {
        let mut conn = connection.lock().await;
        let conn = conn.get_mut_connection()?;

        let found: Option<FunctionDBWithNames> = queries
            .select_versions_at::<FunctionDBWithNames>(
                Some(&at_time),
                Some(&[&FunctionStatus::Active]),
                &(&*collection_id, function_update.name()),
            )?
            .build_query_as()
            .fetch_optional(&mut *conn)
            .await
            .map_err(handle_sql_err)?;

        if found.is_some() {
            Err(UpdateFunctionError::FunctionAlreadyExists(
                function_update.name().clone(),
                collection_name.deref().clone(),
            ))?
        }
    }

    Ok(())
}
