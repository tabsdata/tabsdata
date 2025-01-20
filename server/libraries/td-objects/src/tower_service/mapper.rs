//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::{list_response, ListRequest, ListResponse, ListResult};
use td_common::error::TdError;
use td_tower::extractors::Input;

pub async fn map<Dao, Dto>(Input(dao): Input<Dao>) -> Result<Dto, TdError>
where
    Dto: for<'a> From<&'a Dao>,
{
    Ok(Dto::from(&dao))
}

pub async fn map_list<N, Dao, Dto>(
    Input(request): Input<ListRequest<N>>,
    Input(result): Input<ListResult<Dao>>,
) -> Result<ListResponse<Dto>, TdError>
where
    Dto: for<'a> From<&'a Dao>,
{
    Ok(list_response(
        request.list_params().clone(),
        result.map(|dao| Dto::from(dao)),
    ))
}
