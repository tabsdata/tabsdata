//
// Copyright 2025 Tabs Data Inc.
//

#![allow(dead_code)]

pub mod extractor;
pub mod sql;

use std::ops::Deref;
use td_error::TdError;
use td_objects::crudl::{list_response, ListRequest, ListResponse, ListResult};
use td_tower::extractors::Input;

pub async fn try_from<T, F: for<'a> TryFrom<&'a T, Error = impl Into<TdError>>>(
    Input(input): Input<T>,
) -> Result<F, TdError> {
    F::try_from(input.deref()).map_err(Into::into)
}

pub async fn map_try_from<T, F: for<'a> TryFrom<&'a T, Error = impl Into<TdError>>>(
    Input(input): Input<Vec<T>>,
) -> Result<Vec<F>, TdError> {
    input
        .iter()
        .map(|item| F::try_from(item).map_err(Into::into))
        .collect()
}

pub async fn try_map_list<
    N,
    T,
    B: for<'a> TryFrom<&'a T, Error = impl Into<TdError>>,
    F: for<'a> TryFrom<&'a B, Error = impl Into<TdError>>,
>(
    Input(request): Input<ListRequest<N>>,
    Input(result): Input<ListResult<T>>,
) -> Result<ListResponse<F>, TdError> {
    Ok(list_response(
        request.list_params().clone(),
        result.try_map(|t| {
            let b = B::try_from(t).map_err(Into::into)?;
            F::try_from(&b).map_err(Into::into)
        })?,
    ))
}

pub async fn extract<T, F: for<'a> From<&'a T>>(Input(input): Input<T>) -> Result<F, TdError> {
    Ok(F::from(input.deref()))
}

pub async fn update_from<T, F: Clone + for<'a> TryFrom<(&'a T, F), Error = impl Into<TdError>>>(
    Input(try_from): Input<T>,
    Input(updater): Input<F>,
) -> Result<F, TdError> {
    F::try_from((try_from.deref(), updater.deref().clone())).map_err(Into::into)
}

pub async fn build<T, F: for<'a> TryFrom<&'a T, Error = impl Into<TdError>>>(
    Input(input): Input<T>,
) -> Result<F, TdError> {
    F::try_from(&input).map_err(Into::into)
}

pub async fn map_build<T, F: for<'a> TryFrom<&'a T, Error = impl Into<TdError>>>(
    Input(input): Input<Vec<T>>,
) -> Result<Vec<F>, TdError> {
    input
        .iter()
        .map(|item| F::try_from(item).map_err(Into::into))
        .collect()
}
