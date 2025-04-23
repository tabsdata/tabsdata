//
// Copyright 2025 Tabs Data Inc.
//

use crate::crudl::{list_response, ListRequest, ListResponse, ListResult};
use async_trait::async_trait;
use std::marker::PhantomData;
use std::ops::Deref;
use td_error::TdError;
use td_tower::extractors::Input;

pub struct With<T> {
    _phantom: PhantomData<T>,
}

#[async_trait]
pub trait TryIntoService<T> {
    async fn convert_to<F, E>(input: Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: for<'a> TryFrom<&'a T, Error = E>,
        E: Into<TdError>;
}

#[async_trait]
impl<T> TryIntoService<T> for With<T>
where
    T: Send + Sync,
{
    async fn convert_to<F, E>(Input(input): Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: for<'a> TryFrom<&'a T, Error = E>,
        E: Into<TdError>,
    {
        F::try_from(input.deref()).map_err(Into::into)
    }
}

#[async_trait]
pub trait ConvertIntoMapService<T> {
    async fn vec_convert_to<F, E>(input: Input<Vec<T>>) -> Result<Vec<F>, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: for<'a> TryFrom<&'a T, Error = E>,
        E: Into<TdError>;
}

#[async_trait]
impl<T> ConvertIntoMapService<T> for With<T>
where
    T: Send + Sync,
{
    async fn vec_convert_to<F, E>(Input(input): Input<Vec<T>>) -> Result<Vec<F>, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: for<'a> TryFrom<&'a T, Error = E>,
        E: Into<TdError>,
    {
        input
            .iter()
            .map(|item| F::try_from(item).map_err(Into::into))
            .collect()
    }
}

#[async_trait]
pub trait TryMapListService<T> {
    async fn try_map_list<N, B, F, E>(
        request: Input<ListRequest<N>>,
        result: Input<ListResult<T>>,
    ) -> Result<ListResponse<F>, TdError>
    where
        N: Send + Sync,
        for<'a> T: Send + Sync + 'a,
        B: for<'a> TryFrom<&'a T, Error = E>,
        F: for<'a> TryFrom<&'a B, Error = E>,
        E: Into<TdError>;
}

#[async_trait]
impl<T> TryMapListService<T> for With<T>
where
    T: Send + Sync,
{
    async fn try_map_list<N, B, F, E>(
        Input(request): Input<ListRequest<N>>,
        Input(result): Input<ListResult<T>>,
    ) -> Result<ListResponse<F>, TdError>
    where
        N: Send + Sync,
        for<'a> T: Send + Sync + 'a,
        B: for<'a> TryFrom<&'a T, Error = E>,
        F: for<'a> TryFrom<&'a B, Error = E>,
        E: Into<TdError>,
    {
        Ok(list_response(
            request.list_params().clone(),
            result.try_map(|t| {
                let b = B::try_from(t).map_err(Into::into)?;
                F::try_from(&b).map_err(Into::into)
            })?,
        ))
    }
}

#[async_trait]
pub trait ExtractService<T> {
    async fn extract<F>(input: Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        for<'a> F: From<&'a T>;
}

#[async_trait]
impl<T> ExtractService<T> for With<T>
where
    T: Send + Sync,
{
    async fn extract<F>(Input(input): Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        for<'a> F: From<&'a T>,
    {
        Ok(F::from(input.deref()))
    }
}

#[async_trait]
pub trait ExtractVecService<T> {
    async fn extract_vec<F>(input: Input<Vec<T>>) -> Result<Vec<F>, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        for<'a> F: From<&'a T>;
}

#[async_trait]
impl<T> ExtractVecService<T> for With<T>
where
    T: Send + Sync,
{
    async fn extract_vec<F>(Input(input): Input<Vec<T>>) -> Result<Vec<F>, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        for<'a> F: From<&'a T>,
    {
        let output = input.iter().map(|item| F::from(item)).collect::<Vec<F>>();
        Ok(output)
    }
}

#[async_trait]
pub trait UpdateService<T> {
    async fn update<F, E>(try_from: Input<T>, updater: Input<F>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        for<'a> F: TryFrom<(&'a T, F), Error = E> + Clone + Send + Sync,
        E: Into<TdError>;
}

#[async_trait]
impl<T> UpdateService<T> for With<T>
where
    T: Send + Sync,
{
    async fn update<F, E>(Input(try_from): Input<T>, Input(updater): Input<F>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        for<'a> F: TryFrom<(&'a T, F), Error = E> + Clone + Send + Sync,
        E: Into<TdError>,
    {
        F::try_from((try_from.deref(), updater.deref().clone())).map_err(Into::into)
    }
}

#[async_trait]
pub trait EmptyVecService<T> {
    async fn empty_vec() -> Result<Vec<T>, TdError>;
}

#[async_trait]
impl<T> EmptyVecService<T> for With<T> {
    async fn empty_vec() -> Result<Vec<T>, TdError> {
        Ok(Vec::new())
    }
}

#[async_trait]
pub trait DefaultService<T> {
    async fn default() -> Result<T, TdError>;
}

#[async_trait]
impl<T> DefaultService<T> for With<T>
where
    T: Default,
{
    async fn default() -> Result<T, TdError> {
        Ok(T::default())
    }
}

#[async_trait]
pub trait VecUpdateService<T> {
    async fn vec_update<F, E>(
        try_from: Input<Vec<T>>,
        updater: Input<F>,
    ) -> Result<Vec<F>, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        for<'a> F: TryFrom<(&'a T, F), Error = E> + Clone + Send + Sync,
        E: Into<TdError>;
}

#[async_trait]
impl<T> VecUpdateService<T> for With<T>
where
    T: Send + Sync,
{
    async fn vec_update<F, E>(
        Input(try_from): Input<Vec<T>>,
        Input(updater): Input<F>,
    ) -> Result<Vec<F>, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        for<'a> F: TryFrom<(&'a T, F), Error = E> + Clone + Send + Sync,
        E: Into<TdError>,
    {
        try_from
            .iter()
            .map(|item| F::try_from((item, updater.deref().clone())).map_err(Into::into))
            .collect()
    }
}

#[async_trait]
pub trait SetService<T> {
    async fn set<F>(from: Input<T>, setter: Input<F>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        for<'a> F: From<(&'a T, F)> + Clone + Send + Sync;
}

#[async_trait]
impl<T> SetService<T> for With<T>
where
    T: Send + Sync,
{
    async fn set<F>(Input(from): Input<T>, Input(setter): Input<F>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        for<'a> F: From<(&'a T, F)> + Clone + Send + Sync,
    {
        Ok(F::from((from.deref(), setter.deref().clone())))
    }
}

pub async fn builder<F>() -> Result<F, TdError>
where
    F: for<'a> From<()>,
{
    Ok(F::from(()))
}

#[async_trait]
pub trait BuildService<T> {
    async fn build<F, E>(input: Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: for<'a> TryFrom<&'a T, Error = E>,
        E: Into<TdError>;
}

#[async_trait]
impl<T> BuildService<T> for With<T>
where
    T: Send + Sync,
{
    async fn build<F, E>(Input(input): Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: for<'a> TryFrom<&'a T, Error = E>,
        E: Into<TdError>,
    {
        F::try_from(&input).map_err(Into::into)
    }
}

#[async_trait]
pub trait VecBuildService<T> {
    async fn vec_build<F, E>(input: Input<Vec<T>>) -> Result<Vec<F>, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: for<'a> TryFrom<&'a T, Error = E>,
        E: Into<TdError>;
}

#[async_trait]
impl<T> VecBuildService<T> for With<T>
where
    T: Send + Sync,
{
    async fn vec_build<F, E>(Input(input): Input<Vec<T>>) -> Result<Vec<F>, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: for<'a> TryFrom<&'a T, Error = E>,
        E: Into<TdError>,
    {
        input
            .iter()
            .map(|item| F::try_from(item).map_err(Into::into))
            .collect()
    }
}

/// This one can be used to combine inputs, so BY sql clauses can use all of them as a single one.
/// We might need to generate more combines if needed. We should look for a better way to do this.
pub async fn combine<T: Clone, U: Clone>(
    Input(t): Input<T>,
    Input(u): Input<U>,
) -> Result<(T, U), TdError> {
    Ok((t.deref().clone(), u.deref().clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crudl::{ListParams, RequestContext};
    use crate::types::basic::{AccessTokenId, RoleId, UserId};
    use td_type::Dao;

    #[Dao]
    struct Foo {
        value: i32,
    }

    #[Dao]
    #[td_type(builder(try_from = Foo))]
    struct Var {
        value: i32,
    }

    #[tokio::test]
    async fn test_with_convert_to() -> Result<(), TdError> {
        let input = Input::new(Foo::builder().value(1).build()?);
        let result = With::<Foo>::convert_to::<VarBuilder, _>(input).await?;
        assert_eq!(result.value, Some(1));
        Ok(())
    }

    #[tokio::test]
    async fn test_try_map_list() -> Result<(), TdError> {
        let request: ListRequest<()> = RequestContext::with(
            AccessTokenId::default(),
            UserId::admin(),
            RoleId::sys_admin(),
            true,
        )
        .list((), ListParams::default());
        let request = Input::new(request);

        let result = ListResult::new(vec![Foo::builder().value(1).build()?], false);
        let result = Input::new(result);

        let response = With::<Foo>::try_map_list::<(), VarBuilder, Var, _>(request, result).await?;
        assert_eq!(response.data().len(), 1);
        assert_eq!(response.data()[0].value, 1);
        assert_eq!(*response.offset(), 0);
        assert!(!response.more());
        Ok(())
    }

    #[tokio::test]
    async fn test_extract() -> Result<(), TdError> {
        #[Dao]
        struct ExtractThis {
            #[td_type(extractor)]
            value: i32,
        }

        let input = Input::new(ExtractThis::builder().value(1).build()?);
        let result = With::<ExtractThis>::extract::<i32>(input).await?;
        assert_eq!(result, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_update() -> Result<(), TdError> {
        #[Dao]
        #[td_type(updater(try_from = Foo))]
        struct UpdateThis {
            value: i32,
        }

        let updater = Input::new(Foo::builder().value(2).build()?);
        let input = Input::new(UpdateThis::builder());
        let result = With::<Foo>::update::<UpdateThisBuilder, _>(updater, input).await?;
        let result = result.build()?;
        assert_eq!(result.value, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_set() -> Result<(), TdError> {
        #[Dao]
        struct SetThis {
            #[td_type(setter)]
            value: i32,
        }

        let setter = Input::new(2);
        let input = Input::new(SetThis::builder());
        let result = With::<i32>::set::<SetThisBuilder>(setter, input).await?;
        let result = result.build()?;
        assert_eq!(result.value, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_build() -> Result<(), TdError> {
        let mut builder = Foo::builder();
        builder.value(2);
        let input = Input::new(builder);
        let result = With::<FooBuilder>::build::<Foo, _>(input).await?;
        assert_eq!(result.value, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_combine() -> Result<(), TdError> {
        let foo = Foo::builder().value(1).build()?;
        let var = Var::builder().value(2).build()?;
        let combined = combine(Input::new(foo), Input::new(var)).await?;
        assert_eq!(combined.0.value, 1);
        assert_eq!(combined.1.value, 2);
        Ok(())
    }
}
