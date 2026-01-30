//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::crudl::{Data, IntoData, IntoName, Name};
use crate::dxo::{Extractor, Setter, TryUpdater};
use async_trait::async_trait;
use std::marker::PhantomData;
use std::ops::Deref;
use td_error::{TdError, td_error};
use td_tower::extractors::Input;

pub struct With<T> {
    _phantom: PhantomData<T>,
}

#[async_trait]
pub trait IntoService<T> {
    async fn convert_to<F>(input: Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: for<'a> From<&'a T>;
}

#[async_trait]
impl<T> IntoService<T> for With<T>
where
    T: Send + Sync,
{
    async fn convert_to<F>(Input(input): Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: for<'a> From<&'a T>,
    {
        Ok(F::from(input.deref()))
    }
}

#[async_trait]
pub trait TryIntoService<T> {
    async fn try_convert_to<F, E>(input: Input<T>) -> Result<F, TdError>
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
    async fn try_convert_to<F, E>(Input(input): Input<T>) -> Result<F, TdError>
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
pub trait ExtractService<T> {
    async fn extract<F>(input: Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        T: Extractor<F>;
}

#[async_trait]
impl<T> ExtractService<T> for With<T>
where
    T: Send + Sync,
{
    async fn extract<F>(Input(input): Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        T: Extractor<F>,
    {
        Ok(input.extract())
    }
}

#[async_trait]
pub trait ExtractNameService<T> {
    async fn extract_name<F>(input: Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        T: Extractor<Name<F>>;
}

#[async_trait]
impl<T> ExtractNameService<T> for With<T>
where
    T: Send + Sync,
{
    async fn extract_name<F>(Input(input): Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        T: Extractor<Name<F>>,
    {
        Ok(input.extract().into_name())
    }
}

#[async_trait]
pub trait ExtractDataService<T> {
    async fn extract_data<F>(input: Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        T: Extractor<Data<F>>;
}

#[async_trait]
impl<T> ExtractDataService<T> for With<T>
where
    T: Send + Sync,
{
    async fn extract_data<F>(Input(input): Input<T>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        T: Extractor<Data<F>>,
    {
        Ok(input.extract().into_data())
    }
}

#[async_trait]
pub trait ExtractVecService<T> {
    async fn extract_vec<F>(input: Input<Vec<T>>) -> Result<Vec<F>, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        T: Extractor<F>;
}

#[async_trait]
impl<T> ExtractVecService<T> for With<T>
where
    T: Send + Sync,
{
    async fn extract_vec<F>(Input(input): Input<Vec<T>>) -> Result<Vec<F>, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        T: Extractor<F>,
    {
        let output = input.iter().map(|item| item.extract()).collect::<Vec<F>>();
        Ok(output)
    }
}

#[async_trait]
pub trait UpdateService<T> {
    async fn update<F, E>(try_from: Input<T>, updater: Input<F>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: TryUpdater<T, Error = E> + Clone + Send + Sync,
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
        F: TryUpdater<T, Error = E> + Clone + Send + Sync,
        E: Into<TdError>,
    {
        let mut updater = updater.deref().clone();
        updater
            .try_update_from(try_from.deref())
            .map_err(Into::into)?;
        Ok(updater)
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
pub trait IntoSomeService<T> {
    async fn some(value: Input<T>) -> Result<Option<T>, TdError>
    where
        for<'a> T: Send + Sync + Clone + 'a;
}

#[async_trait]
impl<T> IntoSomeService<T> for With<T> {
    async fn some(Input(value): Input<T>) -> Result<Option<T>, TdError>
    where
        for<'a> T: Send + Sync + Clone + 'a,
    {
        Ok(Some(value.deref().clone()))
    }
}

#[td_error]
pub enum WithError {
    #[error("Option has no value")]
    OptionHasNoValue,
}

impl<T> With<Option<T>> {
    pub async fn unwrap(Input(value): Input<Option<T>>) -> Result<T, TdError>
    where
        T: Send + Sync + Clone,
    {
        value
            .deref()
            .clone()
            .ok_or(WithError::OptionHasNoValue.into())
    }

    pub async fn unwrap_or_default(Input(value): Input<Option<T>>) -> Result<T, TdError>
    where
        T: Default + Send + Sync + Clone,
    {
        Ok(value.deref().clone().unwrap_or_default())
    }

    pub async fn unwrap_or_else<U>(
        Input(value): Input<Option<T>>,
        Input(or): Input<U>,
    ) -> Result<T, TdError>
    where
        T: Send + Sync + Clone,
        U: Into<T> + Clone + Send + Sync,
    {
        match value.deref() {
            Some(v) => Ok(v.clone()),
            None => Ok(or.deref().clone().into()),
        }
    }
}

#[async_trait]
pub trait VecUpdateService<T> {
    async fn vec_update<F, E>(
        try_from: Input<Vec<F>>,
        updater: Input<T>,
    ) -> Result<Vec<F>, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: TryUpdater<T, Error = E> + Clone + Send + Sync,
        E: Into<TdError>;
}

#[async_trait]
impl<T> VecUpdateService<T> for With<T>
where
    T: Send + Sync,
{
    async fn vec_update<F, E>(
        Input(to_update): Input<Vec<F>>,
        Input(updater): Input<T>,
    ) -> Result<Vec<F>, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: TryUpdater<T, Error = E> + Clone + Send + Sync,
        E: Into<TdError>,
    {
        to_update
            .iter()
            .map(|item| {
                let mut item = item.clone();
                item.try_update_from(&*updater).map_err(Into::into)?;
                Ok(item)
            })
            .collect()
    }
}

#[async_trait]
pub trait SetService<T> {
    async fn set<F>(from: Input<T>, setter: Input<F>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: Setter<T> + Clone + Send + Sync;
}

#[async_trait]
impl<T> SetService<T> for With<T>
where
    T: Send + Sync,
{
    async fn set<F>(Input(from): Input<T>, Input(setter): Input<F>) -> Result<F, TdError>
    where
        for<'a> T: Send + Sync + 'a,
        F: Setter<T> + Clone + Send + Sync,
    {
        let mut setter = setter.deref().clone();
        setter.set(from.deref());
        Ok(setter)
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
        let result = With::<Foo>::try_convert_to::<VarBuilder, _>(input).await?;
        assert_eq!(result.value, Some(1));
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

    #[tokio::test]
    async fn test_option() -> Result<(), TdError> {
        #[td_type::typed(string)]
        struct MyString;

        let option = Some(MyString::try_from("test")?);
        let result = With::<Option<MyString>>::unwrap(Input::new(option)).await?;
        assert_eq!(result, MyString::try_from("test")?);

        let option = None;
        let result = With::<Option<MyString>>::unwrap(Input::new(option)).await;
        assert!(result.is_err());

        let option = None;
        let or_else = MyString::try_from("or_else")?;
        let result = With::<Option<MyString>>::unwrap_or_else(
            Input::new(option),
            Input::new(or_else.clone()),
        )
        .await?;
        assert_eq!(result, or_else);

        Ok(())
    }
}
