//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::crudl::{Data, IntoData, IntoName, Name};
use std::marker::PhantomData;
use std::ops::Deref;
use ta_objects::{Buildable, Extractor, Setter, TryUpdater};
use td_error::{TdError, td_error};
use td_tower::extractors::Ref;

pub struct With<T> {
    _phantom: PhantomData<T>,
}

impl<T> With<T> {
    pub async fn convert_to<F>(input: Ref<T>) -> Result<F, TdError>
    where
        F: for<'a> From<&'a T>,
    {
        Ok(F::from(input.deref()))
    }

    pub async fn try_convert_to<F, E>(input: Ref<T>) -> Result<F, TdError>
    where
        F: for<'a> TryFrom<&'a T, Error = E>,
        E: Into<TdError>,
    {
        F::try_from(input.deref()).map_err(Into::into)
    }

    pub async fn vec_convert_to<F, E>(input: Ref<Vec<T>>) -> Result<Vec<F>, TdError>
    where
        F: for<'a> TryFrom<&'a T, Error = E>,
        E: Into<TdError>,
    {
        input
            .iter()
            .map(|item| F::try_from(item).map_err(Into::into))
            .collect()
    }

    pub async fn extract<F>(input: Ref<T>) -> Result<F, TdError>
    where
        T: Extractor<F>,
    {
        Ok(input.extract())
    }

    pub async fn extract_name<F>(input: Ref<T>) -> Result<F, TdError>
    where
        T: Extractor<Name<F>>,
    {
        Ok(input.extract().into_name())
    }

    pub async fn extract_data<F>(input: Ref<T>) -> Result<F, TdError>
    where
        T: Extractor<Data<F>>,
    {
        Ok(input.extract().into_data())
    }

    pub async fn update<F, E>(
        try_from: Ref<T>,
        updater: Ref<F>,
    ) -> Result<F, TdError>
    where
        F: TryUpdater<T, Error = E> + Clone,
        E: Into<TdError>,
    {
        let mut updater = updater.deref().clone();
        updater
            .try_update_from(try_from.deref())
            .map_err(Into::into)?;
        Ok(updater)
    }

    pub async fn empty_vec() -> Result<Vec<T>, TdError> {
        Ok(Vec::new())
    }

    pub async fn default() -> Result<T, TdError>
    where
        T: Default,
    {
        Ok(T::default())
    }

    pub async fn some(value: Ref<T>) -> Result<Option<T>, TdError>
    where
        T: Clone,
    {
        Ok(Some(value.deref().clone()))
    }

    pub async fn set<F>(from: Ref<T>, setter: Ref<F>) -> Result<F, TdError>
    where
        F: Setter<T> + Clone,
    {
        let mut setter = setter.deref().clone();
        setter.set(from.deref());
        Ok(setter)
    }

    pub async fn build<F>(input: Ref<T>) -> Result<F, TdError>
    where
        F: Buildable<Builder = T>,
    {
        F::build(input.deref())
    }
}

#[td_error]
pub enum WithError {
    #[error("Option has no value")]
    OptionHasNoValue,
}

impl<T> With<Option<T>> {
    pub async fn unwrap(value: Ref<Option<T>>) -> Result<T, TdError>
    where
        T: Clone,
    {
        value
            .deref()
            .clone()
            .ok_or(WithError::OptionHasNoValue.into())
    }

    pub async fn unwrap_or_default(value: Ref<Option<T>>) -> Result<T, TdError>
    where
        T: Default + Clone,
    {
        Ok(value.deref().clone().unwrap_or_default())
    }

    pub async fn unwrap_or_else<U>(
        value: Ref<Option<T>>,
        or: Ref<U>,
    ) -> Result<T, TdError>
    where
        T: Clone,
        U: Into<T> + Clone,
    {
        match value.deref() {
            Some(v) => Ok(v.clone()),
            None => Ok(or.deref().clone().into()),
        }
    }
}

pub async fn builder<F: Buildable>() -> Result<F::Builder, TdError> {
    Ok(F::builder())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ta_objects::Buildable;
    use ta_objects::Dao;

    #[ta_objects::typed(i32)]
    pub struct TypedI32;

    #[ta_objects::typed(string(default = "joaquin"))]
    #[derive(Eq, PartialEq)]
    struct TypedString;

    #[Dao]
    struct Foo {
        value: TypedI32,
    }

    impl From<&Foo> for TypedI32 {
        fn from(value: &Foo) -> Self {
            value.value
        }
    }

    #[Dao]
    #[dxo(builder(try_from = Foo))]
    struct Var {
        value: TypedI32,
    }

    #[tokio::test]
    async fn test_convert_to() -> Result<(), TdError> {
        let foo = Foo::builder().try_value(1)?.build()?;
        let result = With::<Foo>::convert_to::<TypedI32>(Ref::new(foo)).await?;
        assert_eq!(*result, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_try_convert_to() -> Result<(), TdError> {
        let foo = Foo::builder().try_value(1)?.build()?;
        let result = With::<Foo>::try_convert_to::<VarBuilder, _>(Ref::new(foo)).await?;
        assert_eq!(result.value.as_deref(), Some(&1));
        Ok(())
    }

    #[tokio::test]
    async fn test_vec_convert_to() -> Result<(), TdError> {
        let items = vec![
            Foo::builder().try_value(1)?.build()?,
            Foo::builder().try_value(2)?.build()?,
        ];
        let result = With::<Foo>::vec_convert_to::<VarBuilder, _>(Ref::new(items)).await?;
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].value.as_deref(), Some(&1));
        assert_eq!(result[1].value.as_deref(), Some(&2));
        Ok(())
    }

    #[tokio::test]
    async fn test_extract() -> Result<(), TdError> {
        #[Dao]
        struct ExtractThis {
            #[dxo(extractor)]
            value: TypedI32,
        }

        let input = ExtractThis::builder().try_value(1)?.build()?;
        let result = With::<ExtractThis>::extract::<TypedI32>(Ref::new(input)).await?;
        assert_eq!(*result, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_extract_name() -> Result<(), TdError> {
        #[Dao]
        struct ExtractNameThis {
            value: TypedI32,
        }

        impl Extractor<Name<TypedI32>> for ExtractNameThis {
            fn extract(&self) -> Name<TypedI32> {
                Name(self.value)
            }
        }

        let input = ExtractNameThis::builder().try_value(5)?.build()?;
        let result = With::<ExtractNameThis>::extract_name::<TypedI32>(Ref::new(input)).await?;
        assert_eq!(*result, 5);
        Ok(())
    }

    #[tokio::test]
    async fn test_extract_data() -> Result<(), TdError> {
        #[Dao]
        struct ExtractDataThis {
            #[dxo(extractor)]
            value: TypedI32,
        }

        impl Extractor<Data<TypedI32>> for ExtractDataThis {
            fn extract(&self) -> Data<TypedI32> {
                Data(self.value)
            }
        }

        let input = ExtractDataThis::builder().try_value(7)?.build()?;
        let result = With::<ExtractDataThis>::extract_data::<TypedI32>(Ref::new(input)).await?;
        assert_eq!(*result, 7);
        Ok(())
    }

    #[tokio::test]
    async fn test_extract_vec() -> Result<(), TdError> {
        #[Dao]
        struct ExtractVecThis {
            #[dxo(extractor)]
            value: TypedI32,
        }

        let items = vec![
            ExtractVecThis::builder().try_value(1)?.build()?,
            ExtractVecThis::builder().try_value(2)?.build()?,
            ExtractVecThis::builder().try_value(3)?.build()?,
        ];
        let result =
            With::<Vec<ExtractVecThis>>::extract::<Vec<TypedI32>>(Ref::new(items)).await?;
        assert_eq!(result.len(), 3);
        assert_eq!(*result[0], 1);
        assert_eq!(*result[1], 2);
        assert_eq!(*result[2], 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_update() -> Result<(), TdError> {
        #[Dao]
        #[dxo(updater(try_from = Foo))]
        struct UpdateThis {
            value: TypedI32,
        }

        let updater = Foo::builder().try_value(2)?.build()?;
        let input = UpdateThis::builder();
        let result =
            With::<Foo>::update::<UpdateThisBuilder, _>(Ref::new(updater), Ref::new(input))
                .await?;
        let result = result.build()?;
        assert_eq!(*result.value, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_vec_update() -> Result<(), TdError> {
        #[Dao]
        #[dxo(updater(try_from = Foo))]
        struct VecUpdateThis {
            value: TypedI32,
        }

        let items = vec![VecUpdateThis::builder(), VecUpdateThis::builder()];
        let updater = Foo::builder().try_value(9)?.build()?;
        let result = With::<Foo>::update::<Vec<VecUpdateThisBuilder>, _>(
            Ref::new(updater),
            Ref::new(items),
        )
        .await?;
        assert_eq!(result.len(), 2);
        let first = result[0].clone().build()?;
        let second = result[1].clone().build()?;
        assert_eq!(*first.value, 9);
        assert_eq!(*second.value, 9);
        Ok(())
    }

    #[tokio::test]
    async fn test_empty_vec() -> Result<(), TdError> {
        let result = With::<TypedI32>::empty_vec().await?;
        assert!(result.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_default() -> Result<(), TdError> {
        let result = With::<Vec<TypedI32>>::default().await?;
        assert!(result.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_some() -> Result<(), TdError> {
        let typed: TypedI32 = 42.try_into()?;
        let result = With::<TypedI32>::some(Ref::new(typed)).await?;
        assert!(result.is_some());
        assert_eq!(*result.unwrap(), 42);
        Ok(())
    }

    #[tokio::test]
    async fn test_set() -> Result<(), TdError> {
        #[Dao]
        struct SetThis {
            #[dxo(setter)]
            value: TypedI32,
        }

        let setter: TypedI32 = 2.try_into()?;
        let input = SetThis::builder();
        let result =
            With::<TypedI32>::set::<SetThisBuilder>(Ref::new(setter), Ref::new(input)).await?;
        let result = result.build()?;
        assert_eq!(*result.value, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_build() -> Result<(), TdError> {
        let mut b = Foo::builder();
        b.try_value(2)?;
        let result = With::<FooBuilder>::build::<Foo>(Ref::new(b)).await?;
        assert_eq!(*result.value, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_vec_build() -> Result<(), TdError> {
        let items = vec![
            Var::builder().try_value(3)?.to_owned(),
            Var::builder().try_value(4)?.to_owned(),
        ];
        let result = With::<Vec<VarBuilder>>::build::<Vec<Var>>(Ref::new(items)).await?;
        assert_eq!(result.len(), 2);
        assert_eq!(*result[0].value, 3);
        assert_eq!(*result[1].value, 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_builder() -> Result<(), TdError> {
        let mut result = builder::<Foo>().await?;
        let result = result.try_value(5)?.build()?;
        assert_eq!(*result.value, 5);
        Ok(())
    }

    #[tokio::test]
    async fn test_unwrap_some() -> Result<(), TdError> {
        let option = Some(TypedString::try_from("test")?);
        let result = With::<Option<TypedString>>::unwrap(Ref::new(option)).await?;
        assert_eq!(result, TypedString::try_from("test")?);
        Ok(())
    }

    #[tokio::test]
    async fn test_unwrap_none() -> Result<(), TdError> {
        let option: Option<TypedString> = None;
        let result = With::<Option<TypedString>>::unwrap(Ref::new(option)).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_unwrap_or_default() -> Result<(), TdError> {
        let some = Some(TypedString::try_from("hello")?);
        let result = With::<Option<TypedString>>::unwrap_or_default(Ref::new(some)).await?;
        assert_eq!(result, TypedString::try_from("hello")?);

        let none: Option<TypedString> = None;
        let result = With::<Option<TypedString>>::unwrap_or_default(Ref::new(none)).await?;
        assert_eq!(*result, *TypedString::default());
        Ok(())
    }

    #[tokio::test]
    async fn test_unwrap_or_else() -> Result<(), TdError> {
        let some = Some(TypedString::try_from("value")?);
        let fallback = TypedString::try_from("fallback")?;
        let result = With::<Option<TypedString>>::unwrap_or_else(
            Ref::new(some),
            Ref::new(fallback.clone()),
        )
        .await?;
        assert_eq!(result, TypedString::try_from("value")?);

        let none: Option<TypedString> = None;
        let fallback = TypedString::try_from("fallback")?;
        let result = With::<Option<TypedString>>::unwrap_or_else(
            Ref::new(none),
            Ref::new(fallback.clone()),
        )
        .await?;
        assert_eq!(result, fallback);
        Ok(())
    }
}
