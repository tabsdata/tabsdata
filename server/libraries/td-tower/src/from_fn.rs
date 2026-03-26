//
//  Copyright 2024 Tabs Data Inc.
//

//! A generic layer to wrap a function as a `tower::Service`. Similar to
//! [`tower::service_fn`] or [`axum::middleware::from_fn`], but more reusable.
//!
//! Useful for creating services from async functions that operate on extracted inputs.

use crate::error::FromHandlerError;
use crate::extractors::FromHandler;
use crate::handler::{Handler, IntoHandler};
use futures_util::future::BoxFuture;
use std::marker::PhantomData;
use std::task;
use td_error::TdError;
use tower::util::BoxCloneSyncService;
use tower::{Service, ServiceBuilder};
use tower_layer::Layer;

/// Creates a new `FromFnLayer` from a function.
pub fn from_fn<F, T>(f: F) -> FromFnLayer<F, T> {
    FromFnLayer {
        f,
        _extractor: PhantomData,
    }
}

/// A layer that wraps a function and produces a service.
#[must_use]
pub struct FromFnLayer<F, T> {
    f: F,
    _extractor: PhantomData<fn() -> T>,
}

impl<F: Clone, T> Clone for FromFnLayer<F, T> {
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            _extractor: PhantomData,
        }
    }
}

impl<I, F, T> Layer<I> for FromFnLayer<F, T>
where
    F: Clone,
    I: Service<Handler, Error = TdError> + Clone + Send + Sync + 'static,
    I::Response: IntoHandler,
    I::Future: Send,
{
    type Service = FromFn<F, T>;

    fn layer(&self, inner: I) -> Self::Service {
        let boxed_inner = BoxCloneSyncService::new(
            ServiceBuilder::new()
                .map_response(IntoHandler::into_handler)
                .service(inner),
        );

        FromFn {
            f: self.f.clone(),
            inner: boxed_inner,
            _extractor: PhantomData,
        }
    }
}

/// Service created from a function and an inner service.
pub struct FromFn<F, T> {
    f: F,
    inner: BoxCloneSyncService<Handler, Handler, TdError>,
    _extractor: PhantomData<fn() -> T>,
}

impl<F: Clone, T> Clone for FromFn<F, T> {
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            inner: self.inner.clone(),
            _extractor: PhantomData,
        }
    }
}

/// Implements `Service` for `FromFn` for tuples of inputs.
macro_rules! impl_service {
    (
        [$($ty:ident),*]
    ) => {
        #[allow(non_snake_case, unused_mut, unused_parens, unused_variables)]
        impl<F, Fut, Out, Err, $($ty,)*> Service<Handler> for FromFn<F, ($($ty,)*)>
        where
            F: FnMut($($ty,)*) -> Fut + Clone + Send + 'static,
            $( $ty: FromHandler + Send, )*
            Fut: std::future::Future<Output = Result<Out, Err>> + Send + 'static,
            Out: Send + Sync + 'static,
            Err: From<TdError> + From<FromHandlerError>
        {
            type Response = Handler;
            type Error = Err;
            type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

            fn poll_ready(&mut self, cx: &mut task::Context<'_>) -> task::Poll<Result<(), Self::Error>> {
                match self.inner.poll_ready(cx) {
                    task::Poll::Ready(Ok(_)) => task::Poll::Ready(Ok(())),
                    task::Poll::Ready(Err(e)) => task::Poll::Ready(Err(e.into())),
                    task::Poll::Pending => task::Poll::Pending,
                }
            }

            fn call(&mut self, mut handler: Handler) -> Self::Future {
                let not_ready_inner = self.inner.clone();
                let mut ready_inner = std::mem::replace(&mut self.inner, not_ready_inner);
                let mut f = self.f.clone();

                Box::pin(async move {
                    #[cfg(feature = "test_tower_metadata")]
                    {
                        use $crate::metadata::{type_of_val, Metadata};

                        let mut used_types: Vec<String> = Vec::new();
                        $(used_types.extend(<$ty as FromHandler>::metadata_used_types());)*
                        Metadata::record_from_fn::<Out>(
                            &mut handler,
                            &type_of_val(&f),
                            &used_types,
                        );
                        return Ok(ready_inner.call(handler).await?);
                    }

                    #[cfg(not(feature = "test_tower_metadata"))]
                    {
                        $(let $ty = $ty::from_handler(&mut handler)?;)*
                        let res = f($($ty,)*).await?;
                        $(<$ty>::post_extract(&mut handler);)*
                        handler.set(res);
                        Ok(ready_inner.call(handler).await?)
                    }
                })
            }
        }
    };
}

#[rustfmt::skip]
macro_rules! all_the_tuples {
    ($name:ident) => {
        $name!([]);
        $name!([T1]);
        $name!([T1, T2]);
        $name!([T1, T2, T3]);
        $name!([T1, T2, T3, T4]);
        $name!([T1, T2, T3, T4, T5]);
        $name!([T1, T2, T3, T4, T5, T6]);
        $name!([T1, T2, T3, T4, T5, T6, T7]);
        $name!([T1, T2, T3, T4, T5, T6, T7, T8]);
        $name!([T1, T2, T3, T4, T5, T6, T7, T8, T9]);
    };
}

all_the_tuples!(impl_service);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctx_service::RawOneshot;
    use crate::default_services::{ServiceEntry, ServiceReturn};
    use crate::extractors::{MutRef, Ref};
    use std::ops::DerefMut;

    #[cfg(feature = "test_tower_metadata")]
    use crate::metadata::{Metadata, type_of_val};

    async fn add_one(x: Ref<i32>) -> Result<i32, TdError> {
        let x = x.as_ref();
        Ok(*x + 1)
    }

    async fn add_one_mut(mut x: MutRef<i32>) -> Result<(), TdError> {
        let x = x.deref_mut();
        *x += 1;
        Ok(())
    }

    #[tokio::test]
    async fn test_from_fn() {
        let service = ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(from_fn(add_one))
            .layer(from_fn(add_one))
            .service(ServiceReturn);

        let response: i32 = service.raw_oneshot(3).await.unwrap();
        assert_eq!(response, 5);

        let service = ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(from_fn(add_one_mut))
            .layer(from_fn(add_one_mut))
            .service(ServiceReturn);

        let response: i32 = service.raw_oneshot(3).await.unwrap();
        assert_eq!(response, 5);
    }

    #[tokio::test]
    async fn test_from_fn_layer_clone() {
        let layer = from_fn(add_one);
        let service = ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(layer.clone())
            .layer(layer.clone())
            .service(ServiceReturn);

        let response: i32 = service.raw_oneshot(3).await.unwrap();
        assert_eq!(response, 5);
    }

    #[tokio::test]
    async fn test_service_clone() {
        let layer = from_fn(add_one);
        let service = ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(layer.clone())
            .layer(layer.clone())
            .service(ServiceReturn);

        let service_clone = service.clone();

        let response: i32 = service.raw_oneshot(3).await.unwrap();
        assert_eq!(response, 5);

        let response_clone: i32 = service_clone.raw_oneshot(3).await.unwrap();
        assert_eq!(response_clone, 5);
    }

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_fn_names() {
        async fn test_layer_fn() -> Result<(), TdError> {
            // Note that this function is not called, as we are only interested in the metadata
            panic!("This should not be called");
        }

        let service = ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(from_fn(add_one))
            .layer(from_fn(test_layer_fn))
            .service(ServiceReturn);

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        response.assert_service::<i32, i32>(&[type_of_val(&add_one), type_of_val(&test_layer_fn)]);
    }
}
