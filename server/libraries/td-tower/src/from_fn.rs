//
//  Copyright 2024 Tabs Data Inc.
//

//! A generic layer to wrap a function as a `tower::Service`. Similar to
//! [`tower::service_fn`] or [`axum::middleware::from_fn`], but more reusable.
//!
//! Useful for creating services from async functions that operate on extracted inputs.

use crate::error::FromHandlerError;
use crate::extractors::{FromHandler, Input};
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
#[derive(Clone)]
pub struct FromFnLayer<F, T> {
    f: F,
    _extractor: PhantomData<T>,
}

impl<I, F, T> Layer<I> for FromFnLayer<F, T>
where
    F: Clone,
    I: Service<Handler, Error = TdError> + Clone + Send + Sync + 'static,
    I::Response: IntoHandler,
    I::Future: Send + 'static,
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
#[derive(Clone)]
pub struct FromFn<F, T> {
    f: F,
    inner: BoxCloneSyncService<Handler, Handler, TdError>,
    _extractor: PhantomData<T>,
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
                    #[cfg(not(feature = "test_tower_metadata"))]
                    {
                        $(let $ty = $ty::from_handler(&handler)?;)*
                        let res = f($($ty,)*).await?;
                        handler.insert(Input(std::sync::Arc::new(res)));
                    }

                    #[cfg(feature = "test_tower_metadata")]
                    {
                        use $crate::metadata::{type_of, type_of_val, MetadataMutex};

                        let Input(metadata) = MetadataMutex::from_handler(&handler)?;
                        let fn_name = type_of_val(&f);
                        metadata.add_fn_name(fn_name.clone()).await;

                        $(metadata.used_type(fn_name.clone(), type_of::<$ty>()).await;)*
                        metadata.created_type(fn_name.clone(), type_of::<Input<Out>>()).await;
                    }

                    Ok(ready_inner.call(handler).await?)
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
    };
}

all_the_tuples!(impl_service);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctx_service::RawOneshot;
    use crate::default_services::{ServiceEntry, ServiceReturn};
    use crate::extractors::Input;

    #[cfg(feature = "test_tower_metadata")]
    use crate::metadata::{MetadataMutex, type_of_val};

    async fn add_one(Input(x): Input<i32>) -> Result<i32, TdError> {
        Ok(*x + 1)
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

        let response: MetadataMutex = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<i32, i32>(&[type_of_val(&add_one), type_of_val(&test_layer_fn)]);
    }
}
