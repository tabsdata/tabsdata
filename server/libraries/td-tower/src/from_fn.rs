//
//  Copyright 2024 Tabs Data Inc.
//

//! A layer that wraps a function and creates a service. Very similar to [`tower::service_fn`] or
//! [`axum::middleware::from_fn`], but in a more generic form. Useful for creating services
//! in a reusable way.

use crate::error::FromHandlerError;
use crate::extractors::{FromHandler, Input};
use crate::handler::{Handler, IntoHandler};
use futures::future::BoxFuture;
use std::fmt::{Debug, Formatter};
use std::{any::type_name, fmt, future::Future, marker::PhantomData, pin::Pin, task};
use tower::{Service, ServiceBuilder, util::BoxCloneService};
use tower_layer::Layer;

/// Creates a new `FromFnLayer` with the given function.
///
/// # Arguments
///
/// * `f` - A function that takes an input and returns a future.
pub fn from_fn<F, T>(f: F) -> FromFnLayer<F, T> {
    FromFnLayer {
        f,
        _extractor: PhantomData,
    }
}

/// A layer that wraps a function and creates a service.
///
/// # Type Parameters
///
/// * `F` - The function type.
/// * `T` - The type of the function's output.
#[must_use]
pub struct FromFnLayer<F, T> {
    f: F,
    _extractor: PhantomData<fn() -> T>,
}

impl<F, T> Clone for FromFnLayer<F, T>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            _extractor: self._extractor,
        }
    }
}

impl<I, F, T> Layer<I> for FromFnLayer<F, T>
where
    F: Clone,
{
    type Service = FromFn<F, I, T>;

    fn layer(&self, inner: I) -> Self::Service {
        FromFn {
            f: self.f.clone(),
            inner,
            _extractor: PhantomData,
        }
    }
}

impl<F, T> Debug for FromFnLayer<F, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FromFnLayer")
            // Write out the type name, without quoting it as `&type_name::<F>()` would
            .field("f", &format_args!("{}", type_name::<F>()))
            .finish()
    }
}

/// A service created from a function and an inner service.
///
/// # Type Parameters
///
/// * `F` - The function type.
/// * `I` - The inner service type.
/// * `T` - The type of the function's output.
pub struct FromFn<F, I, T> {
    f: F,
    inner: I,
    _extractor: PhantomData<fn() -> T>,
}

impl<F, I, T> Clone for FromFn<F, I, T>
where
    F: Clone,
    I: Clone,
{
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            inner: self.inner.clone(),
            _extractor: self._extractor,
        }
    }
}

impl<F, I, T> Debug for FromFn<F, I, T>
where
    I: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FromFnLayer")
            .field("f", &format_args!("{}", type_name::<F>()))
            .field("inner", &self.inner)
            .finish()
    }
}

/// This is the general impl of Service for all FromFn types.
/// The framework will extract and inject values of the given types from the handler, so they
/// can be used by the data conforming the tower.
/// When `test_tower_metadata` feature is enabled, the function name is added to the metadata,
/// and the function conforming the service is not executed. Therefore, not using the handler values.
macro_rules! impl_service {
    (
        [$($ty:ident),*]
    ) => {
        #[allow(non_snake_case, unused_mut, unused_parens, unused_variables)]
        impl<F, Fut, Out, Err, I, $($ty,)*> Service<Handler> for FromFn<F, I, ($($ty,)*)>
        where
            F: FnMut($($ty,)*) -> Fut + Clone + Send + 'static,
            $( $ty: FromHandler + Send, )*
            Fut: Future<Output = Result<Out, Err>> + Send + 'static,
            Out: Send + Sync + 'static,
            I: Service<Handler> + Clone + Send + 'static,
            I::Response: IntoHandler,
            I::Future: Send + 'static,
            Err: From<I::Error> + From<FromHandlerError>
        {
            type Response = Handler;
            type Error = Err;
            type Future = ResponseFuture<Err>;

            fn poll_ready(&mut self, cx: &mut task::Context<'_>) -> task::Poll<Result<(), Self::Error>> {
                match self.inner.poll_ready(cx) {
                    task::Poll::Ready(Ok(_)) => task::Poll::Ready(Ok(())),
                    task::Poll::Ready(Err(e)) => task::Poll::Ready(Err(e.into())),
                    task::Poll::Pending => task::Poll::Pending,
                }
            }

            fn call(&mut self, mut handler: Handler) -> Self::Future {
                let not_ready_inner = self.inner.clone();
                let ready_inner = std::mem::replace(&mut self.inner, not_ready_inner);

                let mut f = self.f.clone();

                let future = Box::pin(async move {
                    #[cfg(not(feature = "test_tower_metadata"))]
                    {
                        // Extract values from handler
                        $(
                            let $ty = $ty::from_handler(&handler)?;
                        )*

                        // Execute function and add result to handler
                        let res = f($($ty,)*).await?;
                        let res = Input(std::sync::Arc::new(res));
                        handler.insert(res);
                    }

                    #[cfg(feature = "test_tower_metadata")]
                    {
                        use $crate::metadata::{type_of, type_of_val, MetadataMutex};

                        // Add fn metadata, and skip actual execution
                        let Input(metadata) = MetadataMutex::from_handler(&handler)?;
                        let fn_name = type_of_val(&f);
                        metadata.add_fn_name(fn_name.clone()).await;

                        // Add types to metadata
                        $(
                            let arg_type_name = type_of::<$ty>();
                            metadata.used_type(fn_name.clone(), arg_type_name).await;
                        )*

                        let res_type_name = type_of::<Input<Out>>();
                        metadata.created_type(fn_name.clone(), res_type_name).await;
                    }

                    let inner = ServiceBuilder::new()
                        .boxed_clone()
                        .map_response(IntoHandler::into_handler)
                        .service(ready_inner);
                    let next = Next { inner };

                    handler = next.run(handler).await?;
                    Ok(handler)
                });

                ResponseFuture::<Err> {
                    inner: future
                }
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
        $name!([T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]);
    };
}

all_the_tuples!(impl_service);

/// The remainder of a tower stack, including the handler.
#[derive(Debug, Clone)]
pub struct Next<Err> {
    inner: BoxCloneService<Handler, Handler, Err>,
}

impl<Err> Next<Err> {
    /// Execute the remaining tower stack.
    pub async fn run(mut self, handler: Handler) -> Result<Handler, Err> {
        self.inner.call(handler).await
    }
}

impl<Err> Service<Handler> for Next<Err> {
    type Response = Handler;
    type Error = Err;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut task::Context<'_>) -> task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, handler: Handler) -> Self::Future {
        self.inner.call(handler)
    }
}

/// A future that resolves to a handler.
pub struct ResponseFuture<Err> {
    inner: BoxFuture<'static, Result<Handler, Err>>,
}

impl<Err> Future for ResponseFuture<Err> {
    type Output = Result<Handler, Err>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctx_service::RawOneshot;
    use crate::default_services::{ServiceEntry, ServiceReturn};
    use crate::extractors::Input;
    #[cfg(feature = "test_tower_metadata")]
    use crate::metadata::{MetadataMutex, type_of_val};

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error("Handler test error: {0}")]
        HandlerError(#[from] FromHandlerError),
    }

    async fn add_one(Input(x): Input<i32>) -> Result<i32, TestError> {
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
        async fn test_layer_fn() -> Result<(), FromHandlerError> {
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
