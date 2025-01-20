//
//  Copyright 2024 Tabs Data Inc.
//

use futures_util::future::BoxFuture;
use std::task::{Context, Poll};
use tower::ServiceExt;
use tower_service::Service;

/// As its name says, a Box Service which is Sync and Clone (and Send).
/// Very similar to [`tower::util::boxed_clone::BoxCloneService`], but Sync.
/// It makes the service type opaque, for easy use in multiple generic contexts.
pub struct BoxSyncCloneService<T, U, E>(
    Box<
        dyn CloneService<T, Response = U, Error = E, Future = BoxFuture<'static, Result<U, E>>>
            + Send
            + Sync,
    >,
);

impl<T, U, E> BoxSyncCloneService<T, U, E> {
    pub fn new<S>(inner: S) -> Self
    where
        S: Service<T, Response = U, Error = E> + Clone + Send + Sync + 'static,
        S::Future: Send + 'static,
    {
        let inner = inner.map_future(|f| Box::pin(f) as _);
        BoxSyncCloneService(Box::new(inner))
    }
}

impl<T, U, E> Service<T> for BoxSyncCloneService<T, U, E> {
    type Response = U;
    type Error = E;
    type Future = BoxFuture<'static, Result<U, E>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        self.0.poll_ready(cx)
    }

    #[inline]
    fn call(&mut self, request: T) -> Self::Future {
        self.0.call(request)
    }
}

impl<T, U, E> Clone for BoxSyncCloneService<T, U, E> {
    fn clone(&self) -> Self {
        Self(self.0.clone_box())
    }
}

/// A trait that makes a service cloneable.
/// Very similar to [`tower::util::boxed_clone::CloneService`], but Sync.
trait CloneService<R>: Service<R> {
    fn clone_box(
        &self,
    ) -> Box<
        dyn CloneService<R, Response = Self::Response, Error = Self::Error, Future = Self::Future>
            + Send
            + Sync,
    >;
}

impl<R, T> CloneService<R> for T
where
    T: Service<R> + Send + Sync + Clone + 'static,
{
    fn clone_box(
        &self,
    ) -> Box<
        dyn CloneService<R, Response = T::Response, Error = T::Error, Future = T::Future>
            + Send
            + Sync,
    > {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::future;
    use std::future::Ready;

    #[derive(Clone)]
    struct TestService;

    impl Service<()> for TestService {
        type Response = &'static str;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: ()) -> Self::Future {
            future::ready(Ok("test response"))
        }
    }

    #[tokio::test]
    async fn test_box_sync_clone_service() {
        let service = TestService;
        let boxed_service = BoxSyncCloneService::new(service);
        let mut cloned_service = boxed_service.clone();
        let response = cloned_service.call(()).await.unwrap();
        assert_eq!(response, "test response");
    }
}
