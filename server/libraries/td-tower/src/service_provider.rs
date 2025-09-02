//
// Copyright 2025 Tabs Data Inc.
//

use crate::ctx_service::CtxResponse;
use std::convert::Infallible;
use tower::util::{BoxCloneSyncService, BoxService};
use tower::{MakeService, ServiceExt, service_fn};
use tower_service::Service;

/// A boxed service type that can be used in multiple generic contexts. It will always
/// wrap the return type in a [`CtxResponse`].
pub type TdBoxService<T, U, E> = BoxService<T, CtxResponse<U>, E>;

/// [`ServiceProvider`] is a wrapper around a [`BoxService`] that allows for
/// creating new instances of the service. It makes the inner service types' opaque,
/// for easy use in multiple generic contexts. It has always a wrapped return type in a
/// [`CtxResponse`].
pub struct ServiceProvider<Req, Res, Err>(
    BoxCloneSyncService<(), TdBoxService<Req, Res, Err>, Infallible>,
);

impl<Req, Res, Err> ServiceProvider<Req, Res, Err> {
    pub fn new<S>(inner: S) -> Self
    where
        S: Service<Req, Response = CtxResponse<Res>, Error = Err> + Clone + Send + Sync + 'static,
        S::Future: Send + 'static,
    {
        let inner = BoxCloneSyncService::new(service_fn(move |_: ()| {
            let inner = inner.clone();
            async move { Ok::<_, Infallible>(inner.boxed()) }
        }));
        ServiceProvider(inner)
    }

    /// Creates a new instance of the boxed service.
    pub async fn make(&self) -> TdBoxService<Req, Res, Err> {
        self.0.clone().make_service(()).await.unwrap()
    }
}

/// A trait for converting a service into a `ServiceProvider`.
pub trait IntoServiceProvider<Req, Res, Err> {
    fn into_service_provider(self) -> ServiceProvider<Req, Res, Err>;
}

impl<S, Req, Res, Err> IntoServiceProvider<Req, Res, Err> for S
where
    S: Service<Req, Response = CtxResponse<Res>, Error = Err> + Clone + Send + Sync + 'static,
    S::Future: Send + 'static,
{
    fn into_service_provider(self) -> ServiceProvider<Req, Res, Err> {
        ServiceProvider::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctx_service::CtxMap;
    use futures_util::future::{self, Ready};
    use std::convert::Infallible;
    use tower_service::Service;

    #[derive(Clone)]
    struct TestService;

    impl Service<()> for TestService {
        type Response = CtxResponse<&'static str>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: ()) -> Self::Future {
            future::ready(Ok(CtxResponse::new("test response", CtxMap::default())))
        }
    }

    #[tokio::test]
    async fn test_service_provider_new() {
        let service = TestService;
        let provider = ServiceProvider::new(service);
        let boxed_service = provider.make().await;
        let response = boxed_service.oneshot(()).await.unwrap();
        assert_eq!(*response, "test response");
    }

    #[tokio::test]
    async fn test_into_service_provider() {
        let service = TestService;
        let provider: ServiceProvider<_, _, _> = service.into_service_provider();
        let boxed_service = provider.make().await;
        let response = boxed_service.oneshot(()).await.unwrap();
        assert_eq!(*response, "test response");
    }
}
