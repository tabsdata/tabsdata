//
//  Copyright 2024 Tabs Data Inc.
//

//! This module contains default Services reusable in different contexts. Usually, services will
//! be created by composing these services with other layer.

use crate::ctx_service::CtxResponse;
use crate::error::{ConnectionError, FromHandlerError};
use crate::extractors::{Connection, ConnectionType, Input, ReqCtx, SrvCtx};
use crate::handler::{Handler, IntoHandler};
use std::any::type_name;
use std::fmt::Display;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use td_database::sql::DbPool;
use td_error::TdError;
use tower::{Layer, Service};
use tracing::{error, trace};

/// Utility trait to ensure that the type is Send + Sync + 'static
pub trait Share: Send + Sync + 'static {}
impl<T: Send + Sync + 'static> Share for T {}

/// ServiceEntry is a layer wrapping InitService.
pub struct ServiceEntry<Res> {
    phantom: PhantomData<Res>,
}

impl<Res> Default for ServiceEntry<Res> {
    fn default() -> Self {
        ServiceEntry {
            phantom: PhantomData,
        }
    }
}

impl<Res> Clone for ServiceEntry<Res> {
    fn clone(&self) -> Self {
        ServiceEntry {
            phantom: PhantomData,
        }
    }
}

impl<S, Res> Layer<S> for ServiceEntry<Res> {
    type Service = InitService<S, Res>;

    fn layer(&self, service: S) -> Self::Service {
        InitService {
            inner: service,
            phantom: PhantomData,
        }
    }
}

/// InitService will initialize the service with the handler. It will also extract the required type on the way out,
/// conditioning the Response type of the whole service.
///
/// With `test_tower_metadata` feature enabled, add the Metadata struct to the handler too.
pub struct InitService<S, Res> {
    inner: S,
    phantom: PhantomData<Res>,
}

impl<S, Res> Clone for InitService<S, Res>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        InitService {
            inner: self.inner.clone(),
            phantom: PhantomData,
        }
    }
}

impl<S, Req, Res, Err> Service<Req> for InitService<S, Res>
where
    S: Service<Handler, Response = Handler, Error = Err> + Clone + Send + 'static,
    S::Future: Send + 'static,
    Req: Send + Sync + 'static,
    Res: Send + Sync + 'static,
    Err: From<FromHandlerError> + Display,
{
    type Response = CtxResponse<Res>;
    type Error = Err;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            // Create handler with initial request
            let mut handler = Handler::new();
            // For convenience, we insert the () into the handler (unit type always present).
            handler.insert(Input(Arc::new(())));
            handler.insert(Input(Arc::new(req)));

            // Also insert context
            handler.insert(ReqCtx::default());

            #[cfg(feature = "test_tower_metadata")]
            {
                use crate::metadata::{MetadataMutex, type_of};

                // Add metadata to handler
                let res_type_name = type_of::<ReqCtx>();
                let metadata = MetadataMutex::with_initial_types(&[res_type_name]);
                handler.insert(Input::new(metadata));
            }

            // And send it to the next service, awaiting the completion
            let mut handler = match inner.call(handler).await {
                Ok(handler) => {
                    trace!("Service completed successfully");
                    Ok(handler)
                }
                Err(e) => {
                    error!("{e}");
                    Err(e)
                }
            }?;

            // Extract the response type from the handler (we don't check for context, just other
            // input types layer might have created)
            let res = handler
                .remove::<Input<Res>>()
                .ok_or(FromHandlerError::NotFound(String::from(type_name::<Res>())))?;
            let res = Arc::try_unwrap(res.0)
                .map_err(|_| FromHandlerError::InternalError(String::from(type_name::<Res>())))?;
            // Also get ctx
            let ctx = handler
                .remove::<ReqCtx>()
                .ok_or(FromHandlerError::NotFound(String::from(
                    type_name::<ReqCtx>(),
                )))?;
            let ctx = ctx
                .arc()
                .lock()
                .await
                .take()
                .ok_or(FromHandlerError::NotFound(String::from(
                    type_name::<ReqCtx>(),
                )))?;
            Ok(CtxResponse::new(res, ctx))
        })
    }
}

/// ContextProvider is a layer wrapping ContextProviderService. It is required that the type is
/// inserted within an Arc.
pub struct SrvCtxProvider<T> {
    context: Arc<T>,
}

impl<T> Clone for SrvCtxProvider<T> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
        }
    }
}

impl<T> SrvCtxProvider<T> {
    pub fn new(context: Arc<T>) -> SrvCtxProvider<T> {
        SrvCtxProvider { context }
    }
}

impl<S, T> Layer<S> for SrvCtxProvider<T> {
    type Service = SrvCtxProviderService<S, T>;

    fn layer(&self, service: S) -> Self::Service {
        SrvCtxProviderService {
            inner: service,
            context: self.context.clone(),
        }
    }
}

/// ContextProviderService is a service that will insert the context into the handler. The inserted
/// context can be extracted by any service in the chain using the appropriate extractor.
pub struct SrvCtxProviderService<S, T> {
    inner: S,
    context: Arc<T>,
}

impl<S: Clone, T> Clone for SrvCtxProviderService<S, T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            context: self.context.clone(),
        }
    }
}

impl<S, T> Service<Handler> for SrvCtxProviderService<S, T>
where
    S: Service<Handler> + Send + Clone + 'static,
    S::Future: Send,
    S::Response: Send,
    T: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut handler: Handler) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let context = self.context.clone();

        Box::pin(async move {
            #[cfg(feature = "test_tower_metadata")]
            {
                use crate::metadata::{MetadataMutex, type_of};

                // Add types to metadata
                let Input(metadata) = MetadataMutex::from_handler(&handler).unwrap();
                let res_type_name = type_of::<SrvCtx<T>>();
                metadata
                    .created_type("ContextProviderService", res_type_name)
                    .await;
            }

            // Insert the context into the handler
            handler.insert(SrvCtx(context));

            // And send it to the next service
            inner.call(handler).await
        })
    }
}

/// ConnectionProvider is a layer wrapping ConnectionProviderService.
#[derive(Clone)]
pub struct ConnectionProvider {
    db: DbPool,
}

impl ConnectionProvider {
    pub fn new(db: DbPool) -> ConnectionProvider {
        ConnectionProvider { db }
    }
}

impl<S> Layer<S> for ConnectionProvider {
    type Service = ConnectionProviderService<S>;

    fn layer(&self, service: S) -> Self::Service {
        ConnectionProviderService {
            inner: service,
            db: self.db.clone(),
        }
    }
}

/// ConnectionProviderService is a service that will create and insert a pool connection into the handler.
#[derive(Clone)]
pub struct ConnectionProviderService<S> {
    inner: S,
    db: DbPool,
}

impl<S, Err> Service<Handler> for ConnectionProviderService<S>
where
    S: Service<Handler, Response = Handler, Error = Err> + Clone + Send + 'static,
    S::Future: Send,
    S::Response: Send,
    Err: From<TdError>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut handler: Handler) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let db = self.db.clone(); // this is not cloning the pool, just its arc
        Box::pin(async move {
            // Create connection
            let connection = db
                .acquire()
                .await
                .map_err(ConnectionError::CannotGetConnection)
                .map_err(TdError::new)?;
            let connection = ConnectionType::PoolConnection(connection).into();
            let connection = Connection::new(connection);

            #[cfg(feature = "test_tower_metadata")]
            {
                use crate::metadata::{MetadataMutex, type_of_val};

                // Add types to metadata
                let Input(metadata) = MetadataMutex::from_handler(&handler).unwrap();
                let res_type_name = type_of_val(&connection);
                metadata
                    .created_type("ConnectionProviderService", res_type_name)
                    .await;
            }

            // Add it to the handler
            handler.insert(connection);

            // And send it to the next service
            inner.call(handler).await
        })
    }
}

/// TransactionProvider is a layer wrapping TransactionProviderService.
#[derive(Clone)]
pub struct TransactionProvider {
    db: DbPool,
}

impl TransactionProvider {
    pub fn new(db: DbPool) -> TransactionProvider {
        TransactionProvider { db }
    }
}

impl<S> Layer<S> for TransactionProvider {
    type Service = TransactionProviderService<S>;

    fn layer(&self, service: S) -> Self::Service {
        TransactionProviderService {
            inner: service,
            db: self.db.clone(),
        }
    }
}

/// TransactionProviderService is a service that will create and insert a transaction into the handler.
/// It will also handle the commit or rollback based on the result of the services chain.
#[derive(Clone)]
pub struct TransactionProviderService<S> {
    inner: S,
    db: DbPool,
}

impl<S> Service<Handler> for TransactionProviderService<S>
where
    S: Service<Handler, Response = Handler, Error = TdError> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: From<TdError>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut handler: Handler) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let db = self.db.clone(); // this is not cloning the pool, just its arc
        Box::pin(async move {
            // Create transaction
            let transaction = db
                .begin()
                .await
                .map_err(ConnectionError::CannotBeginTransaction)
                .map_err(TdError::new)?;
            let transaction = ConnectionType::Transaction(transaction).into();
            let connection = Connection::new(transaction);

            #[cfg(feature = "test_tower_metadata")]
            {
                use crate::metadata::{MetadataMutex, type_of_val};

                // Add types to metadata
                let Input(metadata) = MetadataMutex::from_handler(&handler).unwrap();
                let res_type_name = type_of_val(&connection);
                metadata
                    .created_type("TransactionProviderService", res_type_name)
                    .await;
            }

            // Note this just clones the Arc, not the transaction itself
            handler.insert(connection.clone());

            // Send it to the next service
            let result = inner.call(handler).await;

            #[cfg(feature = "test_tower_metadata")]
            {
                use crate::metadata::{MetadataMutex, type_of_val};

                // Add types to metadata
                let handler = result.as_ref().unwrap();
                let Input(metadata) = MetadataMutex::from_handler(handler).unwrap();
                let res_type_name = type_of_val(&connection);
                metadata
                    .remove_created("TransactionProviderService", res_type_name)
                    .await;
            }

            // Regain the transaction
            let transaction = connection
                .arc()
                .lock()
                .await
                .take()
                .ok_or(ConnectionError::ConnectionLost)?;
            let transaction = if let ConnectionType::Transaction(transaction) = transaction {
                transaction
            } else {
                return Err(ConnectionError::ConnectionLost)?;
            };

            // Commit or rollback based on the result
            match result {
                Ok(response) => {
                    if let Err(e) = transaction.commit().await {
                        return Err(ConnectionError::CannotCommitTransaction(e))?;
                    }
                    Ok(response)
                }
                Err(e) => {
                    if let Err(e) = transaction.rollback().await {
                        error!("Rollback error: {:?}", e);
                    }
                    Err(e)
                }
            }
        })
    }
}

/// ServiceReturn is a service that will return the handler as is. It is useful for the last service in the chain.
#[derive(Default, Clone)]
pub struct ServiceReturn;

impl Service<Handler> for ServiceReturn {
    type Response = Handler;
    // This error is not really used as this service cannot fail, but given that all the tower
    // must implement From<FromHandlerError>, we can just use it here as well.
    type Error = FromHandlerError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, handler: Handler) -> Self::Future {
        Box::pin(async move { Ok(handler) })
    }
}

/// Conditional is a layer wrapping any Handler service with another Service that MUST
/// return ['Condition'] at some point. It will be used to conditionally execute the service.
/// Note that the handler used by this service is passed by the main service. Which means that
/// items inside are shared.
///
/// # Type Parameters
///
/// * `C` - The condition service. It must return Condition(bool) at some point.
/// * `DS` - The conditionally executed service.
/// * `ES` - The else conditionally executed service.
///
/// Example:
/// ```rust
/// use tower::ServiceBuilder;
/// use td_tower::default_services::{conditional, Condition, Do, Else, If, ServiceReturn};
/// use td_tower::error::FromHandlerError;
/// use td_tower::from_fn::from_fn;
///
/// async fn condition() -> Result<Condition, FromHandlerError> {
///    Ok(Condition(true))
/// }
///
/// async fn test() -> Result<(), FromHandlerError> {
///   Ok(())
/// }
///
/// conditional(
///     If(ServiceBuilder::new()
///         .layer(from_fn(condition))
///         .service(ServiceReturn)),
///     Do(ServiceBuilder::new()
///         .layer(from_fn(test))
///         .layer(from_fn(test))
///         .layer(from_fn(test))
///         .service(ServiceReturn)),
///     Else(ServiceReturn),
/// )
pub fn conditional<C, DS, ES>(
    If(condition): If<C>,
    Do(do_service): Do<DS>,
    Else(else_service): Else<ES>,
) -> Conditional<C, DS, ES> {
    Conditional {
        condition: If(condition),
        do_service: Do(do_service),
        else_service: Else(else_service),
    }
}

// These are just so it looks cleaner.
#[derive(Clone)]
pub struct If<C>(pub C);

#[derive(Clone)]
pub struct Do<DS>(pub DS);

#[derive(Clone)]
pub struct Else<ES>(pub ES);

/// Condition is a simple struct to hold a boolean condition. It is used to conditionally execute a service.
pub struct Condition(pub bool);

/// Conditional is a layer wrapping ConditionalService.
pub struct Conditional<C, DS, ES> {
    condition: If<C>,
    do_service: Do<DS>,
    else_service: Else<ES>,
}

impl<C, DS, ES> Clone for Conditional<C, DS, ES>
where
    C: Clone,
    DS: Clone,
    ES: Clone,
{
    fn clone(&self) -> Self {
        Conditional {
            condition: self.condition.clone(),
            do_service: self.do_service.clone(),
            else_service: self.else_service.clone(),
        }
    }
}

impl<C, DS, ES, I> Layer<I> for Conditional<C, DS, ES>
where
    C: Clone,
    DS: Clone,
    ES: Clone,
    I: Clone,
{
    type Service = ConditionalService<C, DS, ES, I>;

    fn layer(&self, inner: I) -> Self::Service {
        ConditionalService {
            condition: self.condition.clone(),
            do_service: self.do_service.clone(),
            else_service: self.else_service.clone(),
            inner,
        }
    }
}

/// ConditionalService is a service that will conditionally execute a service in a service tower.
pub struct ConditionalService<C, DS, ES, I> {
    condition: If<C>,
    do_service: Do<DS>,
    else_service: Else<ES>,
    inner: I,
}

impl<C, DS, ES, I> Clone for ConditionalService<C, DS, ES, I>
where
    C: Clone,
    DS: Clone,
    ES: Clone,
    I: Clone,
{
    fn clone(&self) -> Self {
        ConditionalService {
            condition: self.condition.clone(),
            do_service: self.do_service.clone(),
            else_service: self.else_service.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<C, DS, ES, I> Service<Handler> for ConditionalService<C, DS, ES, I>
where
    C: Service<Handler> + Clone + Send + 'static,
    C::Future: Send,
    C::Response: IntoHandler,
    DS: Service<Handler> + Clone + Send + 'static,
    DS::Future: Send,
    DS::Response: IntoHandler,
    ES: Service<Handler> + Clone + Send + 'static,
    ES::Future: Send,
    ES::Response: IntoHandler,
    I: Service<Handler> + Clone + Send + 'static,
    I::Future: Send,
    C::Error: From<I::Error> + From<DS::Error> + From<ES::Error> + From<FromHandlerError>,
{
    type Response = I::Response;
    type Error = C::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        match (
            self.condition.0.poll_ready(cx),
            self.do_service.0.poll_ready(cx),
            self.else_service.0.poll_ready(cx),
            self.inner.poll_ready(cx),
        ) {
            (
                std::task::Poll::Ready(c_res),
                std::task::Poll::Ready(ds_res),
                std::task::Poll::Ready(es_res),
                std::task::Poll::Ready(i_res),
            ) => std::task::Poll::Ready(c_res.or(ds_res).or(es_res).or(Ok(i_res?))),
            (_, _, _, _) => std::task::Poll::Pending,
        }
    }

    fn call(&mut self, mut handler: Handler) -> Self::Future {
        let clone = self.condition.clone();
        let mut condition = std::mem::replace(&mut self.condition, clone);

        let clone = self.do_service.clone();
        let mut do_service = std::mem::replace(&mut self.do_service, clone);

        let clone = self.else_service.clone();
        let mut else_service = std::mem::replace(&mut self.else_service, clone);

        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            handler = condition.0.call(handler).await?.into_handler();

            #[cfg(not(feature = "test_tower_metadata"))]
            {
                use crate::extractors::FromHandler;

                // Conditional service should always return a Condition
                let condition = Input::<Condition>::from_handler(&handler)?;

                let Input(condition) = condition;
                // Remove it so there are no other false positives
                handler.remove::<Input<Condition>>().unwrap();

                // And execute the service based on the condition
                if condition.0 {
                    handler = do_service.0.call(handler).await?.into_handler();
                } else {
                    handler = else_service.0.call(handler).await?.into_handler();
                }
            }

            #[cfg(feature = "test_tower_metadata")]
            {
                use crate::metadata::{MetadataMutex, type_of};

                // This services uses the Condition
                let Input(metadata) = MetadataMutex::from_handler(&handler)?;
                metadata
                    .used_type("ConditionalService", type_of::<Input<Condition>>())
                    .await;

                // Metadata tests get info from both services
                handler = do_service.0.call(handler).await?.into_handler();
                handler = else_service.0.call(handler).await?.into_handler();
            }

            match inner.call(handler).await {
                Ok(response) => Ok(response),
                Err(e) => Err(e.into()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctx_service::RawOneshot;
    use crate::extractors;
    use crate::extractors::{Input, IntoMutSqlConnection};
    use crate::from_fn::from_fn;
    use sqlx::Connection;
    use std::ops::Deref;
    use std::sync::Arc;
    use td_error::TdError;
    use tower::{ServiceBuilder, ServiceExt};

    #[tokio::test]
    async fn test_init_service() {
        let init_service = InitService {
            inner: ServiceReturn,
            phantom: PhantomData::<i32>,
        };
        let res = init_service.raw_oneshot(1).await;
        assert!(matches!(res, Ok(1)));
    }

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_init_service() {
        use crate::metadata::MetadataMutex;

        let init_service = InitService {
            inner: ServiceReturn,
            phantom: PhantomData::<MetadataMutex>,
        };

        let res = init_service.raw_oneshot(()).await;
        assert!(res.is_ok());
        let metadata = res.unwrap().get();
        metadata.assert_service::<(), ()>(&[]);
    }

    #[tokio::test]
    async fn test_context_provider_layer() {
        let service = ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(SrvCtxProvider::new(Arc::new(String::from("data"))))
            .layer(from_fn(|SrvCtx(c): SrvCtx<String>| async move {
                // Clones context to output
                let s = c.deref().clone();
                Ok::<_, FromHandlerError>(s)
            }))
            .service(ServiceReturn);

        let res: String = service.raw_oneshot(()).await.unwrap();
        assert_eq!(res, "data");
    }

    #[tokio::test]
    async fn test_connection_provider_layer() {
        let db = td_database::test_utils::db().await.unwrap();
        let service = ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(ConnectionProvider::new(db))
            .layer(from_fn(
                |Connection(c): extractors::Connection| async move {
                    let mut conn = c.lock().await;

                    let conn = conn.get_mut_connection()?;
                    let is_ok = conn.ping().await.is_ok();

                    assert!(is_ok);
                    Ok::<_, TdError>(is_ok)
                },
            ))
            .service(ServiceReturn);

        let res: bool = service.raw_oneshot(()).await.unwrap();
        assert!(res);
    }

    #[tokio::test]
    async fn test_transaction_provider_layer() {
        let db = td_database::test_utils::db().await.unwrap();
        let service = ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(TransactionProvider::new(db))
            .layer(from_fn(
                |Connection(c): extractors::Connection| async move {
                    let mut conn = c.lock().await;

                    let conn = conn.get_mut_connection()?;
                    let is_ok = conn.ping().await.is_ok();

                    assert!(is_ok);
                    Ok::<_, TdError>(is_ok)
                },
            ))
            .service(ServiceReturn);

        let res: bool = service.raw_oneshot(()).await.unwrap();
        assert!(res);
    }

    #[tokio::test]
    async fn test_service_return() {
        let mut handler = Handler::new();
        handler.insert(Input::new(1));

        let mut res: Handler = ServiceReturn.oneshot(handler).await.unwrap();
        let res = res.remove::<Input<i32>>().unwrap();
        let res = *res.0.deref();
        assert_eq!(res, 1);
    }

    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_service_conditional() {
        use crate::metadata::{MetadataMutex, type_of_val};

        async fn if_fn() -> Result<Condition, FromHandlerError> {
            Ok(Condition(true))
        }

        async fn do_fn() -> Result<(), FromHandlerError> {
            Ok(())
        }

        async fn else_fn() -> Result<(), FromHandlerError> {
            Ok(())
        }

        let service = ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(conditional(
                If(ServiceBuilder::new()
                    .layer(from_fn(if_fn))
                    .service(ServiceReturn)),
                Do(ServiceBuilder::new()
                    .layer(from_fn(do_fn))
                    .service(ServiceReturn)),
                Else(
                    ServiceBuilder::new()
                        .layer(from_fn(else_fn))
                        .service(ServiceReturn),
                ),
            ))
            .service(ServiceReturn);

        let res: Result<MetadataMutex, FromHandlerError> = service.raw_oneshot(()).await;
        let res = res.unwrap();
        let metadata = res.get();
        metadata.assert_service::<(), ()>(&[
            type_of_val(&if_fn),
            type_of_val(&do_fn),
            type_of_val(&else_fn),
        ]);
    }

    #[tokio::test]
    async fn test_service_conditional() {
        let service = ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(conditional(
                If(ServiceBuilder::new()
                    .layer(from_fn(|condition: Input<bool>| async move {
                        Ok::<_, FromHandlerError>(Condition(*condition.0))
                    }))
                    .service(ServiceReturn)),
                Do(ServiceBuilder::new()
                    .layer(from_fn(|| async { Ok("true".to_string()) }))
                    .service(ServiceReturn)),
                Else(
                    ServiceBuilder::new()
                        .layer(from_fn(|| async { Ok("false".to_string()) }))
                        .service(ServiceReturn),
                ),
            ))
            .service(ServiceReturn);

        let res: Result<String, FromHandlerError> = service.clone().raw_oneshot(true).await;
        let res = res.unwrap();
        assert_eq!(res, "true");

        let res: Result<String, FromHandlerError> = service.clone().raw_oneshot(false).await;
        let res = res.unwrap();
        assert_eq!(res, "false");
    }
}
