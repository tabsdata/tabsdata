//
// Copyright 2025. Tabs Data Inc.
//

use futures::future::{AbortHandle, Abortable};
use pin_project_lite::pin_project;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::task::{JoinError, JoinSet};
use tower::{ServiceBuilder, ServiceExt};
use tracing::debug;

/// Error type for request errors within the scope of a [`CancelMiddleware`].
/// `Inner` is built explicitly, never via `#[from]`: a generic
/// `impl<E> From<E>` overlaps (per coherence's future-compat rules) with any
/// upstream `impl From<X> for X::Assoc`, so a routine dependency bump could
/// break the build (`time` 0.3.48 did).
#[derive(Debug, Error)]
enum CancellationError<E> {
    #[error("request was cancelled")]
    Cancelled,
    #[error(transparent)]
    Inner(E),
}

/// A middleware that allows cancelling in-flight requests.
#[derive(Clone)]
struct CancelMiddleware<S> {
    inner: S,
}

impl<S> CancelMiddleware<S> {
    fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, Req> tower::Service<Req> for CancelMiddleware<S>
where
    S: tower::Service<Req>,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = CancellationError<S::Error>;
    type Future = CancelFuture<S::Response, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(CancellationError::Inner)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let fut = self.inner.call(req);
        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let abortable = Abortable::new(fut, abort_reg);

        CancelFuture {
            inner: Box::pin(abortable),
            handle: Some(abort_handle),
        }
    }
}

/// A future that can be cancelled. If dropped, it will abort the inner future.
struct CancelFuture<T, E> {
    #[allow(clippy::type_complexity)]
    inner: Pin<Box<dyn Future<Output = Result<Result<T, E>, futures::future::Aborted>> + Send>>,
    handle: Option<AbortHandle>,
}

impl<T, E> Future for CancelFuture<T, E> {
    type Output = Result<T, CancellationError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.inner.as_mut().poll(cx) {
            Poll::Ready(Ok(Ok(val))) => Poll::Ready(Ok(val)),
            Poll::Ready(Ok(Err(err))) => Poll::Ready(Err(CancellationError::Inner(err))),
            Poll::Ready(Err(_)) => Poll::Ready(Err(CancellationError::Cancelled)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T, E> Drop for CancelFuture<T, E> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

pin_project! {
    /// A future that yields the index of the original future along with its output.
    struct IndexedFuture<F> {
        index: usize,
        #[pin]
        inner: F,
    }
}

impl<F> IndexedFuture<F> {
    fn new(index: usize, inner: F) -> Self {
        Self { index, inner }
    }
}

impl<F> Future for IndexedFuture<F>
where
    F: Future,
{
    type Output = (usize, F::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<(usize, F::Output)> {
        let index = self.index;

        match self.project().inner.poll(cx) {
            Poll::Ready(val) => Poll::Ready((index, val)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Error returned by the [`run_tasks`] function.
#[derive(Debug)]
pub enum RunError<E: Send + 'static> {
    /// It contains the index of the first task that failed and the corresponding error.
    TaskError(usize, E),
    /// If one of the tasks panicked.
    Panicked(JoinError),
    /// An internal error occurred.
    InternalError(String),
}

/// Async tasks signature for the [`run_tasks`] function.
pub type AsyncLambda<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

/// Runs a list of async tasks with a maximum concurrency limit.
/// If any of the tasks errors, all other running tasks are cancelled.
///
/// If a tasks panics, it is reported as an error, all other running tasks are cancelled.
///
/// If all tasks are successful, a vector of their results is returned, the order of results
/// matches the order of the input tasks.
///
/// Example usage:
///
/// ```rust,no_run
/// # use std::num::NonZero;
/// # use std::time::{Duration, SystemTime};
/// # use td_shuttle::transporter::taskrunner::{run_tasks, AsyncLambda};
/// #
/// # async fn f() {
///     fn async_fn_gen(sleep: u64) -> AsyncLambda<(), &'static str> {
///         // IMPORTANT: the async block must be Box::pin()ed
///         Box::pin(async move {
///             // some work
///             tokio::time::sleep(Duration::from_millis(sleep)).await;
///             Ok(())
///         })
///     }
///
///     run_tasks(NonZero::new(2).unwrap(), vec![async_fn_gen(100), async_fn_gen(200)]).await.unwrap();
/// # }
/// ```
pub async fn run_tasks<T: Send + 'static, E: Debug + Send + 'static>(
    max_concurrency: NonZeroUsize,
    tasks: Vec<AsyncLambda<T, E>>,
) -> Result<Vec<T>, RunError<E>> {
    debug!(
        "Starting run with concurrency {} and {} tasks",
        max_concurrency,
        tasks.len()
    );

    let service = tower::service_fn(|f: AsyncLambda<T, E>| f);

    let svc = ServiceBuilder::new()
        .concurrency_limit(max_concurrency.into())
        .layer_fn(CancelMiddleware::new)
        .service(service);

    let mut set = JoinSet::new();
    let mut results = Vec::with_capacity(tasks.len());
    results.resize_with(tasks.len(), || None);

    // Launch all tasks
    for (idx, f) in tasks.into_iter().enumerate() {
        let fut = svc.clone().oneshot(f);
        set.spawn(IndexedFuture::new(idx, fut));
    }
    debug!("Spawned all tasks");

    // Process task results as they complete
    while let Some(res) = set.join_next().await {
        match res {
            Ok((idx, Ok(val))) => {
                debug!("tasks[{idx}] ended Ok");
                results[idx] = Some(val);
            }
            Ok((idx, Err(CancellationError::Cancelled))) => {
                debug!("tasks[{idx}] canceled");
                set.abort_all(); // kill the others
            }
            Ok((idx, Err(CancellationError::Inner(err)))) => {
                debug!("tasks[{idx}] error: {:?}", err);
                set.abort_all(); // kill the others
                return Err(RunError::TaskError(idx, err));
            }
            Err(join_err) => {
                debug!("A tasks panicked: {join_err}");
                set.abort_all(); // kill the others
                return Err(RunError::Panicked(join_err));
            }
        }
    }
    let results = results
        .into_iter()
        .enumerate()
        .map(|(idx, val)| {
            val.ok_or_else(|| {
                RunError::InternalError(format!(
                    "Result missing from task[{idx}], this should not happen"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>();
    debug!(
        "Run completed: {:?}",
        results
            .as_ref()
            .map(|_| "Ok(..)")
            .map_err(|err| format!("Err({:?})", err))
    );
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZero;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, SystemTime};

    #[tokio::test]
    async fn test_no_tasks() {
        match run_tasks(
            NonZero::new(1).unwrap(),
            Vec::<AsyncLambda<usize, &str>>::new(),
        )
        .await
        {
            Ok(res) => assert!(res.is_empty()),
            Err(e) => panic!("Unexpected error: {e:?}"),
        }
    }

    #[tokio::test]
    async fn test_one_task_ok() {
        match run_tasks::<i32, &str>(NonZero::new(1).unwrap(), vec![Box::pin(async { Ok(42) })])
            .await
        {
            Ok(res) if res.len() == 1 => assert_eq!(42, res[0]),
            Ok(res) => panic!("Unexpected result length: {}", res.len()),
            Err(e) => panic!("Unexpected error: {e:?}"),
        }
    }

    #[tokio::test]
    async fn test_results_ordering() {
        fn async_fn(msg: &'static str, sleep: u64) -> AsyncLambda<&'static str, &'static str> {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(sleep)).await;
                Ok::<_, &'static str>(msg)
            })
        }

        match run_tasks(
            NonZero::new(2).unwrap(),
            vec![async_fn("a", 100), async_fn("b", 0)],
        )
        .await
        {
            Ok(res) if res.len() == 2 => assert_eq!(vec!["a", "b"], res),
            Ok(res) => panic!("Unexpected result length: {}", res.len()),
            Err(e) => panic!("Unexpected error: {e:?}"),
        }
    }

    #[tokio::test]
    async fn test_concurrency() {
        fn async_fn(sleep: u64) -> AsyncLambda<SystemTime, &'static str> {
            Box::pin(async move {
                let started = SystemTime::now();
                tokio::time::sleep(Duration::from_millis(sleep)).await;
                Ok(started)
            })
        }

        match run_tasks(
            NonZero::new(2).unwrap(),
            vec![async_fn(100), async_fn(100), async_fn(100)],
        )
        .await
        {
            Ok(res) if res.len() == 3 => {
                assert!(res[2].duration_since(res[0]).unwrap().as_millis() >= 100);
                // it should be 100 in theory, allowing scheduling overhead
                assert!(res[2].duration_since(res[1]).unwrap().as_millis() >= 50);
            }
            Ok(res) => panic!("Unexpected result length: {}", res.len()),
            Err(e) => panic!("Unexpected error: {e:?}"),
        }
    }

    #[tokio::test]
    async fn test_task_error() {
        fn async_fn(
            msg: &'static str,
            sleep: u64,
            finished: Arc<AtomicBool>,
        ) -> AsyncLambda<(), &'static str> {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(sleep)).await;
                if msg.starts_with("err") {
                    Err::<(), &'static str>("fn_err")
                } else {
                    finished.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }
            })
        }

        let mut finished = Vec::with_capacity(5);
        finished.resize_with(5, || Arc::new(AtomicBool::new(false)));
        let res = run_tasks(
            NonZero::new(5).unwrap(),
            vec![
                async_fn("ok0", 10, finished[0].clone()),
                async_fn("ok1", 20, finished[1].clone()),
                async_fn("ok2", 200, finished[2].clone()),
                async_fn("err3", 100, finished[3].clone()),
                async_fn("ok4", 200, finished[4].clone()),
            ],
        )
        .await;
        match res {
            Ok(_) => panic!("Expected an error"),
            Err(RunError::TaskError(3, "fn_err")) => (),
            Err(e) => panic!("Unexpected error: {e:?}"),
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let expected = vec![true, true, false, false, false];
        let got = finished
            .into_iter()
            .map(|finished| finished.load(std::sync::atomic::Ordering::SeqCst))
            .collect::<Vec<_>>();
        assert_eq!(expected, got);
    }

    #[tokio::test]
    async fn test_task_panic() {
        fn async_fn(
            msg: &'static str,
            sleep: u64,
            finished: Arc<AtomicBool>,
        ) -> AsyncLambda<(), &'static str> {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(sleep)).await;
                if msg.starts_with("panic") {
                    panic!("fn_panic");
                } else {
                    finished.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }
            })
        }

        let mut finished = Vec::with_capacity(5);
        finished.resize_with(5, || Arc::new(AtomicBool::new(false)));
        let res = run_tasks(
            NonZero::new(5).unwrap(),
            vec![
                async_fn("ok0", 10, finished[0].clone()),
                async_fn("ok1", 20, finished[1].clone()),
                async_fn("ok2", 200, finished[2].clone()),
                async_fn("panic3", 100, finished[3].clone()),
                async_fn("ok4", 200, finished[4].clone()),
            ],
        )
        .await;
        match res {
            Ok(_) => panic!("Expected an error"),
            Err(RunError::Panicked(_)) => {}
            Err(e) => panic!("Unexpected error: {e:?}"),
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let expected = vec![true, true, false, false, false];
        let got = finished
            .into_iter()
            .map(|finished| finished.load(std::sync::atomic::Ordering::SeqCst))
            .collect::<Vec<_>>();
        assert_eq!(expected, got);
    }
}
