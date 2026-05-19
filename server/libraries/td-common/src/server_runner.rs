//
// Copyright 2025 Tabs Data Inc.
//

use crate::signal::terminate;
use crate::status::ExitStatus;
use async_trait::async_trait;
use std::error::Error;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time;
use tracing::{Instrument, Level, debug, error, info, span, warn};

/// Trait for runnable server components.
/// All servers must implement this trait to be managed by ServerRunner.
/// Each server should wrap its inner logic in tokio::select! to listen for shutdown signals
/// and handle graceful shutdown. Else, the task will be force-aborted on shutdown.
#[async_trait]
pub trait ServerRunnable: Send + Sync {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    async fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}

#[derive(Default)]
pub struct ServerRunner {
    pub servers: Vec<Box<dyn ServerRunnable>>,
    pub graceful_timeout_secs: Option<Duration>,
}

impl ServerRunner {
    pub fn server<S: ServerRunnable + 'static>(mut self, server: S) -> Self {
        self.servers.push(Box::new(server));
        self
    }

    pub fn graceful_timeout_secs(mut self, timeout: Duration) -> Self {
        self.graceful_timeout_secs = Some(timeout);
        self
    }

    pub async fn run(self) -> Result<ExitStatus, ExitStatus> {
        let (shutdown_tx, shutdown_rx) = shutdown_signal();
        let mut tasks_set = Tasks::new();

        // Spawn all servers
        for server in self.servers {
            let server_name = server.name();
            let shutdown_rx = shutdown_rx.clone();
            let log_span = span!(Level::INFO, "server", name = %server_name);

            tasks_set.spawn(async move {
                debug!("Spawning server '{server_name}'");
                let res = server.run(shutdown_rx).instrument(log_span).await;
                debug!("Despawning finished server '{server_name}'");
                (server_name, res)
            });
        }

        // Monitor tasks
        let mut any_error = false;
        let mut cancellation_triggered = false;

        while let Some(result) = tasks_set.join_next().await {
            match result {
                Ok((server_name, Ok(()))) => {
                    debug!("Server '{server_name}' finished normally");
                }
                Ok((server_name, Err(e))) => {
                    any_error = true;
                    error!("Server '{server_name}' failed with error: {e:#?}");
                    cancel_remaining(
                        &mut tasks_set,
                        &shutdown_tx,
                        self.graceful_timeout_secs,
                        &mut cancellation_triggered,
                    )
                    .await;
                    break;
                }
                Err(join_err) => {
                    any_error = true;
                    error!("Server panicked: {join_err}");
                    cancel_remaining(
                        &mut tasks_set,
                        &shutdown_tx,
                        self.graceful_timeout_secs,
                        &mut cancellation_triggered,
                    )
                    .await;
                    break;
                }
            }
        }

        if any_error {
            error!("Server runner finished with errors");
            Err(ExitStatus::GeneralError)
        } else {
            info!("Server runner finished successfully");
            Ok(ExitStatus::Success)
        }
    }
}

// Helper to create shutdown signal receiver
fn shutdown_signal() -> (watch::Sender<()>, watch::Receiver<()>) {
    let (tx, rx) = watch::channel(());
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        terminate().await;
        info!("process.shutdown.broadcast: sending shutdown to subscribers");
        let _ = tx_clone.send(());
    });
    (tx, rx)
}

type Tasks = tokio::task::JoinSet<(&'static str, Result<(), Box<dyn Error + Send + Sync>>)>;

/// Trigger cancellation via token, wait for graceful shutdown, force-abort if timeout
async fn cancel_remaining(
    set: &mut Tasks,
    shutdown_tx: &watch::Sender<()>,
    graceful_timeout: Option<Duration>,
    cancellation_triggered: &mut bool,
) {
    if !*cancellation_triggered {
        debug!("Triggering cancellation for remaining tasks...");
        let _ = shutdown_tx.send(());
        *cancellation_triggered = true;
    }

    match graceful_timeout {
        Some(timeout) => match time::timeout(timeout, async {
            while let Some(result) = set.join_next().await {
                match result {
                    Ok((server_name, Ok(_))) => {
                        debug!("Server '{server_name}' exited gracefully after cancellation")
                    }
                    Ok((server_name, Err(e))) => {
                        error!(
                            "Server '{server_name}' exited after cancellation with error: {e:#?}"
                        )
                    }
                    Err(e) => error!("Server failed during shutdown: {e:#?}"),
                }
            }
        })
        .await
        {
            Ok(_) => info!("All remaining tasks exited gracefully after cancellation"),
            Err(_) => {
                warn!(
                    "Graceful shutdown timed out after {timeout:?}, force-aborting remaining tasks..."
                );
                set.abort_all();
                while let Some(result) = set.join_next().await {
                    match result {
                        Ok((server_name, Ok(_))) => {
                            warn!("Server '{server_name}' completed gracefully after force-abort")
                        }
                        Ok((server_name, Err(e))) => {
                            error!(
                                "Server '{server_name}' failed after force-abort with error: {e:#?}"
                            )
                        }
                        Err(e) => {
                            error!("Server failed after force-abort: {e:#?}")
                        }
                    }
                }
                warn!("All remaining tasks force-aborted");
            }
        },
        None => {
            while let Some(result) = set.join_next().await {
                match result {
                    Ok((server_name, Ok(_))) => info!("Server '{server_name}' exited gracefully"),
                    Ok((server_name, Err(e))) => {
                        error!("Server '{server_name}' exited with error: {e:#?}")
                    }
                    Err(e) => error!("Server failed during shutdown: {e:#?}",),
                }
            }
            debug!("All remaining tasks exited gracefully");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::watch;
    use tokio::time::{Duration, sleep};
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    // Setup logging for tests
    fn init_tracing() {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::INFO)
            .with_test_writer()
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    }

    type RunBehavior = Arc<
        dyn Fn(
                watch::Receiver<()>,
            ) -> tokio::task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>
            + Send
            + Sync,
    >;

    struct MockServer {
        run_behavior: RunBehavior,
    }

    #[async_trait]
    impl ServerRunnable for MockServer {
        async fn run(
            self: Box<Self>,
            shutdown: watch::Receiver<()>,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            let fut = (self.run_behavior)(shutdown);
            fut.await?
        }
    }

    #[tokio::test]
    async fn test_all_servers_success() {
        init_tracing();

        let servers: Vec<Box<dyn ServerRunnable>> = (0..3)
            .map(|_| {
                Box::new(MockServer {
                    run_behavior: Arc::new(|mut shutdown| {
                        tokio::spawn(async move {
                            tokio::select! {
                                _ = shutdown.changed() => Ok(()),
                                _ = sleep(Duration::from_millis(50)) => Ok(()),
                            }
                        })
                    }),
                }) as Box<dyn ServerRunnable>
            })
            .collect();

        let runner = ServerRunner {
            servers,
            graceful_timeout_secs: Some(Duration::from_secs(1)),
        };
        let status = runner.run().await.into();
        assert!(matches!(status, ExitStatus::Success));
    }

    #[tokio::test]
    async fn test_server_error_triggers_cancel() {
        init_tracing();

        let error_flag = Arc::new(AtomicBool::new(false));

        let servers: Vec<Box<dyn ServerRunnable>> = vec![
            Box::new(MockServer {
                run_behavior: {
                    let error_flag = error_flag.clone();
                    Arc::new(move |_shutdown| {
                        tokio::spawn({
                            let error_flag = error_flag.clone();
                            async move {
                                error_flag.store(true, Ordering::SeqCst);
                                Err("server failed".into())
                            }
                        })
                    })
                },
            }),
            Box::new(MockServer {
                run_behavior: Arc::new(|mut shutdown| {
                    tokio::spawn(async move {
                        tokio::select! {
                            _ = shutdown.changed() => Ok(()),
                            _ = sleep(Duration::from_secs(5)) => Ok(()),
                        }
                    })
                }),
            }),
        ];

        let runner = ServerRunner {
            servers,
            graceful_timeout_secs: Some(Duration::from_secs(1)),
        };
        let status = runner.run().await.into();

        // Error from first server triggers cancellation of second
        assert!(error_flag.load(Ordering::SeqCst));
        assert!(matches!(status, ExitStatus::GeneralError));
    }

    #[tokio::test]
    async fn test_graceful_timeout() {
        init_tracing();

        let servers: Vec<Box<dyn ServerRunnable>> = vec![Box::new(MockServer {
            run_behavior: Arc::new(|_shutdown| {
                tokio::spawn(async move {
                    sleep(Duration::from_secs(10)).await;
                    Ok(())
                })
            }),
        })];

        let runner = ServerRunner {
            servers,
            graceful_timeout_secs: Some(Duration::from_secs(1)),
        };
        let status = runner.run().await.into();

        // The server will be force-aborted because it exceeds the 1 seconds timeout
        assert!(matches!(status, ExitStatus::Success));
    }
}
