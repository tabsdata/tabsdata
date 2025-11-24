//
// Copyright 2025 Tabs Data Inc.
//

use crate::apiserver::server::{Server, ServerError};
use async_trait::async_trait;
use axum::Router;
use http::uri::Scheme;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

#[derive(Debug)]
pub struct PlainServer {
    pub listeners: Vec<TcpListener>,
    pub router: Router,
}

#[async_trait]
impl Server for PlainServer {
    async fn handles(
        self: Box<Self>,
        shutdown: tokio::sync::watch::Receiver<()>,
    ) -> Vec<JoinHandle<Result<(), ServerError>>> {
        let mut handles = Vec::new();
        for listener in self.listeners {
            let dbg_listener = format!("{listener:?}");

            let router = self.router.clone();
            let mut shutdown = shutdown.clone();
            let handle = tokio::spawn(async move {
                info!("Server listening on {dbg_listener}");

                axum::serve(listener, router)
                    .with_graceful_shutdown({
                        let dbg_listener = dbg_listener.clone();
                        async move {
                            shutdown.changed().await.ok();
                            debug!("Stopping server listening on {dbg_listener}");
                        }
                    })
                    .await
                    .map_err(|e| {
                        error!("Failed to run Server listener: {}", e);
                        ServerError::Server(dbg_listener, e)
                    })?;
                Ok(())
            });

            handles.push(handle);
        }
        handles
    }

    fn listeners(&self) -> &Vec<TcpListener> {
        &self.listeners
    }

    fn scheme(&self) -> Scheme {
        Scheme::HTTP
    }
}
