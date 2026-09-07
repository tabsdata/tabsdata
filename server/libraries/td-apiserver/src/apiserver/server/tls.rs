//
// Copyright 2025 Tabsdata Inc.
//

use crate::apiserver::server::{Server, ServerError};
use async_trait::async_trait;
use axum::Router;
use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use http::uri::Scheme;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

#[derive(Debug)]
pub struct TlsServer {
    pub listeners: Vec<TcpListener>,
    pub tls_config: RustlsConfig,
    pub router: Router,
}

#[async_trait]
impl Server for TlsServer {
    async fn handles(
        self: Box<Self>,
        shutdown: tokio::sync::watch::Receiver<()>,
    ) -> Vec<JoinHandle<Result<(), ServerError>>> {
        let mut handles = Vec::new();

        for listener in self.listeners {
            let dbg_listener = format!("{listener:?}");

            let router = self.router.clone();
            let tls_config = self.tls_config.clone();
            let handle = Handle::new();
            let mut shutdown = shutdown.clone();

            let handle_task = tokio::spawn(async move {
                info!("Server listening on {dbg_listener} with TLS");

                let listener = listener
                    .into_std()
                    .map_err(|e| ServerError::StdTcpListener(dbg_listener.clone(), e))?;

                let server = axum_server::from_tcp_rustls(listener, tls_config)
                    .map_err(|e| ServerError::Server(dbg_listener.clone(), e))?
                    .handle(handle.clone())
                    .serve(router.into_make_service_with_connect_info::<SocketAddr>());

                tokio::select! {
                    res = server => {
                        res.map_err(|e| {
                            error!("Failed to run server listener: {dbg_listener}");
                            ServerError::Server(dbg_listener.clone(), e)
                        })
                    }
                    _ = shutdown.changed() => {
                        debug!("Shutdown signal received, stopping listener {dbg_listener}");
                        handle.graceful_shutdown(Some(Duration::from_secs(10)));
                        Ok(())
                    }
                }
            });

            handles.push(handle_task);
        }

        handles
    }

    fn listeners(&self) -> &Vec<TcpListener> {
        &self.listeners
    }

    fn scheme(&self) -> Scheme {
        Scheme::HTTPS
    }
}
