//
// Copyright 2025 Tabs Data Inc.
//

use td_common::logging;
use tracing::{info, span, Level};
use tracing_futures::Instrument;

// MDC (Mapped Diagnostic Context) simple examples.

#[tokio::main]
async fn main() {
    logging::start(Level::DEBUG, None, true);

    // Basic sync example.
    {
        let span = span!(Level::DEBUG, "trx-context", transaction = "example 01");
        let _span = span.enter();
        info!("Start processing");
        info!("Info log inside the span");
        info!("End processing");
    }

    // Basic async example.
    {
        let span = span!(Level::DEBUG, "trx-context", transaction = "example 02");
        async {
            info!("Start processing");
            async_fn().await;
            info!("End processing");
        }
        .instrument(span)
        .await;
    }

    // Basic async example with context trait.
    {
        with_context_span("example 03", async {
            info!("Start processing");
            async_fn().await;
            info!("End processing");
        })
        .await;
    }

    // Basic async example with sub-span.
    {
        with_context_span("example 04", async {
            info!("Start processing");
            async_fn().await;
            async_span_fn().await;
            info!("End processing");
        })
        .await;
    }

    // Basic async example with sub-span and macro.
    {
        with_context_span("example 05", async {
            info!("Start processing");
            async_fn().await;
            async_span_macro_fn().await;
            info!("End processing");
        })
        .await;
    }
}

pub async fn async_fn() {
    info!("Inside async fn");
}

pub async fn async_span_fn() {
    let span = span!(Level::DEBUG, "fn-context", function = "example a");
    let _span = span.enter();
    info!("Inside async span fn");
}

#[tracing::instrument(name = "macro-context", fields(function = "example b"))]
async fn async_span_macro_fn() {
    info!("Inside async fn");
}

async fn with_context_span<F, T>(transaction: &str, future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let span = tracing::span!(Level::DEBUG, "trx-context", transaction = transaction);
    future.instrument(span).await
}
