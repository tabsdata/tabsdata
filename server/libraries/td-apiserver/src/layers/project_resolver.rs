//
// Copyright 2026 Tabsdata Inc.
//

//! Resolves the `{project}` URL path segment to a [`ProjectId`] request
//! extension: a real registry id (unknown → 404 here, before any
//! project-scoped table is touched). The `Project` extractor reads it
//! downstream.
//!
//! The lookup hits `projects_active` (the soft-delete-filtering view), which
//! is NOT under RLS — so no GUC manipulation is needed to read it.

use axum::extract::{Path, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use std::collections::HashMap;
use std::str::FromStr;
use ta_apiserver::status::error_status::ErrorStatus;
use td_database::access::{AccessMode, DbPool, DbScope, Global, Grant};
use td_database::domain::{Apiserver, DomainConnection};
use td_error::TdError;
use td_objects::dxo::project::ActiveProjectDB;
use td_objects::types::basic::{ProjectId, ProjectIdName};
use td_queries::Queries;
use td_services::project::ProjectError;

/// Path-param key used by the `/projects/{project}/...` URLs.
const PROJECT_PARAM: &str = "project";

pub async fn project_resolver_layer(
    State(db): State<DbPool<Global>>,
    Path(params): Path<HashMap<String, String>>,
    request: Request,
    next: Next,
) -> Result<Response, ErrorStatus> {
    let raw = params
        .get(PROJECT_PARAM)
        .filter(|s| !s.is_empty())
        .cloned()
        .ok_or_else(|| TdError::from(ProjectError::MissingProjectSegment))?;

    let id_name = ProjectIdName::from_str(&raw)
        .map_err(|_| TdError::from(ProjectError::ProjectNotFound(raw.clone())))?;
    // Released before the handler runs: a slow request body must not keep a
    // pooled connection checked out.
    let resolved_id = {
        let mut conn = db.read().await?;
        resolve(conn.get(Grant::asserted("project resolution")), &id_name).await?
    }
    .ok_or_else(|| TdError::from(ProjectError::ProjectNotFound(raw.clone())))?;

    let mut request = request;
    request.extensions_mut().insert(resolved_id);
    Ok(next.run(request).await)
}

async fn resolve<S: DbScope, M: AccessMode>(
    conn: &mut DomainConnection<Apiserver, S, M>,
    id_name: &ProjectIdName,
) -> Result<Option<ProjectId>, TdError> {
    let proj: Option<ActiveProjectDB> = conn.fetch_optional::<ActiveProjectDB>(id_name).await?;
    Ok(proj.map(|p| p.id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::middleware::from_fn_with_state;
    use axum::routing::get;
    use std::sync::Arc;
    use std::time::Duration;
    use td_database::domain::DomainPools;
    use td_database::pool::TdPool;
    use td_queries::test_utils::SeedEntry;
    use tokio::sync::Notify;
    use tower::ServiceExt;

    /// Once the `{project}` segment is resolved, the resolver must not
    /// keep holding a pooled connection while the handler runs (here: a
    /// handler still waiting, as on a slow request body). With the handler
    /// parked, every connection of the pool must be available.
    #[td_test::test(db)]
    async fn test_the_resolver_holds_no_connection_while_the_handler_runs(db: TdPool) {
        db.seed().test_project().await.unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let router = Router::new()
            .route(
                "/projects/{project}/parked",
                get({
                    let entered = entered.clone();
                    let release = release.clone();
                    move || async move {
                        entered.notify_one();
                        release.notified().await;
                        "ok"
                    }
                }),
            )
            .layer(from_fn_with_state(
                DbPool::new(DomainPools::of_apiserver(db.clone()), Global),
                project_resolver_layer,
            ));
        let request = tokio::spawn(
            router.oneshot(
                Request::builder()
                    .uri("/projects/test/parked")
                    .body(Body::empty())
                    .unwrap(),
            ),
        );
        entered.notified().await;

        let acquired = tokio::time::timeout(Duration::from_secs(2), async {
            let first = db.begin().await;
            let second = db.begin().await;
            first.is_ok() && second.is_ok()
        })
        .await;
        release.notify_one();
        request.await.expect("request task").expect("response");

        assert!(
            matches!(acquired, Ok(true)),
            "the pool had a connection checked out while the handler ran"
        );
    }
}
